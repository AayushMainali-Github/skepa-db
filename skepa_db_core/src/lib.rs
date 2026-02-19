use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::{fs, io::Write};

pub mod engine;
pub mod parser;
pub mod storage;
pub mod types;

use parser::command::Command;
use storage::{Catalog, DiskStorage};

#[derive(Debug, Clone)]
struct TxState {
    txid: u64,
    staged_ops: Vec<String>,
    touched_tables: std::collections::HashSet<String>,
    table_versions_at_begin: std::collections::HashMap<String, u64>,
    snapshot_catalog: Catalog,
    snapshot_storage: DiskStorage,
}

#[derive(Debug)]
pub struct Database {
    path: PathBuf,
    catalog: Catalog,
    storage: DiskStorage,
    current_tx: Option<TxState>,
    next_txid: u64,
}

impl Database {
    pub fn open(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let storage = DiskStorage::new(path.clone()).expect("Failed to initialize disk storage");
        let catalog_path = path.join("catalog.json");
        let catalog = Catalog::load_from_path(&catalog_path).unwrap_or_else(|_| Catalog::new());

        let mut db = Self {
            path,
            catalog,
            storage,
            current_tx: None,
            next_txid: 1,
        };

        for (table, _) in db.catalog.snapshot_tables() {
            let schema = db
                .catalog
                .schema(&table)
                .expect("Missing schema while bootstrapping");
            db.storage
                .bootstrap_table(&table, schema)
                .expect("Failed to bootstrap table in storage");
        }

        db.replay_wal().expect("Failed to replay WAL");
        db.checkpoint_and_truncate_wal()
            .expect("Failed to checkpoint recovered state");
        db
    }

    pub fn execute(&mut self, input: &str) -> Result<String, String> {
        let cmd = parser::parser::parse(input)?;
        if matches!(cmd, Command::Begin) {
            return self.handle_begin();
        }
        if matches!(cmd, Command::Commit) {
            return self.handle_commit();
        }
        if matches!(cmd, Command::Rollback) {
            return self.handle_rollback();
        }

        if self.current_tx.is_some()
            && matches!(
                cmd,
                Command::Create { .. }
                    | Command::Alter { .. }
                    | Command::CreateIndex { .. }
                    | Command::DropIndex { .. }
            )
        {
            return Err(
                "CREATE/ALTER TABLE and CREATE/DROP INDEX are auto-commit and cannot run inside an active transaction"
                    .to_string(),
            );
        }

        let table_name = match &cmd {
            Command::Create { table, .. } => Some(table.clone()),
            Command::CreateIndex { table, .. } => Some(table.clone()),
            Command::DropIndex { table, .. } => Some(table.clone()),
            Command::Alter { table, .. } => Some(table.clone()),
            Command::Insert { table, .. } => Some(table.clone()),
            Command::Update { table, .. } => Some(table.clone()),
            Command::Delete { table, .. } => Some(table.clone()),
            Command::Select { .. } => None,
            Command::Begin | Command::Commit | Command::Rollback => None,
        };
        let is_schema_write = matches!(
            cmd,
            Command::Create { .. }
                | Command::Alter { .. }
                | Command::CreateIndex { .. }
                | Command::DropIndex { .. }
        );
        let is_wal_write = matches!(
            cmd,
            Command::Insert { .. } | Command::Update { .. } | Command::Delete { .. }
        );
        let is_in_tx = self.current_tx.is_some();

        let pre_catalog = if !is_in_tx && is_wal_write {
            Some(self.catalog.clone())
        } else {
            None
        };
        let pre_storage = if !is_in_tx && is_wal_write {
            Some(self.storage.clone())
        } else {
            None
        };

        let out = engine::execute_command(cmd, &mut self.catalog, &mut self.storage)?;

        if let Some(tx) = &mut self.current_tx {
            if is_wal_write {
                tx.staged_ops.push(input.trim().to_string());
                if let Some(table) = table_name {
                    tx.touched_tables.insert(table);
                }
            }
            return Ok(out);
        }

        if is_wal_write
            && let Err(e) = engine::validate_no_action_constraints(&self.catalog, &self.storage)
        {
            if let (Some(c), Some(s)) = (pre_catalog, pre_storage) {
                self.catalog = c;
                self.storage = s;
            }
            return Err(e);
        }

        if is_schema_write {
            self.save_catalog()?;
            if let Some(table) = table_name {
                self.storage.persist_table(&table)?;
            }
        } else if is_wal_write {
            let txid = self.alloc_txid();
            self.append_wal_line(&format!("BEGIN {}", txid))?;
            self.append_wal_line(&format!("OP {} {}", txid, input.trim()))?;
            self.append_wal_line(&format!("COMMIT {}", txid))?;
            if let Some(table) = table_name {
                self.storage.persist_table(&table)?;
            }
            self.checkpoint_and_truncate_wal()?;
        }

        Ok(out)
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    fn save_catalog(&self) -> Result<(), String> {
        self.catalog.save_to_path(&self.path.join("catalog.json"))
    }

    fn append_wal_line(&self, line: &str) -> Result<(), String> {
        let wal_path = self.path.join("wal.log");
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .map_err(|e| format!("Failed to open WAL: {e}"))?;
        f.write_all(line.trim().as_bytes())
            .map_err(|e| format!("Failed to write WAL entry: {e}"))?;
        f.write_all(b"\n")
            .map_err(|e| format!("Failed to write WAL newline: {e}"))?;
        Ok(())
    }

    fn replay_wal(&mut self) -> Result<(), String> {
        let wal_path = self.path.join("wal.log");
        if !wal_path.exists() {
            return Ok(());
        }
        let content =
            fs::read_to_string(&wal_path).map_err(|e| format!("Failed to read WAL: {e}"))?;

        #[derive(Default)]
        struct ReplayTx {
            first_line: usize,
            committed: bool,
            rolled_back: bool,
            ops: Vec<(usize, String)>,
        }

        let mut txs: std::collections::HashMap<u64, ReplayTx> = std::collections::HashMap::new();

        for (idx, raw_line) in content.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.splitn(3, ' ').collect();
            match parts.first().copied() {
                Some("BEGIN") => {
                    if parts.len() != 2 {
                        return Err(format!(
                            "WAL parse error at line {}: malformed BEGIN record",
                            idx + 1
                        ));
                    }
                    let txid: u64 = parts[1]
                        .parse()
                        .map_err(|_| format!("WAL parse error at line {}: bad txid", idx + 1))?;
                    let tx = txs.entry(txid).or_default();
                    if tx.first_line == 0 {
                        tx.first_line = idx + 1;
                    }
                }
                Some("OP") => {
                    if parts.len() != 3 {
                        return Err(format!(
                            "WAL parse error at line {}: malformed OP record",
                            idx + 1
                        ));
                    }
                    let txid: u64 = parts[1]
                        .parse()
                        .map_err(|_| format!("WAL parse error at line {}: bad txid", idx + 1))?;
                    let tx = txs.entry(txid).or_default();
                    if tx.first_line == 0 {
                        tx.first_line = idx + 1;
                    }
                    tx.ops.push((idx + 1, parts[2].to_string()));
                }
                Some("COMMIT") => {
                    if parts.len() != 2 {
                        return Err(format!(
                            "WAL parse error at line {}: malformed COMMIT record",
                            idx + 1
                        ));
                    }
                    let txid: u64 = parts[1]
                        .parse()
                        .map_err(|_| format!("WAL parse error at line {}: bad txid", idx + 1))?;
                    let tx = txs.entry(txid).or_default();
                    if tx.first_line == 0 {
                        tx.first_line = idx + 1;
                    }
                    tx.committed = true;
                }
                Some("ROLLBACK") => {
                    if parts.len() != 2 {
                        return Err(format!(
                            "WAL parse error at line {}: malformed ROLLBACK record",
                            idx + 1
                        ));
                    }
                    let txid: u64 = parts[1]
                        .parse()
                        .map_err(|_| format!("WAL parse error at line {}: bad txid", idx + 1))?;
                    let tx = txs.entry(txid).or_default();
                    if tx.first_line == 0 {
                        tx.first_line = idx + 1;
                    }
                    tx.rolled_back = true;
                }
                Some(other) => {
                    return Err(format!(
                        "WAL parse error at line {}: unknown record kind '{other}'",
                        idx + 1
                    ));
                }
                None => {}
            }
        }

        let mut ordered_txs: Vec<(usize, ReplayTx)> = txs
            .into_values()
            .filter(|tx| tx.committed && !tx.rolled_back)
            .map(|tx| (tx.first_line, tx))
            .collect();
        ordered_txs.sort_by_key(|(line, _)| *line);

        for (_, tx) in ordered_txs {
            let before_catalog = self.catalog.clone();
            let before_storage = self.storage.clone();
            let mut invalid_tx = false;

            let mut ops = tx.ops;
            ops.sort_by_key(|(line_no, _)| *line_no);

            for (line_no, stmt) in ops {
                let cmd = parser::parser::parse(&stmt)
                    .map_err(|e| format!("WAL parse error at line {}: {}", line_no, e))?;
                if matches!(
                    cmd,
                    Command::Create { .. } | Command::Begin | Command::Commit | Command::Rollback
                ) {
                    continue;
                }
                if let Err(e) = engine::execute_command(cmd, &mut self.catalog, &mut self.storage) {
                    let _ = e;
                    invalid_tx = true;
                    break;
                }
            }

            if invalid_tx
                || engine::validate_no_action_constraints(&self.catalog, &self.storage).is_err()
            {
                self.catalog = before_catalog;
                self.storage = before_storage;
            }
        }

        Ok(())
    }

    fn truncate_wal(&self) -> Result<(), String> {
        fs::write(self.path.join("wal.log"), "").map_err(|e| format!("Failed to truncate WAL: {e}"))
    }

    fn checkpoint_and_truncate_wal(&self) -> Result<(), String> {
        self.storage.checkpoint_all()?;
        self.truncate_wal()
    }

    fn alloc_txid(&mut self) -> u64 {
        let txid = self.next_txid;
        self.next_txid += 1;
        txid
    }

    fn handle_begin(&mut self) -> Result<String, String> {
        if self.current_tx.is_some() {
            return Err("Transaction already active".to_string());
        }
        let mut table_versions_at_begin: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        for (table, _) in self.catalog.snapshot_tables() {
            let ver = self.table_file_version(&table)?;
            table_versions_at_begin.insert(table, ver);
        }
        let tx = TxState {
            txid: self.alloc_txid(),
            staged_ops: Vec::new(),
            touched_tables: std::collections::HashSet::new(),
            table_versions_at_begin,
            snapshot_catalog: self.catalog.clone(),
            snapshot_storage: self.storage.clone(),
        };
        self.current_tx = Some(tx);
        Ok("transaction started".to_string())
    }

    fn handle_commit(&mut self) -> Result<String, String> {
        let snapshot_catalog = self
            .current_tx
            .as_ref()
            .ok_or_else(|| "No active transaction".to_string())?
            .snapshot_catalog
            .clone();
        let snapshot_storage = self
            .current_tx
            .as_ref()
            .ok_or_else(|| "No active transaction".to_string())?
            .snapshot_storage
            .clone();

        if let Err(e) = engine::validate_no_action_constraints(&self.catalog, &self.storage) {
            self.catalog = snapshot_catalog;
            self.storage = snapshot_storage;
            self.current_tx = None;
            return Err(e);
        }

        let tx = self
            .current_tx
            .take()
            .ok_or_else(|| "No active transaction".to_string())?;

        for table in &tx.touched_tables {
            let begin_ver = tx.table_versions_at_begin.get(table).copied().unwrap_or(0);
            let now_ver = self.table_file_version(table)?;
            if now_ver != begin_ver {
                self.reload_from_disk()?;
                return Err(format!(
                    "Transaction conflict on table '{}': data changed outside this transaction",
                    table
                ));
            }
        }

        if !tx.staged_ops.is_empty() {
            self.append_wal_line(&format!("BEGIN {}", tx.txid))?;
            for op in &tx.staged_ops {
                self.append_wal_line(&format!("OP {} {}", tx.txid, op))?;
            }
            self.append_wal_line(&format!("COMMIT {}", tx.txid))?;

            for table in &tx.touched_tables {
                self.storage.persist_table(table)?;
            }
            self.checkpoint_and_truncate_wal()?;
        }
        Ok("transaction committed".to_string())
    }

    fn handle_rollback(&mut self) -> Result<String, String> {
        let tx = self
            .current_tx
            .take()
            .ok_or_else(|| "No active transaction".to_string())?;
        self.catalog = tx.snapshot_catalog;
        self.storage = tx.snapshot_storage;
        Ok("transaction rolled back".to_string())
    }

    fn table_file_version(&self, table: &str) -> Result<u64, String> {
        let path = self.path.join("tables").join(format!("{table}.rows"));
        let bytes = fs::read(&path).map_err(|e| {
            format!(
                "Failed to read table file for '{table}' while checking transaction conflict: {e}"
            )
        })?;
        let mut h = std::collections::hash_map::DefaultHasher::new();
        bytes.hash(&mut h);
        Ok(h.finish())
    }

    fn reload_from_disk(&mut self) -> Result<(), String> {
        let catalog_path = self.path.join("catalog.json");
        let mut storage = DiskStorage::new(self.path.clone())?;
        let catalog = Catalog::load_from_path(&catalog_path).unwrap_or_else(|_| Catalog::new());
        for (table, _) in catalog.snapshot_tables() {
            let schema = catalog
                .schema(&table)
                .map_err(|e| format!("Failed to refresh schema for '{table}': {e}"))?;
            storage
                .bootstrap_table(&table, schema)
                .map_err(|e| format!("Failed to refresh table '{table}' from disk: {e}"))?;
        }
        self.catalog = catalog;
        self.storage = storage;
        self.current_tx = None;
        Ok(())
    }
}
