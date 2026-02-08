use std::path::PathBuf;
use std::{fs, io::Write};

pub mod types;
pub mod parser;
pub mod storage;
pub mod engine;

use parser::command::Command;
use storage::{Catalog, DiskStorage};

#[derive(Debug, Clone)]
struct TxState {
    txid: u64,
    staged_ops: Vec<String>,
    touched_tables: std::collections::HashSet<String>,
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
        let storage = DiskStorage::new(path.clone())
            .expect("Failed to initialize disk storage");
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
            let schema = db.catalog.schema(&table).expect("Missing schema while bootstrapping");
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

        if self.current_tx.is_some() && matches!(cmd, Command::Create { .. }) {
            return Err("CREATE TABLE is auto-commit and cannot run inside an active transaction".to_string());
        }

        let table_name = match &cmd {
            Command::Create { table, .. } => Some(table.clone()),
            Command::Insert { table, .. } => Some(table.clone()),
            Command::Update { table, .. } => Some(table.clone()),
            Command::Delete { table, .. } => Some(table.clone()),
            Command::Select { .. } => None,
            Command::Begin | Command::Commit | Command::Rollback => None,
        };
        let is_create = matches!(cmd, Command::Create { .. });
        let is_wal_write = matches!(cmd, Command::Insert { .. } | Command::Update { .. } | Command::Delete { .. });

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

        if is_create {
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
        let content = fs::read_to_string(&wal_path)
            .map_err(|e| format!("Failed to read WAL: {e}"))?;

        #[derive(Default)]
        struct ReplayTx {
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
                        return Err(format!("WAL parse error at line {}: malformed BEGIN record", idx + 1));
                    }
                    let txid: u64 = parts[1]
                        .parse()
                        .map_err(|_| format!("WAL parse error at line {}: bad txid", idx + 1))?;
                    txs.entry(txid).or_default();
                }
                Some("OP") => {
                    if parts.len() != 3 {
                        return Err(format!("WAL parse error at line {}: malformed OP record", idx + 1));
                    }
                    let txid: u64 = parts[1]
                        .parse()
                        .map_err(|_| format!("WAL parse error at line {}: bad txid", idx + 1))?;
                    txs.entry(txid)
                        .or_default()
                        .ops
                        .push((idx + 1, parts[2].to_string()));
                }
                Some("COMMIT") => {
                    if parts.len() != 2 {
                        return Err(format!("WAL parse error at line {}: malformed COMMIT record", idx + 1));
                    }
                    let txid: u64 = parts[1]
                        .parse()
                        .map_err(|_| format!("WAL parse error at line {}: bad txid", idx + 1))?;
                    txs.entry(txid).or_default().committed = true;
                }
                Some("ROLLBACK") => {
                    if parts.len() != 2 {
                        return Err(format!("WAL parse error at line {}: malformed ROLLBACK record", idx + 1));
                    }
                    let txid: u64 = parts[1]
                        .parse()
                        .map_err(|_| format!("WAL parse error at line {}: bad txid", idx + 1))?;
                    txs.entry(txid).or_default().rolled_back = true;
                }
                Some(other) => {
                    return Err(format!("WAL parse error at line {}: unknown record kind '{other}'", idx + 1));
                }
                None => {}
            }
        }

        let mut ordered: Vec<(usize, String)> = Vec::new();
        for tx in txs.values() {
            if tx.committed && !tx.rolled_back {
                for (line_no, stmt) in &tx.ops {
                    ordered.push((*line_no, stmt.clone()));
                }
            }
        }
        ordered.sort_by_key(|(line_no, _)| *line_no);

        for (line_no, stmt) in ordered {
            let cmd = parser::parser::parse(&stmt)
                .map_err(|e| format!("WAL parse error at line {}: {}", line_no, e))?;
            if matches!(cmd, Command::Create { .. } | Command::Begin | Command::Commit | Command::Rollback) {
                continue;
            }
            engine::execute_command(cmd, &mut self.catalog, &mut self.storage)
                .map_err(|e| format!("WAL apply error at line {}: {}", line_no, e))?;
        }

        Ok(())
    }

    fn truncate_wal(&self) -> Result<(), String> {
        fs::write(self.path.join("wal.log"), "")
            .map_err(|e| format!("Failed to truncate WAL: {e}"))
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
        let tx = TxState {
            txid: self.alloc_txid(),
            staged_ops: Vec::new(),
            touched_tables: std::collections::HashSet::new(),
            snapshot_catalog: self.catalog.clone(),
            snapshot_storage: self.storage.clone(),
        };
        self.current_tx = Some(tx);
        Ok("transaction started".to_string())
    }

    fn handle_commit(&mut self) -> Result<String, String> {
        let tx = self
            .current_tx
            .take()
            .ok_or_else(|| "No active transaction".to_string())?;

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
}
