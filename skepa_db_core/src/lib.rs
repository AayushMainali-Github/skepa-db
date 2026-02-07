use std::path::PathBuf;
use std::{fs, io::Write};

pub mod types;
pub mod parser;
pub mod storage;
pub mod engine;

use parser::command::Command;
use storage::{Catalog, DiskStorage};

#[derive(Debug)]
pub struct Database {
    path: PathBuf,
    catalog: Catalog,
    storage: DiskStorage,
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
        let table_name = match &cmd {
            Command::Create { table, .. } => Some(table.clone()),
            Command::Insert { table, .. } => Some(table.clone()),
            Command::Update { table, .. } => Some(table.clone()),
            Command::Delete { table, .. } => Some(table.clone()),
            Command::Select { .. } => None,
        };
        let is_create = matches!(cmd, Command::Create { .. });
        let is_wal_write = matches!(cmd, Command::Insert { .. } | Command::Update { .. } | Command::Delete { .. });

        let out = engine::execute_command(cmd, &mut self.catalog, &mut self.storage)?;

        if is_create {
            self.save_catalog()?;
            if let Some(table) = table_name {
                self.storage.persist_table(&table)?;
            }
        } else if is_wal_write {
            self.append_wal(input)?;
            if let Some(table) = table_name {
                self.storage.persist_table(&table)?;
            }
            self.truncate_wal()?;
        }

        Ok(out)
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    fn save_catalog(&self) -> Result<(), String> {
        self.catalog.save_to_path(&self.path.join("catalog.json"))
    }

    fn append_wal(&self, sql: &str) -> Result<(), String> {
        let wal_path = self.path.join("wal.log");
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .map_err(|e| format!("Failed to open WAL: {e}"))?;
        f.write_all(sql.trim().as_bytes())
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

        for (idx, line) in content.lines().enumerate() {
            let stmt = line.trim();
            if stmt.is_empty() {
                continue;
            }
            let cmd = parser::parser::parse(stmt)
                .map_err(|e| format!("WAL parse error at line {}: {}", idx + 1, e))?;
            if matches!(cmd, Command::Create { .. }) {
                continue;
            }
            engine::execute_command(cmd, &mut self.catalog, &mut self.storage)
                .map_err(|e| format!("WAL apply error at line {}: {}", idx + 1, e))?;
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
}
