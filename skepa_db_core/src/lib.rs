use std::hash::{Hash, Hasher};
use std::path::Path;
use std::path::PathBuf;
use std::{fs, io::Write};

pub mod config;
pub mod engine;
pub mod error;
pub mod execution_stats;
pub mod parser;
pub mod query_result;
pub mod storage;
pub mod types;

mod legacy_render;
mod recovery;
mod storage_test_hooks;
mod transactions;

use config::DbConfig;
use error::{DbError, DbResult};
use parser::command::Command;
use query_result::QueryResult;
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
    pub fn open(config: DbConfig) -> DbResult<Self> {
        let path = config.path;
        let storage = Self::initialize_storage(&path)?;
        let catalog = Self::load_catalog(&path);

        let mut db = Self {
            path,
            catalog,
            storage,
            current_tx: None,
            next_txid: 1,
        };

        db.bootstrap_tables()?;
        db.recover()?;
        Ok(db)
    }

    pub fn try_open(path: impl Into<PathBuf>) -> DbResult<Self> {
        Self::open(DbConfig::new(path))
    }

    pub fn open_legacy(path: impl Into<PathBuf>) -> Self {
        Self::try_open(path).expect("Failed to open database")
    }

    pub fn execute_legacy(&mut self, input: &str) -> Result<String, String> {
        self.execute(input)
            .map(|result| legacy_render::render_query_result(&result))
            .map_err(|err| err.to_string())
    }

    pub fn execute(&mut self, input: &str) -> DbResult<QueryResult> {
        let cmd = parser::parser::parse(input).map_err(DbError::from)?;
        if matches!(cmd, Command::Begin) {
            return self
                .handle_begin()
                .map(QueryResult::transaction)
                .map_err(DbError::from);
        }
        if matches!(cmd, Command::Commit) {
            return self
                .handle_commit()
                .map(QueryResult::transaction)
                .map_err(DbError::from);
        }
        if matches!(cmd, Command::Rollback) {
            return self
                .handle_rollback()
                .map(QueryResult::transaction)
                .map_err(DbError::from);
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
            return Err(DbError::from(
                "CREATE/ALTER TABLE and CREATE/DROP INDEX are auto-commit and cannot run inside an active transaction"
                    .to_string(),
            ));
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

        let out = engine::execute_command(cmd, &mut self.catalog, &mut self.storage)
            .map_err(DbError::from)?;

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
            return Err(DbError::from(e));
        }

        if is_schema_write {
            self.save_catalog().map_err(DbError::from)?;
            if let Some(table) = table_name {
                self.storage.persist_table(&table).map_err(DbError::from)?;
            }
        } else if is_wal_write {
            let txid = self.alloc_txid();
            self.append_wal_line(&format!("BEGIN {}", txid))
                .map_err(DbError::from)?;
            self.append_wal_line(&format!("OP {} {}", txid, input.trim()))
                .map_err(DbError::from)?;
            self.append_wal_line(&format!("COMMIT {}", txid))
                .map_err(DbError::from)?;
            if let Some(table) = table_name {
                self.storage.persist_table(&table).map_err(DbError::from)?;
            }
            self.checkpoint_and_truncate_wal().map_err(DbError::from)?;
        }

        Ok(out)
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn has_active_transaction(&self) -> bool {
        self.current_tx.is_some()
    }
}
