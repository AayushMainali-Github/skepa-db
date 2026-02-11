use skepa_db_core::parser::command::ColumnDef;
use skepa_db_core::storage::{Catalog, Column, DiskStorage, Schema, StorageEngine};
use skepa_db_core::types::datatype::DataType;
use skepa_db_core::types::value::Value;
use skepa_db_core::Database;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};


fn temp_dir(prefix: &str) -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path = std::env::temp_dir();
    path.push(format!(
        "skepa_db_storage_{}_{}_{}",
        prefix,
        std::process::id(),
        id
    ));
    let _ = std::fs::remove_dir_all(&path);
    path
}


mod catalog;
mod bootstrap;
mod wal_recovery;
mod indexes;
mod row_ids;
mod persistence;


