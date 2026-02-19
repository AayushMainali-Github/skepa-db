use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::storage::Schema;
use crate::storage::engine::StorageEngine;
use crate::types::Row;
use crate::types::datatype::DataType;
use crate::types::value::{Value, parse_value, value_to_string};

/// Disk-backed storage scaffold.
/// For now this keeps rows in-memory during process lifetime while
/// initializing the on-disk layout required for the full disk migration.
#[derive(Debug, Clone)]
pub struct DiskStorage {
    root: PathBuf,
    tables: HashMap<String, Vec<Row>>,
    row_ids: HashMap<String, Vec<u64>>,
    next_row_id: HashMap<String, u64>,
    pk_indexes: HashMap<String, PrimaryIndex>,
    unique_indexes: HashMap<String, Vec<UniqueIndex>>,
    secondary_indexes: HashMap<String, Vec<SecondaryIndex>>,
}

#[derive(Debug, Clone)]
struct PrimaryIndex {
    col_idxs: Vec<usize>,
    map: BTreeMap<String, u64>,
}

#[derive(Debug, Clone)]
struct UniqueIndex {
    cols: Vec<String>,
    col_idxs: Vec<usize>,
    map: BTreeMap<String, u64>,
}

#[derive(Debug, Clone)]
struct SecondaryIndex {
    cols: Vec<String>,
    col_idxs: Vec<usize>,
    map: BTreeMap<String, Vec<u64>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TableIndexSnapshot {
    #[serde(default)]
    pk: Option<IndexSnapshot>,
    #[serde(default)]
    unique: Vec<IndexSnapshot>,
    #[serde(default)]
    secondary: Vec<SecondaryIndexSnapshot>,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexSnapshot {
    #[serde(default)]
    cols: Vec<String>,
    #[serde(default)]
    col_idxs: Vec<usize>,
    #[serde(default)]
    entries: Vec<IndexEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SecondaryIndexSnapshot {
    #[serde(default)]
    cols: Vec<String>,
    #[serde(default)]
    col_idxs: Vec<usize>,
    #[serde(default)]
    entries: Vec<SecondaryIndexEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SecondaryIndexEntry {
    key: String,
    row_ids: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexEntry {
    key: String,
    row_id: u64,
}

include!("disk/lifecycle.rs");
include!("disk/engine_impl.rs");
include!("disk/helpers.rs");
