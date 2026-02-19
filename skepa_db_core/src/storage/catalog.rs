use crate::parser::command::{ColumnDef, ForeignKeyAction, TableConstraintDef};
use crate::storage::schema::{Column, ForeignKeyDef, Schema};
use crate::types::datatype::DataType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Manages table schemas (metadata catalog)
#[derive(Debug, Clone)]
pub struct Catalog {
    tables: HashMap<String, Schema>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CatalogFile {
    tables: HashMap<String, Vec<ColumnFile>>,
    #[serde(default)]
    table_constraints: HashMap<String, TableConstraintFile>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ColumnFile {
    name: String,
    dtype: String,
    #[serde(default)]
    primary_key: bool,
    #[serde(default)]
    unique: bool,
    #[serde(default)]
    not_null: bool,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
struct TableConstraintFile {
    #[serde(default)]
    primary_key: Vec<String>,
    #[serde(default)]
    unique: Vec<Vec<String>>,
    #[serde(default)]
    secondary_indexes: Vec<Vec<String>>,
    #[serde(default)]
    foreign_keys: Vec<ForeignKeyFile>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
struct ForeignKeyFile {
    columns: Vec<String>,
    ref_table: String,
    ref_columns: Vec<String>,
    #[serde(default = "default_on_delete")]
    on_delete: String,
    #[serde(default = "default_on_update")]
    on_update: String,
}

fn default_on_delete() -> String {
    "restrict".to_string()
}

fn default_on_update() -> String {
    "restrict".to_string()
}

include!("catalog/core.inc.rs");
include!("catalog/constraints.inc.rs");
include!("catalog/persistence.inc.rs");
