use std::fs;
use std::path::Path;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::parser::command::ColumnDef;
use crate::types::datatype::DataType;
use crate::storage::schema::{Schema, Column};

/// Manages table schemas (metadata catalog)
#[derive(Debug)]
pub struct Catalog {
    tables: HashMap<String, Schema>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CatalogFile {
    tables: HashMap<String, Vec<ColumnFile>>,
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

impl Catalog {
    /// Creates a new empty catalog
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Checks if a table exists in the catalog
    pub fn exists(&self, table: &str) -> bool {
        self.tables.contains_key(table)
    }

    /// Creates a new table schema in the catalog
    /// Returns an error if the table already exists
    pub fn create_table(
        &mut self,
        table: String,
        cols: Vec<ColumnDef>,
    ) -> Result<(), String> {
        if self.exists(&table) {
            return Err(format!("Table '{}' already exists", table));
        }

        let pk_count = cols.iter().filter(|c| c.primary_key).count();
        if pk_count > 1 {
            return Err("Only one PRIMARY KEY column is supported".to_string());
        }

        let columns: Vec<Column> = cols
            .into_iter()
            .map(|c| Column {
                name: c.name,
                dtype: c.dtype,
                primary_key: c.primary_key,
                unique: c.unique,
                not_null: c.not_null,
            })
            .collect();

        let schema = Schema::new(columns);
        self.tables.insert(table, schema);
        Ok(())
    }

    /// Retrieves the schema for a given table
    /// Returns an error if the table does not exist
    pub fn schema(&self, table: &str) -> Result<&Schema, String> {
        self.tables
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))
    }

    /// Returns cloned table names and schemas for bootstrapping storage.
    pub fn snapshot_tables(&self) -> Vec<(String, Schema)> {
        self.tables
            .iter()
            .map(|(name, schema)| (name.clone(), schema.clone()))
            .collect()
    }

    /// Saves catalog metadata to disk.
    pub fn save_to_path(&self, path: &Path) -> Result<(), String> {
        let mut tables: HashMap<String, Vec<ColumnFile>> = HashMap::new();
        for (table, schema) in &self.tables {
            let cols: Vec<ColumnFile> = schema
                .columns
                .iter()
                .map(|c| {
                    let dtype = match &c.dtype {
                        DataType::Bool => "bool".to_string(),
                        DataType::Int => "int".to_string(),
                        DataType::BigInt => "bigint".to_string(),
                        DataType::Decimal { precision, scale } => {
                            format!("decimal({precision},{scale})")
                        }
                        DataType::VarChar(n) => format!("varchar({n})"),
                        DataType::Text => "text".to_string(),
                        DataType::Date => "date".to_string(),
                        DataType::Timestamp => "timestamp".to_string(),
                        DataType::Uuid => "uuid".to_string(),
                        DataType::Json => "json".to_string(),
                        DataType::Blob => "blob".to_string(),
                    };
                    ColumnFile {
                        name: c.name.clone(),
                        dtype,
                        primary_key: c.primary_key,
                        unique: c.unique,
                        not_null: c.not_null,
                    }
                })
                .collect();
            tables.insert(table.clone(), cols);
        }

        let payload = serde_json::to_string_pretty(&CatalogFile { tables })
            .map_err(|e| format!("Failed to serialize catalog as JSON: {e}"))?;
        fs::write(path, payload).map_err(|e| format!("Failed to write catalog file: {e}"))
    }

    /// Loads catalog metadata from disk.
    pub fn load_from_path(path: &Path) -> Result<Self, String> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let content =
            fs::read_to_string(path).map_err(|e| format!("Failed to read catalog file: {e}"))?;
        if content.trim().is_empty() {
            return Ok(Self::new());
        }

        let file: CatalogFile = serde_json::from_str(&content)
            .map_err(|e| format!("Malformed catalog JSON: {e}"))?;
        let mut tables: HashMap<String, Schema> = HashMap::new();
        for (table, cols) in file.tables {
            let mut columns: Vec<Column> = Vec::new();
            for c in cols {
                let dtype = crate::types::datatype::parse_datatype(&c.dtype)?;
                columns.push(Column {
                    name: c.name,
                    dtype,
                    primary_key: c.primary_key,
                    unique: c.unique,
                    not_null: c.not_null,
                });
            }
            tables.insert(table, Schema::new(columns));
        }

        Ok(Self { tables })
    }
}
