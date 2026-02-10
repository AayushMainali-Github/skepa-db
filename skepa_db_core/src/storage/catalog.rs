use std::fs;
use std::path::Path;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::parser::command::{ColumnDef, TableConstraintDef};
use crate::types::datatype::DataType;
use crate::storage::schema::{Schema, Column, ForeignKeyDef};

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
    foreign_keys: Vec<ForeignKeyFile>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
struct ForeignKeyFile {
    columns: Vec<String>,
    ref_table: String,
    ref_columns: Vec<String>,
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
        table_constraints: Vec<TableConstraintDef>,
    ) -> Result<(), String> {
        if self.exists(&table) {
            return Err(format!("Table '{}' already exists", table));
        }

        let mut primary_key: Vec<String> = Vec::new();
        let mut unique_constraints: Vec<Vec<String>> = Vec::new();
        let mut foreign_keys: Vec<ForeignKeyDef> = Vec::new();

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

        for c in &columns {
            if c.primary_key {
                primary_key.push(c.name.clone());
            }
            if c.unique && !c.primary_key {
                unique_constraints.push(vec![c.name.clone()]);
            }
        }

        if primary_key.len() > 1 {
            return Err("Only one PRIMARY KEY constraint is supported".to_string());
        }

        for tc in table_constraints {
            match tc {
                TableConstraintDef::PrimaryKey(cols) => {
                    if !primary_key.is_empty() {
                        return Err("Only one PRIMARY KEY constraint is supported".to_string());
                    }
                    primary_key = cols;
                }
                TableConstraintDef::Unique(cols) => {
                    unique_constraints.push(cols);
                }
                TableConstraintDef::ForeignKey {
                    columns,
                    ref_table,
                    ref_columns,
                } => {
                    foreign_keys.push(ForeignKeyDef {
                        columns,
                        ref_table,
                        ref_columns,
                    });
                }
            }
        }

        if primary_key.is_empty() {
            // keep empty
        } else {
            for pk_col in &primary_key {
                if let Some(col) = columns.iter().find(|c| &c.name == pk_col) {
                    if !col.not_null {
                        // enforced semantically by PK
                    }
                } else {
                    return Err(format!("PRIMARY KEY references unknown column '{pk_col}'"));
                }
            }
        }

        for uniq in &unique_constraints {
            for c in uniq {
                if columns.iter().all(|col| &col.name != c) {
                    return Err(format!("UNIQUE references unknown column '{c}'"));
                }
            }
        }

        for fk in &foreign_keys {
            if fk.columns.is_empty() || fk.ref_columns.is_empty() {
                return Err("FOREIGN KEY column list cannot be empty".to_string());
            }
            if fk.columns.len() != fk.ref_columns.len() {
                return Err("FOREIGN KEY column count must match referenced column count".to_string());
            }
            for c in &fk.columns {
                if columns.iter().all(|col| &col.name != c) {
                    return Err(format!("FOREIGN KEY references unknown column '{c}'"));
                }
            }
            let parent = self
                .tables
                .get(&fk.ref_table)
                .ok_or_else(|| format!("FOREIGN KEY references unknown table '{}'", fk.ref_table))?;
            for c in &fk.ref_columns {
                if parent.columns.iter().all(|col| &col.name != c) {
                    return Err(format!(
                        "FOREIGN KEY references unknown parent column '{}.{}'",
                        fk.ref_table, c
                    ));
                }
            }

            let ref_is_pk = parent.primary_key == fk.ref_columns;
            let ref_is_unique = parent
                .unique_constraints
                .iter()
                .any(|u| u == &fk.ref_columns);
            if !(ref_is_pk || ref_is_unique) {
                return Err(format!(
                    "FOREIGN KEY reference {}({}) must target PRIMARY KEY or UNIQUE columns",
                    fk.ref_table,
                    fk.ref_columns.join(",")
                ));
            }
        }

        let mut schema = Schema::with_constraints(
            columns,
            primary_key.clone(),
            unique_constraints.clone(),
            foreign_keys.clone(),
        );
        // PK implies NOT NULL on referenced columns.
        for c in &mut schema.columns {
            if primary_key.iter().any(|pk| pk == &c.name) {
                c.not_null = true;
            }
        }
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
        let mut table_constraints: HashMap<String, TableConstraintFile> = HashMap::new();
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
            table_constraints.insert(
                table.clone(),
                TableConstraintFile {
                    primary_key: schema.primary_key.clone(),
                    unique: schema.unique_constraints.clone(),
                    foreign_keys: schema
                        .foreign_keys
                        .iter()
                        .map(|fk| ForeignKeyFile {
                            columns: fk.columns.clone(),
                            ref_table: fk.ref_table.clone(),
                            ref_columns: fk.ref_columns.clone(),
                        })
                        .collect(),
                },
            );
        }

        let payload = serde_json::to_string_pretty(&CatalogFile { tables, table_constraints })
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
        let CatalogFile {
            tables: file_tables,
            table_constraints: file_constraints,
        } = file;
        let mut tables: HashMap<String, Schema> = HashMap::new();
        for (table, cols) in file_tables {
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
            let tc = file_constraints.get(&table).cloned().unwrap_or_default();
            tables.insert(
                table,
                Schema::with_constraints(
                    columns,
                    tc.primary_key,
                    tc.unique,
                    tc.foreign_keys
                        .into_iter()
                        .map(|fk| ForeignKeyDef {
                            columns: fk.columns,
                            ref_table: fk.ref_table,
                            ref_columns: fk.ref_columns,
                        })
                        .collect(),
                ),
            );
        }

        Ok(Self { tables })
    }
}
