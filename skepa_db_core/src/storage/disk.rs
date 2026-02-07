use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::storage::engine::StorageEngine;
use crate::storage::Schema;
use crate::types::datatype::DataType;
use crate::types::value::{parse_value, Value};
use crate::types::Row;

/// Disk-backed storage scaffold.
/// For now this keeps rows in-memory during process lifetime while
/// initializing the on-disk layout required for the full disk migration.
#[derive(Debug)]
pub struct DiskStorage {
    root: PathBuf,
    tables: HashMap<String, Vec<Row>>,
}

impl DiskStorage {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, String> {
        let root = root.into();
        initialize_layout(&root)?;
        Ok(Self {
            root,
            tables: HashMap::new(),
        })
    }

    fn table_file_path(&self, table: &str) -> PathBuf {
        self.root.join("tables").join(format!("{table}.rows"))
    }

    pub fn bootstrap_table(&mut self, table: &str, schema: &Schema) -> Result<(), String> {
        if self.tables.contains_key(table) {
            return Ok(());
        }
        let table_file = self.table_file_path(table);
        if !table_file.exists() {
            File::create(&table_file)
                .map_err(|e| format!("Failed to create table file for '{table}': {e}"))?;
        }

        let content = fs::read_to_string(&table_file)
            .map_err(|e| format!("Failed to read table file for '{table}': {e}"))?;
        let mut rows: Vec<Row> = Vec::new();

        for (line_no, line) in content.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let tokens: Vec<&str> = line.split('\t').collect();
            if tokens.len() != schema.columns.len() {
                return Err(format!(
                    "Malformed row in table '{}' at line {}: expected {} values, got {}",
                    table,
                    line_no + 1,
                    schema.columns.len(),
                    tokens.len()
                ));
            }

            let mut row: Row = Vec::with_capacity(tokens.len());
            for (i, tok) in tokens.iter().enumerate() {
                let dtype = &schema.columns[i].dtype;
                let decoded = decode_token(tok, dtype)?;
                row.push(parse_value(dtype, &decoded)?);
            }
            rows.push(row);
        }

        self.tables.insert(table.to_string(), rows);
        Ok(())
    }

    pub fn checkpoint_all(&self) -> Result<(), String> {
        let mut names: Vec<&String> = self.tables.keys().collect();
        names.sort();
        for table in names {
            self.persist_table(table)?;
        }
        Ok(())
    }

    pub fn persist_table(&self, table: &str) -> Result<(), String> {
        let rows = self
            .tables
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist in storage", table))?;
        let table_file = self.table_file_path(table);
        let mut lines: Vec<String> = Vec::with_capacity(rows.len());
        for row in rows {
            let encoded = row
                .iter()
                .map(encode_value)
                .collect::<Vec<_>>()
                .join("\t");
            lines.push(encoded);
        }
        let payload = if lines.is_empty() {
            String::new()
        } else {
            format!("{}\n", lines.join("\n"))
        };
        fs::write(table_file, payload)
            .map_err(|e| format!("Failed to write table snapshot for '{table}': {e}"))
    }
}

fn initialize_layout(root: &Path) -> Result<(), String> {
    fs::create_dir_all(root).map_err(|e| format!("Failed to create db directory: {e}"))?;
    fs::create_dir_all(root.join("tables"))
        .map_err(|e| format!("Failed to create tables directory: {e}"))?;

    let catalog = root.join("catalog.json");
    if !catalog.exists() {
        File::create(&catalog).map_err(|e| format!("Failed to create catalog file: {e}"))?;
    }

    let wal = root.join("wal.log");
    if !wal.exists() {
        File::create(&wal).map_err(|e| format!("Failed to create WAL file: {e}"))?;
    }

    Ok(())
}

impl StorageEngine for DiskStorage {
    fn create_table(&mut self, table: &str) -> Result<(), String> {
        if self.tables.contains_key(table) {
            return Err(format!("Table '{}' already exists in storage", table));
        }

        let table_file = self.table_file_path(table);
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(table_file)
            .map_err(|e| format!("Failed to create table file for '{table}': {e}"))?;
        self.tables.insert(table.to_string(), Vec::new());
        Ok(())
    }

    fn insert_row(&mut self, table: &str, row: Row) -> Result<(), String> {
        let rows = self
            .tables
            .get_mut(table)
            .ok_or_else(|| format!("Table '{}' does not exist in storage", table))?;
        rows.push(row);
        Ok(())
    }

    fn scan(&self, table: &str) -> Result<&[Row], String> {
        let rows = self
            .tables
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist in storage", table))?;
        Ok(rows.as_slice())
    }

    fn scan_mut(&mut self, table: &str) -> Result<&mut Vec<Row>, String> {
        self.tables
            .get_mut(table)
            .ok_or_else(|| format!("Table '{}' does not exist in storage", table))
    }
}

fn encode_value(v: &Value) -> String {
    match v {
        Value::Int(n) => format!("i:{n}"),
        Value::Text(s) => format!("t:{}", escape_text(s)),
    }
}

fn decode_token(token: &str, dtype: &DataType) -> Result<String, String> {
    let (prefix, raw) = token
        .split_once(':')
        .ok_or_else(|| format!("Malformed value token '{token}'"))?;
    match dtype {
        DataType::Int => {
            if prefix != "i" {
                return Err(format!("Expected int token prefix 'i:' but got '{token}'"));
            }
            Ok(raw.to_string())
        }
        DataType::Text => {
            if prefix != "t" {
                return Err(format!("Expected text token prefix 't:' but got '{token}'"));
            }
            unescape_text(raw)
        }
    }
}

fn escape_text(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
}

fn unescape_text(input: &str) -> Result<String, String> {
    let mut out = String::new();
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some(other) => return Err(format!("Unsupported escape sequence '\\{other}'")),
            None => return Err("Dangling escape at end of text token".to_string()),
        }
    }
    Ok(out)
}
