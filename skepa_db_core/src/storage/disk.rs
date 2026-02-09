use std::collections::HashMap;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::storage::engine::StorageEngine;
use crate::storage::Schema;
use crate::types::datatype::DataType;
use crate::types::value::{parse_value, value_to_string, Value};
use crate::types::Row;

/// Disk-backed storage scaffold.
/// For now this keeps rows in-memory during process lifetime while
/// initializing the on-disk layout required for the full disk migration.
#[derive(Debug, Clone)]
pub struct DiskStorage {
    root: PathBuf,
    tables: HashMap<String, Vec<Row>>,
    pk_indexes: HashMap<String, PrimaryIndex>,
}

#[derive(Debug, Clone)]
struct PrimaryIndex {
    col_idx: usize,
    map: BTreeMap<String, usize>,
}

impl DiskStorage {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, String> {
        let root = root.into();
        initialize_layout(&root)?;
        Ok(Self {
            root,
            tables: HashMap::new(),
            pk_indexes: HashMap::new(),
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
        self.rebuild_primary_index(table, schema)?;
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
        self.pk_indexes.remove(table);
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

    fn lookup_pk_row_index(
        &self,
        table: &str,
        schema: &Schema,
        rhs_token: &str,
    ) -> Result<Option<usize>, String> {
        if schema.primary_key.len() != 1 {
            return Ok(None);
        }
        let pk_col = &schema.primary_key[0];
        let col_idx = schema
            .columns
            .iter()
            .position(|c| c.name == *pk_col)
            .ok_or_else(|| format!("Unknown column '{}' in primary key", pk_col))?;
        let dtype = &schema.columns[col_idx].dtype;
        let rhs = parse_value(dtype, rhs_token)?;
        let key = value_to_string(&rhs);
        Ok(self
            .pk_indexes
            .get(table)
            .and_then(|idx| if idx.col_idx == col_idx { idx.map.get(&key).copied() } else { None }))
    }

    fn rebuild_indexes(&mut self, table: &str, schema: &Schema) -> Result<(), String> {
        self.rebuild_primary_index(table, schema)
    }
}

impl DiskStorage {
    fn rebuild_primary_index(&mut self, table: &str, schema: &Schema) -> Result<(), String> {
        if schema.primary_key.len() != 1 {
            self.pk_indexes.remove(table);
            return Ok(());
        }
        let pk_col = &schema.primary_key[0];
        let col_idx = schema
            .columns
            .iter()
            .position(|c| c.name == *pk_col)
            .ok_or_else(|| format!("Unknown column '{}' in primary key", pk_col))?;
        let rows = self
            .tables
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist in storage", table))?;
        let mut map: BTreeMap<String, usize> = BTreeMap::new();
        for (row_idx, row) in rows.iter().enumerate() {
            let v = row
                .get(col_idx)
                .ok_or_else(|| format!("Row is missing PK column '{}'", pk_col))?;
            map.insert(value_to_string(v), row_idx);
        }
        self.pk_indexes
            .insert(table.to_string(), PrimaryIndex { col_idx, map });
        Ok(())
    }
}

fn encode_value(v: &Value) -> String {
    match v {
        Value::Bool(b) => format!("o:{}", if *b { "1" } else { "0" }),
        Value::Int(n) => format!("i:{n}"),
        Value::BigInt(n) => format!("g:{n}"),
        Value::Decimal(d) => format!("m:{}", d.normalize()),
        Value::VarChar(s) => format!("t:{}", escape_text(s)),
        Value::Text(s) => format!("t:{}", escape_text(s)),
        Value::Date(d) => format!("d:{}", d.format("%Y-%m-%d")),
        Value::Timestamp(ts) => format!("s:{}", ts.format("%Y-%m-%d %H:%M:%S")),
        Value::Uuid(u) => format!("u:{u}"),
        Value::Json(j) => format!("j:{}", escape_text(&j.to_string())),
        Value::Blob(b) => format!("b:{}", hex::encode(b)),
    }
}

fn decode_token(token: &str, dtype: &DataType) -> Result<String, String> {
    let (prefix, raw) = token
        .split_once(':')
        .ok_or_else(|| format!("Malformed value token '{token}'"))?;
    match dtype {
        DataType::Bool => {
            if prefix != "o" {
                return Err(format!("Expected bool token prefix 'o:' but got '{token}'"));
            }
            Ok(match raw {
                "1" => "true".to_string(),
                "0" => "false".to_string(),
                other => {
                    return Err(format!("Malformed bool payload '{other}' in token '{token}'"))
                }
            })
        }
        DataType::Int => {
            if prefix != "i" {
                return Err(format!("Expected int token prefix 'i:' but got '{token}'"));
            }
            Ok(raw.to_string())
        }
        DataType::BigInt => {
            if prefix != "g" {
                return Err(format!("Expected bigint token prefix 'g:' but got '{token}'"));
            }
            Ok(raw.to_string())
        }
        DataType::Decimal { .. } => {
            if prefix != "m" {
                return Err(format!("Expected decimal token prefix 'm:' but got '{token}'"));
            }
            Ok(raw.to_string())
        }
        DataType::VarChar(_) | DataType::Text => {
            if prefix != "t" {
                return Err(format!("Expected text token prefix 't:' but got '{token}'"));
            }
            unescape_text(raw)
        }
        DataType::Date => {
            if prefix != "d" {
                return Err(format!("Expected date token prefix 'd:' but got '{token}'"));
            }
            Ok(raw.to_string())
        }
        DataType::Timestamp => {
            if prefix != "s" {
                return Err(format!("Expected timestamp token prefix 's:' but got '{token}'"));
            }
            Ok(raw.to_string())
        }
        DataType::Uuid => {
            if prefix != "u" {
                return Err(format!("Expected uuid token prefix 'u:' but got '{token}'"));
            }
            Ok(raw.to_string())
        }
        DataType::Json => {
            if prefix != "j" {
                return Err(format!("Expected json token prefix 'j:' but got '{token}'"));
            }
            unescape_text(raw)
        }
        DataType::Blob => {
            if prefix != "b" {
                return Err(format!("Expected blob token prefix 'b:' but got '{token}'"));
            }
            Ok(format!("0x{}", raw))
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
