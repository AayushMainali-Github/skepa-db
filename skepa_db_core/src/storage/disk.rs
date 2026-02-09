use std::collections::HashMap;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};

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
    unique_indexes: HashMap<String, Vec<UniqueIndex>>,
}

#[derive(Debug, Clone)]
struct PrimaryIndex {
    col_idxs: Vec<usize>,
    map: BTreeMap<String, usize>,
}

#[derive(Debug, Clone)]
struct UniqueIndex {
    cols: Vec<String>,
    col_idxs: Vec<usize>,
    map: BTreeMap<String, usize>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TableIndexSnapshot {
    #[serde(default)]
    pk: Option<IndexSnapshot>,
    #[serde(default)]
    unique: Vec<IndexSnapshot>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexEntry {
    key: String,
    row_idx: usize,
}

impl DiskStorage {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, String> {
        let root = root.into();
        initialize_layout(&root)?;
        Ok(Self {
            root,
            tables: HashMap::new(),
            pk_indexes: HashMap::new(),
            unique_indexes: HashMap::new(),
        })
    }

    fn table_file_path(&self, table: &str) -> PathBuf {
        self.root.join("tables").join(format!("{table}.rows"))
    }

    fn index_file_path(&self, table: &str) -> PathBuf {
        self.root.join("indexes").join(format!("{table}.indexes.json"))
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
        if self.load_indexes_from_disk(table, schema).is_err() {
            self.rebuild_indexes_internal(table, schema)?;
            self.persist_indexes(table)?;
        }
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
            .map_err(|e| format!("Failed to write table snapshot for '{table}': {e}"))?;
        self.persist_indexes(table)
    }
}

fn initialize_layout(root: &Path) -> Result<(), String> {
    fs::create_dir_all(root).map_err(|e| format!("Failed to create db directory: {e}"))?;
    fs::create_dir_all(root.join("tables"))
        .map_err(|e| format!("Failed to create tables directory: {e}"))?;
    fs::create_dir_all(root.join("indexes"))
        .map_err(|e| format!("Failed to create indexes directory: {e}"))?;

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
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.index_file_path(table))
            .map_err(|e| format!("Failed to create index file for '{table}': {e}"))?;
        self.tables.insert(table.to_string(), Vec::new());
        self.pk_indexes.remove(table);
        self.unique_indexes.remove(table);
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
        let key = encode_key_parts(&[value_to_string(&rhs)]);
        Ok(self
            .pk_indexes
            .get(table)
            .and_then(|idx| if idx.col_idxs.as_slice() == [col_idx] { idx.map.get(&key).copied() } else { None }))
    }

    fn rebuild_indexes(&mut self, table: &str, schema: &Schema) -> Result<(), String> {
        self.rebuild_indexes_internal(table, schema)
    }

    fn lookup_pk_conflict(
        &self,
        table: &str,
        schema: &Schema,
        candidate: &Row,
        skip_idx: Option<usize>,
    ) -> Result<Option<usize>, String> {
        if schema.primary_key.is_empty() {
            return Ok(None);
        }
        let idx = match self.pk_indexes.get(table) {
            Some(i) => i,
            None => return Ok(None),
        };
        let parts = idx
            .col_idxs
            .iter()
            .map(|i| {
                candidate
                    .get(*i)
                    .map(value_to_string)
                    .ok_or_else(|| "Candidate row missing PK column".to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let key = encode_key_parts(&parts);
        let hit = idx.map.get(&key).copied();
        Ok(match (hit, skip_idx) {
            (Some(found), Some(skip)) if found == skip => None,
            (other, _) => other,
        })
    }

    fn lookup_unique_row_index(
        &self,
        table: &str,
        schema: &Schema,
        column: &str,
        rhs_token: &str,
    ) -> Result<Option<usize>, String> {
        let indexes = match self.unique_indexes.get(table) {
            Some(v) => v,
            None => return Ok(None),
        };
        let Some(col_idx) = schema.columns.iter().position(|c| c.name == column) else {
            return Ok(None);
        };
        let idx = indexes
            .iter()
            .find(|u| u.col_idxs.len() == 1 && u.col_idxs[0] == col_idx);
        let Some(idx) = idx else {
            return Ok(None);
        };
        let dtype = &schema.columns[col_idx].dtype;
        let rhs = parse_value(dtype, rhs_token)?;
        let key = encode_key_parts(&[value_to_string(&rhs)]);
        Ok(idx.map.get(&key).copied())
    }

    fn lookup_unique_conflict(
        &self,
        table: &str,
        _schema: &Schema,
        candidate: &Row,
        skip_idx: Option<usize>,
    ) -> Result<Option<Vec<String>>, String> {
        let indexes = match self.unique_indexes.get(table) {
            Some(v) => v,
            None => return Ok(None),
        };
        for idx in indexes {
            let parts = idx
                .col_idxs
                .iter()
                .map(|i| {
                    candidate
                        .get(*i)
                        .map(value_to_string)
                        .ok_or_else(|| "Candidate row missing UNIQUE column".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let key = encode_key_parts(&parts);
            if let Some(found) = idx.map.get(&key).copied() {
                if skip_idx != Some(found) {
                    return Ok(Some(idx.cols.clone()));
                }
            }
        }
        Ok(None)
    }
}

impl DiskStorage {
    fn persist_indexes(&self, table: &str) -> Result<(), String> {
        let pk = self.pk_indexes.get(table).map(|idx| IndexSnapshot {
            cols: Vec::new(),
            col_idxs: idx.col_idxs.clone(),
            entries: idx
                .map
                .iter()
                .map(|(k, v)| IndexEntry {
                    key: k.clone(),
                    row_idx: *v,
                })
                .collect(),
        });

        let unique = self
            .unique_indexes
            .get(table)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|u| IndexSnapshot {
                cols: u.cols,
                col_idxs: u.col_idxs,
                entries: u
                    .map
                    .into_iter()
                    .map(|(k, v)| IndexEntry { key: k, row_idx: v })
                    .collect(),
            })
            .collect::<Vec<_>>();

        let payload = serde_json::to_string_pretty(&TableIndexSnapshot { pk, unique })
            .map_err(|e| format!("Failed to serialize indexes for '{table}': {e}"))?;
        fs::write(self.index_file_path(table), payload)
            .map_err(|e| format!("Failed to write index file for '{table}': {e}"))
    }

    fn load_indexes_from_disk(&mut self, table: &str, schema: &Schema) -> Result<(), String> {
        let path = self.index_file_path(table);
        if !path.exists() {
            return Err("Index file missing".to_string());
        }
        let content = fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read index file for '{table}': {e}"))?;
        if content.trim().is_empty() {
            return Err("Index file is empty".to_string());
        }
        let snapshot: TableIndexSnapshot = serde_json::from_str(&content)
            .map_err(|e| format!("Malformed index file for '{table}': {e}"))?;

        self.rebuild_indexes_internal(table, schema)?;

        let row_len = self
            .tables
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist in storage", table))?
            .len();

        let mut should_heal = false;

        if let (Some(idx), Some(snap)) = (self.pk_indexes.get_mut(table), snapshot.pk) {
            if idx.col_idxs == snap.col_idxs {
                match validate_snapshot_entries(snap.entries, row_len) {
                    Ok(map) => idx.map = map,
                    Err(_) => should_heal = true,
                }
            } else {
                should_heal = true;
            }
        }

        if let Some(existing) = self.unique_indexes.get_mut(table) {
            for u in existing {
                if let Some(su) = snapshot
                    .unique
                    .iter()
                    .find(|s| s.col_idxs == u.col_idxs && s.cols == u.cols)
                {
                    match validate_snapshot_entries(su.entries.clone(), row_len) {
                        Ok(map) => u.map = map,
                        Err(_) => should_heal = true,
                    }
                } else {
                    should_heal = true;
                }
            }
        }
        if should_heal {
            self.persist_indexes(table)?;
        }
        Ok(())
    }

    fn rebuild_indexes_internal(&mut self, table: &str, schema: &Schema) -> Result<(), String> {
        self.rebuild_primary_index(table, schema)?;
        self.rebuild_unique_indexes(table, schema)
    }

    fn rebuild_primary_index(&mut self, table: &str, schema: &Schema) -> Result<(), String> {
        if schema.primary_key.is_empty() {
            self.pk_indexes.remove(table);
            return Ok(());
        }
        let mut col_idxs: Vec<usize> = Vec::new();
        for pk_col in &schema.primary_key {
            let col_idx = schema
                .columns
                .iter()
                .position(|c| c.name == *pk_col)
                .ok_or_else(|| format!("Unknown column '{}' in primary key", pk_col))?;
            col_idxs.push(col_idx);
        }
        let rows = self
            .tables
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist in storage", table))?;
        let mut map: BTreeMap<String, usize> = BTreeMap::new();
        for (row_idx, row) in rows.iter().enumerate() {
            let mut parts: Vec<String> = Vec::new();
            for (i, pk_col) in col_idxs.iter().zip(schema.primary_key.iter()) {
                let v = row
                    .get(*i)
                    .ok_or_else(|| format!("Row is missing PK column '{}'", pk_col))?;
                parts.push(value_to_string(v));
            }
            map.insert(encode_key_parts(&parts), row_idx);
        }
        self.pk_indexes
            .insert(table.to_string(), PrimaryIndex { col_idxs, map });
        Ok(())
    }

    fn rebuild_unique_indexes(&mut self, table: &str, schema: &Schema) -> Result<(), String> {
        let rows = self
            .tables
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist in storage", table))?;
        let groups = unique_groups(schema)?;
        if groups.is_empty() {
            self.unique_indexes.remove(table);
            return Ok(());
        }
        let mut indexes: Vec<UniqueIndex> = Vec::new();
        for cols in groups {
            let mut col_idxs = Vec::new();
            for c in &cols {
                let i = schema
                    .columns
                    .iter()
                    .position(|x| x.name == *c)
                    .ok_or_else(|| format!("Unknown UNIQUE column '{}'", c))?;
                col_idxs.push(i);
            }
            let mut map: BTreeMap<String, usize> = BTreeMap::new();
            for (row_idx, row) in rows.iter().enumerate() {
                let parts = col_idxs
                    .iter()
                    .map(|i| {
                        row.get(*i)
                            .map(value_to_string)
                            .ok_or_else(|| "Row missing UNIQUE column".to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                map.insert(encode_key_parts(&parts), row_idx);
            }
            indexes.push(UniqueIndex { cols, col_idxs, map });
        }
        self.unique_indexes.insert(table.to_string(), indexes);
        Ok(())
    }
}

fn encode_key_parts(parts: &[String]) -> String {
    // Stable ASCII tuple encoding: each part is length-prefixed.
    let mut out = String::new();
    for p in parts {
        out.push_str(&p.len().to_string());
        out.push(':');
        out.push_str(p);
        out.push(';');
    }
    out
}

fn unique_groups(schema: &Schema) -> Result<Vec<Vec<String>>, String> {
    let mut out: Vec<Vec<String>> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for cols in &schema.unique_constraints {
        let key = cols.join(",");
        if seen.insert(key) {
            out.push(cols.clone());
        }
    }
    for col in &schema.columns {
        if col.unique && !col.primary_key {
            if schema.columns.iter().any(|c| c.name == col.name) {
                let cols = vec![col.name.clone()];
                let key = cols.join(",");
                if seen.insert(key) {
                    out.push(cols);
                }
            } else {
                return Err("Internal schema error while building UNIQUE indexes".to_string());
            }
        }
    }
    Ok(out)
}

fn validate_snapshot_entries(
    entries: Vec<IndexEntry>,
    row_len: usize,
) -> Result<BTreeMap<String, usize>, String> {
    let mut out = BTreeMap::new();
    for e in entries {
        if e.row_idx >= row_len {
            return Err("Index entry row pointer out of range".to_string());
        }
        if out.insert(e.key, e.row_idx).is_some() {
            return Err("Duplicate key in index snapshot".to_string());
        }
    }
    Ok(out)
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
