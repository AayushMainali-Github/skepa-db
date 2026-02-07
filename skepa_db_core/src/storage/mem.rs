use std::collections::HashMap;
use crate::types::Row;
use crate::storage::engine::StorageEngine;

/// In-memory storage implementation using HashMap
#[derive(Debug)]
pub struct MemStorage {
    tables: HashMap<String, Vec<Row>>,
}

impl MemStorage {
    /// Creates a new empty in-memory storage
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }
}

impl StorageEngine for MemStorage {
    fn create_table(&mut self, table: &str) -> Result<(), String> {
        if self.tables.contains_key(table) {
            return Err(format!("Table '{}' already exists in storage", table));
        }
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
