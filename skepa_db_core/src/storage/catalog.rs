use std::collections::HashMap;
use crate::types::datatype::DataType;
use crate::storage::schema::{Schema, Column};

/// Manages table schemas (metadata catalog)
#[derive(Debug)]
pub struct Catalog {
    tables: HashMap<String, Schema>,
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
        cols: Vec<(String, DataType)>,
    ) -> Result<(), String> {
        if self.exists(&table) {
            return Err(format!("Table '{}' already exists", table));
        }

        let columns: Vec<Column> = cols
            .into_iter()
            .map(|(name, dtype)| Column { name, dtype })
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
}
