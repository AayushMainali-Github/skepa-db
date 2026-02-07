use crate::types::Row;

/// Storage engine trait - abstraction for different storage backends
/// (in-memory, disk-based, etc.)
pub trait StorageEngine {
    /// Creates a table in the storage (allocates space for rows)
    fn create_table(&mut self, table: &str) -> Result<(), String>;

    /// Inserts a row into the specified table
    fn insert_row(&mut self, table: &str, row: Row) -> Result<(), String>;

    /// Scans all rows from the specified table
    fn scan(&self, table: &str) -> Result<&[Row], String>;

    /// Mutable access to all rows for in-place updates
    fn scan_mut(&mut self, table: &str) -> Result<&mut Vec<Row>, String>;
}
