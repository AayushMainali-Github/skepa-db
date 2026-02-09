use crate::types::Row;
use crate::storage::Schema;

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

    /// Lookup row index by single-column primary key equality.
    fn lookup_pk_row_index(
        &self,
        _table: &str,
        _schema: &Schema,
        _rhs_token: &str,
    ) -> Result<Option<usize>, String> {
        Ok(None)
    }

    /// Rebuild storage-side indexes for a table after bulk row mutation.
    fn rebuild_indexes(&mut self, _table: &str, _schema: &Schema) -> Result<(), String> {
        Ok(())
    }

    /// Lookup conflicting existing row for the candidate primary-key tuple.
    fn lookup_pk_conflict(
        &self,
        _table: &str,
        _schema: &Schema,
        _candidate: &Row,
        _skip_idx: Option<usize>,
    ) -> Result<Option<usize>, String> {
        Ok(None)
    }

    /// Lookup row index by equality on a single-column UNIQUE constraint.
    fn lookup_unique_row_index(
        &self,
        _table: &str,
        _schema: &Schema,
        _column: &str,
        _rhs_token: &str,
    ) -> Result<Option<usize>, String> {
        Ok(None)
    }

    /// Lookup conflicting existing row for any UNIQUE tuple (single or composite).
    fn lookup_unique_conflict(
        &self,
        _table: &str,
        _schema: &Schema,
        _candidate: &Row,
        _skip_idx: Option<usize>,
    ) -> Result<Option<Vec<String>>, String> {
        Ok(None)
    }
}
