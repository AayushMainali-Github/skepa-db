use crate::types::datatype::DataType;

/// Represents a single column in a table schema
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub dtype: DataType,
    pub primary_key: bool,
    pub unique: bool,
    pub not_null: bool,
}

/// Represents the schema of a table (list of columns)
#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    pub unique_constraints: Vec<Vec<String>>,
}

impl Schema {
    /// Creates a new schema from a list of column definitions
    pub fn new(columns: Vec<Column>) -> Self {
        Self {
            columns,
            primary_key: Vec::new(),
            unique_constraints: Vec::new(),
        }
    }

    pub fn with_constraints(
        columns: Vec<Column>,
        primary_key: Vec<String>,
        unique_constraints: Vec<Vec<String>>,
    ) -> Self {
        Self {
            columns,
            primary_key,
            unique_constraints,
        }
    }

    /// Returns the number of columns in this schema
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}
