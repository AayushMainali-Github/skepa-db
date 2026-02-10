use crate::types::datatype::DataType;
use crate::parser::command::ForeignKeyAction;

#[derive(Debug, Clone)]
pub struct ForeignKeyDef {
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub on_delete: ForeignKeyAction,
}

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
    pub foreign_keys: Vec<ForeignKeyDef>,
}

impl Schema {
    /// Creates a new schema from a list of column definitions
    pub fn new(columns: Vec<Column>) -> Self {
        Self {
            columns,
            primary_key: Vec::new(),
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
        }
    }

    pub fn with_constraints(
        columns: Vec<Column>,
        primary_key: Vec<String>,
        unique_constraints: Vec<Vec<String>>,
        foreign_keys: Vec<ForeignKeyDef>,
    ) -> Self {
        Self {
            columns,
            primary_key,
            unique_constraints,
            foreign_keys,
        }
    }

    /// Returns the number of columns in this schema
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}
