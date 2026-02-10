use crate::types::datatype::DataType;

#[derive(Debug, Clone, PartialEq)]
pub enum CompareOp {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
    Like,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub column: String,
    pub op: CompareOp,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub dtype: DataType,
    pub primary_key: bool,
    pub unique: bool,
    pub not_null: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraintDef {
    PrimaryKey(Vec<String>),
    Unique(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
        on_delete: ForeignKeyAction,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ForeignKeyAction {
    Restrict,
    Cascade,
}

#[derive(Debug)]
pub enum Command {
    Begin,
    Commit,
    Rollback,

    Create {
        table: String,
        columns: Vec<ColumnDef>,
        table_constraints: Vec<TableConstraintDef>,
    },

    Insert {
        table: String,
        values: Vec<String>,
    },

    Update {
        table: String,
        assignments: Vec<Assignment>,
        filter: WhereClause,
    },

    Delete {
        table: String,
        filter: WhereClause,
    },

    Select {
        table: String,
        columns: Option<Vec<String>>,
        filter: Option<WhereClause>,
    },
}
