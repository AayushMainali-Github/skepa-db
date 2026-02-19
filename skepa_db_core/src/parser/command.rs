use crate::types::datatype::DataType;

#[derive(Debug, Clone, PartialEq)]
pub enum CompareOp {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
    Like,
    In,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalOp {
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Predicate {
    pub column: String,
    pub op: CompareOp,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WhereClause {
    Predicate(Predicate),
    Binary {
        left: Box<WhereClause>,
        op: LogicalOp,
        right: Box<WhereClause>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub column: String,
    pub asc: bool,
    pub then_by: Vec<(String, bool)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    pub left_column: String,
    pub right_column: String,
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
        on_update: ForeignKeyAction,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ForeignKeyAction {
    Restrict,
    Cascade,
    SetNull,
    NoAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterAction {
    AddUnique(Vec<String>),
    DropUnique(Vec<String>),
    AddForeignKey {
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
        on_delete: ForeignKeyAction,
        on_update: ForeignKeyAction,
    },
    DropForeignKey {
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
    },
    SetNotNull(String),
    DropNotNull(String),
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Command {
    Begin,
    Commit,
    Rollback,

    Create {
        table: String,
        columns: Vec<ColumnDef>,
        table_constraints: Vec<TableConstraintDef>,
    },
    CreateIndex {
        table: String,
        columns: Vec<String>,
    },
    DropIndex {
        table: String,
        columns: Vec<String>,
    },
    Alter {
        table: String,
        action: AlterAction,
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
        distinct: bool,
        join: Option<JoinClause>,
        columns: Option<Vec<String>>,
        filter: Option<WhereClause>,
        group_by: Option<Vec<String>>,
        having: Option<WhereClause>,
        order_by: Option<OrderBy>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
}
