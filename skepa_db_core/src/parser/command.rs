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

#[derive(Debug)]
pub enum Command {
    Create {
        table: String,
        columns: Vec<(String, DataType)>,
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

    Select {
        table: String,
        columns: Option<Vec<String>>,
        filter: Option<WhereClause>,
    },
}
