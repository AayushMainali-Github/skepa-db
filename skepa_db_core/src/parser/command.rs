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

    Select {
        table: String,
        filter: Option<WhereClause>,
    },
}
