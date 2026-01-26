use crate::types::datatype::DataType;

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
    }
}