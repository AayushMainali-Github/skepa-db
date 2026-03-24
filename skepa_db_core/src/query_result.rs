use crate::storage::Schema;
use crate::types::Row;
use crate::types::value::value_to_string;

#[derive(Debug, Clone)]
pub enum QueryResult {
    Message(String),
    Table { schema: Schema, rows: Vec<Row> },
}

impl QueryResult {
    pub fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }

    pub fn table(schema: Schema, rows: Vec<Row>) -> Self {
        Self::Table { schema, rows }
    }

    pub fn render(&self) -> String {
        match self {
            Self::Message(message) => message.clone(),
            Self::Table { schema, rows } => {
                let header = schema
                    .columns
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join("\t");

                if rows.is_empty() {
                    return header;
                }

                let row_lines = rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(value_to_string)
                            .collect::<Vec<_>>()
                            .join("\t")
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                format!("{header}\n{row_lines}")
            }
        }
    }
}
