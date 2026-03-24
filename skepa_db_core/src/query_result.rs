use crate::execution_stats::ExecutionStats;
use crate::storage::Schema;
use crate::types::Row;
use crate::types::value::value_to_string;

#[derive(Debug, Clone)]
pub enum QueryResult {
    Message {
        message: String,
        stats: ExecutionStats,
    },
    Table {
        schema: Schema,
        rows: Vec<Row>,
        stats: ExecutionStats,
    },
}

impl QueryResult {
    pub fn message(message: impl Into<String>) -> Self {
        Self::Message {
            message: message.into(),
            stats: ExecutionStats::default(),
        }
    }

    pub fn table(schema: Schema, rows: Vec<Row>) -> Self {
        let stats = ExecutionStats {
            rows_returned: Some(rows.len()),
            rows_affected: None,
        };
        Self::Table {
            schema,
            rows,
            stats,
        }
    }

    pub fn with_rows_affected(message: impl Into<String>, rows_affected: usize) -> Self {
        Self::Message {
            message: message.into(),
            stats: ExecutionStats {
                rows_returned: None,
                rows_affected: Some(rows_affected),
            },
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        match self {
            Self::Message { stats, .. } => stats,
            Self::Table { stats, .. } => stats,
        }
    }

    pub fn render(&self) -> String {
        match self {
            Self::Message { message, .. } => message.clone(),
            Self::Table { schema, rows, .. } => {
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
