use crate::execution_stats::ExecutionStats;
use crate::storage::Schema;
use crate::types::Row;
use crate::types::value::value_to_string;

#[derive(Debug, Clone)]
pub enum QueryResult {
    Select {
        schema: Schema,
        rows: Vec<Row>,
        stats: ExecutionStats,
    },
    Mutation {
        message: String,
        rows_affected: usize,
        stats: ExecutionStats,
    },
    SchemaChange {
        message: String,
        stats: ExecutionStats,
    },
    Transaction {
        message: String,
        stats: ExecutionStats,
    },
}

impl QueryResult {
    pub fn select(schema: Schema, rows: Vec<Row>) -> Self {
        let stats = ExecutionStats {
            rows_returned: Some(rows.len()),
            rows_affected: None,
        };
        Self::Select {
            schema,
            rows,
            stats,
        }
    }

    pub fn mutation(message: impl Into<String>, rows_affected: usize) -> Self {
        Self::Mutation {
            message: message.into(),
            rows_affected,
            stats: ExecutionStats {
                rows_returned: None,
                rows_affected: Some(rows_affected),
            },
        }
    }

    pub fn schema_change(message: impl Into<String>) -> Self {
        Self::SchemaChange {
            message: message.into(),
            stats: ExecutionStats::default(),
        }
    }

    pub fn transaction(message: impl Into<String>) -> Self {
        Self::Transaction {
            message: message.into(),
            stats: ExecutionStats::default(),
        }
    }

    pub fn stats(&self) -> &ExecutionStats {
        match self {
            Self::Select { stats, .. } => stats,
            Self::Mutation { stats, .. } => stats,
            Self::SchemaChange { stats, .. } => stats,
            Self::Transaction { stats, .. } => stats,
        }
    }

    pub fn message(&self) -> Option<&str> {
        match self {
            Self::Select { .. } => None,
            Self::Mutation { message, .. } => Some(message),
            Self::SchemaChange { message, .. } => Some(message),
            Self::Transaction { message, .. } => Some(message),
        }
    }

    pub fn rows_affected(&self) -> Option<usize> {
        match self {
            Self::Mutation { rows_affected, .. } => Some(*rows_affected),
            _ => None,
        }
    }

    pub fn render(&self) -> String {
        match self {
            Self::Select { schema, rows, .. } => {
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
            Self::Mutation { message, .. } => message.clone(),
            Self::SchemaChange { message, .. } => message.clone(),
            Self::Transaction { message, .. } => message.clone(),
        }
    }
}
