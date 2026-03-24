use crate::query_result::QueryResult;
use crate::types::value::value_to_string;

pub(crate) fn render_query_result(result: &QueryResult) -> String {
    match result {
        QueryResult::Select { schema, rows, .. } => {
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
        QueryResult::Mutation { message, .. } => message.clone(),
        QueryResult::SchemaChange { message, .. } => message.clone(),
        QueryResult::Transaction { message, .. } => message.clone(),
    }
}
