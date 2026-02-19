use crate::storage::Schema;
use crate::types::Row;
use crate::types::value::value_to_string;

/// Formats a SELECT result as a tab-separated table
pub fn format_select(schema: &Schema, rows: &[Row]) -> String {
    // Build header line with column names
    let header = schema
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect::<Vec<_>>()
        .join("\t");

    // If no rows, return just the header
    if rows.is_empty() {
        return header;
    }

    // Build row lines
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

    // Combine header and rows
    format!("{}\n{}", header, row_lines)
}
