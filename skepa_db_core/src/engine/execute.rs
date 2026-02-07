use crate::engine::format::format_select;
use crate::parser::command::{Assignment, Command, CompareOp, WhereClause};
use crate::storage::{Catalog, Column, Schema, StorageEngine};
use crate::types::datatype::DataType;
use crate::types::value::{parse_value, Value};
use crate::types::Row;

/// Executes a parsed command against the catalog and storage engine
pub fn execute_command(
    cmd: Command,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    match cmd {
        Command::Create { table, columns } => handle_create(table, columns, catalog, storage),
        Command::Insert { table, values } => handle_insert(table, values, catalog, storage),
        Command::Update {
            table,
            assignments,
            filter,
        } => handle_update(table, assignments, filter, catalog, storage),
        Command::Delete { table, filter } => handle_delete(table, filter, catalog, storage),
        Command::Select {
            table,
            columns,
            filter,
        } => handle_select(table, columns, filter, catalog, storage),
    }
}

fn handle_create(
    table: String,
    columns: Vec<(String, crate::types::datatype::DataType)>,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    catalog.create_table(table.clone(), columns)?;
    storage.create_table(&table)?;
    Ok(format!("created table {}", table))
}

fn handle_insert(
    table: String,
    values: Vec<String>,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    let schema = catalog.schema(&table)?;

    if values.len() != schema.column_count() {
        return Err(format!(
            "Expected {} values but got {}",
            schema.column_count(),
            values.len()
        ));
    }

    let mut row: Row = Vec::new();
    for (i, col) in schema.columns.iter().enumerate() {
        let token = &values[i];
        let value = parse_value(&col.dtype, token)?;
        row.push(value);
    }

    storage.insert_row(&table, row)?;
    Ok(format!("inserted 1 row into {}", table))
}

fn handle_select(
    table: String,
    columns: Option<Vec<String>>,
    filter: Option<WhereClause>,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    let schema = catalog.schema(&table)?;
    let rows = storage.scan(&table)?;

    let filtered_rows = if let Some(where_clause) = filter {
        filter_rows(schema, rows, &where_clause)?
    } else {
        rows.to_vec()
    };

    let (out_schema, out_rows) = project_rows(schema, &filtered_rows, columns.as_ref())?;
    Ok(format_select(&out_schema, &out_rows))
}

fn handle_update(
    table: String,
    assignments: Vec<Assignment>,
    filter: WhereClause,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    let schema = catalog.schema(&table)?;

    let mut compiled: Vec<(usize, Value)> = Vec::new();
    for a in assignments {
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name == a.column)
            .ok_or_else(|| format!("Unknown column '{}' in UPDATE", a.column))?;
        let dtype = &schema.columns[idx].dtype;
        let parsed = parse_value(dtype, &a.value)?;
        compiled.push((idx, parsed));
    }

    let where_idx = schema
        .columns
        .iter()
        .position(|c| c.name == filter.column)
        .ok_or_else(|| format!("Unknown column '{}' in WHERE", filter.column))?;
    let where_dtype = &schema.columns[where_idx].dtype;

    let rows = storage.scan_mut(&table)?;
    let mut updated = 0usize;

    for row in rows.iter_mut() {
        let cell = row
            .get(where_idx)
            .ok_or_else(|| format!("Row is missing value for column '{}'", filter.column))?;

        if matches_where(cell, where_dtype, &filter.op, &filter.value)? {
            for (idx, new_value) in &compiled {
                if let Some(slot) = row.get_mut(*idx) {
                    *slot = new_value.clone();
                }
            }
            updated += 1;
        }
    }

    Ok(format!("updated {} row(s) in {}", updated, table))
}

fn handle_delete(
    table: String,
    filter: WhereClause,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    let schema = catalog.schema(&table)?;
    let where_idx = schema
        .columns
        .iter()
        .position(|c| c.name == filter.column)
        .ok_or_else(|| format!("Unknown column '{}' in WHERE", filter.column))?;
    let where_dtype = &schema.columns[where_idx].dtype;

    let rows = storage.scan_mut(&table)?;

    let mut keep_flags: Vec<bool> = Vec::with_capacity(rows.len());
    for row in rows.iter() {
        let should_delete = row_matches(row, where_idx, &filter.column, where_dtype, &filter.op, &filter.value)?;
        keep_flags.push(!should_delete);
    }

    let mut deleted = 0usize;
    let mut kept: Vec<Row> = Vec::with_capacity(rows.len());
    for (row, keep) in rows.drain(..).zip(keep_flags) {
        if keep {
            kept.push(row);
        } else {
            deleted += 1;
        }
    }
    *rows = kept;

    Ok(format!("deleted {} row(s) from {}", deleted, table))
}

fn project_rows(
    schema: &Schema,
    rows: &[Row],
    columns: Option<&Vec<String>>,
) -> Result<(Schema, Vec<Row>), String> {
    let Some(requested_columns) = columns else {
        return Ok((schema.clone(), rows.to_vec()));
    };

    if requested_columns.is_empty() {
        return Ok((schema.clone(), rows.to_vec()));
    }

    let mut selected: Vec<(usize, Column)> = Vec::new();
    for name in requested_columns {
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name == *name)
            .ok_or_else(|| format!("Unknown column '{}' in SELECT list", name))?;
        selected.push((idx, schema.columns[idx].clone()));
    }

    let projected_schema = Schema::new(selected.iter().map(|(_, c)| c.clone()).collect());
    let projected_rows: Vec<Row> = rows
        .iter()
        .map(|row| {
            selected
                .iter()
                .map(|(idx, _)| row[*idx].clone())
                .collect::<Row>()
        })
        .collect();

    Ok((projected_schema, projected_rows))
}

fn filter_rows(
    schema: &crate::storage::Schema,
    rows: &[Row],
    where_clause: &WhereClause,
) -> Result<Vec<Row>, String> {
    let col_idx = schema
        .columns
        .iter()
        .position(|c| c.name == where_clause.column)
        .ok_or_else(|| format!("Unknown column '{}' in WHERE", where_clause.column))?;

    let col_dtype = &schema.columns[col_idx].dtype;
    let mut filtered: Vec<Row> = Vec::new();

    for row in rows {
        if row_matches(
            row,
            col_idx,
            &where_clause.column,
            col_dtype,
            &where_clause.op,
            &where_clause.value,
        )? {
            filtered.push(row.clone());
        }
    }

    Ok(filtered)
}

fn row_matches(
    row: &Row,
    col_idx: usize,
    col_name: &str,
    col_dtype: &DataType,
    op: &CompareOp,
    rhs_token: &str,
) -> Result<bool, String> {
    let cell = row
        .get(col_idx)
        .ok_or_else(|| format!("Row is missing value for column '{}'", col_name))?;
    matches_where(cell, col_dtype, op, rhs_token)
}

fn matches_where(cell: &Value, dtype: &DataType, op: &CompareOp, rhs_token: &str) -> Result<bool, String> {
    match op {
        CompareOp::Eq => {
            let rhs = parse_value(dtype, rhs_token)?;
            Ok(cell == &rhs)
        }
        CompareOp::Gt | CompareOp::Lt | CompareOp::Gte | CompareOp::Lte => match (cell, dtype) {
            (Value::Int(lhs), DataType::Int) => {
                let rhs = match parse_value(dtype, rhs_token)? {
                    Value::Int(n) => n,
                    _ => unreachable!(),
                };

                Ok(match op {
                    CompareOp::Gt => *lhs > rhs,
                    CompareOp::Lt => *lhs < rhs,
                    CompareOp::Gte => *lhs >= rhs,
                    CompareOp::Lte => *lhs <= rhs,
                    _ => unreachable!(),
                })
            }
            (_, DataType::Text) => Err(
                "Operator gt/lt/gte/lte is only valid for int columns. Use '=' or 'like' for text."
                    .to_string(),
            ),
            _ => Err("Type mismatch while evaluating numeric WHERE clause".to_string()),
        },
        CompareOp::Like => match (cell, dtype) {
            (Value::Text(lhs), DataType::Text) => Ok(wildcard_match(lhs, rhs_token)),
            _ => Err("Operator 'like' is only valid for text columns".to_string()),
        },
    }
}

fn wildcard_match(text: &str, pattern: &str) -> bool {
    // Glob-style matching:
    // '*' => zero or more characters
    // '?' => exactly one character
    let text_chars: Vec<char> = text.chars().collect();
    let pat_chars: Vec<char> = pattern.chars().collect();

    let t_len = text_chars.len();
    let p_len = pat_chars.len();

    let mut dp = vec![vec![false; p_len + 1]; t_len + 1];
    dp[0][0] = true;

    for j in 1..=p_len {
        if pat_chars[j - 1] == '*' {
            dp[0][j] = dp[0][j - 1];
        }
    }

    for i in 1..=t_len {
        for j in 1..=p_len {
            match pat_chars[j - 1] {
                '*' => {
                    dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
                }
                '?' => {
                    dp[i][j] = dp[i - 1][j - 1];
                }
                ch => {
                    dp[i][j] = dp[i - 1][j - 1] && text_chars[i - 1] == ch;
                }
            }
        }
    }

    dp[t_len][p_len]
}
