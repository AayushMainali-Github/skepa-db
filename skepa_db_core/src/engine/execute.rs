use crate::engine::format::format_select;
use crate::parser::command::{Assignment, ColumnDef, Command, CompareOp, TableConstraintDef, WhereClause};
use crate::storage::{Catalog, Column, Schema, StorageEngine};
use crate::types::datatype::DataType;
use crate::types::value::{parse_value, Value};
use crate::types::Row;
use std::cmp::Ordering;

/// Executes a parsed command against the catalog and storage engine
pub fn execute_command(
    cmd: Command,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    match cmd {
        Command::Create {
            table,
            columns,
            table_constraints,
        } => handle_create(table, columns, table_constraints, catalog, storage),
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
        Command::Begin | Command::Commit | Command::Rollback => {
            Err("Transaction control is handled by Database".to_string())
        }
    }
}

fn handle_create(
    table: String,
    columns: Vec<ColumnDef>,
    table_constraints: Vec<TableConstraintDef>,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    catalog.create_table(table.clone(), columns, table_constraints)?;
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
        if col.not_null && token.eq_ignore_ascii_case("null") {
            return Err(format!("Column '{}' is NOT NULL", col.name));
        }
        let value = parse_value(&col.dtype, token)?;
        row.push(value);
    }

    let rows = storage.scan(&table)?;

    if !schema.primary_key.is_empty()
        && storage
            .lookup_pk_conflict(&table, schema, &row, None)?
            .is_some()
    {
        return Err(format!(
            "PRIMARY KEY constraint violation on column(s) {}",
            schema.primary_key.join(",")
        ));
    }
    if let Some(cols) = storage.lookup_unique_conflict(&table, schema, &row, None)? {
        return Err(format!(
            "UNIQUE constraint violation on column(s) {}",
            cols.join(",")
        ));
    }

    validate_unique_constraints(schema, rows, &row, None)?;

    storage.insert_row(&table, row)?;
    storage.rebuild_indexes(&table, schema)?;
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
        if where_clause.op == CompareOp::Eq
            && schema.primary_key.len() == 1
            && schema.primary_key.first().is_some_and(|pk| pk == &where_clause.column)
        {
            if let Some(row_idx) = storage.lookup_pk_row_index(&table, schema, &where_clause.value)? {
                match rows.get(row_idx) {
                    Some(r) => vec![r.clone()],
                    None => Vec::new(),
                }
            } else {
                Vec::new()
            }
        } else if where_clause.op == CompareOp::Eq {
            if let Some(row_idx) =
                storage.lookup_unique_row_index(&table, schema, &where_clause.column, &where_clause.value)?
            {
                match rows.get(row_idx) {
                    Some(r) => vec![r.clone()],
                    None => Vec::new(),
                }
            } else {
                filter_rows(schema, rows, &where_clause)?
            }
        } else {
            filter_rows(schema, rows, &where_clause)?
        }
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
        if schema.columns[idx].not_null && a.value.eq_ignore_ascii_case("null") {
            return Err(format!("Column '{}' is NOT NULL", schema.columns[idx].name));
        }
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
    let targeted_row_idx = if filter.op == CompareOp::Eq
        && schema.primary_key.len() == 1
        && schema.primary_key.first().is_some_and(|pk| pk == &filter.column)
    {
        storage.lookup_pk_row_index(&table, schema, &filter.value)?
    } else if filter.op == CompareOp::Eq {
        storage.lookup_unique_row_index(&table, schema, &filter.column, &filter.value)?
    } else {
        None
    };

    let (updated, new_rows, old_indices) = {
        let rows = storage.scan(&table)?;
        let mut updated = 0usize;
        let mut new_rows = rows.to_vec();
        let old_indices: Vec<usize> = (0..rows.len()).collect();

        if let Some(i) = targeted_row_idx {
            if let Some(row) = new_rows.get_mut(i) {
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
        } else {
            for row in new_rows.iter_mut() {
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
        }

        validate_all_unique_constraints(schema, &new_rows)?;
        (updated, new_rows, old_indices)
    };
    storage.replace_rows_with_alignment(&table, new_rows, old_indices)?;
    storage.rebuild_indexes(&table, schema)?;

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
    let targeted_row_idx = if filter.op == CompareOp::Eq
        && schema.primary_key.len() == 1
        && schema.primary_key.first().is_some_and(|pk| pk == &filter.column)
    {
        storage.lookup_pk_row_index(&table, schema, &filter.value)?
    } else if filter.op == CompareOp::Eq {
        storage.lookup_unique_row_index(&table, schema, &filter.column, &filter.value)?
    } else {
        None
    };

    let (deleted, kept_rows, kept_old_indices) = {
        let rows = storage.scan(&table)?;

        let mut deleted = 0usize;
        let mut kept_rows: Vec<Row> = Vec::new();
        let mut kept_old_indices: Vec<usize> = Vec::new();
        if let Some(i) = targeted_row_idx {
            if i < rows.len() {
                let should_delete = row_matches(
                    &rows[i],
                    where_idx,
                    &filter.column,
                    where_dtype,
                    &filter.op,
                    &filter.value,
                )?;
                if should_delete {
                    deleted = 1;
                    for (idx, row) in rows.iter().enumerate() {
                        if idx != i {
                            kept_rows.push(row.clone());
                            kept_old_indices.push(idx);
                        }
                    }
                } else {
                    for (idx, row) in rows.iter().enumerate() {
                        kept_rows.push(row.clone());
                        kept_old_indices.push(idx);
                    }
                }
            }
        } else {
            let mut keep_flags: Vec<bool> = Vec::with_capacity(rows.len());
            for row in rows.iter() {
                let should_delete =
                    row_matches(row, where_idx, &filter.column, where_dtype, &filter.op, &filter.value)?;
                keep_flags.push(!should_delete);
            }

            for (idx, (row, keep)) in rows.iter().cloned().zip(keep_flags).enumerate() {
                if keep {
                    kept_rows.push(row);
                    kept_old_indices.push(idx);
                } else {
                    deleted += 1;
                }
            }
        }
        (deleted, kept_rows, kept_old_indices)
    };
    storage.replace_rows_with_alignment(&table, kept_rows, kept_old_indices)?;
    storage.rebuild_indexes(&table, schema)?;

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

fn validate_unique_constraints(
    schema: &Schema,
    rows: &[Row],
    candidate: &Row,
    skip_idx: Option<usize>,
) -> Result<(), String> {
    for (kind, idxs, cols) in unique_constraint_groups(schema)? {
        for (row_idx, existing) in rows.iter().enumerate() {
            if skip_idx == Some(row_idx) {
                continue;
            }
            let same = idxs.iter().all(|i| existing.get(*i) == candidate.get(*i));
            if same {
                return Err(format!(
                    "{} constraint violation on column(s) {}",
                    kind,
                    cols.join(",")
                ));
            }
        }
    }
    Ok(())
}

fn validate_all_unique_constraints(schema: &Schema, rows: &[Row]) -> Result<(), String> {
    for idx in 0..rows.len() {
        validate_unique_constraints(schema, rows, &rows[idx], Some(idx))?;
    }
    Ok(())
}

fn unique_constraint_groups(
    schema: &Schema,
) -> Result<Vec<(&'static str, Vec<usize>, Vec<String>)>, String> {
    let mut out: Vec<(&'static str, Vec<usize>, Vec<String>)> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    if !schema.primary_key.is_empty() {
        let (idxs, cols) = resolve_cols(schema, &schema.primary_key)?;
        let key = format!("PK:{}", cols.join(","));
        if seen.insert(key) {
            out.push(("PRIMARY KEY", idxs, cols));
        }
    }

    for c in &schema.unique_constraints {
        let (idxs, cols) = resolve_cols(schema, c)?;
        let key = format!("UQ:{}", cols.join(","));
        if seen.insert(key) {
            out.push(("UNIQUE", idxs, cols));
        }
    }

    for col in &schema.columns {
        if col.unique && !col.primary_key {
            let idx = schema
                .columns
                .iter()
                .position(|x| x.name == col.name)
                .ok_or_else(|| "Internal schema error".to_string())?;
            let cols = vec![col.name.clone()];
            let key = format!("UQ:{}", cols.join(","));
            if seen.insert(key) {
                out.push(("UNIQUE", vec![idx], cols));
            }
        }
    }

    Ok(out)
}

fn resolve_cols(schema: &Schema, names: &[String]) -> Result<(Vec<usize>, Vec<String>), String> {
    let mut idxs: Vec<usize> = Vec::new();
    for n in names {
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name == *n)
            .ok_or_else(|| format!("Unknown column '{}' in constraint", n))?;
        idxs.push(idx);
    }
    Ok((idxs, names.to_vec()))
}

fn matches_where(cell: &Value, dtype: &DataType, op: &CompareOp, rhs_token: &str) -> Result<bool, String> {
    match op {
        CompareOp::Eq => {
            let rhs = parse_value(dtype, rhs_token)?;
            Ok(cell == &rhs)
        }
        CompareOp::Gt | CompareOp::Lt | CompareOp::Gte | CompareOp::Lte => {
            let rhs = parse_value(dtype, rhs_token)?;
            let ord = compare_order(cell, &rhs, dtype)?;
            Ok(match op {
                CompareOp::Gt => ord == Ordering::Greater,
                CompareOp::Lt => ord == Ordering::Less,
                CompareOp::Gte => ord != Ordering::Less,
                CompareOp::Lte => ord != Ordering::Greater,
                _ => unreachable!(),
            })
        }
        CompareOp::Like => match (cell, dtype) {
            (Value::Text(lhs), DataType::Text) => Ok(wildcard_match(lhs, rhs_token)),
            (Value::VarChar(lhs), DataType::VarChar(_)) => Ok(wildcard_match(lhs, rhs_token)),
            _ => Err("Operator 'like' is only valid for text columns".to_string()),
        },
    }
}

fn compare_order(lhs: &Value, rhs: &Value, dtype: &DataType) -> Result<Ordering, String> {
    match dtype {
        DataType::Int => match (lhs, rhs) {
            (Value::Int(a), Value::Int(b)) => Ok(a.cmp(b)),
            _ => Err("Type mismatch while evaluating int comparison".to_string()),
        },
        DataType::BigInt => match (lhs, rhs) {
            (Value::BigInt(a), Value::BigInt(b)) => Ok(a.cmp(b)),
            _ => Err("Type mismatch while evaluating bigint comparison".to_string()),
        },
        DataType::Decimal { .. } => match (lhs, rhs) {
            (Value::Decimal(a), Value::Decimal(b)) => Ok(a.cmp(b)),
            _ => Err("Type mismatch while evaluating decimal comparison".to_string()),
        },
        DataType::Date => match (lhs, rhs) {
            (Value::Date(a), Value::Date(b)) => Ok(a.cmp(b)),
            _ => Err("Type mismatch while evaluating date comparison".to_string()),
        },
        DataType::Timestamp => match (lhs, rhs) {
            (Value::Timestamp(a), Value::Timestamp(b)) => Ok(a.cmp(b)),
            _ => Err("Type mismatch while evaluating timestamp comparison".to_string()),
        },
        _ => Err(
            "Operator gt/lt/gte/lte is only valid for int|bigint|decimal|date|timestamp columns."
                .to_string(),
        ),
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
