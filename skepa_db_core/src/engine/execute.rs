use crate::engine::format::format_select;
use crate::parser::command::{AlterAction, Assignment, ColumnDef, Command, CompareOp, ForeignKeyAction, JoinClause, OrderBy, TableConstraintDef, WhereClause};
use crate::storage::{Catalog, Column, Schema, StorageEngine};
use crate::types::datatype::DataType;
use crate::types::value::{parse_value, Value};
use crate::types::Row;
use std::cmp::Ordering;
use crate::storage::schema::ForeignKeyDef;

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
        Command::CreateIndex { table, columns } => {
            handle_create_index(table, columns, catalog, storage)
        }
        Command::DropIndex { table, columns } => handle_drop_index(table, columns, catalog, storage),
        Command::Alter { table, action } => handle_alter(table, action, catalog, storage),
        Command::Insert { table, values } => handle_insert(table, values, catalog, storage),
        Command::Update {
            table,
            assignments,
            filter,
        } => handle_update(table, assignments, filter, catalog, storage),
        Command::Delete { table, filter } => handle_delete(table, filter, catalog, storage),
        Command::Select {
            table,
            join,
            columns,
            filter,
            order_by,
            limit,
        } => handle_select(table, join, columns, filter, order_by, limit, catalog, storage),
        Command::Begin | Command::Commit | Command::Rollback => {
            Err("Transaction control is handled by Database".to_string())
        }
    }
}

fn handle_create_index(
    table: String,
    columns: Vec<String>,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    catalog.add_secondary_index(&table, columns.clone())?;
    let schema = catalog.schema(&table)?;
    storage.rebuild_indexes(&table, schema)?;
    Ok(format!(
        "created index on {}({})",
        table,
        columns.join(",")
    ))
}

fn handle_drop_index(
    table: String,
    columns: Vec<String>,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    catalog.drop_secondary_index(&table, &columns)?;
    let schema = catalog.schema(&table)?;
    storage.rebuild_indexes(&table, schema)?;
    Ok(format!(
        "dropped index on {}({})",
        table,
        columns.join(",")
    ))
}

fn handle_alter(
    table: String,
    action: AlterAction,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    let before = catalog.clone();
    let result = match action {
        AlterAction::AddUnique(cols) => {
            catalog.add_unique_constraint(&table, cols.clone())?;
            let schema = catalog.schema(&table)?;
            let rows = storage.scan(&table)?;
            validate_all_unique_constraints(schema, rows)?;
            storage.rebuild_indexes(&table, schema)?;
            Ok(format!("altered table {}: added unique({})", table, cols.join(",")))
        }
        AlterAction::DropUnique(cols) => {
            catalog.drop_unique_constraint(&table, &cols)?;
            let schema = catalog.schema(&table)?;
            storage.rebuild_indexes(&table, schema)?;
            Ok(format!("altered table {}: dropped unique({})", table, cols.join(",")))
        }
        AlterAction::AddForeignKey {
            columns,
            ref_table,
            ref_columns,
            on_delete,
            on_update,
        } => {
            catalog.add_foreign_key_constraint(
                &table,
                ForeignKeyDef {
                    columns: columns.clone(),
                    ref_table: ref_table.clone(),
                    ref_columns: ref_columns.clone(),
                    on_delete,
                    on_update,
                },
            )?;
            let schema = catalog.schema(&table)?;
            let rows = storage.scan(&table)?;
            validate_all_foreign_keys(catalog, storage, schema, rows)?;
            Ok(format!(
                "altered table {}: added foreign key({}) references {}({})",
                table,
                columns.join(","),
                ref_table,
                ref_columns.join(",")
            ))
        }
        AlterAction::DropForeignKey {
            columns,
            ref_table,
            ref_columns,
        } => {
            catalog.drop_foreign_key_constraint(&table, &columns, &ref_table, &ref_columns)?;
            Ok(format!(
                "altered table {}: dropped foreign key({}) references {}({})",
                table,
                columns.join(","),
                ref_table,
                ref_columns.join(",")
            ))
        }
        AlterAction::SetNotNull(col) => {
            catalog.set_not_null(&table, &col, true)?;
            let schema = catalog.schema(&table)?;
            let rows = storage.scan(&table)?;
            validate_not_null_columns(schema, rows)?;
            Ok(format!("altered table {}: set {} not null", table, col))
        }
        AlterAction::DropNotNull(col) => {
            catalog.set_not_null(&table, &col, false)?;
            Ok(format!("altered table {}: dropped not null on {}", table, col))
        }
    };
    if result.is_err() {
        *catalog = before;
    }
    result
}

pub fn validate_no_action_constraints(
    catalog: &Catalog,
    storage: &dyn StorageEngine,
) -> Result<(), String> {
    for (child_table, child_schema) in catalog.snapshot_tables() {
        if child_schema.foreign_keys.is_empty() {
            continue;
        }
        let child_rows = storage.scan(&child_table)?;
        for fk in &child_schema.foreign_keys {
            if !matches!(fk.on_delete, ForeignKeyAction::NoAction)
                && !matches!(fk.on_update, ForeignKeyAction::NoAction)
            {
                continue;
            }
            let parent_schema = catalog.schema(&fk.ref_table)?;
            let parent_rows = storage.scan(&fk.ref_table)?;
            let child_idxs = resolve_cols_to_idxs(&child_schema, &fk.columns)?;
            let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;
            for cr in child_rows {
                if child_idxs
                    .iter()
                    .any(|i| matches!(cr.get(*i), Some(Value::Null)))
                {
                    continue;
                }
                let found = parent_rows
                    .iter()
                    .any(|pr| tuple_eq(cr, &child_idxs, pr, &parent_idxs));
                if !found {
                    return Err(format!(
                        "FOREIGN KEY NO ACTION violation: '{}' references '{}'",
                        child_table, fk.ref_table
                    ));
                }
            }
        }
    }
    Ok(())
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
    validate_outgoing_foreign_keys(catalog, storage, schema, &row)?;

    storage.insert_row(&table, row)?;
    storage.rebuild_indexes(&table, schema)?;
    Ok(format!("inserted 1 row into {}", table))
}

fn handle_select(
    table: String,
    join: Option<JoinClause>,
    columns: Option<Vec<String>>,
    filter: Option<WhereClause>,
    order_by: Option<OrderBy>,
    limit: Option<usize>,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    let is_join = join.is_some();
    let (select_schema, base_rows): (Schema, Vec<Row>) = if let Some(join_clause) = join {
        build_join_rows(catalog, storage, &table, &join_clause)?
    } else {
        let schema = catalog.schema(&table)?;
        let rows = storage.scan(&table)?;
        (schema.clone(), rows.to_vec())
    };

    let filtered_rows = if let Some(where_clause) = filter {
        if !is_join
            && where_clause.op == CompareOp::Eq
            && select_schema.primary_key.len() == 1
            && select_schema
                .primary_key
                .first()
                .is_some_and(|pk| pk == &where_clause.column)
        {
            if let Some(row_idx) = storage.lookup_pk_row_index(&table, &select_schema, &where_clause.value)? {
                match base_rows.get(row_idx) {
                    Some(r) => vec![r.clone()],
                    None => Vec::new(),
                }
            } else {
                Vec::new()
            }
        } else if !is_join && where_clause.op == CompareOp::Eq {
            if let Some(row_idx) =
                storage.lookup_unique_row_index(&table, &select_schema, &where_clause.column, &where_clause.value)?
            {
                match base_rows.get(row_idx) {
                    Some(r) => vec![r.clone()],
                    None => Vec::new(),
                }
            } else if let Some(row_indices) =
                storage.lookup_secondary_row_indices(&table, &select_schema, &where_clause.column, &where_clause.value)?
            {
                row_indices
                    .into_iter()
                    .filter_map(|i| base_rows.get(i).cloned())
                    .collect()
            } else {
                filter_rows(&select_schema, &base_rows, &where_clause)?
            }
        } else {
            filter_rows(&select_schema, &base_rows, &where_clause)?
        }
    } else {
        base_rows.to_vec()
    };

    let mut ordered_rows = filtered_rows;
    if let Some(ob) = order_by {
        let idx = resolve_column_index(&select_schema, &ob.column, "ORDER BY")?;
        ordered_rows.sort_by(|a, b| compare_for_order(a.get(idx), b.get(idx), ob.asc));
    }
    let limited_rows = if let Some(n) = limit {
        ordered_rows.into_iter().take(n).collect::<Vec<_>>()
    } else {
        ordered_rows
    };

    let (out_schema, out_rows) = project_rows(&select_schema, &limited_rows, columns.as_ref())?;
    Ok(format_select(&out_schema, &out_rows))
}

fn build_join_rows(
    catalog: &Catalog,
    storage: &dyn StorageEngine,
    left_table: &str,
    join: &JoinClause,
) -> Result<(Schema, Vec<Row>), String> {
    let left_schema = catalog.schema(left_table)?;
    let right_schema = catalog.schema(&join.table)?;
    let left_rows = storage.scan(left_table)?;
    let right_rows = storage.scan(&join.table)?;

    let (left_side, left_idx) =
        resolve_join_operand(left_table, left_schema, &join.table, right_schema, &join.left_column)?;
    let (right_side, right_idx) =
        resolve_join_operand(left_table, left_schema, &join.table, right_schema, &join.right_column)?;

    if left_side == right_side {
        return Err("JOIN ON clause must compare one column from each table".to_string());
    }

    let (lidx, ridx) = if left_side {
        (left_idx, right_idx)
    } else {
        (right_idx, left_idx)
    };

    if left_schema.columns[lidx].dtype != right_schema.columns[ridx].dtype {
        return Err("JOIN columns must have the same datatype".to_string());
    }

    let mut out_columns: Vec<Column> = Vec::new();
    for c in &left_schema.columns {
        out_columns.push(Column {
            name: format!("{}.{}", left_table, c.name),
            dtype: c.dtype.clone(),
            primary_key: false,
            unique: false,
            not_null: c.not_null,
        });
    }
    for c in &right_schema.columns {
        out_columns.push(Column {
            name: format!("{}.{}", join.table, c.name),
            dtype: c.dtype.clone(),
            primary_key: false,
            unique: false,
            not_null: c.not_null,
        });
    }

    let mut out_rows: Vec<Row> = Vec::new();
    for lr in left_rows {
        for rr in right_rows {
            if matches!(lr.get(lidx), Some(Value::Null)) || matches!(rr.get(ridx), Some(Value::Null)) {
                continue;
            }
            if lr.get(lidx) == rr.get(ridx) {
                let mut row = lr.clone();
                row.extend(rr.clone());
                out_rows.push(row);
            }
        }
    }

    Ok((Schema::new(out_columns), out_rows))
}

fn resolve_join_operand(
    left_table: &str,
    left_schema: &Schema,
    right_table: &str,
    right_schema: &Schema,
    token: &str,
) -> Result<(bool, usize), String> {
    if let Some((tbl, col)) = token.split_once('.') {
        if tbl == left_table {
            let idx = left_schema
                .columns
                .iter()
                .position(|c| c.name == col)
                .ok_or_else(|| format!("Unknown column '{}' in JOIN", token))?;
            return Ok((true, idx));
        }
        if tbl == right_table {
            let idx = right_schema
                .columns
                .iter()
                .position(|c| c.name == col)
                .ok_or_else(|| format!("Unknown column '{}' in JOIN", token))?;
            return Ok((false, idx));
        }
        return Err(format!("Unknown table '{}' in JOIN", tbl));
    }

    let left_idx = left_schema.columns.iter().position(|c| c.name == token);
    let right_idx = right_schema.columns.iter().position(|c| c.name == token);
    match (left_idx, right_idx) {
        (Some(i), None) => Ok((true, i)),
        (None, Some(i)) => Ok((false, i)),
        (Some(_), Some(_)) => Err(format!(
            "Ambiguous column '{}' in JOIN. Qualify it as {}.{} or {}.{}",
            token, left_table, token, right_table, token
        )),
        (None, None) => Err(format!("Unknown column '{}' in JOIN", token)),
    }
}

fn resolve_column_index(schema: &Schema, name: &str, clause: &str) -> Result<usize, String> {
    if let Some(idx) = schema.columns.iter().position(|c| c.name == name) {
        return Ok(idx);
    }
    if name.contains('.') {
        return Err(format!("Unknown column '{}' in {}", name, clause));
    }

    let suffix = format!(".{}", name);
    let mut matches: Vec<usize> = Vec::new();
    for (idx, c) in schema.columns.iter().enumerate() {
        if c.name.ends_with(&suffix) {
            matches.push(idx);
        }
    }
    match matches.len() {
        1 => Ok(matches[0]),
        0 => Err(format!("Unknown column '{}' in {}", name, clause)),
        _ => Err(format!(
            "Ambiguous column '{}' in {}. Use qualified name table.column",
            name, clause
        )),
    }
}

fn compare_for_order(a: Option<&Value>, b: Option<&Value>, asc: bool) -> Ordering {
    let ord = match (a, b) {
        (Some(Value::Null), Some(Value::Null)) => Ordering::Equal,
        (Some(Value::Null), _) => Ordering::Less,
        (_, Some(Value::Null)) => Ordering::Greater,
        (Some(Value::Bool(x)), Some(Value::Bool(y))) => x.cmp(y),
        (Some(Value::Int(x)), Some(Value::Int(y))) => x.cmp(y),
        (Some(Value::BigInt(x)), Some(Value::BigInt(y))) => x.cmp(y),
        (Some(Value::Decimal(x)), Some(Value::Decimal(y))) => x.cmp(y),
        (Some(Value::VarChar(x)), Some(Value::VarChar(y))) => x.cmp(y),
        (Some(Value::Text(x)), Some(Value::Text(y))) => x.cmp(y),
        (Some(Value::Date(x)), Some(Value::Date(y))) => x.cmp(y),
        (Some(Value::Timestamp(x)), Some(Value::Timestamp(y))) => x.cmp(y),
        (Some(Value::Uuid(x)), Some(Value::Uuid(y))) => x.cmp(y),
        (Some(Value::Json(x)), Some(Value::Json(y))) => x.to_string().cmp(&y.to_string()),
        (Some(Value::Blob(x)), Some(Value::Blob(y))) => x.cmp(y),
        _ => Ordering::Equal,
    };
    if asc { ord } else { ord.reverse() }
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
    let targeted_row_indices = if filter.op == CompareOp::Eq
        && schema.primary_key.len() == 1
        && schema.primary_key.first().is_some_and(|pk| pk == &filter.column)
    {
        storage
            .lookup_pk_row_index(&table, schema, &filter.value)?
            .map(|i| vec![i])
    } else if filter.op == CompareOp::Eq {
        if let Some(i) = storage.lookup_unique_row_index(&table, schema, &filter.column, &filter.value)? {
            Some(vec![i])
        } else {
            storage.lookup_secondary_row_indices(&table, schema, &filter.column, &filter.value)?
        }
    } else {
        None
    };

    let (updated, new_rows, old_indices, old_rows) = {
        let rows = storage.scan(&table)?;
        let old_rows = rows.to_vec();
        let mut updated = 0usize;
        let mut new_rows = rows.to_vec();
        let old_indices: Vec<usize> = (0..rows.len()).collect();

        if let Some(indices) = targeted_row_indices {
            for i in indices {
                if i >= new_rows.len() {
                    continue;
                }
                let row = &mut new_rows[i];
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
        validate_all_foreign_keys(catalog, storage, schema, &new_rows)?;
        validate_restrict_on_parent_update(catalog, storage, &table, schema, &old_rows, &new_rows)?;
        (updated, new_rows, old_indices, old_rows)
    };
    storage.replace_rows_with_alignment(&table, new_rows, old_indices)?;
    let post_parent_rows = storage.scan(&table)?.to_vec();
    apply_on_update_cascade(catalog, storage, &table, schema, &old_rows, &post_parent_rows)?;
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
    let targeted_row_indices = if filter.op == CompareOp::Eq
        && schema.primary_key.len() == 1
        && schema.primary_key.first().is_some_and(|pk| pk == &filter.column)
    {
        storage
            .lookup_pk_row_index(&table, schema, &filter.value)?
            .map(|i| vec![i])
    } else if filter.op == CompareOp::Eq {
        if let Some(i) = storage.lookup_unique_row_index(&table, schema, &filter.column, &filter.value)? {
            Some(vec![i])
        } else {
            storage.lookup_secondary_row_indices(&table, schema, &filter.column, &filter.value)?
        }
    } else {
        None
    };

    let (deleted, kept_rows, kept_old_indices, deleted_rows) = {
        let rows = storage.scan(&table)?;

        let mut deleted = 0usize;
        let mut kept_rows: Vec<Row> = Vec::new();
        let mut kept_old_indices: Vec<usize> = Vec::new();
        let mut deleted_rows: Vec<Row> = Vec::new();
        if let Some(indices) = targeted_row_indices {
            let targets: std::collections::HashSet<usize> = indices.into_iter().collect();
            for (idx, row) in rows.iter().enumerate() {
                if !targets.contains(&idx) {
                    kept_rows.push(row.clone());
                    kept_old_indices.push(idx);
                    continue;
                }
                let should_delete = row_matches(
                    row,
                    where_idx,
                    &filter.column,
                    where_dtype,
                    &filter.op,
                    &filter.value,
                )?;
                if should_delete {
                    validate_restrict_on_parent_delete(catalog, storage, &table, schema, row)?;
                    deleted += 1;
                    deleted_rows.push(row.clone());
                } else {
                    kept_rows.push(row.clone());
                    kept_old_indices.push(idx);
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
                    validate_restrict_on_parent_delete(catalog, storage, &table, schema, &rows[idx])?;
                    deleted += 1;
                    deleted_rows.push(rows[idx].clone());
                }
            }
        }
        (deleted, kept_rows, kept_old_indices, deleted_rows)
    };
    storage.replace_rows_with_alignment(&table, kept_rows, kept_old_indices)?;
    apply_on_delete_cascade(catalog, storage, &table, schema, &deleted_rows)?;
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
        let idx = resolve_column_index(schema, name, "SELECT list")?;
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
    let col_idx = resolve_column_index(schema, &where_clause.column, "WHERE")?;

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
            if idxs.iter().any(|i| matches!(candidate.get(*i), Some(Value::Null)))
                || idxs.iter().any(|i| matches!(existing.get(*i), Some(Value::Null)))
            {
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

fn validate_not_null_columns(schema: &Schema, rows: &[Row]) -> Result<(), String> {
    for row in rows {
        for (idx, col) in schema.columns.iter().enumerate() {
            if col.not_null && matches!(row.get(idx), Some(Value::Null)) {
                return Err(format!("Column '{}' is NOT NULL", col.name));
            }
        }
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

fn validate_all_foreign_keys(
    catalog: &Catalog,
    storage: &dyn StorageEngine,
    schema: &Schema,
    rows: &[Row],
) -> Result<(), String> {
    for r in rows {
        validate_outgoing_foreign_keys(catalog, storage, schema, r)?;
    }
    Ok(())
}

fn validate_outgoing_foreign_keys(
    catalog: &Catalog,
    storage: &dyn StorageEngine,
    schema: &Schema,
    row: &Row,
) -> Result<(), String> {
    for fk in &schema.foreign_keys {
        let parent_schema = catalog.schema(&fk.ref_table)?;
        let parent_rows = storage.scan(&fk.ref_table)?;
        let child_idxs = resolve_cols_to_idxs(schema, &fk.columns)?;
        let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;
        if child_idxs
            .iter()
            .any(|i| matches!(row.get(*i), Some(Value::Null)))
        {
            continue;
        }
        let found = parent_rows.iter().any(|pr| tuple_eq(row, &child_idxs, pr, &parent_idxs));
        if !found {
            return Err(format!(
                "FOREIGN KEY violation on ({}) references {}({})",
                fk.columns.join(","),
                fk.ref_table,
                fk.ref_columns.join(",")
            ));
        }
    }
    Ok(())
}

fn validate_restrict_on_parent_delete(
    catalog: &Catalog,
    storage: &dyn StorageEngine,
    parent_table: &str,
    parent_schema: &Schema,
    parent_row: &Row,
) -> Result<(), String> {
    for (child_table, fk) in incoming_foreign_keys(catalog, parent_table) {
        if fk.on_delete != ForeignKeyAction::Restrict {
            continue;
        }
        let child_schema = catalog.schema(&child_table)?;
        let child_rows = storage.scan(&child_table)?;
        let child_idxs = resolve_cols_to_idxs(child_schema, &fk.columns)?;
        let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;
        if child_rows
            .iter()
            .any(|cr| tuple_eq(cr, &child_idxs, parent_row, &parent_idxs))
        {
            return Err(format!(
                "FOREIGN KEY RESTRICT violation: '{}' is referenced by '{}'",
                parent_table, child_table
            ));
        }
    }
    Ok(())
}

fn validate_restrict_on_parent_update(
    catalog: &Catalog,
    storage: &dyn StorageEngine,
    parent_table: &str,
    parent_schema: &Schema,
    old_rows: &[Row],
    new_rows: &[Row],
) -> Result<(), String> {
    for (old_r, new_r) in old_rows.iter().zip(new_rows.iter()) {
        if old_r == new_r {
            continue;
        }
        for (child_table, fk) in incoming_foreign_keys(catalog, parent_table) {
            if fk.on_update != ForeignKeyAction::Restrict {
                continue;
            }
            let child_schema = catalog.schema(&child_table)?;
            let child_rows = storage.scan(&child_table)?;
            let child_idxs = resolve_cols_to_idxs(child_schema, &fk.columns)?;
            let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;
            let was_referenced = child_rows
                .iter()
                .any(|cr| tuple_eq(cr, &child_idxs, old_r, &parent_idxs));
            if was_referenced && !tuple_eq(old_r, &parent_idxs, new_r, &parent_idxs) {
                return Err(format!(
                    "FOREIGN KEY RESTRICT violation: '{}' is referenced by '{}'",
                    parent_table, child_table
                ));
            }
        }
    }
    Ok(())
}

fn incoming_foreign_keys(catalog: &Catalog, parent_table: &str) -> Vec<(String, ForeignKeyDef)> {
    let mut out = Vec::new();
    for (table, schema) in catalog.snapshot_tables() {
        for fk in schema.foreign_keys {
            if fk.ref_table == parent_table {
                out.push((table.clone(), fk));
            }
        }
    }
    out
}

fn apply_on_delete_cascade(
    catalog: &Catalog,
    storage: &mut dyn StorageEngine,
    parent_table: &str,
    parent_schema: &Schema,
    deleted_parent_rows: &[Row],
) -> Result<(), String> {
    if deleted_parent_rows.is_empty() {
        return Ok(());
    }
    apply_on_delete_set_null(catalog, storage, parent_table, parent_schema, deleted_parent_rows)?;
    for (child_table, fk) in incoming_foreign_keys(catalog, parent_table) {
        if fk.on_delete != ForeignKeyAction::Cascade {
            continue;
        }
        let child_schema = catalog.schema(&child_table)?;
        let child_rows = storage.scan(&child_table)?;
        let child_idxs = resolve_cols_to_idxs(child_schema, &fk.columns)?;
        let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;

        let mut keep_rows: Vec<Row> = Vec::new();
        let mut keep_old_indices: Vec<usize> = Vec::new();
        let mut deleted_child_rows: Vec<Row> = Vec::new();
        for (idx, cr) in child_rows.iter().enumerate() {
            let referenced = deleted_parent_rows
                .iter()
                .any(|pr| tuple_eq(cr, &child_idxs, pr, &parent_idxs));
            if !referenced {
                keep_rows.push(cr.clone());
                keep_old_indices.push(idx);
            } else {
                validate_restrict_on_parent_delete(catalog, storage, &child_table, child_schema, cr)?;
                deleted_child_rows.push(cr.clone());
            }
        }
        storage.replace_rows_with_alignment(&child_table, keep_rows, keep_old_indices)?;
        storage.rebuild_indexes(&child_table, child_schema)?;
        apply_on_delete_cascade(catalog, storage, &child_table, child_schema, &deleted_child_rows)?;
    }
    Ok(())
}

fn apply_on_delete_set_null(
    catalog: &Catalog,
    storage: &mut dyn StorageEngine,
    parent_table: &str,
    parent_schema: &Schema,
    deleted_parent_rows: &[Row],
) -> Result<(), String> {
    for (child_table, fk) in incoming_foreign_keys(catalog, parent_table) {
        if fk.on_delete != ForeignKeyAction::SetNull {
            continue;
        }
        let child_schema = catalog.schema(&child_table)?;
        let child_rows = storage.scan(&child_table)?;
        let child_idxs = resolve_cols_to_idxs(child_schema, &fk.columns)?;
        let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;

        for ci in &child_idxs {
            if child_schema.columns[*ci].not_null {
                return Err(format!(
                    "FOREIGN KEY SET NULL requires nullable child column '{}.{}'",
                    child_table, child_schema.columns[*ci].name
                ));
            }
        }

        let mut updated_child_rows = child_rows.to_vec();
        for cr in &mut updated_child_rows {
            let referenced = deleted_parent_rows
                .iter()
                .any(|pr| tuple_eq(cr, &child_idxs, pr, &parent_idxs));
            if referenced {
                for ci in &child_idxs {
                    cr[*ci] = Value::Null;
                }
            }
        }

        validate_all_unique_constraints(child_schema, &updated_child_rows)?;
        validate_all_foreign_keys(catalog, storage, child_schema, &updated_child_rows)?;
        let keep_old_indices: Vec<usize> = (0..updated_child_rows.len()).collect();
        storage.replace_rows_with_alignment(&child_table, updated_child_rows, keep_old_indices)?;
        storage.rebuild_indexes(&child_table, child_schema)?;
    }
    Ok(())
}

fn apply_on_update_cascade(
    catalog: &Catalog,
    storage: &mut dyn StorageEngine,
    parent_table: &str,
    parent_schema: &Schema,
    old_parent_rows: &[Row],
    new_parent_rows: &[Row],
) -> Result<(), String> {
    if old_parent_rows.len() != new_parent_rows.len() {
        return Err("Internal error: parent row alignment mismatch during ON UPDATE CASCADE".to_string());
    }
    apply_on_update_set_null(catalog, storage, parent_table, parent_schema, old_parent_rows, new_parent_rows)?;
    for (child_table, fk) in incoming_foreign_keys(catalog, parent_table) {
        if fk.on_update != ForeignKeyAction::Cascade {
            continue;
        }
        let child_schema = catalog.schema(&child_table)?;
        let child_rows = storage.scan(&child_table)?;
        let old_child_rows = child_rows.to_vec();
        let child_idxs = resolve_cols_to_idxs(child_schema, &fk.columns)?;
        let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;

        let mut updated_child_rows = old_child_rows.clone();
        for cr in &mut updated_child_rows {
            for (old_pr, new_pr) in old_parent_rows.iter().zip(new_parent_rows.iter()) {
                if tuple_eq(cr, &child_idxs, old_pr, &parent_idxs)
                    && !tuple_eq(old_pr, &parent_idxs, new_pr, &parent_idxs)
                {
                    for (ci, pi) in child_idxs.iter().zip(parent_idxs.iter()) {
                        cr[*ci] = new_pr[*pi].clone();
                    }
                }
            }
        }

        validate_all_unique_constraints(child_schema, &updated_child_rows)?;
        validate_all_foreign_keys(catalog, storage, child_schema, &updated_child_rows)?;
        let keep_old_indices: Vec<usize> = (0..updated_child_rows.len()).collect();
        storage.replace_rows_with_alignment(&child_table, updated_child_rows, keep_old_indices)?;
        let post_child_rows = storage.scan(&child_table)?.to_vec();
        apply_on_update_cascade(
            catalog,
            storage,
            &child_table,
            child_schema,
            &old_child_rows,
            &post_child_rows,
        )?;
        storage.rebuild_indexes(&child_table, child_schema)?;
    }
    Ok(())
}

fn apply_on_update_set_null(
    catalog: &Catalog,
    storage: &mut dyn StorageEngine,
    parent_table: &str,
    parent_schema: &Schema,
    old_parent_rows: &[Row],
    new_parent_rows: &[Row],
) -> Result<(), String> {
    for (child_table, fk) in incoming_foreign_keys(catalog, parent_table) {
        if fk.on_update != ForeignKeyAction::SetNull {
            continue;
        }
        let child_schema = catalog.schema(&child_table)?;
        let child_rows = storage.scan(&child_table)?;
        let child_idxs = resolve_cols_to_idxs(child_schema, &fk.columns)?;
        let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;

        for ci in &child_idxs {
            if child_schema.columns[*ci].not_null {
                return Err(format!(
                    "FOREIGN KEY SET NULL requires nullable child column '{}.{}'",
                    child_table, child_schema.columns[*ci].name
                ));
            }
        }

        let mut updated_child_rows = child_rows.to_vec();
        for cr in &mut updated_child_rows {
            for (old_pr, new_pr) in old_parent_rows.iter().zip(new_parent_rows.iter()) {
                if tuple_eq(cr, &child_idxs, old_pr, &parent_idxs)
                    && !tuple_eq(old_pr, &parent_idxs, new_pr, &parent_idxs)
                {
                    for ci in &child_idxs {
                        cr[*ci] = Value::Null;
                    }
                }
            }
        }

        validate_all_unique_constraints(child_schema, &updated_child_rows)?;
        validate_all_foreign_keys(catalog, storage, child_schema, &updated_child_rows)?;
        let keep_old_indices: Vec<usize> = (0..updated_child_rows.len()).collect();
        storage.replace_rows_with_alignment(&child_table, updated_child_rows, keep_old_indices)?;
        storage.rebuild_indexes(&child_table, child_schema)?;
    }
    Ok(())
}

fn resolve_cols_to_idxs(schema: &Schema, cols: &[String]) -> Result<Vec<usize>, String> {
    cols.iter()
        .map(|c| {
            schema
                .columns
                .iter()
                .position(|x| x.name == *c)
                .ok_or_else(|| format!("Unknown column '{}' in FOREIGN KEY", c))
        })
        .collect()
}

fn tuple_eq(a_row: &Row, a_idxs: &[usize], b_row: &Row, b_idxs: &[usize]) -> bool {
    a_idxs
        .iter()
        .zip(b_idxs.iter())
        .all(|(ai, bi)| a_row.get(*ai) == b_row.get(*bi))
}
