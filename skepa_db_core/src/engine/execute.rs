use crate::engine::format::format_select;
use crate::parser::command::{AlterAction, Assignment, ColumnDef, Command, CompareOp, ForeignKeyAction, JoinClause, JoinType, LogicalOp, OrderBy, TableConstraintDef, WhereClause};
use crate::storage::{Catalog, Column, Schema, StorageEngine};
use crate::types::datatype::DataType;
use crate::types::value::{parse_value, value_to_string, Value};
use crate::types::Row;
use std::cmp::Ordering;
use crate::storage::schema::ForeignKeyDef;
use rust_decimal::Decimal;

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
            distinct,
            join,
            columns,
            filter,
            group_by,
            having,
            order_by,
            limit,
            offset,
        } => handle_select(table, distinct, join, columns, filter, group_by, having, order_by, limit, offset, catalog, storage),
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
    distinct: bool,
    join: Option<JoinClause>,
    columns: Option<Vec<String>>,
    filter: Option<WhereClause>,
    group_by: Option<Vec<String>>,
    having: Option<WhereClause>,
    order_by: Option<OrderBy>,
    limit: Option<usize>,
    offset: Option<usize>,
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
            && simple_eq_filter(&where_clause).is_some()
            && select_schema.primary_key.len() == 1
            && select_schema
                .primary_key
                .first()
                .is_some_and(|pk| pk == &simple_eq_filter(&where_clause).expect("eq").0)
        {
            let (_col, val) = simple_eq_filter(&where_clause).expect("eq");
            if let Some(row_idx) = storage.lookup_pk_row_index(&table, &select_schema, &val)? {
                match base_rows.get(row_idx) {
                    Some(r) => vec![r.clone()],
                    None => Vec::new(),
                }
            } else {
                Vec::new()
            }
        } else if !is_join && simple_eq_filter(&where_clause).is_some() {
            let (col, val) = simple_eq_filter(&where_clause).expect("eq");
            if let Some(row_idx) =
                storage.lookup_unique_row_index(&table, &select_schema, &col, &val)?
            {
                match base_rows.get(row_idx) {
                    Some(r) => vec![r.clone()],
                    None => Vec::new(),
                }
            } else if let Some(row_indices) =
                storage.lookup_secondary_row_indices(&table, &select_schema, &col, &val)?
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

    let is_grouped = has_group_or_aggregate(columns.as_ref(), group_by.as_ref());

    if is_grouped {
        let (post_schema, mut post_rows) = evaluate_grouped_select(
            &select_schema,
            &filtered_rows,
            columns.as_ref(),
            group_by.as_ref(),
        )?;
        if let Some(having_clause) = having.as_ref() {
            post_rows = filter_rows(&post_schema, &post_rows, having_clause)?;
        }
        if distinct {
            post_rows = dedupe_rows(post_rows);
        }

        let mut ordered_rows = post_rows;
        if let Some(ob) = order_by {
            let mut criteria: Vec<(usize, bool)> = Vec::new();
            criteria.push((
                resolve_column_index(&post_schema, &ob.column, "ORDER BY")?,
                ob.asc,
            ));
            for (col, asc) in ob.then_by {
                criteria.push((resolve_column_index(&post_schema, &col, "ORDER BY")?, asc));
            }
            ordered_rows.sort_by(|a, b| {
                for (idx, asc) in &criteria {
                    let ord = compare_for_order(a.get(*idx), b.get(*idx), *asc);
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                Ordering::Equal
            });
        }
        let start = offset.unwrap_or(0);
        let sliced_rows = if let Some(n) = limit {
            ordered_rows
                .into_iter()
                .skip(start)
                .take(n)
                .collect::<Vec<_>>()
        } else {
            ordered_rows.into_iter().skip(start).collect::<Vec<_>>()
        };
        return Ok(format_select(&post_schema, &sliced_rows));
    }

    if having.is_some() {
        return Err("HAVING requires GROUP BY or aggregate functions".to_string());
    }

    if distinct {
        let (out_schema, projected_rows) = project_rows(&select_schema, &filtered_rows, columns.as_ref())?;
        let mut distinct_rows = dedupe_rows(projected_rows);
        if let Some(ob) = order_by {
            let mut criteria: Vec<(usize, bool)> = Vec::new();
            criteria.push((resolve_column_index(&out_schema, &ob.column, "ORDER BY")?, ob.asc));
            for (col, asc) in ob.then_by {
                criteria.push((resolve_column_index(&out_schema, &col, "ORDER BY")?, asc));
            }
            distinct_rows.sort_by(|a, b| {
                for (idx, asc) in &criteria {
                    let ord = compare_for_order(a.get(*idx), b.get(*idx), *asc);
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                Ordering::Equal
            });
        }
        let start = offset.unwrap_or(0);
        let limited_rows = if let Some(n) = limit {
            distinct_rows.into_iter().skip(start).take(n).collect::<Vec<_>>()
        } else {
            distinct_rows.into_iter().skip(start).collect::<Vec<_>>()
        };
        return Ok(format_select(&out_schema, &limited_rows));
    }

    let mut ordered_rows = filtered_rows;
    if let Some(ob) = order_by {
        let mut alias_to_idx: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        if let Some(req_cols) = columns.as_ref() {
            for item in req_cols {
                let (expr, alias) = split_select_alias(item);
                if let Some(a) = alias {
                    if let Ok(idx) = resolve_column_index(&select_schema, &expr, "SELECT list") {
                        alias_to_idx.insert(a, idx);
                    }
                }
            }
        }
        let mut criteria: Vec<(usize, bool)> = Vec::new();
        let first_idx = resolve_column_index(&select_schema, &ob.column, "ORDER BY").or_else(|e| {
            if e.contains("Unknown column") {
                alias_to_idx
                    .get(&ob.column)
                    .copied()
                    .ok_or_else(|| format!("Unknown column '{}' in ORDER BY", ob.column))
            } else {
                Err(e)
            }
        })?;
        criteria.push((first_idx, ob.asc));
        for (col, asc) in ob.then_by {
            let idx = resolve_column_index(&select_schema, &col, "ORDER BY").or_else(|e| {
                if e.contains("Unknown column") {
                    alias_to_idx
                        .get(&col)
                        .copied()
                        .ok_or_else(|| format!("Unknown column '{}' in ORDER BY", col))
                } else {
                    Err(e)
                }
            })?;
            criteria.push((idx, asc));
        }
        ordered_rows.sort_by(|a, b| {
            for (idx, asc) in &criteria {
                let ord = compare_for_order(a.get(*idx), b.get(*idx), *asc);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });
    }
    let start = offset.unwrap_or(0);
    let limited_rows = if let Some(n) = limit {
        ordered_rows
            .into_iter()
            .skip(start)
            .take(n)
            .collect::<Vec<_>>()
    } else {
        ordered_rows.into_iter().skip(start).collect::<Vec<_>>()
    };

    let (out_schema, out_rows) = project_rows(&select_schema, &limited_rows, columns.as_ref())?;
    Ok(format_select(&out_schema, &out_rows))
}

fn dedupe_rows(rows: Vec<Row>) -> Vec<Row> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut out: Vec<Row> = Vec::new();
    for r in rows {
        let key = r
            .iter()
            .map(value_to_string)
            .collect::<Vec<_>>()
            .join("\u{1F}");
        if seen.insert(key) {
            out.push(r);
        }
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateFn {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

fn has_group_or_aggregate(columns: Option<&Vec<String>>, group_by: Option<&Vec<String>>) -> bool {
    if group_by.is_some() {
        return true;
    }
    let Some(cols) = columns else {
        return false;
    };
    cols.iter()
        .any(|c| parse_aggregate_expr(&split_select_alias(c).0).is_some())
}

fn parse_aggregate_expr(token: &str) -> Option<(AggregateFn, String)> {
    let (fname_raw, rest) = token.split_once('(')?;
    let arg = rest.strip_suffix(')')?.trim();
    let func = match fname_raw.to_lowercase().as_str() {
        "count" => AggregateFn::Count,
        "sum" => AggregateFn::Sum,
        "avg" => AggregateFn::Avg,
        "min" => AggregateFn::Min,
        "max" => AggregateFn::Max,
        _ => return None,
    };
    if arg.is_empty() {
        return None;
    }
    Some((func, arg.to_string()))
}

fn parse_aggregate_expr_extended(token: &str) -> Option<(AggregateFn, String, bool)> {
    let (func, arg) = parse_aggregate_expr(token)?;
    let lower = arg.to_lowercase();
    if let Some(rest) = lower.strip_prefix("distinct ") {
        let original_rest = arg[("distinct ".len())..].trim().to_string();
        if rest.trim().is_empty() {
            return None;
        }
        return Some((func, original_rest, true));
    }
    Some((func, arg, false))
}

#[derive(Debug, Clone, Copy)]
struct AggregateMeta {
    func: AggregateFn,
    arg_idx: Option<usize>,
    distinct: bool,
}

fn evaluate_grouped_select(
    schema: &Schema,
    rows: &[Row],
    columns: Option<&Vec<String>>,
    group_by: Option<&Vec<String>>,
) -> Result<(Schema, Vec<Row>), String> {
    let Some(select_cols) = columns else {
        return Err("GROUP BY or aggregates require explicit SELECT columns".to_string());
    };
    if select_cols.is_empty() {
        return Err("SELECT * cannot be used with GROUP BY or aggregate functions".to_string());
    }

    let group_cols = group_by.cloned().unwrap_or_default();
    let mut group_key_indices: Vec<usize> = Vec::new();
    for g in &group_cols {
        group_key_indices.push(resolve_column_index(schema, g, "GROUP BY")?);
    }

    let mut output_columns: Vec<Column> = Vec::new();
    let mut select_items: Vec<(bool, usize, Option<AggregateMeta>)> = Vec::new();
    // (is_agg, source_idx_for_plain, agg meta)
    let mut has_agg = false;
    for sel in select_cols {
        let (sel_expr, sel_alias) = split_select_alias(sel);
        if let Some((agg_fn, arg, is_distinct)) = parse_aggregate_expr_extended(&sel_expr) {
            has_agg = true;
            if is_distinct && arg == "*" {
                return Err("DISTINCT with '*' is not supported in aggregates".to_string());
            }
            let (dtype, arg_idx_opt) = if arg == "*" {
                (DataType::BigInt, None)
            } else {
                let idx = resolve_column_index(schema, &arg, "SELECT aggregate")?;
                let col = &schema.columns[idx];
                let out_dtype = aggregate_output_type(agg_fn, &col.dtype)?;
                (out_dtype, Some(idx))
            };
            output_columns.push(Column {
                name: sel_alias.unwrap_or_else(|| sel_expr.clone()),
                dtype,
                primary_key: false,
                unique: false,
                not_null: false,
            });
            select_items.push((
                true,
                0usize,
                Some(AggregateMeta {
                    func: agg_fn,
                    arg_idx: arg_idx_opt,
                    distinct: is_distinct,
                }),
            ));
        } else {
            let idx = resolve_column_index(schema, &sel_expr, "SELECT list")?;
            if !group_key_indices.contains(&idx) {
                return Err(format!(
                    "Column '{}' must appear in GROUP BY or be used in an aggregate function",
                    sel_expr
                ));
            }
            let mut out_col = schema.columns[idx].clone();
            if let Some(alias) = sel_alias {
                out_col.name = alias;
            }
            output_columns.push(out_col);
            select_items.push((false, idx, None));
        }
    }
    if !has_agg && group_cols.is_empty() {
        return Err("Internal error: grouped select without aggregate/group by".to_string());
    }
    if has_agg && group_cols.is_empty() {
        // Global aggregate: use a single implicit group.
        return evaluate_aggregate_groups(
            schema,
            rows,
            &[],
            &select_items,
            Schema::new(output_columns),
        );
    }
    evaluate_aggregate_groups(
        schema,
        rows,
        &group_key_indices,
        &select_items,
        Schema::new(output_columns),
    )
}

fn evaluate_aggregate_groups(
    schema: &Schema,
    rows: &[Row],
    group_indices: &[usize],
    select_items: &[(bool, usize, Option<AggregateMeta>)],
    out_schema: Schema,
) -> Result<(Schema, Vec<Row>), String> {
    let mut grouped: std::collections::HashMap<String, Vec<Row>> = std::collections::HashMap::new();
    let mut ordered_keys: Vec<String> = Vec::new();

    if group_indices.is_empty() {
        let key = "__all__".to_string();
        grouped.insert(key.clone(), rows.to_vec());
        ordered_keys.push(key);
    } else {
        for r in rows {
            let key = group_indices
                .iter()
                .map(|i| value_to_string(&r[*i]))
                .collect::<Vec<_>>()
                .join("\u{1F}");
            if !grouped.contains_key(&key) {
                ordered_keys.push(key.clone());
            }
            grouped.entry(key).or_default().push(r.clone());
        }
    }

    let mut out_rows: Vec<Row> = Vec::new();
    for key in ordered_keys {
        let group_rows = grouped.get(&key).expect("group key exists");
        if group_rows.is_empty() {
            continue;
        }
        let first = &group_rows[0];
        let mut out: Row = Vec::new();
        for (is_agg, source_idx, agg_meta) in select_items {
            if !*is_agg {
                out.push(first[*source_idx].clone());
                continue;
            }
            let meta = agg_meta.expect("aggregate metadata");
            let v = evaluate_single_aggregate(schema, group_rows, meta)?;
            out.push(v);
        }
        out_rows.push(out);
    }

    Ok((out_schema, out_rows))
}

fn aggregate_output_type(func: AggregateFn, dtype: &DataType) -> Result<DataType, String> {
    match func {
        AggregateFn::Count => Ok(DataType::BigInt),
        AggregateFn::Sum => match dtype {
            DataType::Int => Ok(DataType::Int),
            DataType::BigInt => Ok(DataType::BigInt),
            DataType::Decimal { precision, scale } => Ok(DataType::Decimal {
                precision: *precision,
                scale: *scale,
            }),
            _ => Err("sum() is only valid for int|bigint|decimal".to_string()),
        },
        AggregateFn::Avg => match dtype {
            DataType::Int | DataType::BigInt => Ok(DataType::Decimal {
                precision: 38,
                scale: 6,
            }),
            DataType::Decimal { precision, scale } => Ok(DataType::Decimal {
                precision: *precision,
                scale: (*scale).max(6),
            }),
            _ => Err("avg() is only valid for int|bigint|decimal".to_string()),
        },
        AggregateFn::Min | AggregateFn::Max => Ok(dtype.clone()),
    }
}

fn evaluate_single_aggregate(
    schema: &Schema,
    rows: &[Row],
    meta: AggregateMeta,
) -> Result<Value, String> {
    let func = meta.func;
    let arg_idx = meta.arg_idx;
    let is_distinct = meta.distinct;
    match func {
        AggregateFn::Count => {
            let cnt = if let Some(idx) = arg_idx {
                aggregate_input_values(rows, idx, is_distinct).len() as i128
            } else {
                rows.len() as i128
            };
            Ok(Value::BigInt(cnt))
        }
        AggregateFn::Sum => {
            let idx = arg_idx.ok_or_else(|| "sum(*) is not supported".to_string())?;
            let vals = aggregate_input_values(rows, idx, is_distinct);
            match &schema.columns[idx].dtype {
                DataType::Int => {
                    let mut acc: i64 = 0;
                    for v in &vals {
                        if let Value::Int(v) = v {
                            acc = acc
                                .checked_add(*v)
                                .ok_or_else(|| "sum(int) overflow".to_string())?;
                        }
                    }
                    Ok(Value::Int(acc))
                }
                DataType::BigInt => {
                    let mut acc: i128 = 0;
                    for v in &vals {
                        if let Value::BigInt(v) = v {
                            acc = acc
                                .checked_add(*v)
                                .ok_or_else(|| "sum(bigint) overflow".to_string())?;
                        }
                    }
                    Ok(Value::BigInt(acc))
                }
                DataType::Decimal { .. } => {
                    let mut acc = Decimal::ZERO;
                    for v in &vals {
                        if let Value::Decimal(v) = v {
                            acc += *v;
                        }
                    }
                    Ok(Value::Decimal(acc))
                }
                _ => Err("sum() is only valid for int|bigint|decimal".to_string()),
            }
        }
        AggregateFn::Avg => {
            let idx = arg_idx.ok_or_else(|| "avg(*) is not supported".to_string())?;
            let vals = aggregate_input_values(rows, idx, is_distinct);
            let mut cnt: i128 = 0;
            let mut acc = Decimal::ZERO;
            match &schema.columns[idx].dtype {
                DataType::Int => {
                    for v in &vals {
                        if let Value::Int(v) = v {
                            acc += Decimal::from(*v);
                            cnt += 1;
                        }
                    }
                }
                DataType::BigInt => {
                    for v in &vals {
                        if let Value::BigInt(v) = v {
                            acc += Decimal::from_i128_with_scale(*v, 0);
                            cnt += 1;
                        }
                    }
                }
                DataType::Decimal { .. } => {
                    for v in &vals {
                        if let Value::Decimal(v) = v {
                            acc += *v;
                            cnt += 1;
                        }
                    }
                }
                _ => return Err("avg() is only valid for int|bigint|decimal".to_string()),
            }
            if cnt == 0 {
                return Ok(Value::Null);
            }
            Ok(Value::Decimal(acc / Decimal::from_i128_with_scale(cnt, 0)))
        }
        AggregateFn::Min | AggregateFn::Max => {
            let idx = arg_idx.ok_or_else(|| "min/max(*) is not supported".to_string())?;
            let dtype = &schema.columns[idx].dtype;
            let mut best: Option<Value> = None;
            for v in aggregate_input_values(rows, idx, is_distinct) {
                match &best {
                    None => best = Some(v.clone()),
                    Some(cur) => {
                        let ord = compare_values_for_minmax(cur, &v, dtype)?;
                        if (func == AggregateFn::Min && ord == Ordering::Greater)
                            || (func == AggregateFn::Max && ord == Ordering::Less)
                        {
                            best = Some(v.clone());
                        }
                    }
                }
            }
            Ok(best.unwrap_or(Value::Null))
        }
    }
}

fn aggregate_input_values(rows: &[Row], idx: usize, distinct: bool) -> Vec<Value> {
    if !distinct {
        let mut out: Vec<Value> = Vec::new();
        for r in rows {
            if let Some(v) = r.get(idx) {
                if !matches!(v, Value::Null) {
                    out.push(v.clone());
                }
            }
        }
        return out;
    }
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut out: Vec<Value> = Vec::new();
    for r in rows {
        let Some(v) = r.get(idx) else { continue };
        if matches!(v, Value::Null) {
            continue;
        }
        let key = value_to_string(v);
        if seen.insert(key) {
            out.push(v.clone());
        }
    }
    out
}

fn compare_values_for_minmax(lhs: &Value, rhs: &Value, dtype: &DataType) -> Result<Ordering, String> {
    match dtype {
        DataType::Bool => match (lhs, rhs) {
            (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
            _ => Err("Type mismatch while evaluating min/max(bool)".to_string()),
        },
        DataType::VarChar(_) => match (lhs, rhs) {
            (Value::VarChar(a), Value::VarChar(b)) => Ok(a.cmp(b)),
            _ => Err("Type mismatch while evaluating min/max(varchar)".to_string()),
        },
        DataType::Text => match (lhs, rhs) {
            (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b)),
            _ => Err("Type mismatch while evaluating min/max(text)".to_string()),
        },
        DataType::Uuid => match (lhs, rhs) {
            (Value::Uuid(a), Value::Uuid(b)) => Ok(a.cmp(b)),
            _ => Err("Type mismatch while evaluating min/max(uuid)".to_string()),
        },
        DataType::Blob => match (lhs, rhs) {
            (Value::Blob(a), Value::Blob(b)) => Ok(a.cmp(b)),
            _ => Err("Type mismatch while evaluating min/max(blob)".to_string()),
        },
        DataType::Json => match (lhs, rhs) {
            (Value::Json(a), Value::Json(b)) => Ok(a.to_string().cmp(&b.to_string())),
            _ => Err("Type mismatch while evaluating min/max(json)".to_string()),
        },
        _ => compare_order(lhs, rhs, dtype),
    }
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

    // Join planning: build a hash index on the right side join key.
    // This preserves left-table output order while avoiding O(n*m) scans.
    let mut right_key_to_rows: std::collections::HashMap<String, Vec<Row>> = std::collections::HashMap::new();
    for rr in right_rows {
        let Some(k) = rr.get(ridx) else { continue };
        if matches!(k, Value::Null) {
            continue;
        }
        right_key_to_rows
            .entry(value_to_string(k))
            .or_default()
            .push(rr.clone());
    }

    let mut out_rows: Vec<Row> = Vec::new();
    for lr in left_rows {
        let Some(left_key) = lr.get(lidx) else { continue };
        let matching = if matches!(left_key, Value::Null) {
            None
        } else {
            right_key_to_rows.get(&value_to_string(left_key))
        };
        if let Some(matching_right_rows) = matching {
            for rr in matching_right_rows {
                let mut row = lr.clone();
                row.extend(rr.clone());
                out_rows.push(row);
            }
        } else if join.join_type == JoinType::Left {
            let mut row = lr.clone();
            row.extend(std::iter::repeat(Value::Null).take(right_schema.columns.len()));
            out_rows.push(row);
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

    validate_where_columns(schema, &filter)?;
    let targeted_row_indices = if simple_eq_filter(&filter).is_some()
        && schema.primary_key.len() == 1
        && schema.primary_key.first().is_some_and(|pk| pk == &simple_eq_filter(&filter).expect("eq").0)
    {
        let (_, val) = simple_eq_filter(&filter).expect("eq");
        storage
            .lookup_pk_row_index(&table, schema, &val)?
            .map(|i| vec![i])
    } else if simple_eq_filter(&filter).is_some() {
        let (col, val) = simple_eq_filter(&filter).expect("eq");
        if let Some(i) = storage.lookup_unique_row_index(&table, schema, &col, &val)? {
            Some(vec![i])
        } else {
            storage.lookup_secondary_row_indices(&table, schema, &col, &val)?
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
                if eval_where_row(row, schema, &filter)? {
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
                if eval_where_row(row, schema, &filter)? {
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
    validate_where_columns(schema, &filter)?;
    let targeted_row_indices = if simple_eq_filter(&filter).is_some()
        && schema.primary_key.len() == 1
        && schema.primary_key.first().is_some_and(|pk| pk == &simple_eq_filter(&filter).expect("eq").0)
    {
        let (_, val) = simple_eq_filter(&filter).expect("eq");
        storage
            .lookup_pk_row_index(&table, schema, &val)?
            .map(|i| vec![i])
    } else if simple_eq_filter(&filter).is_some() {
        let (col, val) = simple_eq_filter(&filter).expect("eq");
        if let Some(i) = storage.lookup_unique_row_index(&table, schema, &col, &val)? {
            Some(vec![i])
        } else {
            storage.lookup_secondary_row_indices(&table, schema, &col, &val)?
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
                let should_delete = eval_where_row(row, schema, &filter)?;
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
                let should_delete = eval_where_row(row, schema, &filter)?;
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
        let (expr, alias) = split_select_alias(name);
        let idx = resolve_column_index(schema, &expr, "SELECT list")?;
        let mut out_col = schema.columns[idx].clone();
        if let Some(a) = alias {
            out_col.name = a;
        }
        selected.push((idx, out_col));
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

fn split_select_alias(token: &str) -> (String, Option<String>) {
    let lower = token.to_lowercase();
    if let Some(pos) = lower.rfind(" as ") {
        let expr = token[..pos].trim();
        let alias = token[pos + 4..].trim();
        if !expr.is_empty() && !alias.is_empty() {
            return (expr.to_string(), Some(alias.to_string()));
        }
    }
    (token.trim().to_string(), None)
}

fn filter_rows(
    schema: &crate::storage::Schema,
    rows: &[Row],
    where_clause: &WhereClause,
) -> Result<Vec<Row>, String> {
    validate_where_columns(schema, where_clause)?;
    let mut filtered: Vec<Row> = Vec::new();

    for row in rows {
        if eval_where_row(row, schema, where_clause)? {
            filtered.push(row.clone());
        }
    }

    Ok(filtered)
}

fn validate_where_columns(schema: &Schema, clause: &WhereClause) -> Result<(), String> {
    match clause {
        WhereClause::Predicate(p) => {
            let _ = resolve_column_index(schema, &p.column, "WHERE")?;
            Ok(())
        }
        WhereClause::Binary { left, right, .. } => {
            validate_where_columns(schema, left)?;
            validate_where_columns(schema, right)
        }
    }
}

fn eval_where_row(row: &Row, schema: &Schema, clause: &WhereClause) -> Result<bool, String> {
    match clause {
        WhereClause::Predicate(p) => {
            let col_idx = resolve_column_index(schema, &p.column, "WHERE")?;
            let col_dtype = &schema.columns[col_idx].dtype;
            row_matches(row, col_idx, &p.column, col_dtype, &p.op, &p.value)
        }
        WhereClause::Binary { left, op, right } => {
            let lhs = eval_where_row(row, schema, left)?;
            let rhs = eval_where_row(row, schema, right)?;
            Ok(match op {
                LogicalOp::And => lhs && rhs,
                LogicalOp::Or => lhs || rhs,
            })
        }
    }
}

fn simple_eq_filter(clause: &WhereClause) -> Option<(String, String)> {
    match clause {
        WhereClause::Predicate(p) if p.op == CompareOp::Eq => Some((p.column.clone(), p.value.clone())),
        _ => None,
    }
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
        CompareOp::IsNull => Ok(matches!(cell, Value::Null)),
        CompareOp::IsNotNull => Ok(!matches!(cell, Value::Null)),
        CompareOp::In => {
            let items: Vec<&str> = rhs_token
                .split('\u{1F}')
                .filter(|s| !s.is_empty())
                .collect();
            if items.is_empty() {
                return Err("IN list cannot be empty".to_string());
            }
            for tok in items {
                let rhs = parse_value(dtype, tok)?;
                if cell == &rhs {
                    return Ok(true);
                }
            }
            Ok(false)
        }
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
        let child_idxs = resolve_cols_to_idxs(schema, &fk.columns)?;
        let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;
        if child_idxs
            .iter()
            .any(|i| matches!(row.get(*i), Some(Value::Null)))
        {
            continue;
        }
        let found = fk_parent_exists(catalog, storage, &fk.ref_table, parent_schema, row, &child_idxs, &parent_idxs)?;
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
        let child_idxs = resolve_cols_to_idxs(child_schema, &fk.columns)?;
        let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;
        if fk_child_references_parent(
            storage,
            &child_table,
            child_schema,
            parent_row,
            &child_idxs,
            &parent_idxs,
        )? {
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
            let child_idxs = resolve_cols_to_idxs(child_schema, &fk.columns)?;
            let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;
            let was_referenced = fk_child_references_parent(
                storage,
                &child_table,
                child_schema,
                old_r,
                &child_idxs,
                &parent_idxs,
            )?;
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

fn fk_parent_exists(
    _catalog: &Catalog,
    storage: &dyn StorageEngine,
    parent_table: &str,
    parent_schema: &Schema,
    child_row: &Row,
    child_idxs: &[usize],
    parent_idxs: &[usize],
) -> Result<bool, String> {
    if child_idxs.len() == 1 && parent_idxs.len() == 1 {
        let child_idx = child_idxs[0];
        let parent_idx = parent_idxs[0];
        if let Some(v) = child_row.get(child_idx) {
            let tok = value_to_string(v);
            let parent_col = &parent_schema.columns[parent_idx].name;
            if parent_schema.primary_key.len() == 1
                && parent_schema.primary_key.first().is_some_and(|c| c == parent_col)
                && storage
                    .lookup_pk_row_index(parent_table, parent_schema, &tok)?
                    .is_some()
            {
                return Ok(true);
            }
            if storage
                .lookup_unique_row_index(parent_table, parent_schema, parent_col, &tok)?
                .is_some()
            {
                return Ok(true);
            }
        }
    }

    let parent_rows = storage.scan(parent_table)?;
    Ok(parent_rows
        .iter()
        .any(|pr| tuple_eq(child_row, child_idxs, pr, parent_idxs)))
}

fn fk_child_references_parent(
    storage: &dyn StorageEngine,
    child_table: &str,
    child_schema: &Schema,
    parent_row: &Row,
    child_idxs: &[usize],
    parent_idxs: &[usize],
) -> Result<bool, String> {
    if child_idxs.len() == 1 && parent_idxs.len() == 1 {
        let child_idx = child_idxs[0];
        let parent_idx = parent_idxs[0];
        if let Some(v) = parent_row.get(parent_idx) {
            let tok = value_to_string(v);
            let child_col = &child_schema.columns[child_idx].name;

            if child_schema.primary_key.len() == 1
                && child_schema.primary_key.first().is_some_and(|c| c == child_col)
                && storage
                    .lookup_pk_row_index(child_table, child_schema, &tok)?
                    .is_some()
            {
                return Ok(true);
            }
            if storage
                .lookup_unique_row_index(child_table, child_schema, child_col, &tok)?
                .is_some()
            {
                return Ok(true);
            }
            if storage
                .lookup_secondary_row_indices(child_table, child_schema, child_col, &tok)?
                .is_some_and(|hits| !hits.is_empty())
            {
                return Ok(true);
            }
        }
    }

    let child_rows = storage.scan(child_table)?;
    Ok(child_rows
        .iter()
        .any(|cr| tuple_eq(cr, child_idxs, parent_row, parent_idxs)))
}
