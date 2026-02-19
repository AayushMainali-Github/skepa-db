#[allow(clippy::too_many_arguments)]
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
                if let Some(a) = alias
                    && let Ok(idx) = resolve_column_index(&select_schema, &expr, "SELECT list")
                {
                    alias_to_idx.insert(a, idx);
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
            // Global aggregate over empty input still produces one row
            // (e.g. count(*) = 0, sum/avg/min/max = null).
            if select_items.iter().any(|(is_agg, _, _)| !*is_agg) {
                continue;
            }
            let mut out: Row = Vec::new();
            for (_is_agg, _source_idx, agg_meta) in select_items {
                let meta = agg_meta.expect("aggregate metadata");
                let v = evaluate_single_aggregate(schema, group_rows, meta)?;
                out.push(v);
            }
            out_rows.push(out);
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
            if vals.is_empty() {
                return Ok(Value::Null);
            }
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
            if let Some(v) = r.get(idx)
                && !matches!(v, Value::Null)
            {
                out.push(v.clone());
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
            row.extend(std::iter::repeat_n(Value::Null, right_schema.columns.len()));
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

