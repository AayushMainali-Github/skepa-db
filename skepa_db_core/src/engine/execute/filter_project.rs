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

