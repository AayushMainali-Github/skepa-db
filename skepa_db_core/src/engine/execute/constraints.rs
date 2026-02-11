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

