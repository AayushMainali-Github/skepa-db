use crate::parser::command::{CompareOp, LogicalOp, Predicate, WhereClause};

pub(super) fn parse_compare_op(raw: &str) -> Result<CompareOp, String> {
    match raw.to_lowercase().as_str() {
        "=" | "eq" => Ok(CompareOp::Eq),
        "!=" => Err("Operator '!=' is not supported yet. Use '=' for now.".to_string()),
        ">" | "gt" => Ok(CompareOp::Gt),
        "<" | "lt" => Ok(CompareOp::Lt),
        ">=" | "gte" => Ok(CompareOp::Gte),
        "<=" | "lte" => Ok(CompareOp::Lte),
        "like" => Ok(CompareOp::Like),
        "in" => Ok(CompareOp::In),
        _ => Err(format!(
            "Unknown WHERE operator '{raw}'. Use =|eq|>|gt|<|lt|>=|gte|<=|lte|like|in"
        )),
    }
}

pub(super) fn parse_where_clause(
    tokens: &[String],
    usage_msg: &str,
) -> Result<WhereClause, String> {
    let mut idx = 0usize;
    let expr = parse_or_expr(tokens, &mut idx, usage_msg)?;
    if idx != tokens.len() {
        return Err(usage_msg.to_string());
    }
    Ok(expr)
}

fn parse_or_expr(
    tokens: &[String],
    idx: &mut usize,
    usage_msg: &str,
) -> Result<WhereClause, String> {
    let mut left = parse_and_expr(tokens, idx, usage_msg)?;
    while *idx < tokens.len() && tokens[*idx].eq_ignore_ascii_case("or") {
        *idx += 1;
        let right = parse_and_expr(tokens, idx, usage_msg)?;
        left = WhereClause::Binary {
            left: Box::new(left),
            op: LogicalOp::Or,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_and_expr(
    tokens: &[String],
    idx: &mut usize,
    usage_msg: &str,
) -> Result<WhereClause, String> {
    let mut left = parse_primary_expr(tokens, idx, usage_msg)?;
    while *idx < tokens.len() && tokens[*idx].eq_ignore_ascii_case("and") {
        *idx += 1;
        let right = parse_primary_expr(tokens, idx, usage_msg)?;
        left = WhereClause::Binary {
            left: Box::new(left),
            op: LogicalOp::And,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_primary_expr(
    tokens: &[String],
    idx: &mut usize,
    usage_msg: &str,
) -> Result<WhereClause, String> {
    if *idx >= tokens.len() {
        return Err(usage_msg.to_string());
    }
    if tokens[*idx] == "(" {
        *idx += 1;
        let expr = parse_or_expr(tokens, idx, usage_msg)?;
        if *idx >= tokens.len() || tokens[*idx] != ")" {
            return Err(usage_msg.to_string());
        }
        *idx += 1;
        return Ok(expr);
    }
    parse_predicate(tokens, idx, usage_msg)
}

fn parse_predicate(
    tokens: &[String],
    idx: &mut usize,
    usage_msg: &str,
) -> Result<WhereClause, String> {
    if *idx + 2 < tokens.len()
        && tokens[*idx + 1].eq_ignore_ascii_case("is")
        && tokens[*idx + 2].eq_ignore_ascii_case("null")
    {
        let p = Predicate {
            column: tokens[*idx].clone(),
            op: CompareOp::IsNull,
            value: String::new(),
        };
        *idx += 3;
        return Ok(WhereClause::Predicate(p));
    }
    if *idx + 3 < tokens.len()
        && tokens[*idx + 1].eq_ignore_ascii_case("is")
        && tokens[*idx + 2].eq_ignore_ascii_case("not")
        && tokens[*idx + 3].eq_ignore_ascii_case("null")
    {
        let p = Predicate {
            column: tokens[*idx].clone(),
            op: CompareOp::IsNotNull,
            value: String::new(),
        };
        *idx += 4;
        return Ok(WhereClause::Predicate(p));
    }
    if *idx + 4 < tokens.len() && tokens[*idx + 1].eq_ignore_ascii_case("in") {
        if tokens[*idx + 2] != "(" {
            return Err(usage_msg.to_string());
        }
        let mut vals: Vec<String> = Vec::new();
        let mut i = *idx + 3;
        while i < tokens.len() {
            if tokens[i] == ")" {
                if vals.is_empty() {
                    return Err(usage_msg.to_string());
                }
                let p = Predicate {
                    column: tokens[*idx].clone(),
                    op: CompareOp::In,
                    value: vals.join("\u{1F}"),
                };
                *idx = i + 1;
                return Ok(WhereClause::Predicate(p));
            }
            vals.push(tokens[i].clone());
            i += 1;
            if i < tokens.len() {
                if tokens[i] == ")" {
                    continue;
                }
                if tokens[i] != "," {
                    return Err(usage_msg.to_string());
                }
                i += 1;
                if i >= tokens.len() || tokens[i] == ")" {
                    return Err(usage_msg.to_string());
                }
            }
        }
        return Err(usage_msg.to_string());
    }
    if *idx + 2 < tokens.len() {
        let op = parse_compare_op(&tokens[*idx + 1])?;
        let p = Predicate {
            column: tokens[*idx].clone(),
            op,
            value: tokens[*idx + 2].clone(),
        };
        *idx += 3;
        return Ok(WhereClause::Predicate(p));
    }
    Err(usage_msg.to_string())
}
