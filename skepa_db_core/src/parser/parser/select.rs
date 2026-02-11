use crate::parser::command::{Command, JoinClause, JoinType, OrderBy, WhereClause};
use super::where_clause::parse_where_clause;

pub(super) fn parse_select(tokens: &[String]) -> Result<Command, String> {
    parse_select_projection(tokens)
}

fn parse_select_projection(tokens: &[String]) -> Result<Command, String> {
    let mut distinct = false;
    let projection_start = if tokens.len() > 1 && tokens[1].eq_ignore_ascii_case("distinct") {
        distinct = true;
        2
    } else {
        1
    };
    let from_idx = tokens
        .iter()
        .position(|t| t.eq_ignore_ascii_case("from"))
        .ok_or_else(|| {
            "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>]".to_string()
        })?;

    if tokens.len() < 4 || from_idx <= projection_start {
        return Err(
            "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>]".to_string(),
        );
    }

    let columns = parse_select_columns(&tokens[projection_start..from_idx])?;
    if from_idx + 1 >= tokens.len() {
        return Err("SELECT missing table name after FROM".to_string());
    }
    let table = tokens[from_idx + 1].clone();

    let mut i = from_idx + 2;
    let mut join: Option<JoinClause> = None;
    let mut filter: Option<WhereClause> = None;
    let mut group_by: Option<Vec<String>> = None;
    let mut having: Option<WhereClause> = None;
    let mut order_by: Option<OrderBy> = None;
    let mut limit: Option<usize> = None;
    let mut offset: Option<usize> = None;

    if i < tokens.len() && (tokens[i].eq_ignore_ascii_case("join") || tokens[i].eq_ignore_ascii_case("left")) {
        let (join_type, join_kw_idx) = if tokens[i].eq_ignore_ascii_case("left") {
            if i + 1 >= tokens.len() || !tokens[i + 1].eq_ignore_ascii_case("join") {
                return Err(
                    "Usage: select <col1,col2|*> from <table> [join|left join <table2> on <left_col> = <right_col>] [where <column> <op> <value>] [order by <column> [asc|desc]] [limit <n>] [offset <n>]".to_string(),
                );
            }
            (JoinType::Left, i + 1)
        } else {
            (JoinType::Inner, i)
        };
        if join_kw_idx + 5 >= tokens.len()
            || !tokens[join_kw_idx + 2].eq_ignore_ascii_case("on")
            || tokens[join_kw_idx + 4] != "="
        {
            return Err(
                "Usage: select <col1,col2|*> from <table> [join|left join <table2> on <left_col> = <right_col>] [where <column> <op> <value>] [order by <column> [asc|desc]] [limit <n>] [offset <n>]".to_string(),
            );
        }
        join = Some(JoinClause {
            join_type,
            table: tokens[join_kw_idx + 1].clone(),
            left_column: tokens[join_kw_idx + 3].clone(),
            right_column: tokens[join_kw_idx + 5].clone(),
        });
        i = join_kw_idx + 6;
    }

    if i < tokens.len() && tokens[i].eq_ignore_ascii_case("where") {
        if i + 2 >= tokens.len() {
            return Err(
                "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>] [order by <column> [asc|desc]] [limit <n>] [offset <n>]".to_string(),
            );
        }
        let where_end = find_where_end(tokens, i + 1)?;
        filter = Some(parse_where_clause(
            &tokens[i + 1..where_end],
            "Usage: select <col1,col2|*> from <table> [where <expr>] [order by <column> [asc|desc]] [limit <n>]",
        )?);
        i = where_end;
    }

    if i < tokens.len() && tokens[i].eq_ignore_ascii_case("group") {
        if i + 2 >= tokens.len() || !tokens[i + 1].eq_ignore_ascii_case("by") {
            return Err(
                "Usage: select <col1,col2|*> from <table> [join|left join <table2> on <left_col> = <right_col>] [where <expr>] [group by <col1,col2>] [having <expr>] [order by <column> [asc|desc]] [limit <n>]".to_string(),
            );
        }
        let (grp, next_i) = parse_group_by_columns(tokens, i + 2)?;
        group_by = Some(grp);
        i = next_i;
    }

    if i < tokens.len() && tokens[i].eq_ignore_ascii_case("having") {
        if i + 2 >= tokens.len() {
            return Err(
                "Usage: select <col1,col2|*> from <table> [join|left join <table2> on <left_col> = <right_col>] [where <expr>] [group by <col1,col2>] [having <expr>] [order by <column> [asc|desc]] [limit <n>]".to_string(),
            );
        }
        let having_end = find_having_end(tokens, i + 1)?;
        let having_tokens = normalize_function_tokens(&tokens[i + 1..having_end])?;
        having = Some(parse_where_clause(
            &having_tokens,
            "Usage: select <col1,col2|*> from <table> [where <expr>] [group by <col1,col2>] [having <expr>] [order by <column> [asc|desc]] [limit <n>]",
        )?);
        i = having_end;
    }

    if i < tokens.len() && tokens[i].eq_ignore_ascii_case("order") {
        if i + 2 >= tokens.len() || !tokens[i + 1].eq_ignore_ascii_case("by") {
            return Err(
                "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>] [order by <column> [asc|desc]] [limit <n>] [offset <n>]".to_string(),
            );
        }
        let (ob, next_i) = parse_order_by_list(tokens, i + 2)?;
        order_by = Some(ob);
        i = next_i;
    }

    while i < tokens.len() {
        if tokens[i].eq_ignore_ascii_case("limit") {
            if limit.is_some() {
                return Err("LIMIT specified more than once".to_string());
            }
            if i + 1 >= tokens.len() {
                return Err(
                    "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>] [order by <column> [asc|desc]] [limit <n>] [offset <n>]".to_string(),
                );
            }
            let n = tokens[i + 1]
                .parse::<usize>()
                .map_err(|_| "LIMIT must be a non-negative integer".to_string())?;
            limit = Some(n);
            i += 2;
            continue;
        }
        if tokens[i].eq_ignore_ascii_case("offset") {
            if offset.is_some() {
                return Err("OFFSET specified more than once".to_string());
            }
            if i + 1 >= tokens.len() {
                return Err(
                    "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>] [order by <column> [asc|desc]] [limit <n>] [offset <n>]".to_string(),
                );
            }
            let n = tokens[i + 1]
                .parse::<usize>()
                .map_err(|_| "OFFSET must be a non-negative integer".to_string())?;
            offset = Some(n);
            i += 2;
            continue;
        }
        break;
    }

    if i != tokens.len() {
        return Err(
            "Usage: select <col1,col2|*> from <table> [join|left join <table2> on <left_col> = <right_col>] [where <expr>] [group by <col1,col2>] [having <expr>] [order by <column> [asc|desc]] [limit <n>] [offset <n>]".to_string(),
        );
    }

    Ok(Command::Select {
        table,
        distinct,
        join,
        columns: Some(columns),
        filter,
        group_by,
        having,
        order_by,
        limit,
        offset,
    })
}

fn parse_order_by_list(tokens: &[String], mut i: usize) -> Result<(OrderBy, usize), String> {
    let mut items: Vec<(String, bool)> = Vec::new();
    loop {
        if i >= tokens.len() {
            return Err("ORDER BY requires at least one column".to_string());
        }
        let col = if i + 1 < tokens.len() && tokens[i + 1] == "(" {
            let mut depth = 0usize;
            let mut j = i + 1;
            while j < tokens.len() {
                if tokens[j] == "(" {
                    depth += 1;
                } else if tokens[j] == ")" {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        break;
                    }
                }
                j += 1;
            }
            if j >= tokens.len() || tokens[j] != ")" {
                return Err("Bad ORDER BY function syntax".to_string());
            }
            let args = tokens[i + 2..j].join(" ");
            if args.trim().is_empty() {
                return Err("Bad ORDER BY function syntax".to_string());
            }
            let c = format!("{}({})", tokens[i], args);
            i = j + 1;
            c
        } else {
            let c = tokens[i].clone();
            i += 1;
            c
        };
        let mut asc = true;
        if i < tokens.len() {
            if tokens[i].eq_ignore_ascii_case("asc") {
                asc = true;
                i += 1;
            } else if tokens[i].eq_ignore_ascii_case("desc") {
                asc = false;
                i += 1;
            }
        }
        items.push((col, asc));
        if i < tokens.len() && tokens[i] == "," {
            i += 1;
            continue;
        }
        break;
    }
    let (first_col, first_asc) = items
        .first()
        .cloned()
        .ok_or_else(|| "ORDER BY requires at least one column".to_string())?;
    let then_by = if items.len() > 1 {
        items[1..].to_vec()
    } else {
        Vec::new()
    };
    Ok((
        OrderBy {
            column: first_col,
            asc: first_asc,
            then_by,
        },
        i,
    ))
}

fn parse_select_columns(tokens: &[String]) -> Result<Vec<String>, String> {
    if tokens.len() == 1 && tokens[0] == "*" {
        return Ok(Vec::new());
    }

    let mut columns: Vec<String> = Vec::new();
    let mut i = 0usize;
    while i < tokens.len() {
        if tokens[i] == "," {
            return Err(
                "SELECT column list cannot be empty. Use '*' or comma-separated column names."
                    .to_string(),
            );
        }
        let mut expr = if i + 1 < tokens.len() && tokens[i + 1] == "(" {
            let mut depth = 0usize;
            let mut j = i + 1;
            while j < tokens.len() {
                if tokens[j] == "(" {
                    depth += 1;
                } else if tokens[j] == ")" {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        break;
                    }
                }
                j += 1;
            }
            if j >= tokens.len() || tokens[j] != ")" {
                return Err("Bad SELECT function syntax. Use fn(col), fn(distinct col), or fn(*)".to_string());
            }
            let arg_tokens = &tokens[i + 2..j];
            if arg_tokens.is_empty() {
                return Err("Bad SELECT function syntax. Use fn(col), fn(distinct col), or fn(*)".to_string());
            }
            if arg_tokens[0].eq_ignore_ascii_case("distinct") && arg_tokens.len() < 2 {
                return Err("Bad SELECT function syntax. DISTINCT requires a column".to_string());
            }
            let e = format!("{}({})", tokens[i], arg_tokens.join(" "));
            i = j + 1;
            e
        } else {
            let e = tokens[i].clone();
            i += 1;
            e
        };
        if i < tokens.len() && tokens[i].eq_ignore_ascii_case("as") {
            if i + 1 >= tokens.len() || tokens[i + 1] == "," {
                return Err("Bad SELECT alias syntax. Use: <expr> as <alias>".to_string());
            }
            expr = format!("{expr} as {}", tokens[i + 1]);
            i += 2;
        }
        columns.push(expr);
        if i < tokens.len() {
            if tokens[i] != "," {
                return Err("Bad SELECT column list. Use comma-separated column names.".to_string());
            }
            i += 1;
            if i >= tokens.len() {
                return Err("SELECT column list cannot end with comma".to_string());
            }
        }
    }
    if columns.is_empty() {
        return Err("SELECT column list cannot be empty. Use '*' or comma-separated column names.".to_string());
    }

    Ok(columns)
}

fn parse_group_by_columns(tokens: &[String], mut i: usize) -> Result<(Vec<String>, usize), String> {
    let mut cols: Vec<String> = Vec::new();
    loop {
        if i >= tokens.len() {
            return Err("GROUP BY requires at least one column".to_string());
        }
        if tokens[i] == "," || tokens[i] == "(" || tokens[i] == ")" {
            return Err("Bad GROUP BY column list".to_string());
        }
        cols.push(tokens[i].clone());
        i += 1;
        if i < tokens.len() && tokens[i] == "," {
            i += 1;
            continue;
        }
        break;
    }
    Ok((cols, i))
}


pub(super) fn find_where_end(tokens: &[String], start: usize) -> Result<usize, String> {
    let mut i = start;
    while i < tokens.len() {
        if tokens[i].eq_ignore_ascii_case("group")
            || tokens[i].eq_ignore_ascii_case("having")
            || tokens[i].eq_ignore_ascii_case("order")
            || tokens[i].eq_ignore_ascii_case("limit")
            || tokens[i].eq_ignore_ascii_case("offset")
        {
            return Ok(i);
        }
        i += 1;
    }
    Ok(tokens.len())
}

fn find_having_end(tokens: &[String], start: usize) -> Result<usize, String> {
    let mut i = start;
    while i < tokens.len() {
        if tokens[i].eq_ignore_ascii_case("order")
            || tokens[i].eq_ignore_ascii_case("limit")
            || tokens[i].eq_ignore_ascii_case("offset")
        {
            return Ok(i);
        }
        i += 1;
    }
    Ok(tokens.len())
}

fn normalize_function_tokens(tokens: &[String]) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    let mut i = 0usize;
    while i < tokens.len() {
        if i + 1 < tokens.len() && tokens[i + 1] == "(" {
            let mut depth = 0usize;
            let mut j = i + 1;
            while j < tokens.len() {
                if tokens[j] == "(" {
                    depth += 1;
                } else if tokens[j] == ")" {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        break;
                    }
                }
                j += 1;
            }
            if j >= tokens.len() || tokens[j] != ")" {
                return Err("Bad function syntax in expression".to_string());
            }
            out.push(format!("{}({})", tokens[i], tokens[i + 2..j].join(" ")));
            i = j + 1;
        } else {
            out.push(tokens[i].clone());
            i += 1;
        }
    }
    Ok(out)
}

