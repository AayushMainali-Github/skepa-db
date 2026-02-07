use crate::parser::command::{Command, CompareOp, WhereClause};
use crate::types::datatype::{DataType, parse_datatype};

pub fn parse(input: &str) -> Result<Command, String> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Err("Empty command".to_string());
    }

    let keyword = tokens[0].to_lowercase();

    match keyword.as_str() {
        "create" => parse_create(&tokens),
        "insert" => parse_insert(&tokens),
        "select" => parse_select(&tokens),
        _ => Err(format!("Unknown command '{}'", tokens[0])),
    }
}

fn tokenize(input: &str) -> Result<Vec<String>, String> {
    let mut tokens: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut just_closed_quote = false;

    let mut it = input.chars().peekable();

    while let Some(ch) = it.next() {
        match ch {
            '"' => {
                if just_closed_quote {
                    return Err(
                        "Unexpected quote after closing quote. Add whitespace between tokens."
                            .to_string(),
                    );
                }

                if !in_quotes {
                    if !current.is_empty() {
                        return Err("Quote (\") cannot start in the middle of a token. Add whitespace before the quote."
                            .to_string());
                    }
                    in_quotes = true;
                } else {
                    in_quotes = false;
                    just_closed_quote = true;
                }
            }

            '\\' if in_quotes => {
                match it.peek().copied() {
                    Some('"') => {
                        it.next();
                        current.push('"');
                    }
                    Some('\\') => {
                        it.next();
                        current.push('\\');
                    }
                    _ => {
                        return Err("Invalid escape sequence in quotes. Use \\\" for a quote or \\\\ for a backslash."
                            .to_string());
                    }
                }
                just_closed_quote = false;
            }

            c if c.is_whitespace() && !in_quotes => {
                if just_closed_quote {
                    tokens.push(std::mem::take(&mut current));
                    just_closed_quote = false;
                    continue;
                }

                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }

            _ => {
                if just_closed_quote {
                    return Err("Characters found immediately after a closing quote. Add whitespace after the quoted string."
                        .to_string());
                }
                current.push(ch);
            }
        }
    }

    if in_quotes {
        return Err("Unclosed quote (\") in input".to_string());
    }

    if !current.is_empty() || just_closed_quote {
        tokens.push(current);
    }

    Ok(tokens)
}

fn parse_create(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err("Usage: create <table> <col>:<type> ...".to_string());
    }
    let table = tokens[1].clone();

    let mut cols: Vec<(String, DataType)> = Vec::new();
    for part in &tokens[2..] {
        let (name, dtype) = parse_col_def(part)?;
        cols.push((name, dtype));
    }

    Ok(Command::Create {
        table,
        columns: cols,
    })
}

fn parse_insert(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err("Usage: insert <table> <v1> <v2> ...".to_string());
    }
    let table = tokens[1].clone();
    let values = tokens[2..].to_vec();

    Ok(Command::Insert { table, values })
}

fn parse_select(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() == 2 {
        return Ok(Command::Select {
            table: tokens[1].clone(),
            filter: None,
        });
    }

    if tokens.len() != 6 {
        return Err(
            "Usage: select <table> [where <column> <op> <value>]".to_string(),
        );
    }

    if !tokens[2].eq_ignore_ascii_case("where") {
        return Err(
            "Usage: select <table> [where <column> <op> <value>]".to_string(),
        );
    }

    let op = parse_compare_op(&tokens[4])?;

    Ok(Command::Select {
        table: tokens[1].clone(),
        filter: Some(WhereClause {
            column: tokens[3].clone(),
            op,
            value: tokens[5].clone(),
        }),
    })
}

fn parse_compare_op(raw: &str) -> Result<CompareOp, String> {
    match raw.to_lowercase().as_str() {
        "=" | "eq" => Ok(CompareOp::Eq),
        ">" | "gt" => Ok(CompareOp::Gt),
        "<" | "lt" => Ok(CompareOp::Lt),
        ">=" | "gte" => Ok(CompareOp::Gte),
        "<=" | "lte" => Ok(CompareOp::Lte),
        "like" => Ok(CompareOp::Like),
        _ => Err(format!(
            "Unknown WHERE operator '{raw}'. Use =|eq|>|gt|<|lt|>=|gte|<=|lte|like"
        )),
    }
}

fn parse_col_def(s: &str) -> Result<(String, DataType), String> {
    let mut parts = s.splitn(2, ':');
    let name = parts.next().unwrap_or("").trim();
    let dtype = parts.next().unwrap_or("").trim();

    if name.is_empty() || dtype.is_empty() {
        return Err(format!(
            "Bad column definition '{s}'. Use name:type like id:int"
        ));
    }

    Ok((name.to_string(), parse_datatype(dtype)?))
}
