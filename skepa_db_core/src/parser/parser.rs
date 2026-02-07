use crate::parser::command::{Assignment, Command, CompareOp, WhereClause};
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
        "update" => parse_update(&tokens),
        "delete" => parse_delete(&tokens),
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

            ',' | '(' | ')' if !in_quotes => {
                if just_closed_quote {
                    tokens.push(std::mem::take(&mut current));
                    just_closed_quote = false;
                } else if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                tokens.push(ch.to_string());
            }

            '>' | '<' | '=' | '!' if !in_quotes => {
                if just_closed_quote {
                    tokens.push(std::mem::take(&mut current));
                    just_closed_quote = false;
                } else if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                if let Some('=') = it.peek().copied() {
                    let mut op = String::new();
                    op.push(ch);
                    op.push('=');
                    it.next();
                    tokens.push(op);
                } else {
                    tokens.push(ch.to_string());
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
    // create table <table> ( <col> <type> [, <col> <type> ...] )
    if tokens.len() < 7 {
        return Err("Usage: create table <table> (<col> <type>, ...)".to_string());
    }
    if !tokens[1].eq_ignore_ascii_case("table") {
        return Err("Usage: create table <table> (<col> <type>, ...)".to_string());
    }
    if tokens[3] != "(" || tokens[tokens.len() - 1] != ")" {
        return Err("CREATE requires parenthesized column definitions".to_string());
    }
    let table = tokens[2].clone();

    let mut cols: Vec<(String, DataType)> = Vec::new();
    let mut i = 4usize;
    let end = tokens.len() - 1;

    while i < end {
        if i + 1 >= end {
            return Err("Bad CREATE column list. Use: (id int, name text)".to_string());
        }
        let name = tokens[i].clone();
        let dtype = parse_datatype(&tokens[i + 1])?;
        cols.push((name, dtype));
        i += 2;
        if i < end {
            if tokens[i] != "," {
                return Err("Bad CREATE column list. Columns must be comma-separated.".to_string());
            }
            i += 1;
        }
    }

    if cols.is_empty() {
        return Err("CREATE requires at least one column".to_string());
    }

    Ok(Command::Create {
        table,
        columns: cols,
    })
}

fn parse_insert(tokens: &[String]) -> Result<Command, String> {
    // insert into <table> values (<v1>, <v2>, ...)
    if tokens.len() < 7 {
        return Err("Usage: insert into <table> values (<v1>, <v2>, ...)".to_string());
    }
    if !tokens[1].eq_ignore_ascii_case("into")
        || !tokens[3].eq_ignore_ascii_case("values")
        || tokens[4] != "("
        || tokens[tokens.len() - 1] != ")"
    {
        return Err("Usage: insert into <table> values (<v1>, <v2>, ...)".to_string());
    }
    let table = tokens[2].clone();
    let mut values: Vec<String> = Vec::new();
    let mut i = 5usize;
    let end = tokens.len() - 1;

    while i < end {
        values.push(tokens[i].clone());
        i += 1;
        if i < end {
            if tokens[i] != "," {
                return Err("Bad INSERT values. Values must be comma-separated.".to_string());
            }
            i += 1;
        }
    }

    if values.is_empty() {
        return Err("INSERT requires at least one value".to_string());
    }

    Ok(Command::Insert { table, values })
}

fn parse_update(tokens: &[String]) -> Result<Command, String> {
    // update <table> set <col> = <val> [, <col> = <val> ...] where <col> <op> <val>
    if tokens.len() < 10 {
        return Err(
            "Usage: update <table> set <col> = <value> [, <col> = <value> ...] where <column> <op> <value>"
                .to_string(),
        );
    }

    let table = tokens[1].clone();
    if !tokens[2].eq_ignore_ascii_case("set") {
        return Err(
            "Usage: update <table> set <col> = <value> [, <col> = <value> ...] where <column> <op> <value>"
                .to_string(),
        );
    }

    let where_idx = tokens
        .iter()
        .position(|t| t.eq_ignore_ascii_case("where"))
        .ok_or_else(|| {
            "Usage: update <table> set <col> = <value> [, <col> = <value> ...] where <column> <op> <value>"
                .to_string()
        })?;

    if where_idx <= 3 {
        return Err("UPDATE requires at least one assignment after SET".to_string());
    }

    let set_tokens = &tokens[3..where_idx];
    let mut assignments: Vec<Assignment> = Vec::new();
    let mut i = 0usize;
    while i < set_tokens.len() {
        if i + 2 >= set_tokens.len() {
            return Err("Bad UPDATE assignments. Use: col = value, col = value".to_string());
        }
        if set_tokens[i + 1] != "=" {
            return Err("Bad UPDATE assignments. Use: col = value, col = value".to_string());
        }
        assignments.push(Assignment {
            column: set_tokens[i].clone(),
            value: set_tokens[i + 2].clone(),
        });
        i += 3;
        if i < set_tokens.len() {
            if set_tokens[i] != "," {
                return Err("Bad UPDATE assignments. Use comma between assignments.".to_string());
            }
            i += 1;
        }
    }

    let where_tokens = &tokens[where_idx + 1..];
    if where_tokens.len() != 3 {
        return Err("Bad UPDATE WHERE clause. Use: where <column> <op> <value>".to_string());
    }

    let op = parse_compare_op(&where_tokens[1])?;
    let filter = WhereClause {
        column: where_tokens[0].clone(),
        op,
        value: where_tokens[2].clone(),
    };

    Ok(Command::Update {
        table,
        assignments,
        filter,
    })
}

fn parse_delete(tokens: &[String]) -> Result<Command, String> {
    // delete from <table> where <column> <op> <value>
    if tokens.len() != 7
        || !tokens[1].eq_ignore_ascii_case("from")
        || !tokens[3].eq_ignore_ascii_case("where")
    {
        return Err("Usage: delete from <table> where <column> <op> <value>".to_string());
    }

    let op = parse_compare_op(&tokens[5])?;
    Ok(Command::Delete {
        table: tokens[2].clone(),
        filter: WhereClause {
            column: tokens[4].clone(),
            op,
            value: tokens[6].clone(),
        },
    })
}

fn parse_select(tokens: &[String]) -> Result<Command, String> {
    parse_select_projection(tokens)
}

fn parse_select_projection(tokens: &[String]) -> Result<Command, String> {
    let from_idx = tokens
        .iter()
        .position(|t| t.eq_ignore_ascii_case("from"))
        .ok_or_else(|| {
            "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>]".to_string()
        })?;

    if tokens.len() < 4 || from_idx < 2 {
        return Err(
            "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>]".to_string(),
        );
    }

    let columns = parse_select_columns(&tokens[1..from_idx])?;
    if from_idx + 1 >= tokens.len() {
        return Err("SELECT missing table name after FROM".to_string());
    }
    let table = tokens[from_idx + 1].clone();

    if from_idx + 2 == tokens.len() {
        return Ok(Command::Select {
            table,
            columns: Some(columns),
            filter: None,
        });
    }

    if from_idx + 5 >= tokens.len() || !tokens[from_idx + 2].eq_ignore_ascii_case("where") {
        return Err(
            "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>]".to_string(),
        );
    }
    if from_idx + 6 != tokens.len() {
        return Err(
            "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>]".to_string(),
        );
    }

    let op = parse_compare_op(&tokens[from_idx + 4])?;
    Ok(Command::Select {
        table,
        columns: Some(columns),
        filter: Some(WhereClause {
            column: tokens[from_idx + 3].clone(),
            op,
            value: tokens[from_idx + 5].clone(),
        }),
    })
}

fn parse_select_columns(tokens: &[String]) -> Result<Vec<String>, String> {
    if tokens.len() == 1 && tokens[0] == "*" {
        return Ok(Vec::new());
    }

    let mut columns: Vec<String> = Vec::new();
    let mut expect_col = true;
    for tok in tokens {
        if expect_col {
            if tok == "," {
                return Err(
                    "SELECT column list cannot be empty. Use '*' or comma-separated column names."
                        .to_string(),
                );
            }
            columns.push(tok.clone());
            expect_col = false;
        } else if tok != "," {
            return Err("Bad SELECT column list. Use comma-separated column names.".to_string());
        } else {
            expect_col = true;
        }
    }

    if columns.is_empty() || expect_col {
        return Err("SELECT column list cannot be empty. Use '*' or comma-separated column names.".to_string());
    }

    Ok(columns)
}

fn parse_compare_op(raw: &str) -> Result<CompareOp, String> {
    match raw.to_lowercase().as_str() {
        "=" | "eq" => Ok(CompareOp::Eq),
        "!=" => Err("Operator '!=' is not supported yet. Use '=' for now.".to_string()),
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
