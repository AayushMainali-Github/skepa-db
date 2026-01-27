use crate::parser::command::Command;
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
                    // Something like:  "a""b"  (no whitespace between)
                    return Err(
                        "Unexpected quote after closing quote. Add whitespace between tokens."
                            .to_string(),
                    );
                }

                if !in_quotes {
                    // Opening quote must start a new token
                    if !current.is_empty() {
                        return Err("Quote (\") cannot start in the middle of a token. Add whitespace before the quote."
                            .to_string());
                    }
                    in_quotes = true;
                } else {
                    // Closing quote
                    in_quotes = false;
                    just_closed_quote = true; // now we require whitespace or end-of-input
                }
            }

            '\\' if in_quotes => {
                // Allow escapes inside quotes: \" and \\ (you can add more if you want)
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
                        // If you want to allow unknown escapes, you could just push '\\' here instead.
                        return Err("Invalid escape sequence in quotes. Use \\\" for a quote or \\\\ for a backslash."
                            .to_string());
                    }
                }
                just_closed_quote = false;
            }

            c if c.is_whitespace() && !in_quotes => {
                // If we just closed a quote, finalize the token EVEN IF it's empty ("")
                if just_closed_quote {
                    tokens.push(std::mem::take(&mut current)); // pushes "" too
                    just_closed_quote = false;
                    continue;
                }

                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }

            _ => {
                // If we just closed a quote, only whitespace or end-of-input is allowed next.
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

    // Final token push (also handles quoted tokens at end-of-input)
    if !current.is_empty() || just_closed_quote {
        // `just_closed_quote` allows:  ""   -> empty token
        tokens.push(current);
    }

    Ok(tokens)
}

fn parse_create(tokens: &[String]) -> Result<Command, String> {
    // create <table> <col>:<type> <col>:<type> ...
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
    // insert <table> <v1> <v2> ...
    if tokens.len() < 3 {
        return Err("Usage: insert <table> <v1> <v2> ...".to_string());
    }
    let table = tokens[1].clone();
    let values = tokens[2..].to_vec();

    Ok(Command::Insert { table, values })
}

fn parse_select(tokens: &[String]) -> Result<Command, String> {
    // select <table>
    if tokens.len() != 2 {
        return Err("Usage: select <table>".to_string());
    }
    Ok(Command::Select {
        table: tokens[1].clone(),
    })
}

fn parse_col_def(s: &str) -> Result<(String, DataType), String> {
    // "id:int" -> ("id", Int)
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
