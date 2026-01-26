use crate::parser::command::Command;
use crate::types::datatype::{DataType, parse_datatype};

pub fn parse(input: &str) -> Result<Command, String>{
    let tokens = tokenize(input)?;
    if tokens.is_empty(){
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
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for ch in input.chars() {
        match ch {
            '"' => {
                in_quotes = !in_quotes;
            }
            c if c.is_whitespace() && !in_quotes => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
            }
            _ => current.push(ch),
        }
    }

    if in_quotes {
        return  Err("Unclosed quote (\") in input".to_string());
    }

    if !current.is_empty() {
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

    Ok(Command::Create { table, columns: cols })
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
        return Err(format!("Bad column definition '{s}'. Use name:type like id:int"));
    }

    Ok((name.to_string(), parse_datatype(dtype)?))
}
