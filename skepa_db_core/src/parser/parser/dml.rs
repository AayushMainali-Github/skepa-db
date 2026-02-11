use crate::parser::command::{Assignment, Command};
use super::where_clause::parse_where_clause;

pub(super) fn parse_insert(tokens: &[String]) -> Result<Command, String> {
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
            if i >= end {
                return Err("Bad INSERT values. Trailing comma is not allowed.".to_string());
            }
        }
    }

    if values.is_empty() {
        return Err("INSERT requires at least one value".to_string());
    }

    Ok(Command::Insert { table, values })
}

pub(super) fn parse_update(tokens: &[String]) -> Result<Command, String> {
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
    let filter = parse_where_clause(
        where_tokens,
        "Bad UPDATE WHERE clause. Use: where <expr>, e.g. col = 1, col is null, col in (1,2), with and/or and parentheses",
    )?;

    Ok(Command::Update {
        table,
        assignments,
        filter,
    })
}

pub(super) fn parse_delete(tokens: &[String]) -> Result<Command, String> {
    // delete from <table> where <column> <op> <value>
    if tokens.len() < 6
        || !tokens[1].eq_ignore_ascii_case("from")
        || !tokens[3].eq_ignore_ascii_case("where")
    {
        return Err("Usage: delete from <table> where <expr>".to_string());
    }

    let filter = parse_where_clause(
        &tokens[4..],
        "Usage: delete from <table> where <expr>",
    )?;
    Ok(Command::Delete {
        table: tokens[2].clone(),
        filter,
    })
}
