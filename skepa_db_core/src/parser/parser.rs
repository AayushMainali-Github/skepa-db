use crate::parser::command::Command;

mod alter;
mod common;
mod create;
mod dml;
mod select;
mod tokenizer;
mod tx;
mod where_clause;

pub fn parse(input: &str) -> Result<Command, String> {
    let tokens = tokenizer::tokenize(input)?;
    if tokens.is_empty() {
        return Err(
            "Empty command. Supported commands: begin, commit, rollback, create table, create index, drop index, alter table, insert, update, delete, select, describe"
                .to_string(),
        );
    }

    let keyword = tokens[0].to_lowercase();

    match keyword.as_str() {
        "begin" => tx::parse_begin(&tokens),
        "commit" => tx::parse_commit(&tokens),
        "rollback" => tx::parse_rollback(&tokens),
        "create" => create::parse_create(&tokens),
        "drop" => create::parse_drop(&tokens),
        "alter" => alter::parse_alter(&tokens),
        "insert" => dml::parse_insert(&tokens),
        "update" => dml::parse_update(&tokens),
        "delete" => dml::parse_delete(&tokens),
        "describe" => parse_describe(&tokens),
        "select" => select::parse_select(&tokens),
        _ => Err(format!(
            "Unknown command '{}'. Supported commands: begin, commit, rollback, create table, create index, drop index, alter table, insert, update, delete, select, describe",
            tokens[0]
        )),
    }
}

fn parse_describe(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() != 2 {
        return Err("Usage: describe <table>".to_string());
    }
    Ok(Command::Describe {
        table: tokens[1].clone(),
    })
}
