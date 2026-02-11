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
        return Err("Empty command".to_string());
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
        "select" => select::parse_select(&tokens),
        _ => Err(format!("Unknown command '{}'", tokens[0])),
    }
}
