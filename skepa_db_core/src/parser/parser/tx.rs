use crate::parser::command::Command;

pub(super) fn parse_begin(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() != 1 {
        return Err("Usage: begin".to_string());
    }
    Ok(Command::Begin)
}

pub(super) fn parse_commit(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() != 1 {
        return Err("Usage: commit".to_string());
    }
    Ok(Command::Commit)
}

pub(super) fn parse_rollback(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() != 1 {
        return Err("Usage: rollback".to_string());
    }
    Ok(Command::Rollback)
}
