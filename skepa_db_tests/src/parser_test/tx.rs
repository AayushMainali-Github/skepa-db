use super::*;

#[test]
fn parse_begin_commit_rollback() {
    assert!(matches!(parse("begin").unwrap(), Command::Begin));
    assert!(matches!(parse("commit").unwrap(), Command::Commit));
    assert!(matches!(parse("rollback").unwrap(), Command::Rollback));
}

#[test]
fn parse_begin_commit_rollback_usage_errors() {
    assert!(parse("begin now").unwrap_err().to_lowercase().contains("usage: begin"));
    assert!(parse("commit now").unwrap_err().to_lowercase().contains("usage: commit"));
    assert!(parse("rollback now").unwrap_err().to_lowercase().contains("usage: rollback"));
}

#[test]
fn parse_begin_with_leading_trailing_spaces() {
    assert!(matches!(parse("   begin   ").unwrap(), Command::Begin));
}

#[test]
fn parse_commit_with_leading_trailing_spaces() {
    assert!(matches!(parse("   commit   ").unwrap(), Command::Commit));
}

#[test]
fn parse_rollback_with_leading_trailing_spaces() {
    assert!(matches!(parse("   rollback   ").unwrap(), Command::Rollback));
}

