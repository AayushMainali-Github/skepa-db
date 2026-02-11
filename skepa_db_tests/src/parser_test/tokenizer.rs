use super::*;

#[test]
fn tokenize_allows_escaped_quote_inside_quotes() {
    let cmd = parse(r#"insert into users values (1, "ra\"m")"#).unwrap();

    match cmd {
        Command::Insert { values, .. } => {
            assert_eq!(values, vec!["1".to_string(), r#"ra"m"#.to_string()]);
        }
        _ => panic!("Expected Insert command"),
    }
}

#[test]
fn tokenize_allows_escaped_backslash_inside_quotes() {
    let cmd = parse(r#"insert into users values (1, "path\\to\\file")"#).unwrap();

    match cmd {
        Command::Insert { values, .. } => {
            assert_eq!(values, vec!["1".to_string(), r#"path\to\file"#.to_string()]);
        }
        _ => panic!("Expected Insert command"),
    }
}

#[test]
fn tokenize_rejects_unknown_escape_inside_quotes() {
    let err = parse(r#"insert into users values (1, "hello\nworld")"#).unwrap_err();
    assert!(err.to_lowercase().contains("invalid escape"));
}

#[test]
fn tokenize_rejects_quote_in_middle_of_token() {
    let err = parse(r#"insert into users values (1, aa"aa"aa)"#).unwrap_err();
    assert!(
        err.to_lowercase().contains("cannot start in the middle")
            || err.to_lowercase().contains("characters found immediately")
    );
}

#[test]
fn tokenize_rejects_characters_after_closing_quote() {
    let err = parse(r#"insert into users values (1, "ram"kumar)"#).unwrap_err();
    assert!(err.to_lowercase().contains("after a closing quote"));
}

#[test]
fn tokenize_rejects_adjacent_quoted_tokens_without_space() {
    let err = parse(r#"insert into users values (1, "a""b")"#).unwrap_err();
    assert!(err.to_lowercase().contains("unexpected quote after closing quote"));
}

#[test]
fn tokenizer_handles_parentheses_without_spaces() {
    let cmd = parse("create table u(id int,name text)").unwrap();
    match cmd {
        Command::Create { table, columns, .. } => {
            assert_eq!(table, "u");
            assert_eq!(columns.len(), 2);
        }
        _ => panic!("Expected Create command"),
    }
}

