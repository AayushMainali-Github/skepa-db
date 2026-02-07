use skepa_db_core::parser::command::{Command, CompareOp};
use skepa_db_core::parser::parser::parse;
use skepa_db_core::types::datatype::DataType;

#[test]
fn parse_create_basic() {
    let cmd = parse(r#"create users id:int name:text"#).unwrap();

    match cmd {
        Command::Create { table, columns } => {
            assert_eq!(table, "users");
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].0, "id");
            assert_eq!(columns[0].1, DataType::Int);
            assert_eq!(columns[1].0, "name");
            assert_eq!(columns[1].1, DataType::Text);
        }
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_insert_with_quotes() {
    let cmd = parse(r#"insert users 1 "ram kumar""#).unwrap();

    match cmd {
        Command::Insert { table, values } => {
            assert_eq!(table, "users");
            assert_eq!(values, vec!["1".to_string(), "ram kumar".to_string()]);
        }
        _ => panic!("Expected Insert command"),
    }
}

#[test]
fn parse_select_basic() {
    let cmd = parse("select users").unwrap();

    match cmd {
        Command::Select { table, filter } => {
            assert_eq!(table, "users");
            assert!(filter.is_none());
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_where_eq() {
    let cmd = parse(r#"select users where name = "ram""#).unwrap();

    match cmd {
        Command::Select { table, filter } => {
            assert_eq!(table, "users");
            let f = filter.expect("expected where clause");
            assert_eq!(f.column, "name");
            assert_eq!(f.op, CompareOp::Eq);
            assert_eq!(f.value, "ram");
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_where_keyword_operator() {
    let cmd = parse("select users where age gte 18").unwrap();

    match cmd {
        Command::Select { filter, .. } => {
            let f = filter.expect("expected where clause");
            assert_eq!(f.column, "age");
            assert_eq!(f.op, CompareOp::Gte);
            assert_eq!(f.value, "18");
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_where_like() {
    let cmd = parse(r#"select users where name like "ra*""#).unwrap();

    match cmd {
        Command::Select { filter, .. } => {
            let f = filter.expect("expected where clause");
            assert_eq!(f.column, "name");
            assert_eq!(f.op, CompareOp::Like);
            assert_eq!(f.value, "ra*");
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_empty_command_errors() {
    let err = parse("").unwrap_err();
    assert!(err.to_lowercase().contains("empty"));
}

#[test]
fn parse_unknown_command_errors() {
    let err = parse("drop users").unwrap_err();
    assert!(err.to_lowercase().contains("unknown"));
}

#[test]
fn parse_bad_column_def_errors() {
    let err = parse("create users id name:text").unwrap_err();
    assert!(err.to_lowercase().contains("bad column"));
}

#[test]
fn parse_unclosed_quote_errors() {
    let err = parse(r#"insert users 1 "ram"#).unwrap_err();
    assert!(err.to_lowercase().contains("unclosed quote"));
}

#[test]
fn parse_insert_empty_string_value_allowed() {
    let cmd = parse(r#"insert users 1 "" "#).unwrap();

    match cmd {
        Command::Insert { table, values } => {
            assert_eq!(table, "users");
            assert_eq!(values, vec!["1".to_string(), "".to_string()]);
        }
        _ => panic!("Expected Insert command"),
    }
}

#[test]
fn tokenize_allows_escaped_quote_inside_quotes() {
    let cmd = parse(r#"insert users 1 "ra\"m""#).unwrap();

    match cmd {
        Command::Insert { values, .. } => {
            assert_eq!(values, vec!["1".to_string(), r#"ra"m"#.to_string()]);
        }
        _ => panic!("Expected Insert command"),
    }
}

#[test]
fn tokenize_allows_escaped_backslash_inside_quotes() {
    let cmd = parse(r#"insert users 1 "path\\to\\file""#).unwrap();

    match cmd {
        Command::Insert { values, .. } => {
            assert_eq!(values, vec!["1".to_string(), r#"path\to\file"#.to_string()]);
        }
        _ => panic!("Expected Insert command"),
    }
}

#[test]
fn tokenize_rejects_unknown_escape_inside_quotes() {
    let err = parse(r#"insert users 1 "hello\nworld""#).unwrap_err();
    assert!(err.to_lowercase().contains("invalid escape"));
}

#[test]
fn tokenize_rejects_quote_in_middle_of_token() {
    let err = parse(r#"insert users 1 aa"aa"aa"#).unwrap_err();
    assert!(
        err.to_lowercase().contains("cannot start in the middle")
            || err.to_lowercase().contains("characters found immediately")
    );
}

#[test]
fn tokenize_rejects_characters_after_closing_quote() {
    let err = parse(r#"insert users 1 "ram"kumar"#).unwrap_err();
    assert!(err.to_lowercase().contains("after a closing quote"));
}

#[test]
fn tokenize_rejects_adjacent_quoted_tokens_without_space() {
    let err = parse(r#"insert users 1 "a""b""#).unwrap_err();
    assert!(err.to_lowercase().contains("unexpected quote after closing quote"));
}

#[test]
fn create_rejects_unknown_datatype() {
    let err = parse("create users id:integer name:text").unwrap_err();
    assert!(
        err.to_lowercase().contains("unknown type")
            || err.to_lowercase().contains("use int|text")
    );
}

#[test]
fn select_with_extra_tokens_errors() {
    let err = parse("select users now").unwrap_err();
    assert!(err.to_lowercase().contains("usage: select"));
}

#[test]
fn select_with_bad_where_operator_errors() {
    let err = parse("select users where age between 1").unwrap_err();
    assert!(err.to_lowercase().contains("unknown where operator"));
}

#[test]
fn insert_missing_values_errors() {
    let err = parse("insert users").unwrap_err();
    assert!(err.to_lowercase().contains("usage: insert"));
}

#[test]
fn create_missing_columns_errors() {
    let err = parse("create users").unwrap_err();
    assert!(err.to_lowercase().contains("usage: create"));
}
