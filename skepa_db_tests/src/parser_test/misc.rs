use super::*;

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
    let err = parse("create table users (id text bad)").unwrap_err();
    assert!(
        err.to_lowercase().contains("bad create")
            || err.to_lowercase().contains("unknown column constraint")
    );
}

#[test]
fn parse_unclosed_quote_errors() {
    let err = parse(r#"insert into users values (1, "ram"#).unwrap_err();
    assert!(err.to_lowercase().contains("unclosed quote"));
}

#[test]
fn where_not_equal_not_supported() {
    let err = parse("select * from users where id != 1").unwrap_err();
    assert!(err.to_lowercase().contains("not supported"));
}

#[test]
fn parse_keyword_case_insensitive_commands() {
    let cases = [
        ("CrEaTe TaBlE t (id int)", "create"),
        (r#"InSeRt InTo t VaLuEs (1)"#, "insert"),
        ("UpDaTe t SeT a = 1 WhErE id = 2", "update"),
        ("DeLeTe FrOm t WhErE id = 1", "delete"),
        ("SeLeCt * FrOm t", "select"),
    ];

    for (sql, kind) in cases {
        let cmd = parse(sql).unwrap();
        match (kind, cmd) {
            ("create", Command::Create { table, columns, .. }) => {
                assert_eq!(table, "t");
                assert_eq!(columns.len(), 1);
            }
            ("insert", Command::Insert { table, values }) => {
                assert_eq!(table, "t");
                assert_eq!(values, vec!["1"]);
            }
            ("update", Command::Update { table, .. }) => assert_eq!(table, "t"),
            ("delete", Command::Delete { table, .. }) => assert_eq!(table, "t"),
            ("select", Command::Select { table, .. }) => assert_eq!(table, "t"),
            _ => panic!("unexpected parse kind/match for {kind}"),
        }
    }
}

#[test]
fn parse_drop_index_basic() {
    let cmd = parse("drop index on users (email)").unwrap();
    match cmd {
        Command::DropIndex { table, columns } => {
            assert_eq!(table, "users");
            assert_eq!(columns, vec!["email"]);
        }
        _ => panic!("Expected DropIndex command"),
    }
}

#[test]
fn parse_drop_index_missing_parens_errors() {
    assert!(parse("drop index on users id").is_err());
}

#[test]
fn parse_drop_index_rejects_extra_tokens() {
    assert!(parse("drop index on users (id) now").is_err());
}

