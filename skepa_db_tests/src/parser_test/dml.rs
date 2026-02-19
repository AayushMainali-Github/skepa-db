use super::*;

#[test]
fn parse_insert_with_quotes() {
    let cmd = parse(r#"insert into users values (1, "ram kumar")"#).unwrap();

    match cmd {
        Command::Insert { table, values } => {
            assert_eq!(table, "users");
            assert_eq!(values, vec!["1".to_string(), "ram kumar".to_string()]);
        }
        _ => panic!("Expected Insert command"),
    }
}

#[test]
fn parse_update_basic() {
    let cmd = parse(r#"update users set name = "ravi" where id = 1"#).unwrap();

    match cmd {
        Command::Update {
            table,
            assignments,
            filter,
        } => {
            assert_eq!(table, "users");
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].column, "name");
            assert_eq!(assignments[0].value, "ravi");
            let p = pred(&filter);
            assert_eq!(p.column, "id");
            assert_eq!(p.op, CompareOp::Eq);
            assert_eq!(p.value, "1");
        }
        _ => panic!("Expected Update command"),
    }
}

#[test]
fn parse_update_multiple_assignments() {
    let cmd = parse(r#"update users set name = "ravi", age = 30 where id eq 1"#).unwrap();

    match cmd {
        Command::Update { assignments, .. } => {
            assert_eq!(assignments.len(), 2);
            assert_eq!(assignments[0].column, "name");
            assert_eq!(assignments[0].value, "ravi");
            assert_eq!(assignments[1].column, "age");
            assert_eq!(assignments[1].value, "30");
        }
        _ => panic!("Expected Update command"),
    }
}

#[test]
fn parse_delete_basic() {
    let cmd = parse(r#"delete from users where id = 1"#).unwrap();

    match cmd {
        Command::Delete { table, filter } => {
            assert_eq!(table, "users");
            let p = pred(&filter);
            assert_eq!(p.column, "id");
            assert_eq!(p.op, CompareOp::Eq);
            assert_eq!(p.value, "1");
        }
        _ => panic!("Expected Delete command"),
    }
}

#[test]
fn parse_insert_empty_string_value_allowed() {
    let cmd = parse(r#"insert into users values (1, "")"#).unwrap();

    match cmd {
        Command::Insert { table, values } => {
            assert_eq!(table, "users");
            assert_eq!(values, vec!["1".to_string(), "".to_string()]);
        }
        _ => panic!("Expected Insert command"),
    }
}

#[test]
fn insert_missing_values_errors() {
    let err = parse("insert into users values").unwrap_err();
    assert!(err.to_lowercase().contains("usage: insert"));
}

#[test]
fn update_missing_where_errors() {
    let err = parse(r#"update users set name = "ravi""#).unwrap_err();
    assert!(err.to_lowercase().contains("usage: update"));
}

#[test]
fn delete_missing_where_errors() {
    let err = parse(r#"delete from users"#).unwrap_err();
    assert!(err.to_lowercase().contains("usage: delete"));
}

#[test]
fn update_bad_assignment_pairs_errors() {
    let err = parse(r#"update users set name = "ravi", age where id = 1"#).unwrap_err();
    assert!(err.to_lowercase().contains("bad update assignments"));
}

#[test]
fn insert_requires_into_keyword() {
    let err = parse(r#"insert users values (1, "ram")"#).unwrap_err();
    assert!(err.to_lowercase().contains("usage: insert"));
}

#[test]
fn insert_requires_values_keyword() {
    let err = parse(r#"insert into users (1, "ram")"#).unwrap_err();
    assert!(err.to_lowercase().contains("usage: insert"));
}

#[test]
fn insert_requires_parentheses() {
    let err = parse(r#"insert into users values 1, "ram""#).unwrap_err();
    assert!(err.to_lowercase().contains("usage: insert"));
}

#[test]
fn insert_requires_commas_between_values() {
    let err = parse(r#"insert into users values (1 "ram")"#).unwrap_err();
    assert!(err.to_lowercase().contains("comma"));
}

#[test]
fn insert_allows_no_spaces_around_commas() {
    let cmd = parse(r#"insert into users values(1,"ram")"#).unwrap();
    match cmd {
        Command::Insert { table, values } => {
            assert_eq!(table, "users");
            assert_eq!(values, vec!["1".to_string(), "ram".to_string()]);
        }
        _ => panic!("Expected Insert command"),
    }
}

#[test]
fn insert_rejects_trailing_comma() {
    let err = parse(r#"insert into users values (1, "ram",)"#).unwrap_err();
    assert!(err.to_lowercase().contains("trailing comma"));
}

#[test]
fn update_requires_set_keyword() {
    let err = parse(r#"update users name = "ravi" where id = 1"#).unwrap_err();
    assert!(err.to_lowercase().contains("usage: update"));
}

#[test]
fn update_requires_equals_in_assignment() {
    let err = parse(r#"update users set name "ravi" where id = 1"#).unwrap_err();
    assert!(
        err.to_lowercase().contains("bad update assignments")
            || err.to_lowercase().contains("usage: update")
    );
}

#[test]
fn update_requires_commas_between_assignments() {
    let err = parse(r#"update users set name = "ravi" age = 20 where id = 1"#).unwrap_err();
    assert!(err.to_lowercase().contains("comma"));
}

#[test]
fn update_where_requires_three_tokens() {
    let err = parse(r#"update users set name = "ravi" where id = 1 x"#).unwrap_err();
    assert!(err.to_lowercase().contains("where"));
}

#[test]
fn update_supports_no_spaces_around_equals() {
    let cmd = parse(r#"update users set age=20 where id=1"#).unwrap();
    match cmd {
        Command::Update {
            assignments,
            filter,
            ..
        } => {
            assert_eq!(assignments[0].column, "age");
            assert_eq!(assignments[0].value, "20");
            let pf = pred(&filter);
            assert_eq!(pf.column, "id");
            assert_eq!(pf.value, "1");
        }
        _ => panic!("Expected Update command"),
    }
}

#[test]
fn delete_requires_from_keyword() {
    let err = parse("delete users where id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("usage: delete"));
}

#[test]
fn delete_requires_where_keyword() {
    let err = parse("delete from users id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("usage: delete"));
}

#[test]
fn delete_rejects_unknown_operator() {
    let err = parse("delete from users where id between 1").unwrap_err();
    assert!(err.to_lowercase().contains("unknown where operator"));
}

#[test]
fn parse_insert_single_value() {
    let cmd = parse("insert into t values (1)").unwrap();
    match cmd {
        Command::Insert { values, .. } => assert_eq!(values, vec!["1"]),
        _ => panic!("Expected Insert command"),
    }
}

#[test]
fn parse_insert_blob_value_token() {
    let cmd = parse("insert into t values (0xABCD)").unwrap();
    match cmd {
        Command::Insert { values, .. } => assert_eq!(values, vec!["0xABCD"]),
        _ => panic!("Expected Insert command"),
    }
}

#[test]
fn parse_insert_json_quoted_value() {
    let cmd = parse(r#"insert into t values ("{\"a\":1}")"#).unwrap();
    match cmd {
        Command::Insert { values, .. } => assert_eq!(values, vec![r#"{"a":1}"#]),
        _ => panic!("Expected Insert command"),
    }
}

#[test]
fn parse_update_set_two_columns_no_spaces_after_comma() {
    let cmd = parse("update t set a=1,b=2 where id=3").unwrap();
    match cmd {
        Command::Update { assignments, .. } => {
            assert_eq!(assignments.len(), 2);
            assert_eq!(assignments[0].column, "a");
            assert_eq!(assignments[1].column, "b");
        }
        _ => panic!("Expected Update command"),
    }
}

#[test]
fn parse_update_where_like_operator() {
    let cmd = parse(r#"update t set name = "x" where name like "a*""#).unwrap();
    match cmd {
        Command::Update { filter, .. } => assert_eq!(pred(&filter).op, CompareOp::Like),
        _ => panic!("Expected Update command"),
    }
}

#[test]
fn parse_delete_where_like_operator() {
    let cmd = parse(r#"delete from t where name like "a*""#).unwrap();
    match cmd {
        Command::Delete { filter, .. } => assert_eq!(pred(&filter).op, CompareOp::Like),
        _ => panic!("Expected Delete command"),
    }
}

#[test]
fn parse_delete_not_equal_reports_not_supported() {
    let err = parse("delete from t where a != 1").unwrap_err();
    assert!(err.to_lowercase().contains("not supported"));
}

#[test]
fn parse_update_not_equal_reports_not_supported() {
    let err = parse("update t set a = 1 where b != 2").unwrap_err();
    assert!(err.to_lowercase().contains("not supported"));
}

#[test]
fn parse_update_requires_where_keyword_strictly() {
    let err = parse("update t set a = 1 when id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("usage: update"));
}

#[test]
fn parse_insert_allows_negative_numbers() {
    let cmd = parse("insert into t values (-1, -2)").unwrap();
    match cmd {
        Command::Insert { values, .. } => assert_eq!(values, vec!["-1", "-2"]),
        _ => panic!("Expected Insert command"),
    }
}

#[test]
fn parse_insert_allows_timestamp_token() {
    let cmd = parse(r#"insert into t values ("2025-01-01 10:00:00")"#).unwrap();
    match cmd {
        Command::Insert { values, .. } => assert_eq!(values, vec!["2025-01-01 10:00:00"]),
        _ => panic!("Expected Insert command"),
    }
}

#[test]
fn parse_update_assignment_value_can_be_quoted_spaces() {
    let cmd = parse(r#"update t set name = "hello world" where id = 1"#).unwrap();
    match cmd {
        Command::Update { assignments, .. } => assert_eq!(assignments[0].value, "hello world"),
        _ => panic!("Expected Update command"),
    }
}

#[test]
fn parse_delete_where_value_can_be_quoted_spaces() {
    let cmd = parse(r#"delete from t where name = "hello world""#).unwrap();
    match cmd {
        Command::Delete { filter, .. } => assert_eq!(pred(&filter).value, "hello world"),
        _ => panic!("Expected Delete command"),
    }
}

#[test]
fn parse_update_where_is_null() {
    let cmd = parse("update users set city = \"x\" where city is null").unwrap();
    match cmd {
        Command::Update { filter, .. } => assert_eq!(pred(&filter).op, CompareOp::IsNull),
        _ => panic!("Expected Update command"),
    }
}

#[test]
fn parse_delete_where_is_not_null() {
    let cmd = parse("delete from users where city is not null").unwrap();
    match cmd {
        Command::Delete { filter, .. } => assert_eq!(pred(&filter).op, CompareOp::IsNotNull),
        _ => panic!("Expected Delete command"),
    }
}

#[test]
fn parse_update_where_in_list() {
    let cmd = parse("update users set city = \"x\" where id in (1,2)").unwrap();
    match cmd {
        Command::Update { filter, .. } => assert_eq!(pred(&filter).op, CompareOp::In),
        _ => panic!("Expected Update command"),
    }
}

#[test]
fn parse_delete_where_in_list() {
    let cmd = parse("delete from users where id in (1,2)").unwrap();
    match cmd {
        Command::Delete { filter, .. } => assert_eq!(pred(&filter).op, CompareOp::In),
        _ => panic!("Expected Delete command"),
    }
}

#[test]
fn parse_update_with_trailing_comma_in_set_is_tolerated() {
    let cmd = parse("update users set name = \"a\", where id = 1").unwrap();
    match cmd {
        Command::Update { assignments, .. } => assert_eq!(assignments.len(), 1),
        _ => panic!("expected update"),
    }
}

#[test]
fn parse_update_with_empty_set_errors() {
    assert!(parse("update users set where id = 1").is_err());
}

#[test]
fn parse_delete_with_parenthesized_predicate() {
    let cmd = parse("delete from users where (id = 1)").unwrap();
    match cmd {
        Command::Delete { table, filter } => {
            assert_eq!(table, "users");
            let p = pred(&filter);
            assert_eq!(p.column, "id");
            assert_eq!(p.op, CompareOp::Eq);
            assert_eq!(p.value, "1");
        }
        _ => panic!("expected delete"),
    }
}

#[test]
fn parse_insert_handles_escaped_quote_text() {
    let cmd = parse("insert into users values (1, \"ra\\\"m\")").unwrap();
    match cmd {
        Command::Insert { values, .. } => assert_eq!(values[1], "ra\"m"),
        _ => panic!("expected insert"),
    }
}
