use skepa_db_core::parser::command::{Command, CompareOp};
use skepa_db_core::parser::parser::parse;
use skepa_db_core::types::datatype::DataType;

#[test]
fn parse_create_basic() {
    let cmd = parse(r#"create table users (id int, name text)"#).unwrap();

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
fn parse_select_basic() {
    let cmd = parse("select * from users").unwrap();

    match cmd {
        Command::Select {
            table,
            columns,
            filter,
        } => {
            assert_eq!(table, "users");
            assert_eq!(columns.unwrap(), Vec::<String>::new());
            assert!(filter.is_none());
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_where_eq() {
    let cmd = parse(r#"select * from users where name = "ram""#).unwrap();

    match cmd {
        Command::Select {
            table,
            columns,
            filter,
        } => {
            assert_eq!(table, "users");
            assert_eq!(columns.unwrap(), Vec::<String>::new());
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
    let cmd = parse("select * from users where age gte 18").unwrap();

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
    let cmd = parse(r#"select * from users where name like "ra*""#).unwrap();

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
            assert_eq!(filter.column, "id");
            assert_eq!(filter.op, CompareOp::Eq);
            assert_eq!(filter.value, "1");
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
            assert_eq!(filter.column, "id");
            assert_eq!(filter.op, CompareOp::Eq);
            assert_eq!(filter.value, "1");
        }
        _ => panic!("Expected Delete command"),
    }
}

#[test]
fn parse_select_projection_basic() {
    let cmd = parse("select id,name from users").unwrap();

    match cmd {
        Command::Select {
            table,
            columns,
            filter,
        } => {
            assert_eq!(table, "users");
            assert_eq!(columns.unwrap(), vec!["id".to_string(), "name".to_string()]);
            assert!(filter.is_none());
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_projection_with_where() {
    let cmd = parse(r#"select id,name from users where name like "ra*""#).unwrap();

    match cmd {
        Command::Select {
            table,
            columns,
            filter,
        } => {
            assert_eq!(table, "users");
            assert_eq!(columns.unwrap(), vec!["id".to_string(), "name".to_string()]);
            let f = filter.expect("expected where clause");
            assert_eq!(f.column, "name");
            assert_eq!(f.op, CompareOp::Like);
            assert_eq!(f.value, "ra*");
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_star_from_table() {
    let cmd = parse("select * from users").unwrap();

    match cmd {
        Command::Select {
            table,
            columns,
            filter,
        } => {
            assert_eq!(table, "users");
            assert_eq!(columns.unwrap(), Vec::<String>::new());
            assert!(filter.is_none());
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
    let err = parse("create table users (id text bad)").unwrap_err();
    assert!(err.to_lowercase().contains("bad create"));
}

#[test]
fn parse_unclosed_quote_errors() {
    let err = parse(r#"insert into users values (1, "ram"#).unwrap_err();
    assert!(err.to_lowercase().contains("unclosed quote"));
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
fn create_rejects_unknown_datatype() {
    let err = parse("create table users (id integer, name text)").unwrap_err();
    assert!(
        err.to_lowercase().contains("unknown type")
            || err.to_lowercase().contains("use int|text")
    );
}

#[test]
fn select_with_extra_tokens_errors() {
    let err = parse("select * users now").unwrap_err();
    assert!(err.to_lowercase().contains("usage: select"));
}

#[test]
fn select_projection_missing_from_errors() {
    let err = parse("select id,name users").unwrap_err();
    assert!(err.to_lowercase().contains("usage: select"));
}

#[test]
fn select_with_bad_where_operator_errors() {
    let err = parse("select * from users where age between 1").unwrap_err();
    assert!(err.to_lowercase().contains("unknown where operator"));
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
fn create_missing_columns_errors() {
    let err = parse("create table users").unwrap_err();
    assert!(err.to_lowercase().contains("usage: create"));
}

#[test]
fn create_requires_table_keyword() {
    let err = parse("create users (id int)").unwrap_err();
    assert!(err.to_lowercase().contains("usage: create"));
}

#[test]
fn create_requires_parentheses() {
    let err = parse("create table users id int").unwrap_err();
    assert!(err.to_lowercase().contains("usage: create"));
}

#[test]
fn create_requires_commas_between_columns() {
    let err = parse("create table users (id int name text)").unwrap_err();
    assert!(err.to_lowercase().contains("comma"));
}

#[test]
fn create_trailing_comma_errors() {
    let err = parse("create table users (id int,)").unwrap_err();
    assert!(err.to_lowercase().contains("bad create"));
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
fn select_requires_from_keyword() {
    let err = parse("select id,name users").unwrap_err();
    assert!(err.to_lowercase().contains("usage: select"));
}

#[test]
fn select_column_list_trailing_comma_errors() {
    let err = parse("select id, from users").unwrap_err();
    assert!(err.to_lowercase().contains("column list"));
}

#[test]
fn select_column_list_double_comma_errors() {
    let err = parse("select id,,name from users").unwrap_err();
    assert!(err.to_lowercase().contains("column list"));
}

#[test]
fn select_requires_table_after_from() {
    let err = parse("select * from").unwrap_err();
    assert!(err.to_lowercase().contains("usage: select") || err.to_lowercase().contains("missing table"));
}

#[test]
fn select_where_too_many_tokens_errors() {
    let err = parse("select * from users where id = 1 extra").unwrap_err();
    assert!(err.to_lowercase().contains("usage: select"));
}

#[test]
fn select_supports_no_spaces_around_comma_in_projection() {
    let cmd = parse("select id,name,age from users").unwrap();
    match cmd {
        Command::Select { columns, .. } => {
            assert_eq!(
                columns.unwrap(),
                vec!["id".to_string(), "name".to_string(), "age".to_string()]
            );
        }
        _ => panic!("Expected Select command"),
    }
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
        Command::Update { assignments, filter, .. } => {
            assert_eq!(assignments[0].column, "age");
            assert_eq!(assignments[0].value, "20");
            assert_eq!(filter.column, "id");
            assert_eq!(filter.value, "1");
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
fn where_not_equal_not_supported() {
    let err = parse("select * from users where id != 1").unwrap_err();
    assert!(err.to_lowercase().contains("not supported"));
}

#[test]
fn tokenizer_handles_parentheses_without_spaces() {
    let cmd = parse("create table u(id int,name text)").unwrap();
    match cmd {
        Command::Create { table, columns } => {
            assert_eq!(table, "u");
            assert_eq!(columns.len(), 2);
        }
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_with_extended_types() {
    let cmd = parse(
        "create table t (b bool, i int, bi bigint, d decimal(10,2), v varchar(20), tx text, dt date, ts timestamp, u uuid, j json, bl blob)",
    )
    .unwrap();
    match cmd {
        Command::Create { table, columns } => {
            assert_eq!(table, "t");
            assert_eq!(columns.len(), 11);
        }
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_bad_decimal_params_errors() {
    let err = parse("create table t (d decimal(2,5))").unwrap_err();
    assert!(err.to_lowercase().contains("scale"));
}

#[test]
fn parse_create_bad_varchar_size_errors() {
    let err = parse("create table t (v varchar(0))").unwrap_err();
    assert!(err.to_lowercase().contains("varchar"));
}
