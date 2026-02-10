use skepa_db_core::parser::command::{Command, CompareOp, JoinType};
use skepa_db_core::parser::parser::parse;
use skepa_db_core::types::datatype::DataType;

#[test]
fn parse_create_basic() {
    let cmd = parse(r#"create table users (id int, name text)"#).unwrap();

    match cmd {
        Command::Create { table, columns, .. } => {
            assert_eq!(table, "users");
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].name, "id");
            assert_eq!(columns[0].dtype, DataType::Int);
            assert_eq!(columns[1].name, "name");
            assert_eq!(columns[1].dtype, DataType::Text);
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
            ..
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
            ..
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
            ..
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
            ..
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
            ..
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
    assert!(
        err.to_lowercase().contains("comma")
            || err.to_lowercase().contains("unknown column constraint")
    );
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
        Command::Create { table, columns, .. } => {
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
        Command::Create { table, columns, .. } => {
            assert_eq!(table, "t");
            assert_eq!(columns.len(), 11);
        }
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_with_constraints() {
    let cmd =
        parse("create table users (id int primary key, email text unique, name text not null)")
            .unwrap();
    match cmd {
        Command::Create { columns, .. } => {
            assert!(columns[0].primary_key);
            assert!(columns[0].unique);
            assert!(columns[0].not_null);
            assert!(columns[1].unique);
            assert!(!columns[1].primary_key);
            assert!(columns[2].not_null);
        }
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_primary_key_implies_unique_and_not_null() {
    let cmd = parse("create table t (id int primary key)").unwrap();
    match cmd {
        Command::Create { columns, .. } => {
            assert!(columns[0].primary_key);
            assert!(columns[0].unique);
            assert!(columns[0].not_null);
        }
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_unknown_constraint_token_errors() {
    let err = parse("create table t (id int indexed)").unwrap_err();
    assert!(err.to_lowercase().contains("unknown column constraint"));
}

#[test]
fn parse_create_with_composite_constraints() {
    let cmd = parse(
        "create table m (a int, b int, c text, primary key(a,b), unique(b,c))",
    )
    .unwrap();
    match cmd {
        Command::Create {
            columns,
            table_constraints,
            ..
        } => {
            assert_eq!(columns.len(), 3);
            assert_eq!(table_constraints.len(), 2);
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
fn parse_table_constraint_missing_parentheses_errors() {
    let err = parse("create table t (a int, b int, primary key a,b)").unwrap_err();
    assert!(err.to_lowercase().contains("column list"));
}

#[test]
fn parse_table_constraint_empty_column_list_errors() {
    let err = parse("create table t (a int, unique())").unwrap_err();
    assert!(err.to_lowercase().contains("cannot be empty"));
}

#[test]
fn parse_table_constraint_double_comma_errors() {
    let err = parse("create table t (a int, b int, unique(a,,b))").unwrap_err();
    assert!(err.to_lowercase().contains("bad constraint column list"));
}

#[test]
fn parse_primary_key_constraint_missing_key_keyword_errors() {
    let err = parse("create table t (a int, primary(a))").unwrap_err();
    let e = err.to_lowercase();
    assert!(
        e.contains("bad create")
            || e.contains("unknown")
            || e.contains("column list")
            || e.contains("constraint")
    );
}

#[test]
fn parse_like_operator_on_select_is_case_insensitive_keyword() {
    let cmd = parse(r#"select * from users where name LIKE "a*""#).unwrap();
    match cmd {
        Command::Select { filter, .. } => {
            let f = filter.expect("expected where clause");
            assert_eq!(f.op, CompareOp::Like);
            assert_eq!(f.value, "a*");
        }
        _ => panic!("Expected Select command"),
    }
}

macro_rules! parse_select_op_test {
    ($name:ident, $op:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let cmd = parse(&format!("select * from t where a {} 1", $op)).unwrap();
            match cmd {
                Command::Select { filter, .. } => {
                    assert_eq!(filter.expect("where").op, $expected);
                }
                _ => panic!("Expected Select command"),
            }
        }
    };
}

parse_select_op_test!(parse_select_op_eq_symbol, "=", CompareOp::Eq);
parse_select_op_test!(parse_select_op_eq_word, "eq", CompareOp::Eq);
parse_select_op_test!(parse_select_op_gt_symbol, ">", CompareOp::Gt);
parse_select_op_test!(parse_select_op_gt_word, "gt", CompareOp::Gt);
parse_select_op_test!(parse_select_op_lt_symbol, "<", CompareOp::Lt);
parse_select_op_test!(parse_select_op_lt_word, "lt", CompareOp::Lt);
parse_select_op_test!(parse_select_op_gte_symbol, ">=", CompareOp::Gte);
parse_select_op_test!(parse_select_op_gte_word, "gte", CompareOp::Gte);
parse_select_op_test!(parse_select_op_lte_symbol, "<=", CompareOp::Lte);
parse_select_op_test!(parse_select_op_lte_word, "lte", CompareOp::Lte);
parse_select_op_test!(parse_select_op_like_lower, "like", CompareOp::Like);
parse_select_op_test!(parse_select_op_like_mixed, "LiKe", CompareOp::Like);

#[test]
fn parse_keyword_case_insensitive_create() {
    let cmd = parse("CrEaTe TaBlE t (id int)").unwrap();
    match cmd {
        Command::Create { table, columns, .. } => {
            assert_eq!(table, "t");
            assert_eq!(columns.len(), 1);
        }
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_keyword_case_insensitive_insert() {
    let cmd = parse(r#"InSeRt InTo t VaLuEs (1)"#).unwrap();
    match cmd {
        Command::Insert { table, values } => {
            assert_eq!(table, "t");
            assert_eq!(values, vec!["1"]);
        }
        _ => panic!("Expected Insert command"),
    }
}

#[test]
fn parse_keyword_case_insensitive_update() {
    let cmd = parse("UpDaTe t SeT a = 1 WhErE id = 2").unwrap();
    match cmd {
        Command::Update { table, .. } => assert_eq!(table, "t"),
        _ => panic!("Expected Update command"),
    }
}

#[test]
fn parse_keyword_case_insensitive_delete() {
    let cmd = parse("DeLeTe FrOm t WhErE id = 1").unwrap();
    match cmd {
        Command::Delete { table, .. } => assert_eq!(table, "t"),
        _ => panic!("Expected Delete command"),
    }
}

#[test]
fn parse_keyword_case_insensitive_select() {
    let cmd = parse("SeLeCt * FrOm t").unwrap();
    match cmd {
        Command::Select { table, .. } => assert_eq!(table, "t"),
        _ => panic!("Expected Select command"),
    }
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
        Command::Update { filter, .. } => assert_eq!(filter.op, CompareOp::Like),
        _ => panic!("Expected Update command"),
    }
}

#[test]
fn parse_delete_where_like_operator() {
    let cmd = parse(r#"delete from t where name like "a*""#).unwrap();
    match cmd {
        Command::Delete { filter, .. } => assert_eq!(filter.op, CompareOp::Like),
        _ => panic!("Expected Delete command"),
    }
}

#[test]
fn parse_select_projection_single_column() {
    let cmd = parse("select id from t").unwrap();
    match cmd {
        Command::Select { columns, .. } => assert_eq!(columns.unwrap(), vec!["id"]),
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_projection_four_columns() {
    let cmd = parse("select a,b,c,d from t").unwrap();
    match cmd {
        Command::Select { columns, .. } => {
            assert_eq!(columns.unwrap(), vec!["a", "b", "c", "d"]);
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_star_with_uppercase_where() {
    let cmd = parse("select * from t WHERE id = 1").unwrap();
    match cmd {
        Command::Select { filter, .. } => assert!(filter.is_some()),
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_unknown_op_reports_operator() {
    let err = parse("select * from t where a approx 1").unwrap_err();
    assert!(err.to_lowercase().contains("unknown where operator"));
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
fn parse_create_bool_type() {
    let cmd = parse("create table t (a bool)").unwrap();
    match cmd {
        Command::Create { columns, .. } => assert_eq!(columns.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_bigint_type() {
    let cmd = parse("create table t (a bigint)").unwrap();
    match cmd {
        Command::Create { columns, .. } => assert_eq!(columns.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_date_type() {
    let cmd = parse("create table t (a date)").unwrap();
    match cmd {
        Command::Create { columns, .. } => assert_eq!(columns.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_timestamp_type() {
    let cmd = parse("create table t (a timestamp)").unwrap();
    match cmd {
        Command::Create { columns, .. } => assert_eq!(columns.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_uuid_type() {
    let cmd = parse("create table t (a uuid)").unwrap();
    match cmd {
        Command::Create { columns, .. } => assert_eq!(columns.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_json_type() {
    let cmd = parse("create table t (a json)").unwrap();
    match cmd {
        Command::Create { columns, .. } => assert_eq!(columns.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_blob_type() {
    let cmd = parse("create table t (a blob)").unwrap();
    match cmd {
        Command::Create { columns, .. } => assert_eq!(columns.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_varchar_spaces_in_paren() {
    let cmd = parse("create table t (a varchar ( 10 ))").unwrap();
    match cmd {
        Command::Create { columns, .. } => assert_eq!(columns.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_decimal_spaces_in_paren() {
    let cmd = parse("create table t (a decimal ( 8 , 2 ))").unwrap();
    match cmd {
        Command::Create { columns, .. } => assert_eq!(columns.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_decimal_missing_scale_errors() {
    let err = parse("create table t (a decimal(8))").unwrap_err();
    assert!(err.to_lowercase().contains("decimal"));
}

#[test]
fn parse_create_varchar_missing_size_errors() {
    let err = parse("create table t (a varchar)").unwrap_err();
    assert!(err.to_lowercase().contains("varchar"));
}

#[test]
fn parse_create_table_constraint_unique_single_col() {
    let cmd = parse("create table t (a int, unique(a))").unwrap();
    match cmd {
        Command::Create {
            table_constraints, ..
        } => assert_eq!(table_constraints.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_with_foreign_key_constraint() {
    let cmd = parse("create table orders (id int, user_id int, foreign key(user_id) references users(id))").unwrap();
    match cmd {
        Command::Create { table_constraints, .. } => {
            assert_eq!(table_constraints.len(), 1);
        }
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_with_composite_foreign_key_constraint() {
    let cmd = parse(
        "create table c (a int, b int, foreign key(a,b) references p(x,y))",
    )
    .unwrap();
    match cmd {
        Command::Create { table_constraints, .. } => {
            assert_eq!(table_constraints.len(), 1);
        }
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_foreign_key_missing_references_errors() {
    let err = parse("create table c (a int, foreign key(a) users(id))").unwrap_err();
    assert!(err.to_lowercase().contains("references"));
}

#[test]
fn parse_foreign_key_on_delete_cascade() {
    let cmd = parse(
        "create table c (a int, foreign key(a) references p(id) on delete cascade)",
    )
    .unwrap();
    match cmd {
        Command::Create { table_constraints, .. } => assert_eq!(table_constraints.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_foreign_key_on_delete_restrict() {
    let cmd = parse(
        "create table c (a int, foreign key(a) references p(id) on delete restrict)",
    )
    .unwrap();
    match cmd {
        Command::Create { table_constraints, .. } => assert_eq!(table_constraints.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_foreign_key_unknown_on_delete_action_errors() {
    let err = parse("create table c (a int, foreign key(a) references p(id) on delete nope)")
        .unwrap_err();
    assert!(err.to_lowercase().contains("unknown on delete action"));
}

#[test]
fn parse_foreign_key_missing_parent_table_errors() {
    let err = parse("create table c (a int, foreign key(a) references)").unwrap_err();
    assert!(err.to_lowercase().contains("parent table"));
}

#[test]
fn parse_foreign_key_missing_parent_columns_errors() {
    let err = parse("create table c (a int, foreign key(a) references p)").unwrap_err();
    assert!(err.to_lowercase().contains("column list"));
}

#[test]
fn parse_foreign_key_on_delete_case_insensitive() {
    let cmd = parse(
        "create table c (a int, foreign key(a) references p(id) ON DELETE CASCADE)",
    )
    .unwrap();
    match cmd {
        Command::Create { table_constraints, .. } => assert_eq!(table_constraints.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_foreign_key_can_follow_other_constraints() {
    let cmd = parse(
        "create table c (id int primary key, p int, unique(p), foreign key(p) references parent(id))",
    )
    .unwrap();
    match cmd {
        Command::Create {
            columns,
            table_constraints,
            ..
        } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(table_constraints.len(), 2);
        }
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_two_foreign_keys_in_one_table() {
    let cmd = parse(
        "create table c (a int, b int, foreign key(a) references p1(id), foreign key(b) references p2(id))",
    )
    .unwrap();
    match cmd {
        Command::Create { table_constraints, .. } => assert_eq!(table_constraints.len(), 2),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_foreign_key_on_update_cascade() {
    let cmd = parse(
        "create table c (a int, foreign key(a) references p(id) on update cascade)",
    )
    .unwrap();
    match cmd {
        Command::Create { table_constraints, .. } => assert_eq!(table_constraints.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_foreign_key_on_update_restrict() {
    let cmd = parse(
        "create table c (a int, foreign key(a) references p(id) on update restrict)",
    )
    .unwrap();
    match cmd {
        Command::Create { table_constraints, .. } => assert_eq!(table_constraints.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_foreign_key_unknown_on_update_action_errors() {
    let err = parse("create table c (a int, foreign key(a) references p(id) on update nope)")
        .unwrap_err();
    assert!(err.to_lowercase().contains("unknown on update action"));
}

#[test]
fn parse_foreign_key_on_delete_set_null() {
    let cmd = parse(
        "create table c (a int, foreign key(a) references p(id) on delete set null)",
    )
    .unwrap();
    match cmd {
        Command::Create { table_constraints, .. } => assert_eq!(table_constraints.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_foreign_key_on_update_set_null() {
    let cmd = parse(
        "create table c (a int, foreign key(a) references p(id) on update set null)",
    )
    .unwrap();
    match cmd {
        Command::Create { table_constraints, .. } => assert_eq!(table_constraints.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_foreign_key_on_delete_no_action() {
    let cmd = parse(
        "create table c (a int, foreign key(a) references p(id) on delete no action)",
    )
    .unwrap();
    match cmd {
        Command::Create { table_constraints, .. } => assert_eq!(table_constraints.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_foreign_key_on_update_no_action() {
    let cmd = parse(
        "create table c (a int, foreign key(a) references p(id) on update no action)",
    )
    .unwrap();
    match cmd {
        Command::Create { table_constraints, .. } => assert_eq!(table_constraints.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_foreign_key_composite_with_spaces() {
    let cmd = parse(
        "create table c (a int, b int, foreign key ( a , b ) references p ( x , y ) on delete cascade)",
    )
    .unwrap();
    match cmd {
        Command::Create { table_constraints, .. } => assert_eq!(table_constraints.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_table_constraint_primary_single_col() {
    let cmd = parse("create table t (a int, primary key(a))").unwrap();
    match cmd {
        Command::Create {
            table_constraints, ..
        } => assert_eq!(table_constraints.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_table_constraint_primary_three_cols() {
    let cmd = parse("create table t (a int, b int, c int, primary key(a,b,c))").unwrap();
    match cmd {
        Command::Create {
            table_constraints, ..
        } => assert_eq!(table_constraints.len(), 1),
        _ => panic!("Expected Create command"),
    }
}

#[test]
fn parse_create_multiple_table_constraints() {
    let cmd = parse("create table t (a int, b int, c int, unique(a,b), unique(b,c))").unwrap();
    match cmd {
        Command::Create {
            table_constraints, ..
        } => assert_eq!(table_constraints.len(), 2),
        _ => panic!("Expected Create command"),
    }
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

#[test]
fn parse_update_requires_where_keyword_strictly() {
    let err = parse("update t set a = 1 when id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("usage: update"));
}

#[test]
fn parse_select_rejects_missing_projection() {
    let err = parse("select from t").unwrap_err();
    assert!(err.to_lowercase().contains("usage: select"));
}

#[test]
fn parse_select_rejects_only_comma_projection() {
    let err = parse("select , from t").unwrap_err();
    assert!(err.to_lowercase().contains("column list"));
}

#[test]
fn parse_select_rejects_projection_ending_comma_longer() {
    let err = parse("select a,b, from t").unwrap_err();
    assert!(err.to_lowercase().contains("column list"));
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
fn parse_select_where_value_can_be_quoted_spaces() {
    let cmd = parse(r#"select * from t where name = "hello world""#).unwrap();
    match cmd {
        Command::Select { filter, .. } => assert_eq!(filter.expect("where").value, "hello world"),
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_delete_where_value_can_be_quoted_spaces() {
    let cmd = parse(r#"delete from t where name = "hello world""#).unwrap();
    match cmd {
        Command::Delete { filter, .. } => assert_eq!(filter.value, "hello world"),
        _ => panic!("Expected Delete command"),
    }
}

#[test]
fn parse_create_rejects_unknown_table_constraint_token() {
    let err = parse("create table t (a int, foreign key(a))").unwrap_err();
    assert!(err.to_lowercase().contains("references"));
}

#[test]
fn parse_alter_add_unique() {
    let cmd = parse("alter table t add unique(a,b)").unwrap();
    match cmd {
        Command::Alter { table, .. } => assert_eq!(table, "t"),
        _ => panic!("Expected Alter command"),
    }
}

#[test]
fn parse_alter_drop_unique() {
    let cmd = parse("alter table t drop unique(a)").unwrap();
    match cmd {
        Command::Alter { table, .. } => assert_eq!(table, "t"),
        _ => panic!("Expected Alter command"),
    }
}

#[test]
fn parse_alter_add_foreign_key() {
    let cmd = parse(
        "alter table c add foreign key(a) references p(id) on delete cascade on update no action",
    )
    .unwrap();
    match cmd {
        Command::Alter { table, .. } => assert_eq!(table, "c"),
        _ => panic!("Expected Alter command"),
    }
}

#[test]
fn parse_alter_drop_foreign_key() {
    let cmd = parse("alter table c drop foreign key(a) references p(id)").unwrap();
    match cmd {
        Command::Alter { table, .. } => assert_eq!(table, "c"),
        _ => panic!("Expected Alter command"),
    }
}

#[test]
fn parse_alter_set_not_null() {
    let cmd = parse("alter table t alter column a set not null").unwrap();
    match cmd {
        Command::Alter { table, .. } => assert_eq!(table, "t"),
        _ => panic!("Expected Alter command"),
    }
}

#[test]
fn parse_alter_drop_not_null() {
    let cmd = parse("alter table t alter column a drop not null").unwrap();
    match cmd {
        Command::Alter { table, .. } => assert_eq!(table, "t"),
        _ => panic!("Expected Alter command"),
    }
}

#[test]
fn parse_create_index_basic() {
    let cmd = parse("create index on users (email)").unwrap();
    match cmd {
        Command::CreateIndex { table, columns } => {
            assert_eq!(table, "users");
            assert_eq!(columns, vec!["email"]);
        }
        _ => panic!("Expected CreateIndex command"),
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
fn parse_select_with_order_by_and_limit() {
    let cmd = parse("select id,name from users where age gte 18 order by name desc limit 5").unwrap();
    match cmd {
        Command::Select { table, columns, filter, order_by, limit, .. } => {
            assert_eq!(table, "users");
            assert_eq!(columns.unwrap(), vec!["id", "name"]);
            let f = filter.expect("where");
            assert_eq!(f.column, "age");
            let o = order_by.expect("order by");
            assert_eq!(o.column, "name");
            assert!(!o.asc);
            assert_eq!(limit, Some(5));
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_with_order_by_only() {
    let cmd = parse("select * from users order by id asc").unwrap();
    match cmd {
        Command::Select { order_by, limit, .. } => {
            assert_eq!(limit, None);
            assert_eq!(order_by.expect("order").column, "id");
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_with_limit_only() {
    let cmd = parse("select * from users limit 2").unwrap();
    match cmd {
        Command::Select { limit, .. } => assert_eq!(limit, Some(2)),
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_where_is_null() {
    let cmd = parse("select * from users where city is null").unwrap();
    match cmd {
        Command::Select { filter, .. } => {
            let f = filter.expect("where");
            assert_eq!(f.column, "city");
            assert_eq!(f.op, CompareOp::IsNull);
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_where_is_not_null() {
    let cmd = parse("select * from users where city is not null").unwrap();
    match cmd {
        Command::Select { filter, .. } => {
            let f = filter.expect("where");
            assert_eq!(f.op, CompareOp::IsNotNull);
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_update_where_is_null() {
    let cmd = parse("update users set city = \"x\" where city is null").unwrap();
    match cmd {
        Command::Update { filter, .. } => assert_eq!(filter.op, CompareOp::IsNull),
        _ => panic!("Expected Update command"),
    }
}

#[test]
fn parse_delete_where_is_not_null() {
    let cmd = parse("delete from users where city is not null").unwrap();
    match cmd {
        Command::Delete { filter, .. } => assert_eq!(filter.op, CompareOp::IsNotNull),
        _ => panic!("Expected Delete command"),
    }
}

macro_rules! parse_ok_cases {
    ($( $name:ident => $sql:expr ),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                let out = parse($sql);
                assert!(out.is_ok(), "parse failed for '{}': {:?}", $sql, out.err());
            }
        )*
    };
}

macro_rules! parse_err_cases {
    ($( $name:ident => $sql:expr ),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                assert!(parse($sql).is_err(), "expected parse error for '{}'", $sql);
            }
        )*
    };
}

parse_ok_cases!(
    parse_more_select_order_limit_01 => "select * from t order by a",
    parse_more_select_order_limit_02 => "select * from t order by a asc",
    parse_more_select_order_limit_03 => "select * from t order by a desc",
    parse_more_select_order_limit_04 => "select * from t limit 0",
    parse_more_select_order_limit_05 => "select * from t limit 5",
    parse_more_select_order_limit_06 => "select * from t where a = 1 order by a",
    parse_more_select_order_limit_07 => "select * from t where a = 1 order by a desc",
    parse_more_select_order_limit_08 => "select * from t where a = 1 limit 3",
    parse_more_select_order_limit_09 => "select * from t where a = 1 order by a asc limit 3",
    parse_more_select_order_limit_10 => "select id,name from t order by name desc limit 2",
    parse_more_select_order_limit_11 => "select id from t where a gt 1 order by id asc limit 1",
    parse_more_select_order_limit_12 => "select id from t where a lt 1 order by id desc limit 10",
    parse_more_select_order_limit_13 => "select id from t where a gte 1 order by id limit 10",
    parse_more_select_order_limit_14 => "select id from t where a lte 1 order by id limit 10",
    parse_more_select_order_limit_15 => "select id from t where name like \"a*\" order by id limit 10",
    parse_more_select_order_limit_16 => "SeLeCt * FrOm t OrDeR By a DeSc LiMiT 1",
    parse_more_select_order_limit_17 => "select * from t where a = 1 ORDER BY a ASC LIMIT 1",
    parse_more_select_order_limit_18 => "select * from t where a eq 1 order by a limit 1",
    parse_more_select_order_limit_19 => "select a,b,c from t order by b desc limit 99",
    parse_more_select_order_limit_20 => "select * from t where name like \"?\" order by name asc limit 7",
    parse_more_index_01 => "create index on t (a)",
    parse_more_index_02 => "create index on t (a,b)",
    parse_more_index_03 => "drop index on t (a)",
    parse_more_index_04 => "drop index on t (a,b)",
    parse_more_index_05 => "CrEaTe InDeX On t (a)",
    parse_more_index_06 => "DrOp InDeX On t (a)",
    parse_more_index_07 => "create index on t(a)",
    parse_more_index_08 => "drop index on t(a)",
    parse_more_index_09 => "create index on t (a , b , c)",
    parse_more_index_10 => "drop index on t (a , b , c)",
    parse_more_alter_01 => "alter table t add unique(a)",
    parse_more_alter_02 => "alter table t add unique(a,b)",
    parse_more_alter_03 => "alter table t drop unique(a)",
    parse_more_alter_04 => "alter table t alter column a set not null",
    parse_more_alter_05 => "alter table t alter column a drop not null",
    parse_more_alter_06 => "alter table c add foreign key(a) references p(id)",
    parse_more_alter_07 => "alter table c add foreign key(a,b) references p(x,y)",
    parse_more_alter_08 => "alter table c drop foreign key(a) references p(id)",
    parse_more_alter_09 => "alter table c add foreign key(a) references p(id) on delete cascade",
    parse_more_alter_10 => "alter table c add foreign key(a) references p(id) on update no action",
    parse_more_select_combo_01 => "select * from t where a = 1 order by a desc limit 0",
    parse_more_select_combo_02 => "select a from t where a = 1 order by a desc limit 1",
    parse_more_select_combo_03 => "select a from t where a = 1 order by a desc limit 2",
    parse_more_select_combo_04 => "select a from t where a = 1 order by a desc limit 3",
    parse_more_select_combo_05 => "select a from t where a = 1 order by a desc limit 4",
    parse_more_select_combo_06 => "select a from t where a = 1 order by a desc limit 5",
    parse_more_select_combo_07 => "select a from t where a = 1 order by a desc limit 6",
    parse_more_select_combo_08 => "select a from t where a = 1 order by a desc limit 7",
    parse_more_select_combo_09 => "select a from t where a = 1 order by a desc limit 8",
    parse_more_select_combo_10 => "select a from t where a = 1 order by a desc limit 9",
    parse_more_select_combo_11 => "select a from t where a = 1 order by a asc limit 1",
    parse_more_select_combo_12 => "select a from t where a = 1 order by a asc limit 2",
    parse_more_select_combo_13 => "select a from t where a = 1 order by a asc limit 3",
    parse_more_select_combo_14 => "select a from t where a = 1 order by a asc limit 4",
    parse_more_select_combo_15 => "select a from t where a = 1 order by a asc limit 5",
    parse_more_select_combo_16 => "select a from t where a = 1 order by a asc limit 6",
    parse_more_select_combo_17 => "select a from t where a = 1 order by a asc limit 7",
    parse_more_select_combo_18 => "select a from t where a = 1 order by a asc limit 8",
    parse_more_select_combo_19 => "select a from t where a = 1 order by a asc limit 9",
    parse_more_select_combo_20 => "select a from t where a = 1 order by a asc limit 10"
);

parse_err_cases!(
    parse_more_err_01 => "select * from t order a",
    parse_more_err_02 => "select * from t order by",
    parse_more_err_03 => "select * from t order by a middle",
    parse_more_err_04 => "select * from t limit",
    parse_more_err_05 => "select * from t limit -1",
    parse_more_err_06 => "select * from t limit x",
    parse_more_err_07 => "select * from t where a = 1 limit",
    parse_more_err_08 => "select * from t where a = 1 order by",
    parse_more_err_09 => "create index t (a)",
    parse_more_err_10 => "create index on t a",
    parse_more_err_11 => "drop index t (a)",
    parse_more_err_12 => "drop index on t a",
    parse_more_err_13 => "alter table t add",
    parse_more_err_14 => "alter table t drop",
    parse_more_err_15 => "alter table t alter",
    parse_more_err_16 => "alter table t alter column a set",
    parse_more_err_17 => "alter table t alter column a drop",
    parse_more_err_18 => "select * from t order by a desc limit",
    parse_more_err_19 => "select * from t order by a desc limit none",
    parse_more_err_20 => "select * from t where a = 1 order by a desc limit 1 extra"
);

#[test]
fn parse_more_select_struct_fields_are_populated() {
    let cmd = parse("select id from users where age gte 18 order by id desc limit 4").unwrap();
    match cmd {
        Command::Select {
            table,
            columns,
            filter,
            order_by,
            limit,
            ..
        } => {
            assert_eq!(table, "users");
            assert_eq!(columns.unwrap(), vec!["id"]);
            assert!(filter.is_some());
            assert_eq!(order_by.expect("order").column, "id");
            assert_eq!(limit, Some(4));
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn parse_select_with_inner_join_basic() {
    let cmd = parse("select users.id,profiles.city from users join profiles on users.id = profiles.user_id").unwrap();
    match cmd {
        Command::Select { table, join, columns, .. } => {
            assert_eq!(table, "users");
            let j = join.expect("join");
            assert_eq!(j.join_type, JoinType::Inner);
            assert_eq!(j.table, "profiles");
            assert_eq!(j.left_column, "users.id");
            assert_eq!(j.right_column, "profiles.user_id");
            assert_eq!(columns.unwrap(), vec!["users.id", "profiles.city"]);
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_with_inner_join_where_order_limit() {
    let cmd = parse("select * from users join profiles on users.id = profiles.user_id where profiles.city = \"ny\" order by users.id desc limit 2").unwrap();
    match cmd {
        Command::Select { join, filter, order_by, limit, .. } => {
            assert!(join.is_some());
            assert!(filter.is_some());
            assert_eq!(order_by.expect("order").column, "users.id");
            assert_eq!(limit, Some(2));
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_join_bad_on_syntax_errors() {
    assert!(parse("select * from users join profiles on users.id profiles.user_id").is_err());
}

#[test]
fn parse_select_with_left_join_basic() {
    let cmd = parse("select * from users left join profiles on users.id = profiles.user_id").unwrap();
    match cmd {
        Command::Select { join, .. } => {
            let j = join.expect("left join");
            assert_eq!(j.join_type, JoinType::Left);
            assert_eq!(j.table, "profiles");
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_left_join_requires_join_keyword() {
    assert!(parse("select * from users left profiles on users.id = profiles.user_id").is_err());
}


