use super::*;

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
fn create_rejects_unknown_datatype() {
    let err = parse("create table users (id integer, name text)").unwrap_err();
    assert!(
        err.to_lowercase().contains("unknown type")
            || err.to_lowercase().contains("use int|text")
    );
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
fn parse_create_foreign_key_accepts_on_update_before_on_delete() {
    let cmd = parse(
        "create table c (a int, foreign key(a) references p(id) on update cascade on delete set null)",
    )
    .unwrap();
    match cmd {
        Command::Create {
            table_constraints, ..
        } => {
            assert_eq!(table_constraints.len(), 1);
            match &table_constraints[0] {
                skepa_db_core::parser::command::TableConstraintDef::ForeignKey {
                    on_delete,
                    on_update,
                    ..
                } => {
                    assert_eq!(
                        *on_update,
                        skepa_db_core::parser::command::ForeignKeyAction::Cascade
                    );
                    assert_eq!(
                        *on_delete,
                        skepa_db_core::parser::command::ForeignKeyAction::SetNull
                    );
                }
                _ => panic!("Expected foreign key table constraint"),
            }
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
fn parse_create_rejects_unknown_table_constraint_token() {
    let err = parse("create table t (a int, foreign key(a))").unwrap_err();
    assert!(err.to_lowercase().contains("references"));
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
fn parse_create_index_missing_table_errors() {
    assert!(parse("create index on (id)").is_err());
}

