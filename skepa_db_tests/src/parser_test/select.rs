use super::*;

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
            let p = pred(&f);
            assert_eq!(p.column, "name");
            assert_eq!(p.op, CompareOp::Eq);
            assert_eq!(p.value, "ram");
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
            let p = pred(&f);
            assert_eq!(p.column, "age");
            assert_eq!(p.op, CompareOp::Gte);
            assert_eq!(p.value, "18");
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
            let p = pred(&f);
            assert_eq!(p.column, "name");
            assert_eq!(p.op, CompareOp::Like);
            assert_eq!(p.value, "ra*");
        }
        _ => panic!("Expected Select command"),
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
            let p = pred(&f);
            assert_eq!(p.column, "name");
            assert_eq!(p.op, CompareOp::Like);
            assert_eq!(p.value, "ra*");
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn select_with_extra_tokens_errors() {
    let err = parse("select * users now").unwrap_err();
    assert!(err.to_lowercase().contains("usage: select"));
}

#[test]
fn select_with_bad_where_operator_errors() {
    let err = parse("select * from users where age between 1").unwrap_err();
    assert!(err.to_lowercase().contains("unknown where operator"));
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
fn parse_like_operator_on_select_is_case_insensitive_keyword() {
    let cmd = parse(r#"select * from users where name LIKE "a*""#).unwrap();
    match cmd {
        Command::Select { filter, .. } => {
            let f = filter.expect("expected where clause");
            let pf = pred(&f);
            assert_eq!(pf.op, CompareOp::Like);
            assert_eq!(pf.value, "a*");
        }
        _ => panic!("Expected Select command"),
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
fn parse_select_where_value_can_be_quoted_spaces() {
    let cmd = parse(r#"select * from t where name = "hello world""#).unwrap();
    match cmd {
        Command::Select { filter, .. } => { let wf = filter.expect("where"); assert_eq!(pred(&wf).value, "hello world") },
        _ => panic!("Expected Select command"),
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
            assert_eq!(pred(&f).column, "age");
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
            let ob = order_by.expect("order");
            assert_eq!(ob.column, "id");
            assert!(ob.then_by.is_empty());
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_with_multi_order_by() {
    let cmd = parse("select * from users order by city asc, id desc").unwrap();
    match cmd {
        Command::Select { order_by, .. } => {
            let ob = order_by.expect("order");
            assert_eq!(ob.column, "city");
            assert_eq!(ob.then_by, vec![("id".to_string(), false)]);
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
            let pf = pred(&f);
            assert_eq!(pf.column, "city");
            assert_eq!(pf.op, CompareOp::IsNull);
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
            assert_eq!(pred(&f).op, CompareOp::IsNotNull);
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_where_in_list() {
    let cmd = parse("select * from users where id in (1,2,3)").unwrap();
    match cmd {
        Command::Select { filter, .. } => {
            let f = filter.expect("where");
            let pf = pred(&f);
            assert_eq!(pf.column, "id");
            assert_eq!(pf.op, CompareOp::In);
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_where_in_invalid_syntax_errors() {
    assert!(parse("select * from users where id in 1,2").is_err());
    assert!(parse("select * from users where id in ()").is_err());
    assert!(parse("select * from users where id in (1,)").is_err());
}

#[test]
fn parse_select_where_and_chain() {
    let cmd = parse("select * from users where age gt 18 and city = \"ny\"").unwrap();
    match cmd {
        Command::Select { filter, .. } => {
            let f = filter.expect("where");
            match f {
                WhereClause::Binary { .. } => {}
                _ => panic!("expected binary where expression"),
            }
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_where_or_chain() {
    let cmd = parse("select * from users where city = \"ny\" or city = \"la\"").unwrap();
    match cmd {
        Command::Select { filter, .. } => { match filter.expect("where") { WhereClause::Binary { .. } => {}, _ => panic!("expected binary where expression") } },
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_where_parentheses() {
    let cmd = parse("select * from users where (age gt 18 or city = \"ny\") and city is not null").unwrap();
    match cmd {
        Command::Select { filter, .. } => match filter.expect("where") {
            WhereClause::Binary { .. } => {}
            _ => panic!("expected binary expression"),
        },
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_multi_order_by_trailing_comma_errors() {
    assert!(parse("select * from t order by a,").is_err());
}

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

#[test]
fn parse_select_group_by_basic() {
    let cmd = parse("select city,count(*) from users group by city").unwrap();
    match cmd {
        Command::Select { columns, group_by, .. } => {
            assert_eq!(columns.unwrap(), vec!["city", "count(*)"]);
            assert_eq!(group_by.expect("group by"), vec!["city"]);
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_group_by_where_order_limit() {
    let cmd = parse("select city,sum(age) from users where age gte 10 group by city order by city asc limit 3").unwrap();
    match cmd {
        Command::Select { filter, group_by, order_by, limit, .. } => {
            assert!(filter.is_some());
            assert_eq!(group_by.expect("group by"), vec!["city"]);
            assert_eq!(order_by.expect("order").column, "city");
            assert_eq!(limit, Some(3));
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_aggregate_without_group_by() {
    let cmd = parse("select count(*),avg(age) from users").unwrap();
    match cmd {
        Command::Select { columns, group_by, .. } => {
            assert_eq!(columns.unwrap(), vec!["count(*)", "avg(age)"]);
            assert!(group_by.is_none());
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_count_distinct_without_group_by() {
    let cmd = parse("select count(distinct city) from users").unwrap();
    match cmd {
        Command::Select { columns, .. } => {
            assert_eq!(columns.unwrap(), vec!["count(distinct city)"]);
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_sum_distinct_without_group_by() {
    let cmd = parse("select sum(distinct age) from users").unwrap();
    match cmd {
        Command::Select { columns, .. } => {
            assert_eq!(columns.unwrap(), vec!["sum(distinct age)"]);
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_count_distinct_with_group_by_having() {
    let cmd = parse(
        "select city,count(distinct name) from users group by city having count(distinct name) gt 0",
    )
    .unwrap();
    match cmd {
        Command::Select {
            columns,
            group_by,
            having,
            ..
        } => {
            assert_eq!(
                columns.unwrap(),
                vec!["city".to_string(), "count(distinct name)".to_string()]
            );
            assert_eq!(group_by.expect("group by"), vec!["city"]);
            let hv = having.expect("having");
            let p = pred(&hv);
            assert_eq!(p.column, "count(distinct name)");
            assert_eq!(p.op, CompareOp::Gt);
            assert_eq!(p.value, "0");
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_group_by_missing_by_errors() {
    assert!(parse("select city from users group city").is_err());
}

#[test]
fn parse_select_group_by_having() {
    let cmd = parse("select city,count(*) from users group by city having count(*) gt 1 order by city asc").unwrap();
    match cmd {
        Command::Select { group_by, having, .. } => {
            assert_eq!(group_by.expect("group by"), vec!["city"]);
            let h = having.expect("having");
            let p = pred(&h);
            assert_eq!(p.column, "count(*)");
            assert_eq!(p.op, CompareOp::Gt);
            assert_eq!(p.value, "1");
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_having_without_group_by_is_parsed() {
    let cmd = parse("select count(*) from users having count(*) gt 0").unwrap();
    match cmd {
        Command::Select { having, .. } => assert!(having.is_some()),
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_group_having_order_by_aggregate() {
    let cmd = parse("select city,count(*) from users group by city having count(*) gt 0 order by count(*) desc").unwrap();
    match cmd {
        Command::Select { order_by, .. } => {
            let ob = order_by.expect("order by");
            assert_eq!(ob.column, "count(*)");
            assert!(!ob.asc);
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_with_offset_only() {
    let cmd = parse("select * from users offset 3").unwrap();
    match cmd {
        Command::Select { offset, limit, .. } => {
            assert_eq!(offset, Some(3));
            assert_eq!(limit, None);
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_with_limit_and_offset() {
    let cmd = parse("select * from users order by id asc limit 5 offset 2").unwrap();
    match cmd {
        Command::Select { offset, limit, .. } => {
            assert_eq!(offset, Some(2));
            assert_eq!(limit, Some(5));
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_with_offset_then_limit() {
    let cmd = parse("select * from users order by id asc offset 2 limit 5").unwrap();
    match cmd {
        Command::Select { offset, limit, .. } => {
            assert_eq!(offset, Some(2));
            assert_eq!(limit, Some(5));
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_duplicate_limit_or_offset_errors() {
    assert!(parse("select * from users limit 1 limit 2").is_err());
    assert!(parse("select * from users offset 1 offset 2").is_err());
}

#[test]
fn parse_select_with_aliases() {
    let cmd = parse("select id as uid,name as uname from users").unwrap();
    match cmd {
        Command::Select { columns, .. } => {
            assert_eq!(
                columns.unwrap(),
                vec!["id as uid".to_string(), "name as uname".to_string()]
            );
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_order_by_alias() {
    let cmd = parse("select city,count(*) as c from users group by city order by c desc").unwrap();
    match cmd {
        Command::Select { columns, order_by, .. } => {
            assert_eq!(columns.unwrap(), vec!["city".to_string(), "count(*) as c".to_string()]);
            let ob = order_by.expect("order by");
            assert_eq!(ob.column, "c");
            assert!(!ob.asc);
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_distinct_basic() {
    let cmd = parse("select distinct city from users").unwrap();
    match cmd {
        Command::Select { distinct, columns, .. } => {
            assert!(distinct);
            assert_eq!(columns.unwrap(), vec!["city".to_string()]);
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_distinct_star() {
    let cmd = parse("select distinct * from users").unwrap();
    match cmd {
        Command::Select {
            distinct, columns, ..
        } => {
            assert!(distinct);
            assert_eq!(columns.unwrap(), Vec::<String>::new());
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_order_by_function_with_spaced_parens() {
    let cmd = parse("select city,count(*) from users group by city order by count ( * ) desc")
        .unwrap();
    match cmd {
        Command::Select { order_by, .. } => {
            let ob = order_by.expect("order by");
            assert_eq!(ob.column, "count(*)");
            assert!(!ob.asc);
        }
        _ => panic!("Expected Select command"),
    }
}

#[test]
fn parse_select_with_empty_group_by_errors() {
    assert!(parse("select city,count(*) from users group by").is_err());
}

#[test]
fn parse_select_with_empty_having_errors() {
    assert!(parse("select city,count(*) from users group by city having").is_err());
}

#[test]
fn parse_select_order_by_missing_column_errors() {
    assert!(parse("select * from users order by").is_err());
}

#[test]
fn parse_select_order_by_trailing_comma_errors() {
    assert!(parse("select * from users order by id,").is_err());
}

#[test]
fn parse_select_limit_non_numeric_errors() {
    assert!(parse("select * from users limit two").is_err());
}

#[test]
fn parse_select_offset_non_numeric_errors() {
    assert!(parse("select * from users offset two").is_err());
}

#[test]
fn parse_select_limit_negative_errors() {
    assert!(parse("select * from users limit -1").is_err());
}

#[test]
fn parse_select_offset_negative_errors() {
    assert!(parse("select * from users offset -1").is_err());
}

#[test]
fn parse_select_where_nested_parentheses() {
    let cmd = parse("select * from users where ((age gt 18) and (city = \"ny\"))").unwrap();
    match cmd {
        Command::Select { filter, .. } => assert!(filter.is_some()),
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_select_join_with_missing_on_rhs_errors() {
    assert!(parse("select * from users join p on users.id = ").is_err());
}

#[test]
fn parse_select_having_without_group_with_order_and_limit() {
    let cmd = parse("select count(*) from users having count(*) gt 0 order by count(*) desc limit 1").unwrap();
    match cmd {
        Command::Select {
            columns,
            having,
            order_by,
            limit,
            ..
        } => {
            assert_eq!(columns.unwrap(), vec!["count(*)".to_string()]);
            assert!(having.is_some());
            assert!(order_by.is_some());
            assert_eq!(limit, Some(1));
        }
        _ => panic!("expected select"),
    }
}

