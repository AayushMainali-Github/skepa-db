use super::*;

#[test]
fn test_select_group_by_count_star() {
    let mut db = test_db();
    db.execute("create table users (id int, city text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ny")"#).unwrap();
    db.execute(r#"insert into users values (2, "ny")"#).unwrap();
    db.execute(r#"insert into users values (3, "la")"#).unwrap();
    let out = db
        .execute("select city,count(*) from users group by city order by city asc")
        .unwrap();
    assert_select_result(
        out,
        &["city", "count(*)"],
        vec![
            vec![Value::Text("la".to_string()), Value::BigInt(1)],
            vec![Value::Text("ny".to_string()), Value::BigInt(2)],
        ],
    );
}

#[test]
fn test_select_group_by_sum_avg_min_max() {
    let mut db = test_db();
    db.execute("create table users (id int, city text, age int)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ny", 10)"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "ny", 20)"#)
        .unwrap();
    db.execute(r#"insert into users values (3, "la", 30)"#)
        .unwrap();
    let out = db
        .execute("select city,sum(age),avg(age),min(age),max(age) from users group by city order by city asc")
        .unwrap();
    assert_select_result(
        out,
        &["city", "sum(age)", "avg(age)", "min(age)", "max(age)"],
        vec![
            vec![
                Value::Text("la".to_string()),
                Value::Int(30),
                Value::Decimal("30".parse().unwrap()),
                Value::Int(30),
                Value::Int(30),
            ],
            vec![
                Value::Text("ny".to_string()),
                Value::Int(30),
                Value::Decimal("15".parse().unwrap()),
                Value::Int(10),
                Value::Int(20),
            ],
        ],
    );
}

#[test]
fn test_select_aggregate_global_no_group_by() {
    let mut db = test_db();
    db.execute("create table users (id int, age int)").unwrap();
    db.execute("insert into users values (1, 10)").unwrap();
    db.execute("insert into users values (2, 20)").unwrap();
    let out = db
        .execute("select count(*),sum(age),avg(age),min(age),max(age) from users")
        .unwrap();
    assert_select_result(
        out,
        &["count(*)", "sum(age)", "avg(age)", "min(age)", "max(age)"],
        vec![vec![
            Value::BigInt(2),
            Value::Int(30),
            Value::Decimal("15".parse().unwrap()),
            Value::Int(10),
            Value::Int(20),
        ]],
    );
}

#[test]
fn test_select_aggregate_count_column_skips_nulls() {
    let mut db = test_db();
    db.execute("create table t (id int, city text)").unwrap();
    db.execute(r#"insert into t values (1, "ny")"#).unwrap();
    db.execute("insert into t values (2, null)").unwrap();
    let out = db.execute("select count(city),count(*) from t").unwrap();
    assert_select_result(
        out,
        &["count(city)", "count(*)"],
        vec![vec![Value::BigInt(1), Value::BigInt(2)]],
    );
}

#[test]
fn test_select_aggregate_count_distinct_global() {
    let mut db = test_db();
    db.execute("create table t (id int, city text)").unwrap();
    db.execute(r#"insert into t values (1, "ny")"#).unwrap();
    db.execute(r#"insert into t values (2, "ny")"#).unwrap();
    db.execute(r#"insert into t values (3, "la")"#).unwrap();
    db.execute("insert into t values (4, null)").unwrap();
    let out = db
        .execute("select count(distinct city),count(city),count(*) from t")
        .unwrap();
    assert_select_result(
        out,
        &["count(distinct city)", "count(city)", "count(*)"],
        vec![vec![Value::BigInt(2), Value::BigInt(3), Value::BigInt(4)]],
    );
}

#[test]
fn test_select_aggregate_sum_avg_distinct_global() {
    let mut db = test_db();
    db.execute("create table t (vi int, vd decimal(10,2))")
        .unwrap();
    db.execute("insert into t values (10, 1.00)").unwrap();
    db.execute("insert into t values (10, 1.00)").unwrap();
    db.execute("insert into t values (20, 2.00)").unwrap();
    db.execute("insert into t values (null, null)").unwrap();
    let out = db
        .execute("select sum(distinct vi),sum(vi),avg(distinct vd) from t")
        .unwrap();
    assert_select_result(
        out,
        &["sum(distinct vi)", "sum(vi)", "avg(distinct vd)"],
        vec![vec![
            Value::Int(30),
            Value::Int(40),
            Value::Decimal("1.5".parse().unwrap()),
        ]],
    );
}

#[test]
fn test_select_aggregate_min_max_distinct_global() {
    let mut db = test_db();
    db.execute("create table t (city text)").unwrap();
    db.execute(r#"insert into t values ("ny")"#).unwrap();
    db.execute(r#"insert into t values ("ny")"#).unwrap();
    db.execute(r#"insert into t values ("la")"#).unwrap();
    db.execute("insert into t values (null)").unwrap();
    let out = db
        .execute("select min(distinct city),max(distinct city),min(city),max(city) from t")
        .unwrap();
    assert_select_result(
        out,
        &[
            "min(distinct city)",
            "max(distinct city)",
            "min(city)",
            "max(city)",
        ],
        vec![vec![
            Value::Text("la".to_string()),
            Value::Text("ny".to_string()),
            Value::Text("la".to_string()),
            Value::Text("ny".to_string()),
        ]],
    );
}

#[test]
fn test_select_aggregate_distinct_star_errors() {
    let mut db = test_db();
    db.execute_legacy("create table t (id int)").unwrap();
    db.execute_legacy("insert into t values (1)").unwrap();
    let err = db
        .execute_legacy("select count(distinct *) from t")
        .unwrap_err();
    assert!(err.to_lowercase().contains("distinct") && err.contains("*"));
}

#[test]
fn test_select_aggregate_count_distinct_group_by() {
    let mut db = test_db();
    db.execute("create table t (city text, name text)").unwrap();
    db.execute(r#"insert into t values ("ny", "ram")"#).unwrap();
    db.execute(r#"insert into t values ("ny", "ram")"#).unwrap();
    db.execute(r#"insert into t values ("ny", "avi")"#).unwrap();
    db.execute(r#"insert into t values ("la", "sam")"#).unwrap();
    db.execute("insert into t values (\"la\", null)").unwrap();
    let out = db
        .execute("select city,count(distinct name) from t group by city order by city asc")
        .unwrap();
    assert_select_result(
        out,
        &["city", "count(distinct name)"],
        vec![
            vec![Value::Text("la".to_string()), Value::BigInt(1)],
            vec![Value::Text("ny".to_string()), Value::BigInt(2)],
        ],
    );
}

#[test]
fn test_select_having_count_distinct_filters_groups() {
    let mut db = test_db();
    db.execute("create table t (city text, name text)").unwrap();
    db.execute(r#"insert into t values ("ny", "ram")"#).unwrap();
    db.execute(r#"insert into t values ("ny", "avi")"#).unwrap();
    db.execute(r#"insert into t values ("la", "sam")"#).unwrap();
    db.execute(r#"insert into t values ("la", "sam")"#).unwrap();
    let out = db
        .execute(
            "select city,count(distinct name) from t group by city having count(distinct name) gt 1 order by city asc",
        )
        .unwrap();
    assert_select_result(
        out,
        &["city", "count(distinct name)"],
        vec![vec![Value::Text("ny".to_string()), Value::BigInt(2)]],
    );
}

#[test]
fn test_select_group_by_requires_non_aggregate_columns_in_group() {
    let mut db = test_db();
    db.execute_legacy("create table t (id int, city text)")
        .unwrap();
    db.execute_legacy(r#"insert into t values (1, "ny")"#)
        .unwrap();
    let err = db
        .execute_legacy("select id,count(*) from t group by city")
        .unwrap_err();
    assert!(err.to_lowercase().contains("must appear in group by"));
}

#[test]
fn test_select_aggregate_rejects_star_projection() {
    let mut db = test_db();
    db.execute_legacy("create table t (id int, city text)")
        .unwrap();
    let err = db
        .execute_legacy("select * from t group by city")
        .unwrap_err();
    assert!(err.to_lowercase().contains("cannot be used with group by"));
}

#[test]
fn test_select_aggregate_rejects_invalid_sum_type() {
    let mut db = test_db();
    db.execute_legacy("create table t (id int, city text)")
        .unwrap();
    db.execute_legacy(r#"insert into t values (1, "ny")"#)
        .unwrap();
    let err = db.execute_legacy("select sum(city) from t").unwrap_err();
    assert!(
        err.to_lowercase()
            .contains("only valid for int|bigint|decimal")
    );
}

#[test]
fn test_select_group_by_with_where_before_group() {
    let mut db = test_db();
    db.execute("create table t (id int, city text, age int)")
        .unwrap();
    db.execute(r#"insert into t values (1, "ny", 10)"#).unwrap();
    db.execute(r#"insert into t values (2, "ny", 20)"#).unwrap();
    db.execute(r#"insert into t values (3, "la", 30)"#).unwrap();
    let out = db
        .execute("select city,count(*) from t where age gte 20 group by city order by city asc")
        .unwrap();
    assert_select_result(
        out,
        &["city", "count(*)"],
        vec![
            vec![Value::Text("la".to_string()), Value::BigInt(1)],
            vec![Value::Text("ny".to_string()), Value::BigInt(1)],
        ],
    );
}

#[test]
fn test_select_group_by_having_filters_groups() {
    let mut db = test_db();
    db.execute("create table t (id int, city text)").unwrap();
    db.execute(r#"insert into t values (1, "ny")"#).unwrap();
    db.execute(r#"insert into t values (2, "ny")"#).unwrap();
    db.execute(r#"insert into t values (3, "la")"#).unwrap();
    let out = db
        .execute("select city,count(*) from t group by city having count(*) gt 1 order by city asc")
        .unwrap();
    assert_select_result(
        out,
        &["city", "count(*)"],
        vec![vec![Value::Text("ny".to_string()), Value::BigInt(2)]],
    );
}

#[test]
fn test_select_global_aggregate_having_true() {
    let mut db = test_db();
    db.execute("create table t (id int)").unwrap();
    db.execute("insert into t values (1)").unwrap();
    let out = db
        .execute("select count(*) from t having count(*) gte 1")
        .unwrap();
    assert_select_result(out, &["count(*)"], vec![vec![Value::BigInt(1)]]);
}

#[test]
fn test_select_global_aggregate_having_false_returns_header_only() {
    let mut db = test_db();
    db.execute("create table t (id int)").unwrap();
    db.execute("insert into t values (1)").unwrap();
    let out = db
        .execute("select count(*) from t having count(*) gt 1")
        .unwrap();
    assert_select_result(out, &["count(*)"], vec![]);
}

#[test]
fn test_select_having_without_group_or_aggregate_errors() {
    let mut db = test_db();
    db.execute_legacy("create table t (id int)").unwrap();
    let err = db
        .execute_legacy("select id from t having id = 1")
        .unwrap_err();
    assert!(
        err.to_lowercase()
            .contains("having requires group by or aggregate")
    );
}

#[test]
fn test_select_group_by_order_by_count_desc() {
    let mut db = test_db();
    db.execute("create table t (id int, city text)").unwrap();
    db.execute(r#"insert into t values (1, "ny")"#).unwrap();
    db.execute(r#"insert into t values (2, "ny")"#).unwrap();
    db.execute(r#"insert into t values (3, "la")"#).unwrap();
    let out = db
        .execute("select city,count(*) from t group by city order by count(*) desc")
        .unwrap();
    assert_select_result(
        out,
        &["city", "count(*)"],
        vec![
            vec![Value::Text("ny".to_string()), Value::BigInt(2)],
            vec![Value::Text("la".to_string()), Value::BigInt(1)],
        ],
    );
}

#[test]
fn test_select_group_by_order_by_sum_desc() {
    let mut db = test_db();
    db.execute("create table t (id int, city text, v int)")
        .unwrap();
    db.execute(r#"insert into t values (1, "ny", 10)"#).unwrap();
    db.execute(r#"insert into t values (2, "ny", 20)"#).unwrap();
    db.execute(r#"insert into t values (3, "la", 5)"#).unwrap();
    let out = db
        .execute("select city,sum(v) from t group by city order by sum(v) desc")
        .unwrap();
    assert_select_result(
        out,
        &["city", "sum(v)"],
        vec![
            vec![Value::Text("ny".to_string()), Value::Int(30)],
            vec![Value::Text("la".to_string()), Value::Int(5)],
        ],
    );
}

#[test]
fn test_select_projection_alias_headers() {
    let mut db = test_db();
    seed_users_3(&mut db);
    let out = db
        .execute("select id as uid,name as uname from users order by id asc")
        .unwrap();
    assert_select_result(
        out,
        &["uid", "uname"],
        vec![
            vec![Value::Int(1), Value::Text("a".to_string())],
            vec![Value::Int(2), Value::Text("b".to_string())],
            vec![Value::Int(3), Value::Text("c".to_string())],
        ],
    );
}

#[test]
fn test_select_order_by_non_grouped_alias() {
    let mut db = test_db();
    seed_users_3(&mut db);
    let out = db
        .execute("select id as uid,name from users order by uid desc")
        .unwrap();
    assert_select_result(
        out,
        &["uid", "name"],
        vec![
            vec![Value::Int(3), Value::Text("c".to_string())],
            vec![Value::Int(2), Value::Text("b".to_string())],
            vec![Value::Int(1), Value::Text("a".to_string())],
        ],
    );
}

#[test]
fn test_select_order_by_grouped_alias() {
    let mut db = test_db();
    db.execute("create table t (id int, city text)").unwrap();
    db.execute(r#"insert into t values (1, "ny")"#).unwrap();
    db.execute(r#"insert into t values (2, "ny")"#).unwrap();
    db.execute(r#"insert into t values (3, "la")"#).unwrap();
    let out = db
        .execute("select city,count(*) as c from t group by city order by c desc")
        .unwrap();
    assert_select_result(
        out,
        &["city", "c"],
        vec![
            vec![Value::Text("ny".to_string()), Value::BigInt(2)],
            vec![Value::Text("la".to_string()), Value::BigInt(1)],
        ],
    );
}

#[test]
fn test_select_distinct_single_column() {
    let mut db = test_db();
    db.execute("create table t (id int, city text)").unwrap();
    db.execute(r#"insert into t values (1, "ny")"#).unwrap();
    db.execute(r#"insert into t values (2, "ny")"#).unwrap();
    db.execute(r#"insert into t values (3, "la")"#).unwrap();
    let out = db
        .execute("select distinct city from t order by city asc")
        .unwrap();
    assert_select_result(
        out,
        &["city"],
        vec![
            vec![Value::Text("la".to_string())],
            vec![Value::Text("ny".to_string())],
        ],
    );
}

#[test]
fn test_select_distinct_multiple_columns() {
    let mut db = test_db();
    db.execute("create table t (id int, city text)").unwrap();
    db.execute(r#"insert into t values (1, "ny")"#).unwrap();
    db.execute(r#"insert into t values (1, "ny")"#).unwrap();
    db.execute(r#"insert into t values (2, "ny")"#).unwrap();
    let out = db
        .execute("select distinct id,city from t order by id asc")
        .unwrap();
    assert_select_result(
        out,
        &["id", "city"],
        vec![
            vec![Value::Int(1), Value::Text("ny".to_string())],
            vec![Value::Int(2), Value::Text("ny".to_string())],
        ],
    );
}

#[test]
fn test_select_distinct_with_limit_offset() {
    let mut db = test_db();
    db.execute("create table t (id int)").unwrap();
    db.execute("insert into t values (1)").unwrap();
    db.execute("insert into t values (1)").unwrap();
    db.execute("insert into t values (2)").unwrap();
    db.execute("insert into t values (3)").unwrap();
    let out = db
        .execute("select distinct id from t order by id asc offset 1 limit 1")
        .unwrap();
    assert_select_result(out, &["id"], vec![vec![Value::Int(2)]]);
}

#[test]
fn test_select_offset_with_grouped_result() {
    let mut db = test_db();
    db.execute("create table t (id int, city text)").unwrap();
    db.execute(r#"insert into t values (1, "ny")"#).unwrap();
    db.execute(r#"insert into t values (2, "ny")"#).unwrap();
    db.execute(r#"insert into t values (3, "la")"#).unwrap();
    let out = db
        .execute("select city,count(*) from t group by city order by city asc offset 1")
        .unwrap();
    assert_select_result(
        out,
        &["city", "count(*)"],
        vec![vec![Value::Text("ny".to_string()), Value::BigInt(2)]],
    );
}

#[test]
fn test_select_global_aggregates_on_empty_table_returns_one_row() {
    let mut db = test_db();
    db.execute("create table t (id int, v int)").unwrap();
    let out = db
        .execute("select count(*),count(v),sum(v),avg(v),min(v),max(v) from t")
        .unwrap();
    assert_select_result(
        out,
        &[
            "count(*)", "count(v)", "sum(v)", "avg(v)", "min(v)", "max(v)",
        ],
        vec![vec![
            Value::BigInt(0),
            Value::BigInt(0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ]],
    );
}

#[test]
fn test_select_global_aggregate_with_where_no_rows_returns_one_row() {
    let mut db = test_db();
    db.execute("create table t (id int, v int)").unwrap();
    db.execute("insert into t values (1, 10)").unwrap();
    let out = db
        .execute("select count(*),sum(v),avg(v),min(v),max(v) from t where v gt 99")
        .unwrap();
    assert_select_result(
        out,
        &["count(*)", "sum(v)", "avg(v)", "min(v)", "max(v)"],
        vec![vec![
            Value::BigInt(0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ]],
    );
}
