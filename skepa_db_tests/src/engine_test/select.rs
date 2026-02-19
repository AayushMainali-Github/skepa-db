use super::*;

#[test]
fn test_select_from_missing_table() {
    let mut db = test_db();
    let result = db.execute("select * from nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_select_where_eq_int() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram", 20)"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "alice", 30)"#)
        .unwrap();

    let result = db.execute("select * from users where age = 30").unwrap();
    assert_eq!(result, "id\tname\tage\n2\talice\t30");
}

#[test]
fn test_select_where_eq_text() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "alice")"#)
        .unwrap();

    let result = db
        .execute(r#"select * from users where name eq "ram""#)
        .unwrap();
    assert_eq!(result, "id\tname\n1\tram");
}

#[test]
fn test_select_where_gt_lt_gte_lte() {
    let mut db = test_db();
    db.execute("create table nums (id int, value int)").unwrap();
    db.execute("insert into nums values (1, 10)").unwrap();
    db.execute("insert into nums values (2, 20)").unwrap();
    db.execute("insert into nums values (3, 30)").unwrap();

    assert_eq!(
        db.execute("select * from nums where value gt 20").unwrap(),
        "id\tvalue\n3\t30"
    );
    assert_eq!(
        db.execute("select * from nums where value < 20").unwrap(),
        "id\tvalue\n1\t10"
    );
    assert_eq!(
        db.execute("select * from nums where value gte 20").unwrap(),
        "id\tvalue\n2\t20\n3\t30"
    );
    assert_eq!(
        db.execute("select * from nums where value <= 20").unwrap(),
        "id\tvalue\n1\t10\n2\t20"
    );
}

#[test]
fn test_select_where_like_patterns() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "ravi")"#)
        .unwrap();
    db.execute(r#"insert into users values (3, "amir")"#)
        .unwrap();

    assert_eq!(
        db.execute(r#"select * from users where name like "ra*""#)
            .unwrap(),
        "id\tname\n1\tram\n2\travi"
    );
    assert_eq!(
        db.execute(r#"select * from users where name like "*ir""#)
            .unwrap(),
        "id\tname\n3\tamir"
    );
    assert_eq!(
        db.execute(r#"select * from users where name like "*av*""#)
            .unwrap(),
        "id\tname\n2\travi"
    );
}

#[test]
fn test_select_where_like_single_char_wildcard() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "rom")"#)
        .unwrap();
    db.execute(r#"insert into users values (3, "ravi")"#)
        .unwrap();

    assert_eq!(
        db.execute(r#"select * from users where name like "r?m""#)
            .unwrap(),
        "id\tname\n1\tram\n2\trom"
    );
    assert_eq!(
        db.execute(r#"select * from users where name like "??vi""#)
            .unwrap(),
        "id\tname\n3\travi"
    );
    assert_eq!(
        db.execute(r#"select * from users where name like "r??""#)
            .unwrap(),
        "id\tname\n1\tram\n2\trom"
    );
}

#[test]
fn test_select_where_unknown_column_errors() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    let result = db.execute("select * from users where age = 10");
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_lowercase()
            .contains("unknown column")
    );
}

#[test]
fn test_select_where_text_comparison_with_gt_errors() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();

    let result = db.execute("select * from users where name gt 1");
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_lowercase()
            .contains("only valid for int")
    );
}

#[test]
fn test_select_where_like_on_int_errors() {
    let mut db = test_db();
    db.execute("create table users (id int, age int)").unwrap();
    db.execute("insert into users values (1, 20)").unwrap();

    let result = db.execute(r#"select * from users where age like "2*""#);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_lowercase()
            .contains("only valid for text")
    );
}

#[test]
fn test_select_specific_columns() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram", 20)"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "alice", 30)"#)
        .unwrap();

    let result = db.execute("select id,name from users").unwrap();
    assert_eq!(result, "id\tname\n1\tram\n2\talice");
}

#[test]
fn test_select_specific_columns_with_where() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram", 20)"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "alice", 30)"#)
        .unwrap();

    let result = db
        .execute("select name from users where age gte 30")
        .unwrap();
    assert_eq!(result, "name\nalice");
}

#[test]
fn test_select_star_from_with_where() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram", 20)"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "alice", 30)"#)
        .unwrap();

    let result = db.execute("select * from users where age > 20").unwrap();
    assert_eq!(result, "id\tname\tage\n2\talice\t30");
}

#[test]
fn test_select_unknown_projected_column_errors() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();

    let result = db.execute("select id,age from users");
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_lowercase()
            .contains("unknown column 'age' in select list")
    );
}

#[test]
fn test_select_where_in_numeric_and_text() {
    let mut db = test_db();
    db.execute("create table users (id int, city text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ny")"#).unwrap();
    db.execute(r#"insert into users values (2, "la")"#).unwrap();
    db.execute(r#"insert into users values (3, "sf")"#).unwrap();

    let out_ids = db
        .execute("select id from users where id in (1,3) order by id asc")
        .unwrap();
    assert_eq!(out_ids, "id\n1\n3");

    let out_city = db
        .execute(r#"select id from users where city in ("la","sf") order by id asc"#)
        .unwrap();
    assert_eq!(out_city, "id\n2\n3");
}

#[test]
fn test_select_where_and_or() {
    let mut db = test_db();
    db.execute("create table users (id int, age int, city text)")
        .unwrap();
    db.execute(r#"insert into users values (1, 20, "ny")"#)
        .unwrap();
    db.execute(r#"insert into users values (2, 17, "ny")"#)
        .unwrap();
    db.execute(r#"insert into users values (3, 19, "la")"#)
        .unwrap();
    db.execute(r#"insert into users values (4, 22, "sf")"#)
        .unwrap();

    let and_out = db
        .execute(r#"select id from users where age gte 18 and city = "ny" order by id asc"#)
        .unwrap();
    assert_eq!(and_out, "id\n1");

    let or_out = db
        .execute(r#"select id from users where city = "la" or city = "sf" order by id asc"#)
        .unwrap();
    assert_eq!(or_out, "id\n3\n4");
}

#[test]
fn test_select_where_parentheses_precedence() {
    let mut db = test_db();
    db.execute("create table users (id int, age int, city text)")
        .unwrap();
    db.execute(r#"insert into users values (1, 20, "ny")"#)
        .unwrap();
    db.execute(r#"insert into users values (2, 20, "la")"#)
        .unwrap();
    db.execute(r#"insert into users values (3, 16, "ny")"#)
        .unwrap();
    db.execute(r#"insert into users values (4, 16, "la")"#)
        .unwrap();

    let out = db
        .execute(r#"select id from users where (age gte 18 and city = "la") or city = "ny" order by id asc"#)
        .unwrap();
    assert_eq!(out, "id\n1\n2\n3");
}

#[test]
fn test_select_projection_with_spaces_after_comma() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();

    let out = db.execute("select id, name from users").unwrap();
    assert_eq!(out, "id\tname\n1\tram");
}

#[test]
fn test_select_projection_duplicate_columns() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();

    let out = db.execute("select id,id,name from users").unwrap();
    assert_eq!(out, "id\tid\tname\n1\t1\tram");
}

#[test]
fn test_select_like_exact_match_without_wildcard() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "ramesh")"#)
        .unwrap();

    let out = db
        .execute(r#"select * from users where name like "ram""#)
        .unwrap();
    assert_eq!(out, "id\tname\n1\tram");
}

#[test]
fn test_select_like_star_matches_all() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "alice")"#)
        .unwrap();

    let out = db
        .execute(r#"select * from users where name like "*""#)
        .unwrap();
    assert_eq!(out, "id\tname\n1\tram\n2\talice");
}

#[test]
fn test_select_like_question_matches_single_char_only() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a")"#).unwrap();
    db.execute(r#"insert into users values (2, "ab")"#).unwrap();

    let out = db
        .execute(r#"select * from users where name like "?""#)
        .unwrap();
    assert_eq!(out, "id\tname\n1\ta");
}

#[test]
fn test_select_on_empty_table_with_where_keeps_header() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    let out = db
        .execute(r#"select * from users where name like "*""#)
        .unwrap();
    assert_eq!(out, "id\tname");
}

#[test]
fn test_select_on_table_with_one_column() {
    let mut db = test_db();
    db.execute("create table t (id int)").unwrap();
    db.execute("insert into t values (1)").unwrap();
    assert_eq!(db.execute("select * from t").unwrap(), "id\n1");
}

#[test]
fn test_select_order_by_and_limit() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)")
        .unwrap();
    db.execute(r#"insert into users values (1, "alice", 30)"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "bob", 20)"#)
        .unwrap();
    db.execute(r#"insert into users values (3, "charlie", 25)"#)
        .unwrap();

    let out = db
        .execute("select id,name from users order by age desc limit 2")
        .unwrap();
    assert_eq!(out, "id\tname\n1\talice\n3\tcharlie");
}

#[test]
fn test_select_order_by_multiple_columns() {
    let mut db = test_db();
    db.execute("create table users (id int, city text, age int)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ny", 30)"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "la", 20)"#)
        .unwrap();
    db.execute(r#"insert into users values (3, "ny", 25)"#)
        .unwrap();
    db.execute(r#"insert into users values (4, "la", 40)"#)
        .unwrap();

    let out = db
        .execute("select id,city from users order by city asc, id desc")
        .unwrap();
    assert_eq!(out, "id\tcity\n4\tla\n2\tla\n3\tny\n1\tny");
}

#[test]
fn test_select_order_by_text_asc() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "z")"#).unwrap();
    db.execute(r#"insert into users values (2, "a")"#).unwrap();
    db.execute(r#"insert into users values (3, "m")"#).unwrap();

    let out = db.execute("select * from users order by name asc").unwrap();
    assert_eq!(out, "id\tname\n2\ta\n3\tm\n1\tz");
}

#[test]
fn engine_more_order_by_unknown_column_errors() {
    let mut db = test_db();
    seed_users_3(&mut db);
    let err = db
        .execute("select * from users order by missing asc")
        .unwrap_err();
    assert!(err.to_lowercase().contains("unknown column"));
}

#[test]
fn test_select_offset_only() {
    let mut db = test_db();
    seed_users_3(&mut db);
    let out = db
        .execute("select * from users order by id asc offset 1")
        .unwrap();
    assert_eq!(out, "id\tname\tage\n2\tb\t20\n3\tc\t10");
}

#[test]
fn test_select_limit_and_offset() {
    let mut db = test_db();
    seed_users_3(&mut db);
    let out = db
        .execute("select * from users order by id asc limit 1 offset 1")
        .unwrap();
    assert_eq!(out, "id\tname\tage\n2\tb\t20");
}

#[test]
fn test_select_offset_then_limit() {
    let mut db = test_db();
    seed_users_3(&mut db);
    let out = db
        .execute("select * from users order by id asc offset 1 limit 1")
        .unwrap();
    assert_eq!(out, "id\tname\tage\n2\tb\t20");
}

#[test]
fn test_select_order_by_nulls_asc_then_desc() {
    let mut db = test_db();
    db.execute("create table t (id int, city text)").unwrap();
    db.execute(r#"insert into t values (1, "ny")"#).unwrap();
    db.execute("insert into t values (2, null)").unwrap();
    db.execute(r#"insert into t values (3, "la")"#).unwrap();

    let asc = db.execute("select id from t order by city asc").unwrap();
    assert_eq!(asc, "id\n2\n3\n1");

    let desc = db.execute("select id from t order by city desc").unwrap();
    assert_eq!(desc, "id\n1\n3\n2");
}

#[test]
fn test_select_distinct_keeps_single_null() {
    let mut db = test_db();
    db.execute("create table t (city text)").unwrap();
    db.execute(r#"insert into t values ("ny")"#).unwrap();
    db.execute("insert into t values (null)").unwrap();
    db.execute("insert into t values (null)").unwrap();

    let out = db
        .execute("select distinct city from t order by city asc")
        .unwrap();
    assert_eq!(out, "city\nnull\nny");
}
