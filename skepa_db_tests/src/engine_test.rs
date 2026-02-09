use skepa_db_core::Database;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

fn test_db() -> Database {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_test_{}_{}", std::process::id(), id));
    let _ = std::fs::remove_dir_all(&path);
    Database::open(path)
}
#[test]
fn test_create_table() {
    let mut db = test_db();
    let result = db.execute("create table users (id int, name text)").unwrap();
    assert_eq!(result, "created table users");
}

#[test]
fn test_create_and_select_empty() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    let result = db.execute("select * from users").unwrap();
    assert_eq!(result, "id\tname");
}

#[test]
fn test_create_insert_select() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();

    let insert_result = db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    assert_eq!(insert_result, "inserted 1 row into users");

    let select_result = db.execute("select * from users").unwrap();
    assert_eq!(select_result, "id\tname\n1\tram");
}

#[test]
fn test_insert_multiple_rows() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into users values (2, "alice")"#).unwrap();

    let result = db.execute("select * from users").unwrap();
    assert_eq!(result, "id\tname\n1\tram\n2\talice");
}

#[test]
fn test_insert_into_missing_table() {
    let mut db = test_db();
    let result = db.execute(r#"insert into users values (1, "ram")"#);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_insert_wrong_value_count() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();

    let result = db.execute(r#"insert into users values (1, "ram", "extra")"#);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Expected 2 values but got 3"));
}

#[test]
fn test_insert_type_mismatch_int() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();

    let result = db.execute(r#"insert into users values ("abc", "ram")"#);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("expected int"));
}

#[test]
fn test_select_from_missing_table() {
    let mut db = test_db();
    let result = db.execute("select * from nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_create_duplicate_table() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();

    let result = db.execute("create table users (id int, name text)");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists"));
}

#[test]
fn test_multiple_tables() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute("create table products (name text, price int)").unwrap();

    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into products values ("laptop", 1000)"#).unwrap();

    let users = db.execute("select * from users").unwrap();
    assert_eq!(users, "id\tname\n1\tram");

    let products = db.execute("select * from products").unwrap();
    assert_eq!(products, "name\tprice\nlaptop\t1000");
}

#[test]
fn test_empty_string_text_value() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "")"#).unwrap();

    let result = db.execute("select * from users").unwrap();
    assert_eq!(result, "id\tname\n1\t");
}

#[test]
fn test_text_with_spaces() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram kumar")"#).unwrap();

    let result = db.execute("select * from users").unwrap();
    assert_eq!(result, "id\tname\n1\tram kumar");
}

#[test]
fn test_negative_integers() {
    let mut db = test_db();
    db.execute("create table temps (id int, value int)").unwrap();
    db.execute("insert into temps values (1, -10)").unwrap();

    let result = db.execute("select * from temps").unwrap();
    assert_eq!(result, "id\tvalue\n1\t-10");
}

#[test]
fn test_large_integers() {
    let mut db = test_db();
    db.execute("create table nums (id int, value int)").unwrap();
    db.execute("insert into nums values (1, 999999999)").unwrap();

    let result = db.execute("select * from nums").unwrap();
    assert_eq!(result, "id\tvalue\n1\t999999999");
}

#[test]
fn test_select_where_eq_int() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)").unwrap();
    db.execute(r#"insert into users values (1, "ram", 20)"#).unwrap();
    db.execute(r#"insert into users values (2, "alice", 30)"#).unwrap();

    let result = db.execute("select * from users where age = 30").unwrap();
    assert_eq!(result, "id\tname\tage\n2\talice\t30");
}

#[test]
fn test_select_where_eq_text() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into users values (2, "alice")"#).unwrap();

    let result = db.execute(r#"select * from users where name eq "ram""#).unwrap();
    assert_eq!(result, "id\tname\n1\tram");
}

#[test]
fn test_select_where_gt_lt_gte_lte() {
    let mut db = test_db();
    db.execute("create table nums (id int, value int)").unwrap();
    db.execute("insert into nums values (1, 10)").unwrap();
    db.execute("insert into nums values (2, 20)").unwrap();
    db.execute("insert into nums values (3, 30)").unwrap();

    assert_eq!(db.execute("select * from nums where value gt 20").unwrap(), "id\tvalue\n3\t30");
    assert_eq!(db.execute("select * from nums where value < 20").unwrap(), "id\tvalue\n1\t10");
    assert_eq!(db.execute("select * from nums where value gte 20").unwrap(), "id\tvalue\n2\t20\n3\t30");
    assert_eq!(db.execute("select * from nums where value <= 20").unwrap(), "id\tvalue\n1\t10\n2\t20");
}

#[test]
fn test_select_where_like_patterns() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into users values (2, "ravi")"#).unwrap();
    db.execute(r#"insert into users values (3, "amir")"#).unwrap();

    assert_eq!(db.execute(r#"select * from users where name like "ra*""#).unwrap(), "id\tname\n1\tram\n2\travi");
    assert_eq!(db.execute(r#"select * from users where name like "*ir""#).unwrap(), "id\tname\n3\tamir");
    assert_eq!(db.execute(r#"select * from users where name like "*av*""#).unwrap(), "id\tname\n2\travi");
}

#[test]
fn test_select_where_like_single_char_wildcard() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into users values (2, "rom")"#).unwrap();
    db.execute(r#"insert into users values (3, "ravi")"#).unwrap();

    assert_eq!(db.execute(r#"select * from users where name like "r?m""#).unwrap(), "id\tname\n1\tram\n2\trom");
    assert_eq!(db.execute(r#"select * from users where name like "??vi""#).unwrap(), "id\tname\n3\travi");
    assert_eq!(db.execute(r#"select * from users where name like "r??""#).unwrap(), "id\tname\n1\tram\n2\trom");
}

#[test]
fn test_select_where_unknown_column_errors() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    let result = db.execute("select * from users where age = 10");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("unknown column"));
}

#[test]
fn test_select_where_text_comparison_with_gt_errors() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();

    let result = db.execute("select * from users where name gt 1");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("only valid for int"));
}

#[test]
fn test_select_where_like_on_int_errors() {
    let mut db = test_db();
    db.execute("create table users (id int, age int)").unwrap();
    db.execute("insert into users values (1, 20)").unwrap();

    let result = db.execute(r#"select * from users where age like "2*""#);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("only valid for text"));
}

#[test]
fn test_select_specific_columns() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)").unwrap();
    db.execute(r#"insert into users values (1, "ram", 20)"#).unwrap();
    db.execute(r#"insert into users values (2, "alice", 30)"#).unwrap();

    let result = db.execute("select id,name from users").unwrap();
    assert_eq!(result, "id\tname\n1\tram\n2\talice");
}

#[test]
fn test_select_specific_columns_with_where() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)").unwrap();
    db.execute(r#"insert into users values (1, "ram", 20)"#).unwrap();
    db.execute(r#"insert into users values (2, "alice", 30)"#).unwrap();

    let result = db.execute("select name from users where age gte 30").unwrap();
    assert_eq!(result, "name\nalice");
}

#[test]
fn test_select_star_from_with_where() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)").unwrap();
    db.execute(r#"insert into users values (1, "ram", 20)"#).unwrap();
    db.execute(r#"insert into users values (2, "alice", 30)"#).unwrap();

    let result = db.execute("select * from users where age > 20").unwrap();
    assert_eq!(result, "id\tname\tage\n2\talice\t30");
}

#[test]
fn test_select_unknown_projected_column_errors() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();

    let result = db.execute("select id,age from users");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("unknown column 'age' in select list"));
}

#[test]
fn test_update_single_column_where_eq() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)").unwrap();
    db.execute(r#"insert into users values (1, "ram", 20)"#).unwrap();
    db.execute(r#"insert into users values (2, "alice", 30)"#).unwrap();

    let out = db.execute(r#"update users set name = "ravi" where id = 1"#).unwrap();
    assert_eq!(out, "updated 1 row(s) in users");

    let result = db.execute("select * from users").unwrap();
    assert_eq!(result, "id\tname\tage\n1\travi\t20\n2\talice\t30");
}

#[test]
fn test_update_multiple_columns() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)").unwrap();
    db.execute(r#"insert into users values (1, "ram", 20)"#).unwrap();

    let out = db.execute(r#"update users set name = "ravi", age = 25 where id eq 1"#).unwrap();
    assert_eq!(out, "updated 1 row(s) in users");

    let result = db.execute("select * from users").unwrap();
    assert_eq!(result, "id\tname\tage\n1\travi\t25");
}

#[test]
fn test_update_where_like() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)").unwrap();
    db.execute(r#"insert into users values (1, "ram", 20)"#).unwrap();
    db.execute(r#"insert into users values (2, "rom", 30)"#).unwrap();
    db.execute(r#"insert into users values (3, "alice", 40)"#).unwrap();

    let out = db.execute(r#"update users set age = 99 where name like "r?m""#).unwrap();
    assert_eq!(out, "updated 2 row(s) in users");

    let result = db.execute("select * from users").unwrap();
    assert_eq!(result, "id\tname\tage\n1\tram\t99\n2\trom\t99\n3\talice\t40");
}

#[test]
fn test_update_unknown_set_column_errors() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();

    let result = db.execute(r#"update users set age = 20 where id = 1"#);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("unknown column 'age' in update"));
}

#[test]
fn test_update_type_mismatch_errors() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)").unwrap();
    db.execute(r#"insert into users values (1, "ram", 20)"#).unwrap();

    let result = db.execute(r#"update users set age = "bad" where id = 1"#);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("expected int"));
}

#[test]
fn test_delete_where_eq() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into users values (2, "alice")"#).unwrap();

    let out = db.execute("delete from users where id = 1").unwrap();
    assert_eq!(out, "deleted 1 row(s) from users");

    let result = db.execute("select * from users").unwrap();
    assert_eq!(result, "id\tname\n2\talice");
}

#[test]
fn test_delete_where_like() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into users values (2, "rom")"#).unwrap();
    db.execute(r#"insert into users values (3, "alice")"#).unwrap();

    let out = db.execute(r#"delete from users where name like "r?m""#).unwrap();
    assert_eq!(out, "deleted 2 row(s) from users");

    let result = db.execute("select * from users").unwrap();
    assert_eq!(result, "id\tname\n3\talice");
}

#[test]
fn test_delete_unknown_column_errors() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();

    let result = db.execute("delete from users where age = 10");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("unknown column"));
}

#[test]
fn test_delete_no_match_returns_zero() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();

    let out = db.execute("delete from users where id = 99").unwrap();
    assert_eq!(out, "deleted 0 row(s) from users");
}

#[test]
fn test_delete_numeric_gt() {
    let mut db = test_db();
    db.execute("create table nums (id int, v int)").unwrap();
    db.execute("insert into nums values (1, 10)").unwrap();
    db.execute("insert into nums values (2, 20)").unwrap();
    db.execute("insert into nums values (3, 30)").unwrap();

    let out = db.execute("delete from nums where v > 15").unwrap();
    assert_eq!(out, "deleted 2 row(s) from nums");
    assert_eq!(db.execute("select * from nums").unwrap(), "id\tv\n1\t10");
}

#[test]
fn test_delete_numeric_lte() {
    let mut db = test_db();
    db.execute("create table nums (id int, v int)").unwrap();
    db.execute("insert into nums values (1, 10)").unwrap();
    db.execute("insert into nums values (2, 20)").unwrap();
    db.execute("insert into nums values (3, 30)").unwrap();

    let out = db.execute("delete from nums where v <= 20").unwrap();
    assert_eq!(out, "deleted 2 row(s) from nums");
    assert_eq!(db.execute("select * from nums").unwrap(), "id\tv\n3\t30");
}

#[test]
fn test_delete_like_star_removes_all_rows() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into users values (2, "alice")"#).unwrap();

    let out = db.execute(r#"delete from users where name like "*""#).unwrap();
    assert_eq!(out, "deleted 2 row(s) from users");
    assert_eq!(db.execute("select * from users").unwrap(), "id\tname");
}

#[test]
fn test_delete_like_question_single_char() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "a")"#).unwrap();
    db.execute(r#"insert into users values (2, "bb")"#).unwrap();

    let out = db.execute(r#"delete from users where name like "?""#).unwrap();
    assert_eq!(out, "deleted 1 row(s) from users");
    assert_eq!(db.execute("select * from users").unwrap(), "id\tname\n2\tbb");
}

#[test]
fn test_delete_text_with_gt_errors() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();

    let result = db.execute("delete from users where name > 1");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("only valid for int"));
}

#[test]
fn test_update_no_match_returns_zero() {
    let mut db = test_db();
    db.execute("create table users (id int, age int)").unwrap();
    db.execute("insert into users values (1, 20)").unwrap();

    let out = db.execute("update users set age = 21 where id = 99").unwrap();
    assert_eq!(out, "updated 0 row(s) in users");
}

#[test]
fn test_update_unknown_where_column_errors() {
    let mut db = test_db();
    db.execute("create table users (id int, age int)").unwrap();
    db.execute("insert into users values (1, 20)").unwrap();

    let result = db.execute("update users set age = 21 where missing = 1");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("unknown column"));
}

#[test]
fn test_update_where_like_on_int_errors() {
    let mut db = test_db();
    db.execute("create table users (id int, age int)").unwrap();
    db.execute("insert into users values (1, 20)").unwrap();

    let result = db.execute(r#"update users set age = 21 where age like "2*""#);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("only valid for text"));
}

#[test]
fn test_update_changes_multiple_rows_with_numeric_predicate() {
    let mut db = test_db();
    db.execute("create table nums (id int, v int)").unwrap();
    db.execute("insert into nums values (1, 10)").unwrap();
    db.execute("insert into nums values (2, 20)").unwrap();
    db.execute("insert into nums values (3, 30)").unwrap();

    let out = db.execute("update nums set v = 0 where v >= 20").unwrap();
    assert_eq!(out, "updated 2 row(s) in nums");
    assert_eq!(db.execute("select * from nums").unwrap(), "id\tv\n1\t10\n2\t0\n3\t0");
}

#[test]
fn test_update_can_set_text_empty_string() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();

    let out = db.execute(r#"update users set name = "" where id = 1"#).unwrap();
    assert_eq!(out, "updated 1 row(s) in users");
    assert_eq!(db.execute("select * from users").unwrap(), "id\tname\n1\t");
}

#[test]
fn test_select_projection_with_spaces_after_comma() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();

    let out = db.execute("select id, name from users").unwrap();
    assert_eq!(out, "id\tname\n1\tram");
}

#[test]
fn test_select_projection_duplicate_columns() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();

    let out = db.execute("select id,id,name from users").unwrap();
    assert_eq!(out, "id\tid\tname\n1\t1\tram");
}

#[test]
fn test_select_like_exact_match_without_wildcard() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into users values (2, "ramesh")"#).unwrap();

    let out = db.execute(r#"select * from users where name like "ram""#).unwrap();
    assert_eq!(out, "id\tname\n1\tram");
}

#[test]
fn test_select_like_star_matches_all() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into users values (2, "alice")"#).unwrap();

    let out = db.execute(r#"select * from users where name like "*""#).unwrap();
    assert_eq!(out, "id\tname\n1\tram\n2\talice");
}

#[test]
fn test_select_like_question_matches_single_char_only() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "a")"#).unwrap();
    db.execute(r#"insert into users values (2, "ab")"#).unwrap();

    let out = db.execute(r#"select * from users where name like "?""#).unwrap();
    assert_eq!(out, "id\tname\n1\ta");
}

#[test]
fn test_select_on_empty_table_with_where_keeps_header() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    let out = db.execute(r#"select * from users where name like "*""#).unwrap();
    assert_eq!(out, "id\tname");
}

#[test]
fn test_delete_from_missing_table_errors() {
    let mut db = test_db();
    let result = db.execute("delete from users where id = 1");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("does not exist"));
}

#[test]
fn test_update_missing_table_errors() {
    let mut db = test_db();
    let result = db.execute("update users set id = 1 where id = 1");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("does not exist"));
}

#[test]
fn test_extended_types_insert_and_select() {
    let mut db = test_db();
    db.execute("create table t (b bool, i int, bi bigint, d decimal(10,2), v varchar(5), tx text, dt date, ts timestamp, u uuid, j json, bl blob)").unwrap();
    db.execute(r#"insert into t values (true, 12, 999999999999, 12.34, "hello", "world", 2025-01-02, "2025-01-02 03:04:05", 550e8400-e29b-41d4-a716-446655440000, "{\"a\":1}", 0xDEADBEEF)"#).unwrap();
    let out = db.execute("select * from t").unwrap();
    assert!(out.contains("true"));
    assert!(out.contains("999999999999"));
    assert!(out.contains("12.34"));
    assert!(out.contains("2025-01-02"));
    assert!(out.contains("550e8400-e29b-41d4-a716-446655440000"));
    assert!(out.contains("{\"a\":1}"));
    assert!(out.contains("0xDEADBEEF"));
}

#[test]
fn test_varchar_length_enforced() {
    let mut db = test_db();
    db.execute("create table t (v varchar(3))").unwrap();
    let err = db
        .execute(r#"insert into t values ("hello")"#)
        .unwrap_err();
    assert!(err.to_lowercase().contains("varchar(3)"));
}

#[test]
fn test_decimal_precision_and_scale_enforced() {
    let mut db = test_db();
    db.execute("create table t (d decimal(5,2))").unwrap();
    let e1 = db.execute("insert into t values (12345.67)").unwrap_err();
    assert!(e1.to_lowercase().contains("precision"));
    let e2 = db.execute("insert into t values (12.345)").unwrap_err();
    assert!(e2.to_lowercase().contains("scale"));
}

#[test]
fn test_date_and_timestamp_comparisons() {
    let mut db = test_db();
    db.execute("create table t (dt date, ts timestamp)").unwrap();
    db.execute(r#"insert into t values (2025-01-01, "2025-01-01 10:00:00")"#).unwrap();
    db.execute(r#"insert into t values (2025-01-02, "2025-01-02 10:00:00")"#).unwrap();
    let d = db.execute("select * from t where dt > 2025-01-01").unwrap();
    assert!(d.contains("2025-01-02"));
    let t = db
        .execute(r#"select * from t where ts >= "2025-01-02 10:00:00""#)
        .unwrap();
    assert!(t.contains("2025-01-02"));
}

#[test]
fn test_bigint_and_decimal_comparisons() {
    let mut db = test_db();
    db.execute("create table t (bi bigint, d decimal(8,2))").unwrap();
    db.execute("insert into t values (10, 1.10)").unwrap();
    db.execute("insert into t values (20, 2.20)").unwrap();
    let b = db.execute("select * from t where bi >= 20").unwrap();
    assert!(b.contains("20"));
    let d = db.execute("select * from t where d > 1.50").unwrap();
    assert!(d.contains("2.2") || d.contains("2.20"));
}

#[test]
fn test_primary_key_constraint_insert() {
    let mut db = test_db();
    db.execute("create table users (id int primary key, name text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    let err = db
        .execute(r#"insert into users values (1, "alice")"#)
        .unwrap_err();
    assert!(err.to_lowercase().contains("primary key"));
}

#[test]
fn test_unique_constraint_insert() {
    let mut db = test_db();
    db.execute("create table users (id int, email text unique)").unwrap();
    db.execute(r#"insert into users values (1, "a@x.com")"#).unwrap();
    let err = db
        .execute(r#"insert into users values (2, "a@x.com")"#)
        .unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
}

#[test]
fn test_not_null_constraint_insert() {
    let mut db = test_db();
    db.execute("create table users (id int, name text not null)").unwrap();
    let err = db
        .execute(r#"insert into users values (1, null)"#)
        .unwrap_err();
    assert!(err.to_lowercase().contains("not null"));
}

#[test]
fn test_unique_constraint_update() {
    let mut db = test_db();
    db.execute("create table users (id int, email text unique)").unwrap();
    db.execute(r#"insert into users values (1, "a@x.com")"#).unwrap();
    db.execute(r#"insert into users values (2, "b@x.com")"#).unwrap();
    let err = db
        .execute(r#"update users set email = "a@x.com" where id = 2"#)
        .unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
}

#[test]
fn test_multiple_primary_keys_rejected() {
    let mut db = test_db();
    let err = db
        .execute("create table t (id int primary key, code int primary key)")
        .unwrap_err();
    assert!(err.to_lowercase().contains("only one primary key"));
}

#[test]
fn test_primary_key_rejects_null_insert() {
    let mut db = test_db();
    db.execute("create table t (id int primary key, name text)").unwrap();
    let err = db.execute(r#"insert into t values (null, "ram")"#).unwrap_err();
    assert!(err.to_lowercase().contains("not null"));
}

#[test]
fn test_not_null_rejects_null_on_update() {
    let mut db = test_db();
    db.execute("create table t (id int primary key, name text not null)").unwrap();
    db.execute(r#"insert into t values (1, "ram")"#).unwrap();
    let err = db.execute("update t set name = null where id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("not null"));
}

#[test]
fn test_primary_key_violation_on_update() {
    let mut db = test_db();
    db.execute("create table t (id int primary key, name text)").unwrap();
    db.execute(r#"insert into t values (1, "a")"#).unwrap();
    db.execute(r#"insert into t values (2, "b")"#).unwrap();
    let err = db.execute("update t set id = 1 where id = 2").unwrap_err();
    assert!(err.to_lowercase().contains("primary key"));
}

#[test]
fn test_constraint_persistence_after_reopen() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_constraints_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open(path.clone());
        db.execute("create table t (id int primary key, email text unique, name text not null)")
            .unwrap();
        db.execute(r#"insert into t values (1, "a@x.com", "ram")"#).unwrap();
    }
    {
        let mut db = Database::open(path.clone());
        let e1 = db
            .execute(r#"insert into t values (1, "b@x.com", "bob")"#)
            .unwrap_err();
        assert!(e1.to_lowercase().contains("primary key"));
        let e2 = db
            .execute(r#"insert into t values (2, "a@x.com", "bob")"#)
            .unwrap_err();
        assert!(e2.to_lowercase().contains("unique"));
        let e3 = db
            .execute(r#"insert into t values (3, "c@x.com", null)"#)
            .unwrap_err();
        assert!(e3.to_lowercase().contains("not null"));
    }
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_composite_primary_key_constraint() {
    let mut db = test_db();
    db.execute("create table t (a int, b int, val text, primary key(a,b))")
        .unwrap();
    db.execute(r#"insert into t values (1, 1, "x")"#).unwrap();
    db.execute(r#"insert into t values (1, 2, "y")"#).unwrap();
    let err = db
        .execute(r#"insert into t values (1, 1, "dup")"#)
        .unwrap_err();
    assert!(err.to_lowercase().contains("primary key"));
}

#[test]
fn test_composite_unique_constraint() {
    let mut db = test_db();
    db.execute("create table t (a int, b int, c int, unique(a,b))")
        .unwrap();
    db.execute("insert into t values (1, 1, 10)").unwrap();
    db.execute("insert into t values (1, 2, 10)").unwrap();
    let err = db.execute("insert into t values (1, 1, 11)").unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
}

#[test]
fn test_composite_unique_violation_on_update() {
    let mut db = test_db();
    db.execute("create table t (a int, b int, unique(a,b))").unwrap();
    db.execute("insert into t values (1, 1)").unwrap();
    db.execute("insert into t values (1, 2)").unwrap();
    let err = db.execute("update t set b = 1 where b = 2").unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
}

#[test]
fn test_persistence_reopen_insert() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_persist_{}_insert", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)").unwrap();
        db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    }

    {
        let mut db = Database::open(path.clone());
        let out = db.execute("select * from users").unwrap();
        assert_eq!(out, "id\tname\n1\tram");
    }

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_persistence_reopen_update_delete() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_persist_{}_ud", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text, age int)").unwrap();
        db.execute(r#"insert into users values (1, "ram", 20)"#).unwrap();
        db.execute(r#"insert into users values (2, "alice", 30)"#).unwrap();
        db.execute(r#"update users set age = 99 where id = 1"#).unwrap();
        db.execute(r#"delete from users where name = "alice""#).unwrap();
    }

    {
        let mut db = Database::open(path.clone());
        let out = db.execute("select * from users").unwrap();
        assert_eq!(out, "id\tname\tage\n1\tram\t99");
    }

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_transaction_commit_persists_changes() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    assert_eq!(db.execute("begin").unwrap(), "transaction started");
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    assert_eq!(db.execute("commit").unwrap(), "transaction committed");
    assert_eq!(db.execute("select * from users").unwrap(), "id\tname\n1\tram");
}

#[test]
fn test_transaction_rollback_discards_changes() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute("begin").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    assert_eq!(db.execute("rollback").unwrap(), "transaction rolled back");
    assert_eq!(db.execute("select * from users").unwrap(), "id\tname");
}

#[test]
fn test_transaction_is_visible_inside_tx_before_commit() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute("begin").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    assert_eq!(db.execute("select * from users").unwrap(), "id\tname\n1\tram");
    db.execute("rollback").unwrap();
}

#[test]
fn test_nested_begin_is_rejected() {
    let mut db = test_db();
    db.execute("begin").unwrap();
    let err = db.execute("begin").unwrap_err();
    assert!(err.to_lowercase().contains("already active"));
}

#[test]
fn test_commit_without_active_tx_errors() {
    let mut db = test_db();
    let err = db.execute("commit").unwrap_err();
    assert!(err.to_lowercase().contains("no active transaction"));
}

#[test]
fn test_rollback_without_active_tx_errors() {
    let mut db = test_db();
    let err = db.execute("rollback").unwrap_err();
    assert!(err.to_lowercase().contains("no active transaction"));
}

#[test]
fn test_create_inside_transaction_is_rejected() {
    let mut db = test_db();
    db.execute("begin").unwrap();
    let err = db.execute("create table t (id int)").unwrap_err();
    assert!(err.to_lowercase().contains("cannot run inside an active transaction"));
}

#[test]
fn test_transaction_commit_persists_after_reopen() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_tx_commit_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)").unwrap();
        db.execute("begin").unwrap();
        db.execute(r#"insert into users values (1, "ram")"#).unwrap();
        db.execute("commit").unwrap();
    }

    {
        let mut db = Database::open(path.clone());
        assert_eq!(db.execute("select * from users").unwrap(), "id\tname\n1\tram");
    }

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_transaction_rollback_not_persisted_after_reopen() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_tx_rollback_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)").unwrap();
        db.execute("begin").unwrap();
        db.execute(r#"insert into users values (1, "ram")"#).unwrap();
        db.execute("rollback").unwrap();
    }

    {
        let mut db = Database::open(path.clone());
        assert_eq!(db.execute("select * from users").unwrap(), "id\tname");
    }

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_transaction_commit_with_multiple_operations() {
    let mut db = test_db();
    db.execute("create table users (id int primary key, name text, age int)")
        .unwrap();
    db.execute("begin").unwrap();
    db.execute(r#"insert into users values (1, "a", 10)"#).unwrap();
    db.execute(r#"insert into users values (2, "b", 20)"#).unwrap();
    db.execute(r#"update users set age = 21 where id = 2"#).unwrap();
    db.execute(r#"delete from users where name = "a""#).unwrap();
    db.execute("commit").unwrap();
    assert_eq!(db.execute("select * from users").unwrap(), "id\tname\tage\n2\tb\t21");
}

#[test]
fn test_transaction_rollback_reverts_update_and_delete() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a", 10)"#).unwrap();
    db.execute(r#"insert into users values (2, "b", 20)"#).unwrap();

    db.execute("begin").unwrap();
    db.execute("update users set age = 99 where id = 1").unwrap();
    db.execute(r#"delete from users where name = "b""#).unwrap();
    db.execute("rollback").unwrap();

    assert_eq!(
        db.execute("select * from users").unwrap(),
        "id\tname\tage\n1\ta\t10\n2\tb\t20"
    );
}

#[test]
fn test_transaction_rollback_restores_after_constraint_error() {
    let mut db = test_db();
    db.execute("create table users (id int primary key, email text unique)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a@x.com")"#).unwrap();
    db.execute(r#"insert into users values (2, "b@x.com")"#).unwrap();

    db.execute("begin").unwrap();
    let err = db
        .execute(r#"update users set email = "a@x.com" where id = 2"#)
        .unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
    db.execute("rollback").unwrap();

    assert_eq!(
        db.execute("select * from users").unwrap(),
        "id\temail\n1\ta@x.com\n2\tb@x.com"
    );
}

#[test]
fn test_transaction_commit_without_mutations_is_allowed() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute("begin").unwrap();
    let out = db.execute("commit").unwrap();
    assert_eq!(out, "transaction committed");
    assert_eq!(db.execute("select * from users").unwrap(), "id\tname");
}

#[test]
fn test_select_still_works_while_transaction_active() {
    let mut db = test_db();
    db.execute("create table t (id int, name text)").unwrap();
    db.execute(r#"insert into t values (1, "a")"#).unwrap();
    db.execute("begin").unwrap();
    db.execute(r#"insert into t values (2, "b")"#).unwrap();
    assert_eq!(db.execute("select name from t where id = 1").unwrap(), "name\na");
    db.execute("rollback").unwrap();
}

#[test]
fn test_composite_primary_key_violation_on_update() {
    let mut db = test_db();
    db.execute("create table t (a int, b int, primary key(a,b))")
        .unwrap();
    db.execute("insert into t values (1, 1)").unwrap();
    db.execute("insert into t values (1, 2)").unwrap();
    let err = db.execute("update t set b = 1 where b = 2").unwrap_err();
    assert!(err.to_lowercase().contains("primary key"));
}

#[test]
fn test_pk_eq_select_path_returns_single_row() {
    let mut db = test_db();
    db.execute("create table users (id int primary key, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a")"#).unwrap();
    db.execute(r#"insert into users values (2, "b")"#).unwrap();
    db.execute(r#"insert into users values (3, "c")"#).unwrap();

    let out = db.execute("select * from users where id = 2").unwrap();
    assert_eq!(out, "id\tname\n2\tb");
}

#[test]
fn test_pk_eq_update_path_updates_only_target_row() {
    let mut db = test_db();
    db.execute("create table users (id int primary key, name text, age int)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a", 10)"#).unwrap();
    db.execute(r#"insert into users values (2, "b", 20)"#).unwrap();

    let out = db.execute(r#"update users set age = 99 where id = 2"#).unwrap();
    assert_eq!(out, "updated 1 row(s) in users");
    assert_eq!(
        db.execute("select * from users").unwrap(),
        "id\tname\tage\n1\ta\t10\n2\tb\t99"
    );
}

#[test]
fn test_pk_eq_delete_path_deletes_only_target_row() {
    let mut db = test_db();
    db.execute("create table users (id int primary key, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a")"#).unwrap();
    db.execute(r#"insert into users values (2, "b")"#).unwrap();

    let out = db.execute("delete from users where id = 1").unwrap();
    assert_eq!(out, "deleted 1 row(s) from users");
    assert_eq!(db.execute("select * from users").unwrap(), "id\tname\n2\tb");
}

#[test]
fn test_pk_update_reindexes_for_future_pk_lookup() {
    let mut db = test_db();
    db.execute("create table users (id int primary key, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a")"#).unwrap();

    db.execute("update users set id = 10 where id = 1").unwrap();
    assert_eq!(db.execute("select * from users where id = 10").unwrap(), "id\tname\n10\ta");
    assert_eq!(db.execute("select * from users where id = 1").unwrap(), "id\tname");
}

