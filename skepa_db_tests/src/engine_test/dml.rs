use super::*;

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
fn test_update_where_is_null() {
    let mut db = test_db();
    db.execute("create table users (id int, city text)").unwrap();
    db.execute("insert into users values (1, null)").unwrap();
    db.execute(r#"insert into users values (2, "la")"#).unwrap();

    let msg = db
        .execute(r#"update users set city = "ny" where city is null"#)
        .unwrap();
    assert_eq!(msg, "updated 1 row(s) in users");
    let out = db.execute("select * from users order by id asc").unwrap();
    assert_eq!(out, "id\tcity\n1\tny\n2\tla");
}

#[test]
fn test_update_where_in() {
    let mut db = test_db();
    db.execute("create table users (id int, city text)").unwrap();
    db.execute(r#"insert into users values (1, "a")"#).unwrap();
    db.execute(r#"insert into users values (2, "b")"#).unwrap();
    db.execute(r#"insert into users values (3, "c")"#).unwrap();

    let msg = db
        .execute(r#"update users set city = "x" where id in (1,3)"#)
        .unwrap();
    assert_eq!(msg, "updated 2 row(s) in users");
    let out = db.execute("select * from users order by id asc").unwrap();
    assert_eq!(out, "id\tcity\n1\tx\n2\tb\n3\tx");
}

#[test]
fn test_delete_where_in() {
    let mut db = test_db();
    db.execute("create table users (id int, city text)").unwrap();
    db.execute(r#"insert into users values (1, "a")"#).unwrap();
    db.execute(r#"insert into users values (2, "b")"#).unwrap();
    db.execute(r#"insert into users values (3, "c")"#).unwrap();

    let msg = db.execute("delete from users where id in (2,3)").unwrap();
    assert_eq!(msg, "deleted 2 row(s) from users");
    let out = db.execute("select * from users").unwrap();
    assert_eq!(out, "id\tcity\n1\ta");
}

#[test]
fn test_where_in_type_mismatch_errors() {
    let mut db = test_db();
    db.execute("create table users (id int, city text)").unwrap();
    db.execute(r#"insert into users values (1, "a")"#).unwrap();
    let err = db
        .execute(r#"select * from users where id in ("x","y")"#)
        .unwrap_err();
    assert!(err.contains("Expected int"));
}

#[test]
fn test_update_where_and() {
    let mut db = test_db();
    db.execute("create table users (id int, age int, city text)").unwrap();
    db.execute(r#"insert into users values (1, 20, "ny")"#).unwrap();
    db.execute(r#"insert into users values (2, 20, "la")"#).unwrap();

    let msg = db
        .execute(r#"update users set city = "x" where age = 20 and city = "ny""#)
        .unwrap();
    assert_eq!(msg, "updated 1 row(s) in users");
    let out = db.execute("select * from users order by id asc").unwrap();
    assert_eq!(out, "id\tage\tcity\n1\t20\tx\n2\t20\tla");
}

#[test]
fn test_delete_where_or() {
    let mut db = test_db();
    db.execute("create table users (id int, city text)").unwrap();
    db.execute(r#"insert into users values (1, "ny")"#).unwrap();
    db.execute(r#"insert into users values (2, "la")"#).unwrap();
    db.execute(r#"insert into users values (3, "sf")"#).unwrap();

    let msg = db
        .execute(r#"delete from users where city = "ny" or city = "sf""#)
        .unwrap();
    assert_eq!(msg, "deleted 2 row(s) from users");
    let out = db.execute("select * from users").unwrap();
    assert_eq!(out, "id\tcity\n2\tla");
}

#[test]
fn test_update_where_parentheses() {
    let mut db = test_db();
    db.execute("create table users (id int, age int, city text)").unwrap();
    db.execute(r#"insert into users values (1, 20, "ny")"#).unwrap();
    db.execute(r#"insert into users values (2, 16, "la")"#).unwrap();
    db.execute(r#"insert into users values (3, 16, "ny")"#).unwrap();

    let msg = db
        .execute(r#"update users set city = "x" where (age gte 18 and city = "ny") or id = 2"#)
        .unwrap();
    assert_eq!(msg, "updated 2 row(s) in users");
    let out = db.execute("select * from users order by id asc").unwrap();
    assert_eq!(out, "id\tage\tcity\n1\t20\tx\n2\t16\tx\n3\t16\tny");
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
fn test_update_pk_eq_no_match_returns_zero() {
    let mut db = test_db();
    db.execute("create table t (id int primary key, v int)").unwrap();
    db.execute("insert into t values (1, 10)").unwrap();
    let out = db.execute("update t set v = 99 where id = 999").unwrap();
    assert_eq!(out, "updated 0 row(s) in t");
}

#[test]
fn test_delete_pk_eq_no_match_returns_zero() {
    let mut db = test_db();
    db.execute("create table t (id int primary key, v int)").unwrap();
    db.execute("insert into t values (1, 10)").unwrap();
    let out = db.execute("delete from t where id = 999").unwrap();
    assert_eq!(out, "deleted 0 row(s) from t");
}

#[test]
fn test_update_same_value_still_reports_updated_row() {
    let mut db = test_db();
    db.execute("create table t (id int, v int)").unwrap();
    db.execute("insert into t values (1, 10)").unwrap();
    let out = db.execute("update t set v = 10 where id = 1").unwrap();
    assert_eq!(out, "updated 1 row(s) in t");
}

#[test]
fn test_delete_all_rows_using_like_star_then_insert_again() {
    let mut db = test_db();
    db.execute("create table t (id int, name text)").unwrap();
    db.execute(r#"insert into t values (1, "a")"#).unwrap();
    db.execute(r#"insert into t values (2, "b")"#).unwrap();
    db.execute(r#"delete from t where name like "*""#).unwrap();
    db.execute(r#"insert into t values (3, "c")"#).unwrap();
    assert_eq!(db.execute("select * from t").unwrap(), "id\tname\n3\tc");
}

