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

