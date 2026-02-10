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
fn test_select_inner_join_basic() {
    let mut db = test_db();
    db.execute("create table users (id int primary key, name text)").unwrap();
    db.execute("create table profiles (user_id int, city text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into users values (2, "avi")"#).unwrap();
    db.execute(r#"insert into profiles values (1, "ny")"#).unwrap();
    db.execute(r#"insert into profiles values (2, "la")"#).unwrap();

    let out = db
        .execute("select users.id,profiles.city from users join profiles on users.id = profiles.user_id order by users.id asc")
        .unwrap();
    assert_eq!(out, "users.id\tprofiles.city\n1\tny\n2\tla");
}

#[test]
fn test_select_inner_join_star_projection() {
    let mut db = test_db();
    db.execute("create table u (id int, name text)").unwrap();
    db.execute("create table p (uid int, city text)").unwrap();
    db.execute(r#"insert into u values (1, "a")"#).unwrap();
    db.execute(r#"insert into p values (1, "x")"#).unwrap();

    let out = db.execute("select * from u join p on u.id = p.uid").unwrap();
    assert_eq!(out, "u.id\tu.name\tp.uid\tp.city\n1\ta\t1\tx");
}

#[test]
fn test_select_inner_join_where_order_limit() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute("create table profiles (user_id int, city text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into users values (2, "avi")"#).unwrap();
    db.execute(r#"insert into users values (3, "sam")"#).unwrap();
    db.execute(r#"insert into profiles values (1, "ny")"#).unwrap();
    db.execute(r#"insert into profiles values (2, "ny")"#).unwrap();
    db.execute(r#"insert into profiles values (3, "la")"#).unwrap();

    let out = db.execute(r#"select users.id,profiles.city from users join profiles on users.id = profiles.user_id where profiles.city = "ny" order by users.id desc limit 1"#).unwrap();
    assert_eq!(out, "users.id\tprofiles.city\n2\tny");
}

#[test]
fn test_select_inner_join_with_unqualified_unique_column_reference() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute("create table profiles (user_id int, city text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into profiles values (1, "ny")"#).unwrap();

    let out = db.execute(r#"select city from users join profiles on users.id = profiles.user_id where city = "ny""#).unwrap();
    assert_eq!(out, "profiles.city\nny");
}

#[test]
fn test_select_inner_join_ambiguous_projection_errors() {
    let mut db = test_db();
    db.execute("create table a (id int)").unwrap();
    db.execute("create table b (id int)").unwrap();
    let err = db
        .execute("select id from a join b on a.id = b.id")
        .unwrap_err();
    assert!(err.contains("Ambiguous column 'id'"));
}

#[test]
fn test_select_inner_join_ambiguous_where_errors() {
    let mut db = test_db();
    db.execute("create table a (id int)").unwrap();
    db.execute("create table b (id int)").unwrap();
    db.execute("insert into a values (1)").unwrap();
    db.execute("insert into b values (1)").unwrap();
    let err = db
        .execute("select * from a join b on a.id = b.id where id = 1")
        .unwrap_err();
    assert!(err.contains("Ambiguous column 'id'"));
}

#[test]
fn test_select_inner_join_ambiguous_order_by_errors() {
    let mut db = test_db();
    db.execute("create table a (id int)").unwrap();
    db.execute("create table b (id int)").unwrap();
    db.execute("insert into a values (1)").unwrap();
    db.execute("insert into b values (1)").unwrap();
    let err = db
        .execute("select * from a join b on a.id = b.id order by id asc")
        .unwrap_err();
    assert!(err.contains("Ambiguous column 'id'"));
}

#[test]
fn test_select_inner_join_unknown_join_table_errors() {
    let mut db = test_db();
    db.execute("create table a (id int)").unwrap();
    let err = db
        .execute("select * from a join b on a.id = b.id")
        .unwrap_err();
    assert!(err.contains("Table 'b' does not exist"));
}

#[test]
fn test_select_inner_join_unknown_on_column_errors() {
    let mut db = test_db();
    db.execute("create table a (id int)").unwrap();
    db.execute("create table b (id int)").unwrap();
    let err = db
        .execute("select * from a join b on a.missing = b.id")
        .unwrap_err();
    assert!(err.contains("Unknown column 'a.missing' in JOIN"));
}

#[test]
fn test_select_inner_join_on_same_side_errors() {
    let mut db = test_db();
    db.execute("create table a (id int)").unwrap();
    db.execute("create table b (id int)").unwrap();
    let err = db
        .execute("select * from a join b on a.id = a.id")
        .unwrap_err();
    assert!(err.contains("must compare one column from each table"));
}

#[test]
fn test_select_inner_join_type_mismatch_errors() {
    let mut db = test_db();
    db.execute("create table a (id int)").unwrap();
    db.execute("create table b (id text)").unwrap();
    let err = db
        .execute("select * from a join b on a.id = b.id")
        .unwrap_err();
    assert!(err.contains("same datatype"));
}

#[test]
fn test_select_inner_join_null_join_key_does_not_match() {
    let mut db = test_db();
    db.execute("create table a (id int, name text)").unwrap();
    db.execute("create table b (id int, city text)").unwrap();
    db.execute(r#"insert into a values (null, "ram")"#).unwrap();
    db.execute(r#"insert into b values (null, "ny")"#).unwrap();
    let out = db.execute("select * from a join b on a.id = b.id").unwrap();
    assert_eq!(out, "a.id\ta.name\tb.id\tb.city");
}

#[test]
fn test_select_inner_join_one_to_many_returns_all_matches() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute("create table posts (user_id int, title text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into users values (2, "avi")"#).unwrap();
    db.execute(r#"insert into posts values (1, "p1")"#).unwrap();
    db.execute(r#"insert into posts values (1, "p2")"#).unwrap();
    db.execute(r#"insert into posts values (2, "p3")"#).unwrap();

    let out = db
        .execute("select users.id,posts.title from users join posts on users.id = posts.user_id order by posts.title asc")
        .unwrap();
    assert_eq!(out, "users.id\tposts.title\n1\tp1\n1\tp2\n2\tp3");
}

#[test]
fn test_select_inner_join_many_to_one_returns_all_matches() {
    let mut db = test_db();
    db.execute("create table users (id int, city text)").unwrap();
    db.execute("create table city_info (city text, zone text)").unwrap();
    db.execute(r#"insert into users values (1, "ny")"#).unwrap();
    db.execute(r#"insert into users values (2, "ny")"#).unwrap();
    db.execute(r#"insert into users values (3, "la")"#).unwrap();
    db.execute(r#"insert into city_info values ("ny", "east")"#).unwrap();
    db.execute(r#"insert into city_info values ("la", "west")"#).unwrap();

    let out = db
        .execute("select users.id,city_info.zone from users join city_info on users.city = city_info.city order by users.id asc")
        .unwrap();
    assert_eq!(out, "users.id\tcity_info.zone\n1\teast\n2\teast\n3\twest");
}

#[test]
fn test_select_left_join_includes_unmatched_left_rows() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute("create table profiles (user_id int, city text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into users values (2, "avi")"#).unwrap();
    db.execute(r#"insert into users values (3, "sam")"#).unwrap();
    db.execute(r#"insert into profiles values (1, "ny")"#).unwrap();
    db.execute(r#"insert into profiles values (2, "la")"#).unwrap();

    let out = db
        .execute("select users.id,profiles.city from users left join profiles on users.id = profiles.user_id order by users.id asc")
        .unwrap();
    assert_eq!(out, "users.id\tprofiles.city\n1\tny\n2\tla\n3\tnull");
}

#[test]
fn test_select_left_join_where_on_right_column_filters_null_rows_out() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute("create table profiles (user_id int, city text)").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    db.execute(r#"insert into users values (2, "avi")"#).unwrap();
    db.execute(r#"insert into users values (3, "sam")"#).unwrap();
    db.execute(r#"insert into profiles values (1, "ny")"#).unwrap();
    db.execute(r#"insert into profiles values (2, "la")"#).unwrap();

    let out = db
        .execute(r#"select users.id from users left join profiles on users.id = profiles.user_id where profiles.city = "ny" order by users.id asc"#)
        .unwrap();
    assert_eq!(out, "users.id\n1");
}

#[test]
fn test_select_left_join_with_null_left_key_still_included() {
    let mut db = test_db();
    db.execute("create table a (id int, name text)").unwrap();
    db.execute("create table b (id int, city text)").unwrap();
    db.execute(r#"insert into a values (null, "ram")"#).unwrap();
    db.execute(r#"insert into a values (1, "avi")"#).unwrap();
    db.execute(r#"insert into b values (1, "ny")"#).unwrap();

    let out = db
        .execute("select a.name,b.city from a left join b on a.id = b.id order by a.name asc")
        .unwrap();
    assert_eq!(out, "a.name\tb.city\navi\tny\nram\tnull");
}

#[test]
fn test_select_left_join_order_limit() {
    let mut db = test_db();
    db.execute("create table a (id int)").unwrap();
    db.execute("create table b (id int, v text)").unwrap();
    db.execute("insert into a values (1)").unwrap();
    db.execute("insert into a values (2)").unwrap();
    db.execute("insert into a values (3)").unwrap();
    db.execute(r#"insert into b values (1, "x")"#).unwrap();
    db.execute(r#"insert into b values (2, "y")"#).unwrap();

    let out = db
        .execute("select a.id,b.v from a left join b on a.id = b.id order by a.id desc limit 2")
        .unwrap();
    assert_eq!(out, "a.id\tb.v\n3\tnull\n2\ty");
}

#[test]
fn test_select_where_is_null_and_is_not_null() {
    let mut db = test_db();
    db.execute("create table users (id int, city text)").unwrap();
    db.execute(r#"insert into users values (1, "ny")"#).unwrap();
    db.execute("insert into users values (2, null)").unwrap();

    let out_null = db.execute("select id from users where city is null order by id asc").unwrap();
    assert_eq!(out_null, "id\n2");

    let out_not_null = db
        .execute("select id from users where city is not null order by id asc")
        .unwrap();
    assert_eq!(out_not_null, "id\n1");
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
fn test_delete_where_is_not_null() {
    let mut db = test_db();
    db.execute("create table users (id int, city text)").unwrap();
    db.execute("insert into users values (1, null)").unwrap();
    db.execute(r#"insert into users values (2, "la")"#).unwrap();
    db.execute(r#"insert into users values (3, "ny")"#).unwrap();

    let msg = db.execute("delete from users where city is not null").unwrap();
    assert_eq!(msg, "deleted 2 row(s) from users");
    let out = db.execute("select * from users").unwrap();
    assert_eq!(out, "id\tcity\n1\tnull");
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

#[test]
fn test_composite_pk_insert_conflict_still_rejected() {
    let mut db = test_db();
    db.execute("create table t (a int, b int, v text, primary key(a,b))")
        .unwrap();
    db.execute(r#"insert into t values (1, 1, "x")"#).unwrap();
    let err = db.execute(r#"insert into t values (1, 1, "y")"#).unwrap_err();
    assert!(err.to_lowercase().contains("primary key"));
}

#[test]
fn test_composite_pk_persists_and_rejects_after_reopen() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_composite_pk_reopen_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open(path.clone());
        db.execute("create table t (a int, b int, primary key(a,b))")
            .unwrap();
        db.execute("insert into t values (1, 1)").unwrap();
    }

    {
        let mut db = Database::open(path.clone());
        let err = db.execute("insert into t values (1, 1)").unwrap_err();
        assert!(err.to_lowercase().contains("primary key"));
    }

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_unique_eq_select_path_returns_single_row() {
    let mut db = test_db();
    db.execute("create table users (id int, email text unique, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a@x.com", "a")"#).unwrap();
    db.execute(r#"insert into users values (2, "b@x.com", "b")"#).unwrap();

    let out = db.execute(r#"select * from users where email = "b@x.com""#).unwrap();
    assert_eq!(out, "id\temail\tname\n2\tb@x.com\tb");
}

#[test]
fn test_unique_eq_update_path_updates_only_target_row() {
    let mut db = test_db();
    db.execute("create table users (id int, email text unique, age int)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a@x.com", 10)"#).unwrap();
    db.execute(r#"insert into users values (2, "b@x.com", 20)"#).unwrap();

    let out = db
        .execute(r#"update users set age = 99 where email = "b@x.com""#)
        .unwrap();
    assert_eq!(out, "updated 1 row(s) in users");
    assert_eq!(
        db.execute("select * from users").unwrap(),
        "id\temail\tage\n1\ta@x.com\t10\n2\tb@x.com\t99"
    );
}

#[test]
fn test_unique_eq_delete_path_deletes_only_target_row() {
    let mut db = test_db();
    db.execute("create table users (id int, email text unique)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a@x.com")"#).unwrap();
    db.execute(r#"insert into users values (2, "b@x.com")"#).unwrap();

    let out = db
        .execute(r#"delete from users where email = "a@x.com""#)
        .unwrap();
    assert_eq!(out, "deleted 1 row(s) from users");
    assert_eq!(db.execute("select * from users").unwrap(), "id\temail\n2\tb@x.com");
}

#[test]
fn test_unique_update_reindexes_for_future_unique_lookup() {
    let mut db = test_db();
    db.execute("create table users (id int, email text unique)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a@x.com")"#).unwrap();

    db.execute(r#"update users set email = "z@x.com" where id = 1"#).unwrap();
    assert_eq!(
        db.execute(r#"select * from users where email = "z@x.com""#).unwrap(),
        "id\temail\n1\tz@x.com"
    );
    assert_eq!(
        db.execute(r#"select * from users where email = "a@x.com""#).unwrap(),
        "id\temail"
    );
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
fn test_select_unique_eq_no_match_header_only() {
    let mut db = test_db();
    db.execute("create table t (id int, email text unique)").unwrap();
    db.execute(r#"insert into t values (1, "a@x.com")"#).unwrap();
    let out = db.execute(r#"select * from t where email = "x@x.com""#).unwrap();
    assert_eq!(out, "id\temail");
}

#[test]
fn test_update_unique_eq_no_match_returns_zero() {
    let mut db = test_db();
    db.execute("create table t (id int, email text unique, v int)").unwrap();
    db.execute(r#"insert into t values (1, "a@x.com", 1)"#).unwrap();
    let out = db
        .execute(r#"update t set v = 2 where email = "x@x.com""#)
        .unwrap();
    assert_eq!(out, "updated 0 row(s) in t");
}

#[test]
fn test_delete_unique_eq_no_match_returns_zero() {
    let mut db = test_db();
    db.execute("create table t (id int, email text unique)").unwrap();
    db.execute(r#"insert into t values (1, "a@x.com")"#).unwrap();
    let out = db
        .execute(r#"delete from t where email = "x@x.com""#)
        .unwrap();
    assert_eq!(out, "deleted 0 row(s) from t");
}

#[test]
fn test_composite_unique_insert_duplicate_rejected_after_many_rows() {
    let mut db = test_db();
    db.execute("create table t (a int, b int, c int, unique(a,b))")
        .unwrap();
    for i in 0..20 {
        db.execute(&format!("insert into t values ({}, {}, {})", i, i + 1, i + 2))
            .unwrap();
    }
    let err = db.execute("insert into t values (5, 6, 999)").unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
}

#[test]
fn test_composite_unique_update_to_duplicate_rejected() {
    let mut db = test_db();
    db.execute("create table t (a int, b int, unique(a,b))").unwrap();
    db.execute("insert into t values (1, 1)").unwrap();
    db.execute("insert into t values (2, 2)").unwrap();
    let err = db.execute("update t set a = 1, b = 1 where a = 2").unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
}

#[test]
fn test_transaction_multiple_updates_then_rollback() {
    let mut db = test_db();
    db.execute("create table t (id int primary key, v int)").unwrap();
    db.execute("insert into t values (1, 10)").unwrap();
    db.execute("insert into t values (2, 20)").unwrap();
    db.execute("begin").unwrap();
    db.execute("update t set v = 11 where id = 1").unwrap();
    db.execute("update t set v = 22 where id = 2").unwrap();
    db.execute("rollback").unwrap();
    assert_eq!(db.execute("select * from t").unwrap(), "id\tv\n1\t10\n2\t20");
}

#[test]
fn test_transaction_multiple_deletes_then_rollback() {
    let mut db = test_db();
    db.execute("create table t (id int primary key, v int)").unwrap();
    db.execute("insert into t values (1, 10)").unwrap();
    db.execute("insert into t values (2, 20)").unwrap();
    db.execute("begin").unwrap();
    db.execute("delete from t where id = 1").unwrap();
    db.execute("delete from t where id = 2").unwrap();
    db.execute("rollback").unwrap();
    assert_eq!(db.execute("select * from t").unwrap(), "id\tv\n1\t10\n2\t20");
}

#[test]
fn test_transaction_commit_then_new_begin_allowed() {
    let mut db = test_db();
    db.execute("create table t (id int)").unwrap();
    db.execute("begin").unwrap();
    db.execute("commit").unwrap();
    let out = db.execute("begin").unwrap();
    assert_eq!(out, "transaction started");
}

#[test]
fn test_transaction_rollback_then_new_begin_allowed() {
    let mut db = test_db();
    db.execute("create table t (id int)").unwrap();
    db.execute("begin").unwrap();
    db.execute("rollback").unwrap();
    let out = db.execute("begin").unwrap();
    assert_eq!(out, "transaction started");
}

#[test]
fn test_select_on_table_with_one_column() {
    let mut db = test_db();
    db.execute("create table t (id int)").unwrap();
    db.execute("insert into t values (1)").unwrap();
    assert_eq!(db.execute("select * from t").unwrap(), "id\n1");
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

#[test]
fn test_foreign_key_insert_requires_parent_row() {
    let mut db = test_db();
    db.execute("create table users (id int primary key)").unwrap();
    db.execute(
        "create table orders (id int, user_id int, foreign key(user_id) references users(id))",
    )
    .unwrap();
    let err = db.execute("insert into orders values (1, 99)").unwrap_err();
    assert!(err.to_lowercase().contains("foreign key"));
}

#[test]
fn test_foreign_key_insert_succeeds_when_parent_exists() {
    let mut db = test_db();
    db.execute("create table users (id int primary key)").unwrap();
    db.execute(
        "create table orders (id int, user_id int, foreign key(user_id) references users(id))",
    )
    .unwrap();
    db.execute("insert into users values (1)").unwrap();
    db.execute("insert into orders values (1, 1)").unwrap();
    assert_eq!(db.execute("select * from orders").unwrap(), "id\tuser_id\n1\t1");
}

#[test]
fn test_foreign_key_restrict_blocks_parent_delete() {
    let mut db = test_db();
    db.execute("create table users (id int primary key)").unwrap();
    db.execute(
        "create table orders (id int, user_id int, foreign key(user_id) references users(id))",
    )
    .unwrap();
    db.execute("insert into users values (1)").unwrap();
    db.execute("insert into orders values (1, 1)").unwrap();
    let err = db.execute("delete from users where id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("foreign key restrict"));
}

#[test]
fn test_foreign_key_restrict_blocks_parent_update() {
    let mut db = test_db();
    db.execute("create table users (id int primary key)").unwrap();
    db.execute(
        "create table orders (id int, user_id int, foreign key(user_id) references users(id))",
    )
    .unwrap();
    db.execute("insert into users values (1)").unwrap();
    db.execute("insert into orders values (1, 1)").unwrap();
    let err = db.execute("update users set id = 2 where id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("foreign key restrict"));
}

#[test]
fn test_foreign_key_update_child_requires_parent() {
    let mut db = test_db();
    db.execute("create table users (id int primary key)").unwrap();
    db.execute(
        "create table orders (id int, user_id int, foreign key(user_id) references users(id))",
    )
    .unwrap();
    db.execute("insert into users values (1)").unwrap();
    db.execute("insert into orders values (1, 1)").unwrap();
    let err = db
        .execute("update orders set user_id = 2 where id = 1")
        .unwrap_err();
    assert!(err.to_lowercase().contains("foreign key"));
}

#[test]
fn test_composite_foreign_key_enforced() {
    let mut db = test_db();
    db.execute("create table parent (a int, b int, primary key(a,b))")
        .unwrap();
    db.execute(
        "create table child (id int, a int, b int, foreign key(a,b) references parent(a,b))",
    )
    .unwrap();
    db.execute("insert into parent values (1, 2)").unwrap();
    db.execute("insert into child values (1, 1, 2)").unwrap();
    let err = db.execute("insert into child values (2, 9, 9)").unwrap_err();
    assert!(err.to_lowercase().contains("foreign key"));
}

#[test]
fn test_foreign_key_on_delete_cascade_deletes_children() {
    let mut db = test_db();
    db.execute("create table users (id int primary key)").unwrap();
    db.execute(
        "create table orders (id int, user_id int, foreign key(user_id) references users(id) on delete cascade)",
    )
    .unwrap();
    db.execute("insert into users values (1)").unwrap();
    db.execute("insert into users values (2)").unwrap();
    db.execute("insert into orders values (1, 1)").unwrap();
    db.execute("insert into orders values (2, 2)").unwrap();

    db.execute("delete from users where id = 1").unwrap();
    assert_eq!(db.execute("select * from orders").unwrap(), "id\tuser_id\n2\t2");
}

#[test]
fn test_composite_foreign_key_on_delete_cascade() {
    let mut db = test_db();
    db.execute("create table parent (a int, b int, primary key(a,b))")
        .unwrap();
    db.execute(
        "create table child (id int, a int, b int, foreign key(a,b) references parent(a,b) on delete cascade)",
    )
    .unwrap();
    db.execute("insert into parent values (1, 2)").unwrap();
    db.execute("insert into parent values (3, 4)").unwrap();
    db.execute("insert into child values (1, 1, 2)").unwrap();
    db.execute("insert into child values (2, 3, 4)").unwrap();

    db.execute("delete from parent where a = 1").unwrap();
    assert_eq!(db.execute("select * from child").unwrap(), "id\ta\tb\n2\t3\t4");
}

#[test]
fn test_fk_create_rejects_unknown_parent_table() {
    let mut db = test_db();
    db.execute("create table child (id int, p int, foreign key(p) references parent(id))")
        .unwrap_err();
}

#[test]
fn test_fk_create_rejects_unknown_parent_column() {
    let mut db = test_db();
    db.execute("create table parent (id int primary key)").unwrap();
    let err = db
        .execute("create table child (id int, p int, foreign key(p) references parent(missing))")
        .unwrap_err();
    assert!(err.to_lowercase().contains("unknown parent column"));
}

#[test]
fn test_fk_create_rejects_unknown_child_column() {
    let mut db = test_db();
    db.execute("create table parent (id int primary key)").unwrap();
    let err = db
        .execute("create table child (id int, foreign key(missing) references parent(id))")
        .unwrap_err();
    assert!(err.to_lowercase().contains("unknown column"));
}

#[test]
fn test_fk_create_rejects_mismatched_column_count() {
    let mut db = test_db();
    db.execute("create table parent (a int, b int, primary key(a,b))")
        .unwrap();
    let err = db
        .execute("create table child (x int, foreign key(x) references parent(a,b))")
        .unwrap_err();
    assert!(err.to_lowercase().contains("column count"));
}

#[test]
fn test_fk_create_requires_parent_key_or_unique() {
    let mut db = test_db();
    db.execute("create table parent (id int, v int)").unwrap();
    let err = db
        .execute("create table child (id int, p int, foreign key(p) references parent(id))")
        .unwrap_err();
    assert!(err.to_lowercase().contains("primary key or unique"));
}

#[test]
fn test_fk_create_allows_parent_unique_reference() {
    let mut db = test_db();
    db.execute("create table parent (id int, code text unique)").unwrap();
    db.execute("create table child (id int, code text, foreign key(code) references parent(code))")
        .unwrap();
}

#[test]
fn test_fk_restrict_parent_delete_allowed_when_unreferenced() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id))")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into p values (2)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    let out = db.execute("delete from p where id = 2").unwrap();
    assert_eq!(out, "deleted 1 row(s) from p");
}

#[test]
fn test_fk_cascade_deletes_multiple_child_rows() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute(
        "create table c (id int, pid int, foreign key(pid) references p(id) on delete cascade)",
    )
    .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("insert into c values (2, 1)").unwrap();
    db.execute("delete from p where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid");
}

#[test]
fn test_fk_cascade_and_restrict_can_coexist() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c1 (id int, pid int, foreign key(pid) references p(id) on delete cascade)")
        .unwrap();
    db.execute("create table c2 (id int, pid int, foreign key(pid) references p(id))")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c1 values (1, 1)").unwrap();
    db.execute("insert into c2 values (1, 1)").unwrap();
    let err = db.execute("delete from p where id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("restrict"));
}

#[test]
fn test_fk_child_update_to_existing_parent_succeeds() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id))")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into p values (2)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("update c set pid = 2 where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\t2");
}

#[test]
fn test_fk_parent_update_non_key_column_allowed() {
    let mut db = test_db();
    db.execute("create table p (id int primary key, name text)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id))")
        .unwrap();
    db.execute(r#"insert into p values (1, "a")"#).unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute(r#"update p set name = "b" where id = 1"#).unwrap();
    assert_eq!(db.execute("select * from p").unwrap(), "id\tname\n1\tb");
}

#[test]
fn test_fk_composite_restrict_blocks_parent_delete() {
    let mut db = test_db();
    db.execute("create table p (a int, b int, primary key(a,b))").unwrap();
    db.execute("create table c (id int, a int, b int, foreign key(a,b) references p(a,b))")
        .unwrap();
    db.execute("insert into p values (1, 2)").unwrap();
    db.execute("insert into c values (1, 1, 2)").unwrap();
    let err = db.execute("delete from p where a = 1").unwrap_err();
    assert!(err.to_lowercase().contains("restrict"));
}

#[test]
fn test_foreign_key_on_update_restrict_blocks_parent_key_update() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on update restrict)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    let err = db.execute("update p set id = 2 where id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("restrict"));
}

#[test]
fn test_foreign_key_on_update_cascade_updates_child_rows() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on update cascade)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("update p set id = 2 where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\t2");
}

#[test]
fn test_composite_foreign_key_on_update_cascade() {
    let mut db = test_db();
    db.execute("create table p (a int, b int, primary key(a,b))").unwrap();
    db.execute(
        "create table c (id int, a int, b int, foreign key(a,b) references p(a,b) on update cascade)",
    )
    .unwrap();
    db.execute("insert into p values (1, 2)").unwrap();
    db.execute("insert into c values (1, 1, 2)").unwrap();
    db.execute("update p set a = 3 where a = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\ta\tb\n1\t3\t2");
}

#[test]
fn test_foreign_key_on_update_cascade_multiple_children_rows() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on update cascade)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("insert into c values (2, 1)").unwrap();
    db.execute("update p set id = 2 where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\t2\n2\t2");
}

#[test]
fn test_foreign_key_on_delete_set_null_sets_child_to_null() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on delete set null)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("delete from p where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\tnull");
}

#[test]
fn test_foreign_key_on_update_set_null_sets_child_to_null() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on update set null)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("update p set id = 2 where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\tnull");
}

#[test]
fn test_foreign_key_set_null_requires_nullable_child_columns() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    let err = db
        .execute("create table c (id int, pid int not null, foreign key(pid) references p(id) on delete set null)")
        .unwrap_err();
    assert!(err.to_lowercase().contains("set null requires nullable"));
}

#[test]
fn test_foreign_key_on_delete_no_action_behaves_like_restrict() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on delete no action)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    let err = db.execute("delete from p where id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("no action"));
}

#[test]
fn test_foreign_key_on_update_no_action_behaves_like_restrict() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on update no action)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    let err = db.execute("update p set id = 2 where id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("no action"));
}

#[test]
fn test_foreign_key_no_action_deferred_until_commit_can_be_fixed_in_tx() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on update no action)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();

    db.execute("begin").unwrap();
    db.execute("update p set id = 2 where id = 1").unwrap();
    db.execute("update c set pid = 2 where id = 1").unwrap();
    let out = db.execute("commit").unwrap();
    assert_eq!(out, "transaction committed");
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\t2");
}

#[test]
fn test_foreign_key_no_action_commit_fails_if_still_violated() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on update no action)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();

    db.execute("begin").unwrap();
    db.execute("update p set id = 2 where id = 1").unwrap();
    let err = db.execute("commit").unwrap_err();
    assert!(err.to_lowercase().contains("no action"));
    assert_eq!(db.execute("select * from p").unwrap(), "id\n1");
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\t1");
}

#[test]
fn test_alter_add_unique_enforces_existing_data() {
    let mut db = test_db();
    db.execute("create table t (id int, email text)").unwrap();
    db.execute(r#"insert into t values (1, "a@x.com")"#).unwrap();
    db.execute(r#"insert into t values (2, "a@x.com")"#).unwrap();
    let err = db.execute("alter table t add unique(email)").unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
}

#[test]
fn test_alter_add_and_drop_unique() {
    let mut db = test_db();
    db.execute("create table t (id int, email text)").unwrap();
    db.execute("alter table t add unique(email)").unwrap();
    db.execute(r#"insert into t values (1, "a@x.com")"#).unwrap();
    let err = db.execute(r#"insert into t values (2, "a@x.com")"#).unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
    db.execute("alter table t drop unique(email)").unwrap();
    db.execute(r#"insert into t values (2, "a@x.com")"#).unwrap();
}

#[test]
fn test_alter_add_foreign_key_enforces_existing_rows() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int)").unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("insert into c values (2, 99)").unwrap();
    let err = db
        .execute("alter table c add foreign key(pid) references p(id)")
        .unwrap_err();
    assert!(err.to_lowercase().contains("foreign key"));
}

#[test]
fn test_alter_add_and_drop_foreign_key() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int)").unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("alter table c add foreign key(pid) references p(id)")
        .unwrap();
    let err = db.execute("insert into c values (2, 99)").unwrap_err();
    assert!(err.to_lowercase().contains("foreign key"));
    db.execute("alter table c drop foreign key(pid) references p(id)")
        .unwrap();
    db.execute("insert into c values (2, 99)").unwrap();
}

#[test]
fn test_alter_set_not_null_enforces_existing_rows() {
    let mut db = test_db();
    db.execute("create table t (id int, name text)").unwrap();
    db.execute("insert into t values (1, null)").unwrap();
    let err = db
        .execute("alter table t alter column name set not null")
        .unwrap_err();
    assert!(err.to_lowercase().contains("not null"));
}

#[test]
fn test_alter_set_and_drop_not_null() {
    let mut db = test_db();
    db.execute("create table t (id int, name text)").unwrap();
    db.execute("insert into t values (1, \"a\")").unwrap();
    db.execute("alter table t alter column name set not null").unwrap();
    let err = db.execute("insert into t values (2, null)").unwrap_err();
    assert!(err.to_lowercase().contains("not null"));
    db.execute("alter table t alter column name drop not null")
        .unwrap();
    db.execute("insert into t values (2, null)").unwrap();
}

#[test]
fn test_alter_table_not_allowed_inside_transaction() {
    let mut db = test_db();
    db.execute("create table t (id int)").unwrap();
    db.execute("begin").unwrap();
    let err = db.execute("alter table t add unique(id)").unwrap_err();
    assert!(err.to_lowercase().contains("auto-commit"));
    db.execute("rollback").unwrap();
}

#[test]
fn test_create_and_drop_secondary_index() {
    let mut db = test_db();
    db.execute("create table users (id int, email text)").unwrap();
    let out = db.execute("create index on users (email)").unwrap();
    assert_eq!(out, "created index on users(email)");
    let out = db.execute("drop index on users (email)").unwrap();
    assert_eq!(out, "dropped index on users(email)");
}

#[test]
fn test_secondary_index_select_update_delete_eq_paths() {
    let mut db = test_db();
    db.execute("create table users (id int, city text, age int)").unwrap();
    db.execute(r#"create index on users (city)"#).unwrap();
    db.execute(r#"insert into users values (1, "ny", 10)"#).unwrap();
    db.execute(r#"insert into users values (2, "ny", 20)"#).unwrap();
    db.execute(r#"insert into users values (3, "la", 30)"#).unwrap();

    let out = db.execute(r#"select * from users where city = "ny""#).unwrap();
    assert_eq!(out, "id\tcity\tage\n1\tny\t10\n2\tny\t20");

    let out = db.execute(r#"update users set age = 99 where city = "ny""#).unwrap();
    assert_eq!(out, "updated 2 row(s) in users");

    let out = db.execute(r#"delete from users where city = "ny""#).unwrap();
    assert_eq!(out, "deleted 2 row(s) from users");
    assert_eq!(db.execute("select * from users").unwrap(), "id\tcity\tage\n3\tla\t30");
}

#[test]
fn test_select_order_by_and_limit() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)").unwrap();
    db.execute(r#"insert into users values (1, "alice", 30)"#).unwrap();
    db.execute(r#"insert into users values (2, "bob", 20)"#).unwrap();
    db.execute(r#"insert into users values (3, "charlie", 25)"#).unwrap();

    let out = db
        .execute("select id,name from users order by age desc limit 2")
        .unwrap();
    assert_eq!(out, "id\tname\n1\talice\n3\tcharlie");
}

#[test]
fn test_select_order_by_text_asc() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)").unwrap();
    db.execute(r#"insert into users values (1, "z")"#).unwrap();
    db.execute(r#"insert into users values (2, "a")"#).unwrap();
    db.execute(r#"insert into users values (3, "m")"#).unwrap();

    let out = db.execute("select * from users order by name asc").unwrap();
    assert_eq!(out, "id\tname\n2\ta\n3\tm\n1\tz");
}

#[test]
fn test_index_not_allowed_inside_transaction() {
    let mut db = test_db();
    db.execute("create table users (id int, email text)").unwrap();
    db.execute("begin").unwrap();
    let err = db.execute("create index on users (email)").unwrap_err();
    assert!(err.to_lowercase().contains("auto-commit"));
    db.execute("rollback").unwrap();
}

#[test]
fn test_foreign_key_insert_with_null_child_key_is_allowed() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id))")
        .unwrap();
    db.execute("insert into c values (1, null)").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\tnull");
}

#[test]
fn test_foreign_key_update_child_to_null_is_allowed() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id))")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("update c set pid = null where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\tnull");
}

#[test]
fn test_composite_foreign_key_insert_with_partial_null_is_allowed() {
    let mut db = test_db();
    db.execute("create table p (a int, b int, primary key(a,b))")
        .unwrap();
    db.execute("create table c (id int, a int, b int, foreign key(a,b) references p(a,b))")
        .unwrap();
    db.execute("insert into c values (1, null, 2)").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\ta\tb\n1\tnull\t2");
}

#[test]
fn test_unique_single_column_allows_multiple_nulls() {
    let mut db = test_db();
    db.execute("create table t (id int, email text unique)").unwrap();
    db.execute("insert into t values (1, null)").unwrap();
    db.execute("insert into t values (2, null)").unwrap();
    assert_eq!(db.execute("select * from t").unwrap(), "id\temail\n1\tnull\n2\tnull");
}

#[test]
fn test_unique_composite_allows_multiple_rows_with_null_member() {
    let mut db = test_db();
    db.execute("create table t (a int, b int, unique(a,b))").unwrap();
    db.execute("insert into t values (1, null)").unwrap();
    db.execute("insert into t values (1, null)").unwrap();
    assert_eq!(db.execute("select * from t").unwrap(), "a\tb\n1\tnull\n1\tnull");
}

#[test]
fn test_foreign_key_on_delete_cascade_propagates_to_grandchildren() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute(
        "create table c (id int primary key, pid int, foreign key(pid) references p(id) on delete cascade)",
    )
    .unwrap();
    db.execute(
        "create table g (id int, cid int, foreign key(cid) references c(id) on delete cascade)",
    )
    .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (10, 1)").unwrap();
    db.execute("insert into g values (100, 10)").unwrap();

    db.execute("delete from p where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid");
    assert_eq!(db.execute("select * from g").unwrap(), "id\tcid");
}

#[test]
fn test_foreign_key_on_delete_set_null_then_child_cascade_propagates() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute(
        "create table c (id int primary key, pid int, foreign key(pid) references p(id) on delete set null)",
    )
    .unwrap();
    db.execute(
        "create table g (id int, cid int, foreign key(cid) references c(id) on delete cascade)",
    )
    .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (10, 1)").unwrap();
    db.execute("insert into g values (100, 10)").unwrap();

    db.execute("delete from p where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n10\tnull");
    assert_eq!(db.execute("select * from g").unwrap(), "id\tcid\n100\t10");
}

#[test]
fn test_foreign_key_on_update_cascade_propagates_to_grandchildren() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute(
        "create table c (id int primary key, foreign key(id) references p(id) on update cascade)",
    )
    .unwrap();
    db.execute(
        "create table g (id int, cid int, foreign key(cid) references c(id) on update cascade)",
    )
    .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1)").unwrap();
    db.execute("insert into g values (100, 1)").unwrap();

    db.execute("update p set id = 2 where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\n2");
    assert_eq!(db.execute("select * from g").unwrap(), "id\tcid\n100\t2");
}


fn seed_users_3(db: &mut Database) {
    db.execute("create table users (id int, name text, age int)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a", 30)"#).unwrap();
    db.execute(r#"insert into users values (2, "b", 20)"#).unwrap();
    db.execute(r#"insert into users values (3, "c", 10)"#).unwrap();
}

fn row_count(output: &str) -> usize {
    let lines: Vec<&str> = output.lines().collect();
    if lines.is_empty() {
        0
    } else {
        lines.len() - 1
    }
}

macro_rules! limit_cases {
    ($( $name:ident => ($limit:expr, $expected:expr) ),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                let mut db = test_db();
                seed_users_3(&mut db);
                let out = db
                    .execute(&format!("select * from users order by id asc limit {}", $limit))
                    .unwrap();
                assert_eq!(row_count(&out), $expected);
            }
        )*
    };
}

limit_cases!(
    engine_more_limit_00 => (0, 0),
    engine_more_limit_01 => (1, 1),
    engine_more_limit_02 => (2, 2),
    engine_more_limit_03 => (3, 3),
    engine_more_limit_04 => (4, 3),
    engine_more_limit_05 => (5, 3),
    engine_more_limit_06 => (6, 3),
    engine_more_limit_07 => (7, 3),
    engine_more_limit_08 => (8, 3),
    engine_more_limit_09 => (9, 3),
    engine_more_limit_10 => (10, 3),
    engine_more_limit_11 => (11, 3),
    engine_more_limit_12 => (12, 3),
    engine_more_limit_13 => (13, 3),
    engine_more_limit_14 => (14, 3),
    engine_more_limit_15 => (15, 3),
    engine_more_limit_16 => (16, 3),
    engine_more_limit_17 => (17, 3),
    engine_more_limit_18 => (18, 3),
    engine_more_limit_19 => (19, 3),
    engine_more_limit_20 => (20, 3),
    engine_more_limit_21 => (21, 3),
    engine_more_limit_22 => (22, 3),
    engine_more_limit_23 => (23, 3),
    engine_more_limit_24 => (24, 3),
);

macro_rules! order_cases {
    ($( $name:ident => $sql:expr ),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                let mut db = test_db();
                seed_users_3(&mut db);
                let out = db.execute($sql).unwrap();
                assert!(!out.trim().is_empty());
                assert!(out.lines().count() >= 1);
            }
        )*
    };
}

order_cases!(
    engine_more_order_01 => "select * from users order by id asc",
    engine_more_order_02 => "select * from users order by id desc",
    engine_more_order_03 => "select * from users order by age asc",
    engine_more_order_04 => "select * from users order by age desc",
    engine_more_order_05 => "select * from users order by name asc",
    engine_more_order_06 => "select * from users order by name desc",
    engine_more_order_07 => "select id from users order by id asc",
    engine_more_order_08 => "select id from users order by id desc",
    engine_more_order_09 => "select name from users order by age asc",
    engine_more_order_10 => "select name from users order by age desc",
    engine_more_order_11 => "select * from users where age gte 10 order by id asc",
    engine_more_order_12 => "select * from users where age gte 10 order by id desc",
    engine_more_order_13 => "select * from users where name like \"*\" order by name asc",
    engine_more_order_14 => "select * from users where name like \"*\" order by name desc",
    engine_more_order_15 => "select id,name from users order by name asc limit 2",
    engine_more_order_16 => "select id,name from users order by name desc limit 2",
    engine_more_order_17 => "select id,name from users where id gte 1 order by id asc limit 2",
    engine_more_order_18 => "select id,name from users where id gte 1 order by id desc limit 2",
    engine_more_order_19 => "select id,name from users where id lte 3 order by age asc limit 3",
    engine_more_order_20 => "select id,name from users where id lte 3 order by age desc limit 3"
);

macro_rules! index_eq_cases {
    ($( $name:ident => $city:expr ),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                let mut db = test_db();
                db.execute("create table users (id int, city text, score int)")
                    .unwrap();
                db.execute("create index on users (city)").unwrap();
                db.execute(r#"insert into users values (1, "ny", 10)"#).unwrap();
                db.execute(r#"insert into users values (2, "ny", 20)"#).unwrap();
                db.execute(r#"insert into users values (3, "la", 30)"#).unwrap();
                let out = db
                    .execute(&format!(r#"select * from users where city = "{}""#, $city))
                    .unwrap();
                if $city == "ny" {
                    assert_eq!(row_count(&out), 2);
                } else if $city == "la" {
                    assert_eq!(row_count(&out), 1);
                } else {
                    assert_eq!(row_count(&out), 0);
                }
            }
        )*
    };
}

index_eq_cases!(
    engine_more_index_eq_01 => "ny",
    engine_more_index_eq_02 => "la",
    engine_more_index_eq_03 => "sf",
    engine_more_index_eq_04 => "ny",
    engine_more_index_eq_05 => "la",
    engine_more_index_eq_06 => "sf",
    engine_more_index_eq_07 => "ny",
    engine_more_index_eq_08 => "la",
    engine_more_index_eq_09 => "sf",
    engine_more_index_eq_10 => "ny",
    engine_more_index_eq_11 => "la",
    engine_more_index_eq_12 => "sf",
    engine_more_index_eq_13 => "ny",
    engine_more_index_eq_14 => "la",
    engine_more_index_eq_15 => "sf",
);

#[test]
fn engine_more_order_by_unknown_column_errors() {
    let mut db = test_db();
    seed_users_3(&mut db);
    let err = db.execute("select * from users order by missing asc").unwrap_err();
    assert!(err.to_lowercase().contains("unknown column"));
}


