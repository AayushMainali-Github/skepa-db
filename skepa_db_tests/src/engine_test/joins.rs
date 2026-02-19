use super::*;

#[test]
fn test_select_inner_join_basic() {
    let mut db = test_db();
    db.execute("create table users (id int primary key, name text)")
        .unwrap();
    db.execute("create table profiles (user_id int, city text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "avi")"#)
        .unwrap();
    db.execute(r#"insert into profiles values (1, "ny")"#)
        .unwrap();
    db.execute(r#"insert into profiles values (2, "la")"#)
        .unwrap();

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

    let out = db
        .execute("select * from u join p on u.id = p.uid")
        .unwrap();
    assert_eq!(out, "u.id\tu.name\tp.uid\tp.city\n1\ta\t1\tx");
}

#[test]
fn test_select_inner_join_where_order_limit() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute("create table profiles (user_id int, city text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "avi")"#)
        .unwrap();
    db.execute(r#"insert into users values (3, "sam")"#)
        .unwrap();
    db.execute(r#"insert into profiles values (1, "ny")"#)
        .unwrap();
    db.execute(r#"insert into profiles values (2, "ny")"#)
        .unwrap();
    db.execute(r#"insert into profiles values (3, "la")"#)
        .unwrap();

    let out = db.execute(r#"select users.id,profiles.city from users join profiles on users.id = profiles.user_id where profiles.city = "ny" order by users.id desc limit 1"#).unwrap();
    assert_eq!(out, "users.id\tprofiles.city\n2\tny");
}

#[test]
fn test_select_inner_join_with_unqualified_unique_column_reference() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute("create table profiles (user_id int, city text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    db.execute(r#"insert into profiles values (1, "ny")"#)
        .unwrap();

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
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute("create table posts (user_id int, title text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "avi")"#)
        .unwrap();
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
    db.execute("create table users (id int, city text)")
        .unwrap();
    db.execute("create table city_info (city text, zone text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ny")"#).unwrap();
    db.execute(r#"insert into users values (2, "ny")"#).unwrap();
    db.execute(r#"insert into users values (3, "la")"#).unwrap();
    db.execute(r#"insert into city_info values ("ny", "east")"#)
        .unwrap();
    db.execute(r#"insert into city_info values ("la", "west")"#)
        .unwrap();

    let out = db
        .execute("select users.id,city_info.zone from users join city_info on users.city = city_info.city order by users.id asc")
        .unwrap();
    assert_eq!(out, "users.id\tcity_info.zone\n1\teast\n2\teast\n3\twest");
}

#[test]
fn test_select_left_join_includes_unmatched_left_rows() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute("create table profiles (user_id int, city text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "avi")"#)
        .unwrap();
    db.execute(r#"insert into users values (3, "sam")"#)
        .unwrap();
    db.execute(r#"insert into profiles values (1, "ny")"#)
        .unwrap();
    db.execute(r#"insert into profiles values (2, "la")"#)
        .unwrap();

    let out = db
        .execute("select users.id,profiles.city from users left join profiles on users.id = profiles.user_id order by users.id asc")
        .unwrap();
    assert_eq!(out, "users.id\tprofiles.city\n1\tny\n2\tla\n3\tnull");
}

#[test]
fn test_select_left_join_where_on_right_column_filters_null_rows_out() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute("create table profiles (user_id int, city text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "avi")"#)
        .unwrap();
    db.execute(r#"insert into users values (3, "sam")"#)
        .unwrap();
    db.execute(r#"insert into profiles values (1, "ny")"#)
        .unwrap();
    db.execute(r#"insert into profiles values (2, "la")"#)
        .unwrap();

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
