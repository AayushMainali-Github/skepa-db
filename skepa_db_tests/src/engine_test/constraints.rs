use super::*;

#[test]
fn test_select_where_is_null_and_is_not_null() {
    let mut db = test_db();
    db.execute("create table users (id int, city text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ny")"#).unwrap();
    db.execute("insert into users values (2, null)").unwrap();

    let out_null = db
        .execute("select id from users where city is null order by id asc")
        .unwrap();
    assert_eq!(out_null, "id\n2");

    let out_not_null = db
        .execute("select id from users where city is not null order by id asc")
        .unwrap();
    assert_eq!(out_not_null, "id\n1");
}

#[test]
fn test_delete_where_is_not_null() {
    let mut db = test_db();
    db.execute("create table users (id int, city text)")
        .unwrap();
    db.execute("insert into users values (1, null)").unwrap();
    db.execute(r#"insert into users values (2, "la")"#).unwrap();
    db.execute(r#"insert into users values (3, "ny")"#).unwrap();

    let msg = db
        .execute("delete from users where city is not null")
        .unwrap();
    assert_eq!(msg, "deleted 2 row(s) from users");
    let out = db.execute("select * from users").unwrap();
    assert_eq!(out, "id\tcity\n1\tnull");
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
    let err = db.execute(r#"insert into t values ("hello")"#).unwrap_err();
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
    db.execute("create table t (dt date, ts timestamp)")
        .unwrap();
    db.execute(r#"insert into t values (2025-01-01, "2025-01-01 10:00:00")"#)
        .unwrap();
    db.execute(r#"insert into t values (2025-01-02, "2025-01-02 10:00:00")"#)
        .unwrap();
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
    db.execute("create table t (bi bigint, d decimal(8,2))")
        .unwrap();
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
    db.execute("create table users (id int primary key, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    let err = db
        .execute(r#"insert into users values (1, "alice")"#)
        .unwrap_err();
    assert!(err.to_lowercase().contains("primary key"));
}

#[test]
fn test_unique_constraint_insert() {
    let mut db = test_db();
    db.execute("create table users (id int, email text unique)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a@x.com")"#)
        .unwrap();
    let err = db
        .execute(r#"insert into users values (2, "a@x.com")"#)
        .unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
}

#[test]
fn test_not_null_constraint_insert() {
    let mut db = test_db();
    db.execute("create table users (id int, name text not null)")
        .unwrap();
    let err = db
        .execute(r#"insert into users values (1, null)"#)
        .unwrap_err();
    assert!(err.to_lowercase().contains("not null"));
}

#[test]
fn test_unique_constraint_update() {
    let mut db = test_db();
    db.execute("create table users (id int, email text unique)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a@x.com")"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "b@x.com")"#)
        .unwrap();
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
    db.execute("create table t (id int primary key, name text)")
        .unwrap();
    let err = db
        .execute(r#"insert into t values (null, "ram")"#)
        .unwrap_err();
    assert!(err.to_lowercase().contains("not null"));
}

#[test]
fn test_not_null_rejects_null_on_update() {
    let mut db = test_db();
    db.execute("create table t (id int primary key, name text not null)")
        .unwrap();
    db.execute(r#"insert into t values (1, "ram")"#).unwrap();
    let err = db
        .execute("update t set name = null where id = 1")
        .unwrap_err();
    assert!(err.to_lowercase().contains("not null"));
}

#[test]
fn test_primary_key_violation_on_update() {
    let mut db = test_db();
    db.execute("create table t (id int primary key, name text)")
        .unwrap();
    db.execute(r#"insert into t values (1, "a")"#).unwrap();
    db.execute(r#"insert into t values (2, "b")"#).unwrap();
    let err = db.execute("update t set id = 1 where id = 2").unwrap_err();
    assert!(err.to_lowercase().contains("primary key"));
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
    db.execute("create table t (a int, b int, unique(a,b))")
        .unwrap();
    db.execute("insert into t values (1, 1)").unwrap();
    db.execute("insert into t values (1, 2)").unwrap();
    let err = db.execute("update t set b = 1 where b = 2").unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
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
fn test_composite_pk_insert_conflict_still_rejected() {
    let mut db = test_db();
    db.execute("create table t (a int, b int, v text, primary key(a,b))")
        .unwrap();
    db.execute(r#"insert into t values (1, 1, "x")"#).unwrap();
    let err = db
        .execute(r#"insert into t values (1, 1, "y")"#)
        .unwrap_err();
    assert!(err.to_lowercase().contains("primary key"));
}

#[test]
fn test_unique_eq_select_path_returns_single_row() {
    let mut db = test_db();
    db.execute("create table users (id int, email text unique, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a@x.com", "a")"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "b@x.com", "b")"#)
        .unwrap();

    let out = db
        .execute(r#"select * from users where email = "b@x.com""#)
        .unwrap();
    assert_eq!(out, "id\temail\tname\n2\tb@x.com\tb");
}

#[test]
fn test_unique_eq_update_path_updates_only_target_row() {
    let mut db = test_db();
    db.execute("create table users (id int, email text unique, age int)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a@x.com", 10)"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "b@x.com", 20)"#)
        .unwrap();

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
    db.execute(r#"insert into users values (1, "a@x.com")"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "b@x.com")"#)
        .unwrap();

    let out = db
        .execute(r#"delete from users where email = "a@x.com""#)
        .unwrap();
    assert_eq!(out, "deleted 1 row(s) from users");
    assert_eq!(
        db.execute("select * from users").unwrap(),
        "id\temail\n2\tb@x.com"
    );
}

#[test]
fn test_unique_update_reindexes_for_future_unique_lookup() {
    let mut db = test_db();
    db.execute("create table users (id int, email text unique)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a@x.com")"#)
        .unwrap();

    db.execute(r#"update users set email = "z@x.com" where id = 1"#)
        .unwrap();
    assert_eq!(
        db.execute(r#"select * from users where email = "z@x.com""#)
            .unwrap(),
        "id\temail\n1\tz@x.com"
    );
    assert_eq!(
        db.execute(r#"select * from users where email = "a@x.com""#)
            .unwrap(),
        "id\temail"
    );
}

#[test]
fn test_select_unique_eq_no_match_header_only() {
    let mut db = test_db();
    db.execute("create table t (id int, email text unique)")
        .unwrap();
    db.execute(r#"insert into t values (1, "a@x.com")"#)
        .unwrap();
    let out = db
        .execute(r#"select * from t where email = "x@x.com""#)
        .unwrap();
    assert_eq!(out, "id\temail");
}

#[test]
fn test_update_unique_eq_no_match_returns_zero() {
    let mut db = test_db();
    db.execute("create table t (id int, email text unique, v int)")
        .unwrap();
    db.execute(r#"insert into t values (1, "a@x.com", 1)"#)
        .unwrap();
    let out = db
        .execute(r#"update t set v = 2 where email = "x@x.com""#)
        .unwrap();
    assert_eq!(out, "updated 0 row(s) in t");
}

#[test]
fn test_delete_unique_eq_no_match_returns_zero() {
    let mut db = test_db();
    db.execute("create table t (id int, email text unique)")
        .unwrap();
    db.execute(r#"insert into t values (1, "a@x.com")"#)
        .unwrap();
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
        db.execute(&format!(
            "insert into t values ({}, {}, {})",
            i,
            i + 1,
            i + 2
        ))
        .unwrap();
    }
    let err = db.execute("insert into t values (5, 6, 999)").unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
}

#[test]
fn test_composite_unique_update_to_duplicate_rejected() {
    let mut db = test_db();
    db.execute("create table t (a int, b int, unique(a,b))")
        .unwrap();
    db.execute("insert into t values (1, 1)").unwrap();
    db.execute("insert into t values (2, 2)").unwrap();
    let err = db
        .execute("update t set a = 1, b = 1 where a = 2")
        .unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
}

#[test]
fn test_alter_add_unique_enforces_existing_data() {
    let mut db = test_db();
    db.execute("create table t (id int, email text)").unwrap();
    db.execute(r#"insert into t values (1, "a@x.com")"#)
        .unwrap();
    db.execute(r#"insert into t values (2, "a@x.com")"#)
        .unwrap();
    let err = db.execute("alter table t add unique(email)").unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
}

#[test]
fn test_alter_add_and_drop_unique() {
    let mut db = test_db();
    db.execute("create table t (id int, email text)").unwrap();
    db.execute("alter table t add unique(email)").unwrap();
    db.execute(r#"insert into t values (1, "a@x.com")"#)
        .unwrap();
    let err = db
        .execute(r#"insert into t values (2, "a@x.com")"#)
        .unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
    db.execute("alter table t drop unique(email)").unwrap();
    db.execute(r#"insert into t values (2, "a@x.com")"#)
        .unwrap();
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
    db.execute("alter table t alter column name set not null")
        .unwrap();
    let err = db.execute("insert into t values (2, null)").unwrap_err();
    assert!(err.to_lowercase().contains("not null"));
    db.execute("alter table t alter column name drop not null")
        .unwrap();
    db.execute("insert into t values (2, null)").unwrap();
}

#[test]
fn test_unique_single_column_allows_multiple_nulls() {
    let mut db = test_db();
    db.execute("create table t (id int, email text unique)")
        .unwrap();
    db.execute("insert into t values (1, null)").unwrap();
    db.execute("insert into t values (2, null)").unwrap();
    assert_eq!(
        db.execute("select * from t").unwrap(),
        "id\temail\n1\tnull\n2\tnull"
    );
}

#[test]
fn test_unique_composite_allows_multiple_rows_with_null_member() {
    let mut db = test_db();
    db.execute("create table t (a int, b int, unique(a,b))")
        .unwrap();
    db.execute("insert into t values (1, null)").unwrap();
    db.execute("insert into t values (1, null)").unwrap();
    assert_eq!(
        db.execute("select * from t").unwrap(),
        "a\tb\n1\tnull\n1\tnull"
    );
}

#[test]
fn test_alter_add_unique_failure_rolls_back_catalog_state() {
    let mut db = test_db();
    db.execute("create table t (id int, email text)").unwrap();
    db.execute(r#"insert into t values (1, "dup@x.com")"#)
        .unwrap();
    db.execute(r#"insert into t values (2, "dup@x.com")"#)
        .unwrap();

    let err = db.execute("alter table t add unique(email)").unwrap_err();
    assert!(err.to_lowercase().contains("unique"));

    // UNIQUE must not remain partially applied after failed ALTER.
    db.execute(r#"insert into t values (3, "dup@x.com")"#)
        .unwrap();
    assert_eq!(
        db.execute("select id from t order by id asc").unwrap(),
        "id\n1\n2\n3"
    );
}

#[test]
fn test_alter_add_fk_failure_rolls_back_catalog_state() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int)").unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 999)").unwrap();

    let err = db
        .execute("alter table c add foreign key(pid) references p(id)")
        .unwrap_err();
    assert!(err.to_lowercase().contains("foreign key"));

    // FK must not remain partially applied after failed ALTER.
    db.execute("insert into c values (2, 888)").unwrap();
    assert_eq!(
        db.execute("select id,pid from c order by id asc").unwrap(),
        "id\tpid\n1\t999\n2\t888"
    );
}
