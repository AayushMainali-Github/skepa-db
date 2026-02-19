use super::*;

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
fn test_alter_add_fk_on_delete_cascade_applies_behavior() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int)").unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into p values (2)").unwrap();
    db.execute("insert into c values (10, 1)").unwrap();
    db.execute("insert into c values (11, 2)").unwrap();

    db.execute("alter table c add foreign key(pid) references p(id) on delete cascade")
        .unwrap();
    db.execute("delete from p where id = 1").unwrap();

    assert_eq!(
        db.execute("select id,pid from c order by id asc").unwrap(),
        "id\tpid\n11\t2"
    );
}

#[test]
fn test_alter_add_fk_on_update_cascade_applies_behavior() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int)").unwrap();
    db.execute("insert into p values (7)").unwrap();
    db.execute("insert into c values (1, 7)").unwrap();

    db.execute("alter table c add foreign key(pid) references p(id) on update cascade")
        .unwrap();
    db.execute("update p set id = 70 where id = 7").unwrap();

    assert_eq!(
        db.execute("select id,pid from c order by id asc").unwrap(),
        "id\tpid\n1\t70"
    );
}

#[test]
fn test_alter_add_fk_on_delete_set_null_applies_behavior() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int)").unwrap();
    db.execute("insert into p values (5)").unwrap();
    db.execute("insert into c values (1, 5)").unwrap();

    db.execute("alter table c add foreign key(pid) references p(id) on delete set null")
        .unwrap();
    db.execute("delete from p where id = 5").unwrap();

    assert_eq!(
        db.execute("select id,pid from c order by id asc").unwrap(),
        "id\tpid\n1\tnull"
    );
}

#[test]
fn test_alter_drop_one_fk_keeps_other_fk_enforced() {
    let mut db = test_db();
    db.execute("create table p1 (id int primary key)").unwrap();
    db.execute("create table p2 (id int primary key)").unwrap();
    db.execute("create table c (id int, p1_id int, p2_id int)").unwrap();
    db.execute("insert into p1 values (1)").unwrap();
    db.execute("insert into p2 values (2)").unwrap();
    db.execute("insert into c values (1, 1, 2)").unwrap();

    db.execute("alter table c add foreign key(p1_id) references p1(id)")
        .unwrap();
    db.execute("alter table c add foreign key(p2_id) references p2(id)")
        .unwrap();
    db.execute("alter table c drop foreign key(p1_id) references p1(id)")
        .unwrap();

    // Dropped FK should no longer be enforced.
    db.execute("insert into c values (2, 999, 2)").unwrap();
    // Remaining FK must still be enforced.
    let err = db.execute("insert into c values (3, 1, 999)").unwrap_err();
    assert!(err.to_lowercase().contains("foreign key"));
}

