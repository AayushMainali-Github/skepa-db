use super::*;

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

