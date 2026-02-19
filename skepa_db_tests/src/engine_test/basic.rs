use super::*;

#[test]
fn test_create_table() {
    let mut db = test_db();
    let result = db
        .execute("create table users (id int, name text)")
        .unwrap();
    assert_eq!(result, "created table users");
}

#[test]
fn test_create_and_select_empty() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    let result = db.execute("select * from users").unwrap();
    assert_eq!(result, "id\tname");
}

#[test]
fn test_create_insert_select() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();

    let insert_result = db
        .execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    assert_eq!(insert_result, "inserted 1 row into users");

    let select_result = db.execute("select * from users").unwrap();
    assert_eq!(select_result, "id\tname\n1\tram");
}

#[test]
fn test_create_duplicate_table() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();

    let result = db.execute("create table users (id int, name text)");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists"));
}

#[test]
fn test_multiple_tables() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute("create table products (name text, price int)")
        .unwrap();

    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    db.execute(r#"insert into products values ("laptop", 1000)"#)
        .unwrap();

    let users = db.execute("select * from users").unwrap();
    assert_eq!(users, "id\tname\n1\tram");

    let products = db.execute("select * from products").unwrap();
    assert_eq!(products, "name\tprice\nlaptop\t1000");
}

#[test]
fn test_empty_string_text_value() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "")"#).unwrap();

    let result = db.execute("select * from users").unwrap();
    assert_eq!(result, "id\tname\n1\t");
}

#[test]
fn test_text_with_spaces() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram kumar")"#)
        .unwrap();

    let result = db.execute("select * from users").unwrap();
    assert_eq!(result, "id\tname\n1\tram kumar");
}

#[test]
fn test_negative_integers() {
    let mut db = test_db();
    db.execute("create table temps (id int, value int)")
        .unwrap();
    db.execute("insert into temps values (1, -10)").unwrap();

    let result = db.execute("select * from temps").unwrap();
    assert_eq!(result, "id\tvalue\n1\t-10");
}

#[test]
fn test_large_integers() {
    let mut db = test_db();
    db.execute("create table nums (id int, value int)").unwrap();
    db.execute("insert into nums values (1, 999999999)")
        .unwrap();

    let result = db.execute("select * from nums").unwrap();
    assert_eq!(result, "id\tvalue\n1\t999999999");
}
