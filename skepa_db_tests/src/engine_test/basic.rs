use super::*;

#[test]
fn test_create_table() {
    let mut db = test_db();
    let result = db
        .execute("create table users (id int, name text)")
        .unwrap();
    assert_schema_change_result(result, "created table users");
}

#[test]
fn test_create_and_select_empty() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    let result = db.execute("select * from users").unwrap();
    assert_select_result(result, &["id", "name"], vec![]);
}

#[test]
fn test_create_insert_select() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();

    let insert_result = db
        .execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    assert_mutation_result(insert_result, "inserted 1 row into users", 1);

    let select_result = db.execute("select * from users").unwrap();
    assert_select_result(
        select_result,
        &["id", "name"],
        vec![vec![Value::Int(1), Value::Text("ram".to_string())]],
    );
}

#[test]
fn test_insert_uses_trailing_default_values() {
    let mut db = test_db();
    db.execute(r#"create table users (id int, name text default "anon", active bool default true)"#)
        .unwrap();

    db.execute("insert into users values (1)").unwrap();

    let result = db.execute("select * from users").unwrap();
    assert_select_result(
        result,
        &["id", "name", "active"],
        vec![vec![
            Value::Int(1),
            Value::Text("anon".to_string()),
            Value::Bool(true),
        ]],
    );
}

#[test]
fn test_explicit_null_does_not_use_default() {
    let mut db = test_db();
    db.execute(r#"create table users (id int, name text default "anon")"#)
        .unwrap();

    db.execute("insert into users values (1, null)").unwrap();

    let result = db.execute("select * from users").unwrap();
    assert_select_result(
        result,
        &["id", "name"],
        vec![vec![Value::Int(1), Value::Null]],
    );
}

#[test]
fn test_missing_non_default_column_still_errors() {
    let mut db = test_db();
    db.execute(r#"create table users (id int, name text default "anon", age int)"#)
        .unwrap();

    let err = db.execute_legacy("insert into users values (1)").unwrap_err();
    assert!(err.contains("Missing column 'age' has no DEFAULT"));
}

#[test]
fn test_invalid_default_value_errors_at_create_time() {
    let mut db = test_db();
    let err = db
        .execute_legacy(r#"create table users (id int, active bool default "yes")"#)
        .unwrap_err();
    assert!(err.contains("Invalid DEFAULT for column 'active'"));
}

#[test]
fn test_create_duplicate_table() {
    let mut db = test_db();
    db.execute_legacy("create table users (id int, name text)")
        .unwrap();

    let result = db.execute_legacy("create table users (id int, name text)");
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
    assert_select_result(
        users,
        &["id", "name"],
        vec![vec![Value::Int(1), Value::Text("ram".to_string())]],
    );

    let products = db.execute("select * from products").unwrap();
    assert_select_result(
        products,
        &["name", "price"],
        vec![vec![Value::Text("laptop".to_string()), Value::Int(1000)]],
    );
}

#[test]
fn test_empty_string_text_value() {
    let mut db = test_db();
    db.execute_legacy("create table users (id int, name text)")
        .unwrap();
    db.execute_legacy(r#"insert into users values (1, "")"#)
        .unwrap();

    let result = db.execute_legacy("select * from users").unwrap();
    assert_eq!(result, "id\tname\n1\t");
}

#[test]
fn test_text_with_spaces() {
    let mut db = test_db();
    db.execute_legacy("create table users (id int, name text)")
        .unwrap();
    db.execute_legacy(r#"insert into users values (1, "ram kumar")"#)
        .unwrap();

    let result = db.execute_legacy("select * from users").unwrap();
    assert_eq!(result, "id\tname\n1\tram kumar");
}

#[test]
fn test_negative_integers() {
    let mut db = test_db();
    db.execute_legacy("create table temps (id int, value int)")
        .unwrap();
    db.execute_legacy("insert into temps values (1, -10)")
        .unwrap();

    let result = db.execute_legacy("select * from temps").unwrap();
    assert_eq!(result, "id\tvalue\n1\t-10");
}

#[test]
fn test_large_integers() {
    let mut db = test_db();
    db.execute_legacy("create table nums (id int, value int)")
        .unwrap();
    db.execute_legacy("insert into nums values (1, 999999999)")
        .unwrap();

    let result = db.execute_legacy("select * from nums").unwrap();
    assert_eq!(result, "id\tvalue\n1\t999999999");
}
