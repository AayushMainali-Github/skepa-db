use skepa_db_core::Database;

#[test]
fn test_create_table() {
    let mut db = Database::open("./test_db");
    let result = db.execute("create users id:int name:text").unwrap();
    assert_eq!(result, "created table users");
}

#[test]
fn test_create_and_select_empty() {
    let mut db = Database::open("./test_db");
    db.execute("create users id:int name:text").unwrap();
    let result = db.execute("select users").unwrap();
    
    // Should show header only
    assert_eq!(result, "id\tname");
}

#[test]
fn test_create_insert_select() {
    let mut db = Database::open("./test_db");
    
    // Create table
    db.execute("create users id:int name:text").unwrap();
    
    // Insert row
    let insert_result = db.execute(r#"insert users 1 "ram""#).unwrap();
    assert_eq!(insert_result, "inserted 1 row into users");
    
    // Select and verify
    let select_result = db.execute("select users").unwrap();
    assert_eq!(select_result, "id\tname\n1\tram");
}

#[test]
fn test_insert_multiple_rows() {
    let mut db = Database::open("./test_db");
    
    db.execute("create users id:int name:text").unwrap();
    db.execute(r#"insert users 1 "ram""#).unwrap();
    db.execute(r#"insert users 2 "alice""#).unwrap();
    
    let result = db.execute("select users").unwrap();
    assert_eq!(result, "id\tname\n1\tram\n2\talice");
}

#[test]
fn test_insert_into_missing_table() {
    let mut db = Database::open("./test_db");
    
    let result = db.execute(r#"insert users 1 "ram""#);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("does not exist"));
}

#[test]
fn test_insert_wrong_value_count() {
    let mut db = Database::open("./test_db");
    
    db.execute("create users id:int name:text").unwrap();
    
    // Try to insert 3 values when only 2 columns exist
    let result = db.execute(r#"insert users 1 "ram" "extra""#);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("Expected 2 values but got 3"));
}

#[test]
fn test_insert_type_mismatch_int() {
    let mut db = Database::open("./test_db");
    
    db.execute("create users id:int name:text").unwrap();
    
    // Try to insert text where int is expected
    let result = db.execute(r#"insert users "abc" "ram""#);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_lowercase().contains("expected int"));
}

#[test]
fn test_select_from_missing_table() {
    let mut db = Database::open("./test_db");
    
    let result = db.execute("select nonexistent");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("does not exist"));
}

#[test]
fn test_create_duplicate_table() {
    let mut db = Database::open("./test_db");
    
    db.execute("create users id:int name:text").unwrap();
    
    // Try to create again
    let result = db.execute("create users id:int name:text");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("already exists"));
}

#[test]
fn test_multiple_tables() {
    let mut db = Database::open("./test_db");
    
    // Create two tables
    db.execute("create users id:int name:text").unwrap();
    db.execute("create products name:text price:int").unwrap();
    
    // Insert into both
    db.execute(r#"insert users 1 "ram""#).unwrap();
    db.execute(r#"insert products "laptop" 1000"#).unwrap();
    
    // Select from both
    let users = db.execute("select users").unwrap();
    assert_eq!(users, "id\tname\n1\tram");
    
    let products = db.execute("select products").unwrap();
    assert_eq!(products, "name\tprice\nlaptop\t1000");
}

#[test]
fn test_empty_string_text_value() {
    let mut db = Database::open("./test_db");
    
    db.execute("create users id:int name:text").unwrap();
    db.execute(r#"insert users 1 """#).unwrap();
    
    let result = db.execute("select users").unwrap();
    assert_eq!(result, "id\tname\n1\t");
}

#[test]
fn test_text_with_spaces() {
    let mut db = Database::open("./test_db");
    
    db.execute("create users id:int name:text").unwrap();
    db.execute(r#"insert users 1 "ram kumar""#).unwrap();
    
    let result = db.execute("select users").unwrap();
    assert_eq!(result, "id\tname\n1\tram kumar");
}

#[test]
fn test_negative_integers() {
    let mut db = Database::open("./test_db");
    
    db.execute("create temps id:int value:int").unwrap();
    db.execute("insert temps 1 -10").unwrap();
    
    let result = db.execute("select temps").unwrap();
    assert_eq!(result, "id\tvalue\n1\t-10");
}

#[test]
fn test_large_integers() {
    let mut db = Database::open("./test_db");
    
    db.execute("create nums id:int value:int").unwrap();
    db.execute("insert nums 1 999999999").unwrap();
    
    let result = db.execute("select nums").unwrap();
    assert_eq!(result, "id\tvalue\n1\t999999999");
}
