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

    assert_eq!(result, "id\tname");
}

#[test]
fn test_create_insert_select() {
    let mut db = Database::open("./test_db");

    db.execute("create users id:int name:text").unwrap();

    let insert_result = db.execute(r#"insert users 1 "ram""#).unwrap();
    assert_eq!(insert_result, "inserted 1 row into users");

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

    let result = db.execute(r#"insert users 1 "ram" "extra""#);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("Expected 2 values but got 3"));
}

#[test]
fn test_insert_type_mismatch_int() {
    let mut db = Database::open("./test_db");

    db.execute("create users id:int name:text").unwrap();

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

    let result = db.execute("create users id:int name:text");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("already exists"));
}

#[test]
fn test_multiple_tables() {
    let mut db = Database::open("./test_db");

    db.execute("create users id:int name:text").unwrap();
    db.execute("create products name:text price:int").unwrap();

    db.execute(r#"insert users 1 "ram""#).unwrap();
    db.execute(r#"insert products "laptop" 1000"#).unwrap();

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

#[test]
fn test_select_where_eq_int() {
    let mut db = Database::open("./test_db");

    db.execute("create users id:int name:text age:int").unwrap();
    db.execute(r#"insert users 1 "ram" 20"#).unwrap();
    db.execute(r#"insert users 2 "alice" 30"#).unwrap();

    let result = db.execute("select users where age = 30").unwrap();
    assert_eq!(result, "id\tname\tage\n2\talice\t30");
}

#[test]
fn test_select_where_eq_text() {
    let mut db = Database::open("./test_db");

    db.execute("create users id:int name:text").unwrap();
    db.execute(r#"insert users 1 "ram""#).unwrap();
    db.execute(r#"insert users 2 "alice""#).unwrap();

    let result = db.execute(r#"select users where name eq "ram""#).unwrap();
    assert_eq!(result, "id\tname\n1\tram");
}

#[test]
fn test_select_where_gt_lt_gte_lte() {
    let mut db = Database::open("./test_db");

    db.execute("create nums id:int value:int").unwrap();
    db.execute("insert nums 1 10").unwrap();
    db.execute("insert nums 2 20").unwrap();
    db.execute("insert nums 3 30").unwrap();

    assert_eq!(
        db.execute("select nums where value gt 20").unwrap(),
        "id\tvalue\n3\t30"
    );
    assert_eq!(
        db.execute("select nums where value < 20").unwrap(),
        "id\tvalue\n1\t10"
    );
    assert_eq!(
        db.execute("select nums where value gte 20").unwrap(),
        "id\tvalue\n2\t20\n3\t30"
    );
    assert_eq!(
        db.execute("select nums where value <= 20").unwrap(),
        "id\tvalue\n1\t10\n2\t20"
    );
}

#[test]
fn test_select_where_like_patterns() {
    let mut db = Database::open("./test_db");

    db.execute("create users id:int name:text").unwrap();
    db.execute(r#"insert users 1 "ram""#).unwrap();
    db.execute(r#"insert users 2 "ravi""#).unwrap();
    db.execute(r#"insert users 3 "amir""#).unwrap();

    assert_eq!(
        db.execute(r#"select users where name like "ra*""#).unwrap(),
        "id\tname\n1\tram\n2\travi"
    );
    assert_eq!(
        db.execute(r#"select users where name like "*ir""#).unwrap(),
        "id\tname\n3\tamir"
    );
    assert_eq!(
        db.execute(r#"select users where name like "*av*""#).unwrap(),
        "id\tname\n2\travi"
    );
}

#[test]
fn test_select_where_like_single_char_wildcard() {
    let mut db = Database::open("./test_db");

    db.execute("create users id:int name:text").unwrap();
    db.execute(r#"insert users 1 "ram""#).unwrap();
    db.execute(r#"insert users 2 "rom""#).unwrap();
    db.execute(r#"insert users 3 "ravi""#).unwrap();

    assert_eq!(
        db.execute(r#"select users where name like "r?m""#).unwrap(),
        "id\tname\n1\tram\n2\trom"
    );
    assert_eq!(
        db.execute(r#"select users where name like "??vi""#).unwrap(),
        "id\tname\n3\travi"
    );
    assert_eq!(
        db.execute(r#"select users where name like "r??""#).unwrap(),
        "id\tname\n1\tram\n2\trom"
    );
}

#[test]
fn test_select_where_unknown_column_errors() {
    let mut db = Database::open("./test_db");

    db.execute("create users id:int name:text").unwrap();
    let result = db.execute("select users where age = 10");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("unknown column"));
}

#[test]
fn test_select_where_text_comparison_with_gt_errors() {
    let mut db = Database::open("./test_db");

    db.execute("create users id:int name:text").unwrap();
    db.execute(r#"insert users 1 "ram""#).unwrap();

    let result = db.execute("select users where name gt 1");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("only valid for int"));
}

#[test]
fn test_select_where_like_on_int_errors() {
    let mut db = Database::open("./test_db");

    db.execute("create users id:int age:int").unwrap();
    db.execute("insert users 1 20").unwrap();

    let result = db.execute(r#"select users where age like "2*""#);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_lowercase().contains("only valid for text"));
}

