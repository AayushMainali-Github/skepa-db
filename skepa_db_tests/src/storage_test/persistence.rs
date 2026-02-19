use super::*;

#[test]
fn diskstorage_persist_bootstrap_roundtrip() {
    let root = temp_dir("persist_bootstrap");
    let mut storage = DiskStorage::new(root.clone()).unwrap();
    storage.create_table("users").unwrap();
    storage
        .insert_row("users", vec![Value::Int(1), Value::Text("ram".to_string())])
        .unwrap();
    storage.persist_table("users").unwrap();

    let schema = Schema::new(vec![
        Column {
            name: "id".to_string(),
            dtype: DataType::Int,
            primary_key: false,
            unique: false,
            not_null: false,
        },
        Column {
            name: "name".to_string(),
            dtype: DataType::Text,
            primary_key: false,
            unique: false,
            not_null: false,
        },
    ]);

    let mut loaded = DiskStorage::new(root).unwrap();
    loaded.bootstrap_table("users", &schema).unwrap();
    let rows = loaded.scan("users").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int(1));
    assert_eq!(rows[0][1], Value::Text("ram".to_string()));
}

#[test]
fn diskstorage_text_escape_roundtrip() {
    let root = temp_dir("escape_roundtrip");
    let mut storage = DiskStorage::new(root.clone()).unwrap();
    storage.create_table("users").unwrap();
    storage
        .insert_row(
            "users",
            vec![
                Value::Int(1),
                Value::Text("line1\nline2\tpath\\file".to_string()),
            ],
        )
        .unwrap();
    storage.persist_table("users").unwrap();

    let schema = Schema::new(vec![
        Column {
            name: "id".to_string(),
            dtype: DataType::Int,
            primary_key: false,
            unique: false,
            not_null: false,
        },
        Column {
            name: "name".to_string(),
            dtype: DataType::Text,
            primary_key: false,
            unique: false,
            not_null: false,
        },
    ]);

    let mut loaded = DiskStorage::new(root).unwrap();
    loaded.bootstrap_table("users", &schema).unwrap();
    let rows = loaded.scan("users").unwrap();
    assert_eq!(
        rows[0][1],
        Value::Text("line1\nline2\tpath\\file".to_string())
    );
}

#[test]
fn checkpoint_writes_table_files() {
    let root = temp_dir("checkpoint_files");
    let mut storage = DiskStorage::new(root.clone()).unwrap();
    storage.create_table("users").unwrap();
    storage.create_table("products").unwrap();
    storage
        .insert_row("users", vec![Value::Int(1), Value::Text("ram".to_string())])
        .unwrap();
    storage
        .insert_row(
            "products",
            vec![Value::Text("laptop".to_string()), Value::Int(1000)],
        )
        .unwrap();

    storage.checkpoint_all().unwrap();
    let users_rows = std::fs::read_to_string(root.join("tables").join("users.rows")).unwrap();
    let products_rows = std::fs::read_to_string(root.join("tables").join("products.rows")).unwrap();
    assert!(!users_rows.trim().is_empty());
    assert!(!products_rows.trim().is_empty());
}

#[test]
fn reopen_is_idempotent_no_duplicate_rows() {
    let path = temp_dir("reopen_idempotent");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)")
            .unwrap();
        db.execute(r#"insert into users values (1, "ram")"#)
            .unwrap();
    }
    {
        let mut db = Database::open(path.clone());
        assert_eq!(
            db.execute("select * from users").unwrap(),
            "id\tname\n1\tram"
        );
    }
    {
        let mut db = Database::open(path.clone());
        assert_eq!(
            db.execute("select * from users").unwrap(),
            "id\tname\n1\tram"
        );
    }
}

#[test]
fn persistence_roundtrip_extended_types() {
    let path = temp_dir("persist_ext");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table t (b bool, bi bigint, d decimal(6,2), v varchar(5), dt date, ts timestamp, u uuid, j json, bl blob)")
            .unwrap();
        db.execute(r#"insert into t values (true, 123456, 12.34, "hello", 2025-01-02, "2025-01-02 03:04:05", 550e8400-e29b-41d4-a716-446655440000, "{\"k\":1}", 0xABCD)"#)
            .unwrap();
    }
    {
        let mut db = Database::open(path.clone());
        let out = db.execute("select * from t").unwrap();
        assert!(out.contains("true"));
        assert!(out.contains("123456"));
        assert!(out.contains("12.34"));
        assert!(out.contains("hello"));
        assert!(out.contains("2025-01-02"));
        assert!(out.contains("550e8400-e29b-41d4-a716-446655440000"));
        assert!(out.contains("{\"k\":1}"));
        assert!(out.contains("0xABCD"));
    }
}

#[test]
fn diskstorage_persists_null_values_roundtrip() {
    let root = temp_dir("null_roundtrip");
    {
        let mut db = Database::open(root.clone());
        db.execute("create table t (id int, name text)").unwrap();
        db.execute("insert into t values (1, null)").unwrap();
    }
    {
        let mut db = Database::open(root.clone());
        assert_eq!(db.execute("select * from t").unwrap(), "id\tname\n1\tnull");
    }
}
