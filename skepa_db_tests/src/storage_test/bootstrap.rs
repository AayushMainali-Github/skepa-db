use super::*;

#[test]
fn bootstrap_malformed_row_count_errors() {
    let root = temp_dir("bad_row_count");
    std::fs::create_dir_all(root.join("tables")).unwrap();
    std::fs::write(root.join("tables").join("users.rows"), "i:1\n").unwrap();

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

    let mut storage = DiskStorage::new(root).unwrap();
    let err = storage.bootstrap_table("users", &schema).unwrap_err();
    assert!(err.to_lowercase().contains("expected 2 values"));
}

#[test]
fn bootstrap_bad_type_prefix_errors() {
    let root = temp_dir("bad_prefix");
    std::fs::create_dir_all(root.join("tables")).unwrap();
    std::fs::write(root.join("tables").join("users.rows"), "t:abc\tt:name\n").unwrap();

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

    let mut storage = DiskStorage::new(root).unwrap();
    let err = storage.bootstrap_table("users", &schema).unwrap_err();
    assert!(err.to_lowercase().contains("expected int token prefix"));
}

#[test]
fn bootstrap_dangling_escape_errors() {
    let root = temp_dir("dangling_escape");
    std::fs::create_dir_all(root.join("tables")).unwrap();
    std::fs::write(root.join("tables").join("users.rows"), "i:1\tt:abc\\\n").unwrap();

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

    let mut storage = DiskStorage::new(root).unwrap();
    let err = storage.bootstrap_table("users", &schema).unwrap_err();
    assert!(err.to_lowercase().contains("dangling escape"));
}

#[test]
fn bootstrap_unsupported_escape_errors() {
    let root = temp_dir("unsupported_escape");
    std::fs::create_dir_all(root.join("tables")).unwrap();
    std::fs::write(root.join("tables").join("users.rows"), "i:1\tt:a\\x\n").unwrap();

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

    let mut storage = DiskStorage::new(root).unwrap();
    let err = storage.bootstrap_table("users", &schema).unwrap_err();
    assert!(err.to_lowercase().contains("unsupported escape sequence"));
}

#[test]
fn bootstrap_row_id_prefix_only_without_payload_errors() {
    let root = temp_dir("rowid_prefix_only");
    std::fs::create_dir_all(root.join("tables")).unwrap();
    std::fs::write(root.join("tables").join("users.rows"), "@1|\n").unwrap();

    let schema = Schema::new(vec![Column {
        name: "id".to_string(),
        dtype: DataType::Int,
        primary_key: false,
        unique: false,
        not_null: false,
    }]);

    let mut storage = DiskStorage::new(root).unwrap();
    let err = storage.bootstrap_table("users", &schema).unwrap_err();
    assert!(err.to_lowercase().contains("expected 1 values"));
}

#[test]
fn empty_wal_file_is_handled() {
    let path = temp_dir("empty_wal");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)")
            .unwrap();
    }
    std::fs::write(path.join("wal.log"), "").unwrap();
    let mut reopened = Database::open(path);
    let out = reopened.execute("select * from users").unwrap();
    assert_eq!(out, "id\tname");
}
