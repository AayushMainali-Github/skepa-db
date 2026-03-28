use super::*;
use skepa_db_core::config::DbConfig;

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
        let mut db = Database::open_legacy(path.clone());
        db.execute_legacy("create table users (id int, name text)")
            .unwrap();
    }
    std::fs::write(path.join("wal.log"), "").unwrap();
    let mut reopened = Database::open_legacy(path);
    let out = reopened.execute_legacy("select * from users").unwrap();
    assert_eq!(out, "id\tname");
}

#[test]
fn malformed_catalog_falls_back_to_empty_database_on_open() {
    let path = temp_dir("malformed_catalog_fallback");
    std::fs::create_dir_all(path.join("tables")).unwrap();
    std::fs::create_dir_all(path.join("indexes")).unwrap();
    std::fs::write(path.join("catalog.json"), "{ not valid json").unwrap();
    std::fs::write(path.join("wal.log"), "").unwrap();

    let mut db = Database::open_legacy(path.clone());
    let err = db.execute_legacy("select * from users").unwrap_err();
    assert!(err.to_lowercase().contains("does not exist"));

    db.execute_legacy("create table users (id int, name text)")
        .unwrap();
    db.execute_legacy(r#"insert into users values (1, "ram")"#)
        .unwrap();

    let reopened = Catalog::load_from_path(&path.join("catalog.json")).unwrap();
    assert!(reopened.schema("users").is_ok());
}

#[test]
fn newer_catalog_format_is_rejected_on_open() {
    let path = temp_dir("future_catalog_open_reject");
    std::fs::create_dir_all(path.join("tables")).unwrap();
    std::fs::create_dir_all(path.join("indexes")).unwrap();
    std::fs::write(
        path.join("catalog.json"),
        format!(
            r#"{{
  "format_version": {},
  "tables": {{}},
  "table_constraints": {{}}
}}"#,
            skepa_db_core::STORAGE_FORMAT_VERSION + 1
        ),
    )
    .unwrap();
    std::fs::write(path.join("wal.log"), "").unwrap();

    let err = Database::open(DbConfig::new(path)).unwrap_err();
    assert!(err.to_string().contains("newer than supported version"));
}
