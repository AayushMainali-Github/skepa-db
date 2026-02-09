use skepa_db_core::parser::command::ColumnDef;
use skepa_db_core::storage::{Catalog, Column, DiskStorage, Schema, StorageEngine};
use skepa_db_core::types::datatype::DataType;
use skepa_db_core::types::value::Value;
use skepa_db_core::Database;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

fn temp_dir(prefix: &str) -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path = std::env::temp_dir();
    path.push(format!(
        "skepa_db_storage_{}_{}_{}",
        prefix,
        std::process::id(),
        id
    ));
    let _ = std::fs::remove_dir_all(&path);
    path
}

#[test]
fn catalog_save_load_roundtrip() {
    let mut catalog = Catalog::new();
    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    dtype: DataType::Int,
                    primary_key: false,
                    unique: false,
                    not_null: false,
                },
                ColumnDef {
                    name: "name".to_string(),
                    dtype: DataType::Text,
                    primary_key: false,
                    unique: false,
                    not_null: false,
                },
            ],
            vec![],
        )
        .unwrap();

    let path = temp_dir("catalog_roundtrip");
    std::fs::create_dir_all(&path).unwrap();
    let catalog_path = path.join("catalog.json");
    catalog.save_to_path(&catalog_path).unwrap();

    let loaded = Catalog::load_from_path(&catalog_path).unwrap();
    let schema = loaded.schema("users").unwrap();
    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.columns[0].name, "id");
    assert_eq!(schema.columns[1].name, "name");
}

#[test]
fn catalog_load_missing_file_is_empty() {
    let path = temp_dir("catalog_missing").join("missing_catalog.json");
    let loaded = Catalog::load_from_path(&path).unwrap();
    assert!(loaded.schema("users").is_err());
}

#[test]
fn diskstorage_persist_bootstrap_roundtrip() {
    let root = temp_dir("persist_bootstrap");
    let mut storage = DiskStorage::new(root.clone()).unwrap();
    storage.create_table("users").unwrap();
    storage
        .insert_row(
            "users",
            vec![Value::Int(1), Value::Text("ram".to_string())],
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
fn checkpoint_writes_table_files() {
    let root = temp_dir("checkpoint_files");
    let mut storage = DiskStorage::new(root.clone()).unwrap();
    storage.create_table("users").unwrap();
    storage.create_table("products").unwrap();
    storage
        .insert_row(
            "users",
            vec![Value::Int(1), Value::Text("ram".to_string())],
        )
        .unwrap();
    storage
        .insert_row(
            "products",
            vec![Value::Text("laptop".to_string()), Value::Int(1000)],
        )
        .unwrap();

    storage.checkpoint_all().unwrap();
    let users_rows = std::fs::read_to_string(root.join("tables").join("users.rows")).unwrap();
    let products_rows =
        std::fs::read_to_string(root.join("tables").join("products.rows")).unwrap();
    assert!(!users_rows.trim().is_empty());
    assert!(!products_rows.trim().is_empty());
}

#[test]
fn wal_is_truncated_after_write() {
    let path = temp_dir("wal_truncate");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)").unwrap();
        db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    }
    let wal = std::fs::read_to_string(path.join("wal.log")).unwrap();
    assert_eq!(wal, "");
}

#[test]
fn reopen_is_idempotent_no_duplicate_rows() {
    let path = temp_dir("reopen_idempotent");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)").unwrap();
        db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    }
    {
        let mut db = Database::open(path.clone());
        assert_eq!(db.execute("select * from users").unwrap(), "id\tname\n1\tram");
    }
    {
        let mut db = Database::open(path.clone());
        assert_eq!(db.execute("select * from users").unwrap(), "id\tname\n1\tram");
    }
}

#[test]
fn empty_wal_file_is_handled() {
    let path = temp_dir("empty_wal");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)").unwrap();
    }
    std::fs::write(path.join("wal.log"), "").unwrap();
    let mut reopened = Database::open(path);
    let out = reopened.execute("select * from users").unwrap();
    assert_eq!(out, "id\tname");
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
fn recovery_ignores_uncommitted_wal_transaction() {
    let path = temp_dir("wal_uncommitted_ignored");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)").unwrap();
    }

    // Simulate crash after BEGIN + OP, before COMMIT.
    std::fs::write(
        path.join("wal.log"),
        "BEGIN 42\nOP 42 insert into users values (1, \"ram\")\n",
    )
    .unwrap();

    {
        let mut db = Database::open(path.clone());
        let out = db.execute("select * from users").unwrap();
        assert_eq!(out, "id\tname");
    }
}

#[test]
fn recovery_replays_committed_wal_transaction() {
    let path = temp_dir("wal_committed_replayed");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)").unwrap();
    }

    // Simulate crash after COMMIT record is durable but before checkpoint.
    std::fs::write(
        path.join("wal.log"),
        "BEGIN 7\nOP 7 insert into users values (1, \"ram\")\nCOMMIT 7\n",
    )
    .unwrap();

    {
        let mut db = Database::open(path.clone());
        let out = db.execute("select * from users").unwrap();
        assert_eq!(out, "id\tname\n1\tram");
    }
}

#[test]
fn recovery_replays_only_committed_when_wal_has_mixed_transactions() {
    let path = temp_dir("wal_mixed_recovery");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)").unwrap();
    }

    std::fs::write(
        path.join("wal.log"),
        concat!(
            "BEGIN 1\n",
            "OP 1 insert into users values (1, \"a\")\n",
            "COMMIT 1\n",
            "BEGIN 2\n",
            "OP 2 insert into users values (2, \"b\")\n",
            // no COMMIT for tx 2
            "BEGIN 3\n",
            "OP 3 insert into users values (3, \"c\")\n",
            "COMMIT 3\n"
        ),
    )
    .unwrap();

    {
        let mut db = Database::open(path.clone());
        let out = db.execute("select * from users").unwrap();
        assert_eq!(out, "id\tname\n1\ta\n3\tc");
    }
}

#[test]
fn recovery_ignores_explicitly_rolled_back_transaction() {
    let path = temp_dir("wal_rolled_back_ignored");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)").unwrap();
    }

    std::fs::write(
        path.join("wal.log"),
        "BEGIN 10\nOP 10 insert into users values (1, \"ram\")\nROLLBACK 10\n",
    )
    .unwrap();

    {
        let mut db = Database::open(path.clone());
        let out = db.execute("select * from users").unwrap();
        assert_eq!(out, "id\tname");
    }
}

#[test]
fn recovery_commit_without_ops_is_noop() {
    let path = temp_dir("wal_commit_noop");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)").unwrap();
    }

    std::fs::write(path.join("wal.log"), "BEGIN 99\nCOMMIT 99\n").unwrap();

    {
        let mut db = Database::open(path.clone());
        let out = db.execute("select * from users").unwrap();
        assert_eq!(out, "id\tname");
    }
}

#[test]
fn recovery_ignores_commit_for_unknown_transaction() {
    let path = temp_dir("wal_unknown_commit");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)").unwrap();
    }

    std::fs::write(path.join("wal.log"), "COMMIT 123\n").unwrap();

    {
        let mut db = Database::open(path.clone());
        let out = db.execute("select * from users").unwrap();
        assert_eq!(out, "id\tname");
    }
}
