use super::*;

#[test]
fn test_constraint_persistence_after_reopen() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_constraints_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open_legacy(path.clone());
        db.execute_legacy(
            "create table t (id int primary key, email text unique, name text not null)",
        )
        .unwrap();
        db.execute_legacy(r#"insert into t values (1, "a@x.com", "ram")"#)
            .unwrap();
    }
    {
        let mut db = Database::open_legacy(path.clone());
        let e1 = db
            .execute_legacy(r#"insert into t values (1, "b@x.com", "bob")"#)
            .unwrap_err();
        assert!(e1.to_lowercase().contains("primary key"));
        let e2 = db
            .execute_legacy(r#"insert into t values (2, "a@x.com", "bob")"#)
            .unwrap_err();
        assert!(e2.to_lowercase().contains("unique"));
        let e3 = db
            .execute_legacy(r#"insert into t values (3, "c@x.com", null)"#)
            .unwrap_err();
        assert!(e3.to_lowercase().contains("not null"));
    }
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_persistence_reopen_insert() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_persist_{}_insert", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open_legacy(path.clone());
        db.execute_legacy("create table users (id int, name text)")
            .unwrap();
        db.execute_legacy(r#"insert into users values (1, "ram")"#)
            .unwrap();
    }

    {
        let mut db = Database::open_legacy(path.clone());
        let out = db.execute_legacy("select * from users").unwrap();
        assert_eq!(out, "id\tname\n1\tram");
    }

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_persistence_reopen_update_delete() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_persist_{}_ud", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open_legacy(path.clone());
        db.execute_legacy("create table users (id int, name text, age int)")
            .unwrap();
        db.execute_legacy(r#"insert into users values (1, "ram", 20)"#)
            .unwrap();
        db.execute_legacy(r#"insert into users values (2, "alice", 30)"#)
            .unwrap();
        db.execute_legacy(r#"update users set age = 99 where id = 1"#)
            .unwrap();
        db.execute_legacy(r#"delete from users where name = "alice""#)
            .unwrap();
    }

    {
        let mut db = Database::open_legacy(path.clone());
        let out = db.execute_legacy("select * from users").unwrap();
        assert_eq!(out, "id\tname\tage\n1\tram\t99");
    }

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_composite_pk_persists_and_rejects_after_reopen() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!(
        "skepa_db_composite_pk_reopen_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open_legacy(path.clone());
        db.execute_legacy("create table t (a int, b int, primary key(a,b))")
            .unwrap();
        db.execute_legacy("insert into t values (1, 1)").unwrap();
    }

    {
        let mut db = Database::open_legacy(path.clone());
        let err = db
            .execute_legacy("insert into t values (1, 1)")
            .unwrap_err();
        assert!(err.to_lowercase().contains("primary key"));
    }

    let _ = std::fs::remove_dir_all(&path);
}
#[test]
fn test_default_values_persist_after_reopen() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_defaults_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    {
        let mut db = Database::open_legacy(path.clone());
        db.execute(r#"create table users (id int, name text default "anon")"#)
            .unwrap();
    }

    let mut reopened = Database::open_legacy(path.clone());
    reopened.execute("insert into users values (1)").unwrap();
    let result = reopened.execute("select * from users").unwrap();
    assert_select_result(
        result,
        &["id", "name"],
        vec![vec![Value::Int(1), Value::Text("anon".to_string())]],
    );
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_describe_reflects_schema_changes_after_reopen() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_describe_reopen_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open_legacy(path.clone());
        db.execute(r#"create table users (id int primary key, name text default "anon")"#)
            .unwrap();
        db.execute("alter table users alter column name set not null")
            .unwrap();
        db.execute("create index on users (name)").unwrap();
        db.execute("drop index on users (name)").unwrap();
    }

    let mut reopened = Database::open_legacy(path.clone());
    let result = reopened.execute("describe users").unwrap();
    assert_select_result(
        result,
        &[
            "column",
            "type",
            "primary_key",
            "unique",
            "not_null",
            "default",
            "indexes",
        ],
        vec![
            vec![
                Value::Text("id".to_string()),
                Value::Text("int".to_string()),
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(true),
                Value::Null,
                Value::Text("".to_string()),
            ],
            vec![
                Value::Text("name".to_string()),
                Value::Text("text".to_string()),
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(true),
                Value::Text("anon".to_string()),
                Value::Text("".to_string()),
            ],
        ],
    );
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_select_stats_after_reopen() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_stats_reopen_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open_legacy(path.clone());
        db.execute("create table users (id int primary key, age int)")
            .unwrap();
        db.execute("insert into users values (1, 20)").unwrap();
        db.execute("insert into users values (2, 30)").unwrap();
    }

    let mut reopened = Database::open_legacy(path.clone());
    match reopened.execute("select * from users where age = 30").unwrap() {
        QueryResult::Select { stats, .. } => {
            assert_eq!(stats.rows_scanned, Some(2));
            assert_eq!(stats.index_used, Some(false));
        }
        other => panic!("expected select result, got {other:?}"),
    }
    match reopened.execute("select * from users where id = 2").unwrap() {
        QueryResult::Select { stats, .. } => {
            assert_eq!(stats.rows_scanned, Some(1));
            assert_eq!(stats.index_used, Some(true));
        }
        other => panic!("expected select result, got {other:?}"),
    }

    let _ = std::fs::remove_dir_all(&path);
}
