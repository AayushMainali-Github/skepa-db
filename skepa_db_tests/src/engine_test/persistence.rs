use super::*;

#[test]
fn test_constraint_persistence_after_reopen() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_constraints_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open(path.clone());
        db.execute("create table t (id int primary key, email text unique, name text not null)")
            .unwrap();
        db.execute(r#"insert into t values (1, "a@x.com", "ram")"#).unwrap();
    }
    {
        let mut db = Database::open(path.clone());
        let e1 = db
            .execute(r#"insert into t values (1, "b@x.com", "bob")"#)
            .unwrap_err();
        assert!(e1.to_lowercase().contains("primary key"));
        let e2 = db
            .execute(r#"insert into t values (2, "a@x.com", "bob")"#)
            .unwrap_err();
        assert!(e2.to_lowercase().contains("unique"));
        let e3 = db
            .execute(r#"insert into t values (3, "c@x.com", null)"#)
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
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)").unwrap();
        db.execute(r#"insert into users values (1, "ram")"#).unwrap();
    }

    {
        let mut db = Database::open(path.clone());
        let out = db.execute("select * from users").unwrap();
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
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text, age int)").unwrap();
        db.execute(r#"insert into users values (1, "ram", 20)"#).unwrap();
        db.execute(r#"insert into users values (2, "alice", 30)"#).unwrap();
        db.execute(r#"update users set age = 99 where id = 1"#).unwrap();
        db.execute(r#"delete from users where name = "alice""#).unwrap();
    }

    {
        let mut db = Database::open(path.clone());
        let out = db.execute("select * from users").unwrap();
        assert_eq!(out, "id\tname\tage\n1\tram\t99");
    }

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_composite_pk_persists_and_rejects_after_reopen() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_composite_pk_reopen_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open(path.clone());
        db.execute("create table t (a int, b int, primary key(a,b))")
            .unwrap();
        db.execute("insert into t values (1, 1)").unwrap();
    }

    {
        let mut db = Database::open(path.clone());
        let err = db.execute("insert into t values (1, 1)").unwrap_err();
        assert!(err.to_lowercase().contains("primary key"));
    }

    let _ = std::fs::remove_dir_all(&path);
}

