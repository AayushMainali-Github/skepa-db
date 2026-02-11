use super::*;

#[test]
fn index_snapshot_file_is_written_on_persist() {
    let path = temp_dir("index_snapshot_written");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int primary key, email text unique, name text)")
            .unwrap();
        db.execute(r#"insert into users values (1, "a@x.com", "a")"#).unwrap();
        db.execute(r#"insert into users values (2, "b@x.com", "b")"#).unwrap();
    }

    let idx_path = path.join("indexes").join("users.indexes.json");
    let content = std::fs::read_to_string(idx_path).unwrap();
    assert!(!content.trim().is_empty());
    assert!(content.contains("\"pk\""));
    assert!(content.contains("\"unique\""));
}


#[test]
fn corrupt_index_file_falls_back_to_rebuild_on_open() {
    let path = temp_dir("index_corrupt_fallback");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int primary key, email text unique)")
            .unwrap();
        db.execute(r#"insert into users values (1, "a@x.com")"#).unwrap();
        db.execute(r#"insert into users values (2, "b@x.com")"#).unwrap();
    }

    // Corrupt index snapshot; bootstrap should rebuild from rows.
    std::fs::write(path.join("indexes").join("users.indexes.json"), "{bad json").unwrap();

    {
        let mut db = Database::open(path.clone());
        assert_eq!(
            db.execute(r#"select * from users where id = 2"#).unwrap(),
            "id\temail\n2\tb@x.com"
        );
        assert_eq!(
            db.execute(r#"select * from users where email = "a@x.com""#).unwrap(),
            "id\temail\n1\ta@x.com"
        );
    }
}


#[test]
fn duplicate_key_in_index_snapshot_self_heals() {
    let path = temp_dir("index_dup_key_heal");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int primary key, email text unique)")
            .unwrap();
        db.execute(r#"insert into users values (1, "a@x.com")"#).unwrap();
        db.execute(r#"insert into users values (2, "b@x.com")"#).unwrap();
    }

    // Duplicate key for PK index snapshot.
    std::fs::write(
        path.join("indexes").join("users.indexes.json"),
        r#"{
  "pk": {
    "cols": [],
    "col_idxs": [0],
    "entries": [
      { "key": "1:1;", "row_id": 1 },
      { "key": "1:1;", "row_id": 2 }
    ]
  },
  "unique": []
}"#,
    )
    .unwrap();

    {
        let mut db = Database::open(path.clone());
        assert_eq!(
            db.execute("select * from users where id = 2").unwrap(),
            "id\temail\n2\tb@x.com"
        );
    }

    let healed = std::fs::read_to_string(path.join("indexes").join("users.indexes.json")).unwrap();
    assert!(!healed.contains(r#""key": "1:1;""#) || healed.matches(r#""key": "1:1;""#).count() == 1);
}


#[test]
fn out_of_range_row_pointer_in_index_snapshot_self_heals() {
    let path = temp_dir("index_row_ptr_heal");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int primary key, email text unique)")
            .unwrap();
        db.execute(r#"insert into users values (1, "a@x.com")"#).unwrap();
    }

    std::fs::write(
        path.join("indexes").join("users.indexes.json"),
        r#"{
  "pk": {
    "cols": [],
    "col_idxs": [0],
    "entries": [
      { "key": "1:1;", "row_id": 99 }
    ]
  },
  "unique": []
}"#,
    )
    .unwrap();

    {
        let mut db = Database::open(path.clone());
        assert_eq!(
            db.execute("select * from users where id = 1").unwrap(),
            "id\temail\n1\ta@x.com"
        );
    }

    let healed = std::fs::read_to_string(path.join("indexes").join("users.indexes.json")).unwrap();
    assert!(healed.contains(r#""row_id": 1"#));
}


#[test]
fn index_directory_exists_after_db_open() {
    let path = temp_dir("index_dir_exists");
    let _db = Database::open(path.clone());
    assert!(path.join("indexes").exists());
}


#[test]
fn index_file_created_on_create_table() {
    let path = temp_dir("index_file_create_table");
    let mut db = Database::open(path.clone());
    db.execute("create table users (id int primary key)").unwrap();
    assert!(path.join("indexes").join("users.indexes.json").exists());
}


#[test]
fn empty_index_file_triggers_rebuild_fallback() {
    let path = temp_dir("index_empty_fallback");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int primary key, email text unique)")
            .unwrap();
        db.execute(r#"insert into users values (1, "a@x.com")"#).unwrap();
    }
    std::fs::write(path.join("indexes").join("users.indexes.json"), "").unwrap();
    {
        let mut db = Database::open(path.clone());
        assert_eq!(db.execute("select * from users where id = 1").unwrap(), "id\temail\n1\ta@x.com");
    }
}


#[test]
fn pk_index_snapshot_uses_row_id_field() {
    let path = temp_dir("pk_index_row_id_field");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table t (id int primary key, v text)").unwrap();
        db.execute(r#"insert into t values (1, "a")"#).unwrap();
        db.execute(r#"insert into t values (2, "b")"#).unwrap();
    }
    let content = std::fs::read_to_string(path.join("indexes").join("t.indexes.json")).unwrap();
    assert!(content.contains("\"row_id\""));
    assert!(!content.contains("\"row_idx\""));
}


#[test]
fn unique_index_snapshot_uses_row_id_field() {
    let path = temp_dir("uq_index_row_id_field");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table t (id int, email text unique)").unwrap();
        db.execute(r#"insert into t values (1, "a@x.com")"#).unwrap();
    }
    let content = std::fs::read_to_string(path.join("indexes").join("t.indexes.json")).unwrap();
    assert!(content.contains("\"row_id\""));
    assert!(!content.contains("\"row_idx\""));
}


#[test]
fn pk_lookup_still_works_after_middle_delete_and_reopen() {
    let path = temp_dir("pk_lookup_after_delete_reopen");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table t (id int primary key, v text)").unwrap();
        db.execute(r#"insert into t values (1, "a")"#).unwrap();
        db.execute(r#"insert into t values (2, "b")"#).unwrap();
        db.execute(r#"insert into t values (3, "c")"#).unwrap();
        db.execute("delete from t where id = 2").unwrap();
    }
    {
        let mut db = Database::open(path.clone());
        assert_eq!(db.execute("select * from t where id = 3").unwrap(), "id\tv\n3\tc");
    }
}


#[test]
fn unique_lookup_still_works_after_middle_delete_and_reopen() {
    let path = temp_dir("uq_lookup_after_delete_reopen");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table t (id int, email text unique)").unwrap();
        db.execute(r#"insert into t values (1, "a@x.com")"#).unwrap();
        db.execute(r#"insert into t values (2, "b@x.com")"#).unwrap();
        db.execute(r#"insert into t values (3, "c@x.com")"#).unwrap();
        db.execute(r#"delete from t where email = "b@x.com""#).unwrap();
    }
    {
        let mut db = Database::open(path.clone());
        assert_eq!(
            db.execute(r#"select * from t where email = "c@x.com""#).unwrap(),
            "id\temail\n3\tc@x.com"
        );
    }
}


#[test]
fn pk_index_self_heal_when_snapshot_references_unknown_row_id() {
    let path = temp_dir("pk_unknown_row_id_heal");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table t (id int primary key)").unwrap();
        db.execute("insert into t values (1)").unwrap();
    }
    std::fs::write(
        path.join("indexes").join("t.indexes.json"),
        r#"{
  "pk": { "cols": [], "col_idxs": [0], "entries": [ { "key": "1:1;", "row_id": 999 } ] },
  "unique": []
}"#,
    )
    .unwrap();
    {
        let mut db = Database::open(path.clone());
        assert_eq!(db.execute("select * from t where id = 1").unwrap(), "id\n1");
    }
}


#[test]
fn unique_index_self_heal_when_snapshot_references_unknown_row_id() {
    let path = temp_dir("uq_unknown_row_id_heal");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table t (id int, email text unique)").unwrap();
        db.execute(r#"insert into t values (1, "a@x.com")"#).unwrap();
    }
    std::fs::write(
        path.join("indexes").join("t.indexes.json"),
        r#"{
  "pk": null,
  "unique": [
    { "cols": ["email"], "col_idxs": [1], "entries": [ { "key": "7:a@x.com;", "row_id": 999 } ] }
  ]
}"#,
    )
    .unwrap();
    {
        let mut db = Database::open(path.clone());
        assert_eq!(
            db.execute(r#"select * from t where email = "a@x.com""#).unwrap(),
            "id\temail\n1\ta@x.com"
        );
    }
}


#[test]
fn unique_index_does_not_block_multiple_null_values() {
    let root = temp_dir("unique_null_index");
    {
        let mut db = Database::open(root.clone());
        db.execute("create table t (id int, email text unique)").unwrap();
        db.execute("insert into t values (1, null)").unwrap();
        db.execute("insert into t values (2, null)").unwrap();
    }
    {
        let mut db = Database::open(root.clone());
        assert_eq!(
            db.execute("select * from t").unwrap(),
            "id\temail\n1\tnull\n2\tnull"
        );
    }
}


#[test]
fn secondary_index_persists_across_reopen() {
    let path = temp_dir("secondary_index_persist");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, city text)").unwrap();
        db.execute("create index on users (city)").unwrap();
        db.execute(r#"insert into users values (1, "ny")"#).unwrap();
        db.execute(r#"insert into users values (2, "la")"#).unwrap();
    }
    {
        let mut db = Database::open(path.clone());
        let out = db.execute(r#"select * from users where city = "ny""#).unwrap();
        assert_eq!(out, "id\tcity\n1\tny");
    }
}

#[test]
fn reopen_select_index_lookup_multiple_values() {
    let path = temp_dir("reopen_select");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, city text)").unwrap();
        db.execute("create index on users (city)").unwrap();
        db.execute(r#"insert into users values (1, "ny")"#).unwrap();
        db.execute(r#"insert into users values (2, "la")"#).unwrap();
        db.execute(r#"insert into users values (3, "ny")"#).unwrap();
    }
    {
        let mut db = Database::open(path.clone());
        let cases = [("ny", 2usize), ("la", 1usize), ("sf", 0usize)];
        for (city, expected_rows) in cases {
            let out = db
                .execute(&format!(
                    r#"select * from users where city = "{}" order by id asc"#,
                    city
                ))
                .unwrap();
            let rows = if out.is_empty() { 0 } else { out.lines().count() - 1 };
            assert_eq!(rows, expected_rows, "city={city}");
        }
    }
}

#[test]
fn secondary_index_snapshot_with_duplicate_key_self_heals() {
    let path = temp_dir("secondary_dup_key_heal");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, city text)").unwrap();
        db.execute("create index on users (city)").unwrap();
        db.execute(r#"insert into users values (1, "ny")"#).unwrap();
        db.execute(r#"insert into users values (2, "la")"#).unwrap();
    }

    std::fs::write(
        path.join("indexes").join("users.indexes.json"),
        r#"{
  "pk": null,
  "unique": [],
  "secondary": [
    { "cols": ["city"], "col_idxs": [1], "entries": [
      { "key": "2:ny;", "row_ids": [1] },
      { "key": "2:ny;", "row_ids": [2] }
    ] }
  ]
}"#,
    )
    .unwrap();

    {
        let mut db = Database::open(path.clone());
        let out = db
            .execute(r#"select * from users where city = "la" order by id asc"#)
            .unwrap();
        assert_eq!(out, "id\tcity\n2\tla");
    }
}

#[test]
fn secondary_index_snapshot_with_empty_row_ids_self_heals() {
    let path = temp_dir("secondary_empty_rowids_heal");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, city text)").unwrap();
        db.execute("create index on users (city)").unwrap();
        db.execute(r#"insert into users values (1, "ny")"#).unwrap();
    }

    std::fs::write(
        path.join("indexes").join("users.indexes.json"),
        r#"{
  "pk": null,
  "unique": [],
  "secondary": [
    { "cols": ["city"], "col_idxs": [1], "entries": [
      { "key": "2:ny;", "row_ids": [] }
    ] }
  ]
}"#,
    )
    .unwrap();

    {
        let mut db = Database::open(path.clone());
        let out = db.execute(r#"select * from users where city = "ny""#).unwrap();
        assert_eq!(out, "id\tcity\n1\tny");
    }
}

#[test]
fn secondary_index_snapshot_with_unknown_row_id_self_heals() {
    let path = temp_dir("secondary_unknown_rowid_heal");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, city text)").unwrap();
        db.execute("create index on users (city)").unwrap();
        db.execute(r#"insert into users values (1, "ny")"#).unwrap();
    }

    std::fs::write(
        path.join("indexes").join("users.indexes.json"),
        r#"{
  "pk": null,
  "unique": [],
  "secondary": [
    { "cols": ["city"], "col_idxs": [1], "entries": [
      { "key": "2:ny;", "row_ids": [999] }
    ] }
  ]
}"#,
    )
    .unwrap();

    {
        let mut db = Database::open(path.clone());
        let out = db.execute(r#"select * from users where city = "ny""#).unwrap();
        assert_eq!(out, "id\tcity\n1\tny");
    }
}

#[test]
fn unique_index_snapshot_col_idxs_mismatch_self_heals() {
    let path = temp_dir("unique_colidx_mismatch_heal");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table t (id int, email text unique)").unwrap();
        db.execute(r#"insert into t values (1, "a@x.com")"#).unwrap();
    }

    std::fs::write(
        path.join("indexes").join("t.indexes.json"),
        r#"{
  "pk": null,
  "unique": [
    { "cols": ["email"], "col_idxs": [0], "entries": [ { "key": "7:a@x.com;", "row_id": 1 } ] }
  ],
  "secondary": []
}"#,
    )
    .unwrap();

    {
        let mut db = Database::open(path.clone());
        let out = db.execute(r#"select * from t where email = "a@x.com""#).unwrap();
        assert_eq!(out, "id\temail\n1\ta@x.com");
    }
}

#[test]
fn pk_index_snapshot_col_idxs_mismatch_self_heals() {
    let path = temp_dir("pk_colidx_mismatch_heal");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table t (id int primary key, name text)").unwrap();
        db.execute(r#"insert into t values (1, "a")"#).unwrap();
    }

    std::fs::write(
        path.join("indexes").join("t.indexes.json"),
        r#"{
  "pk": { "cols": [], "col_idxs": [1], "entries": [ { "key": "1:1;", "row_id": 1 } ] },
  "unique": [],
  "secondary": []
}"#,
    )
    .unwrap();

    {
        let mut db = Database::open(path.clone());
        let out = db.execute("select * from t where id = 1").unwrap();
        assert_eq!(out, "id\tname\n1\ta");
    }
}




