use super::*;

#[test]
fn row_ids_are_persisted_with_row_prefix() {
    let path = temp_dir("row_id_prefix_persist");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)").unwrap();
        db.execute(r#"insert into users values (1, "a")"#).unwrap();
        db.execute(r#"insert into users values (2, "b")"#).unwrap();
    }
    let rows = std::fs::read_to_string(path.join("tables").join("users.rows")).unwrap();
    assert!(rows.lines().all(|l| l.starts_with('@') && l.contains("|\t")));
}


#[test]
fn row_ids_remain_stable_for_survivors_after_delete() {
    let path = temp_dir("row_id_stable_after_delete");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int primary key, name text)").unwrap();
        db.execute(r#"insert into users values (1, "a")"#).unwrap();
        db.execute(r#"insert into users values (2, "b")"#).unwrap();
        db.execute(r#"insert into users values (3, "c")"#).unwrap();
        db.execute("delete from users where id = 2").unwrap();
    }
    let rows = std::fs::read_to_string(path.join("tables").join("users.rows")).unwrap();
    let ids = rows
        .lines()
        .map(|l| {
            let end = l.find('|').unwrap();
            l[1..end].parse::<u64>().unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![1, 3]);
}


#[test]
fn row_ids_survive_reopen_and_append_monotonic() {
    let path = temp_dir("row_id_reopen_monotonic");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table users (id int, name text)").unwrap();
        db.execute(r#"insert into users values (1, "a")"#).unwrap();
        db.execute(r#"insert into users values (2, "b")"#).unwrap();
    }
    {
        let mut db = Database::open(path.clone());
        db.execute(r#"insert into users values (3, "c")"#).unwrap();
    }
    let rows = std::fs::read_to_string(path.join("tables").join("users.rows")).unwrap();
    let ids = rows
        .lines()
        .map(|l| {
            let end = l.find('|').unwrap();
            l[1..end].parse::<u64>().unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![1, 2, 3]);
}


#[test]
fn row_id_format_backward_compat_without_prefix() {
    let root = temp_dir("row_id_backward_compat");
    std::fs::create_dir_all(root.join("tables")).unwrap();
    std::fs::write(root.join("tables").join("users.rows"), "i:1\tt:a\ni:2\tt:b\n").unwrap();
    let mut db = Database::open(root.clone());
    db.execute("create table users2 (id int, name text)").unwrap();
    // Existing table bootstrap path still works and rewrites with row-id prefixes on persist.
    let mut storage = DiskStorage::new(root.clone()).unwrap();
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
    storage.bootstrap_table("users", &schema).unwrap();
    storage.persist_table("users").unwrap();
    let rows = std::fs::read_to_string(root.join("tables").join("users.rows")).unwrap();
    assert!(rows.lines().all(|l| l.starts_with('@')));
}


