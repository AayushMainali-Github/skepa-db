use super::*;

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


