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

#[test]
fn recovery_replays_committed_delete_with_on_delete_cascade() {
    let path = temp_dir("wal_fk_delete_cascade_replay");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table p (id int primary key)").unwrap();
        db.execute("create table c (id int, pid int)").unwrap();
        db.execute("alter table c add foreign key(pid) references p(id) on delete cascade")
            .unwrap();
        db.execute("insert into p values (1)").unwrap();
        db.execute("insert into c values (10, 1)").unwrap();
    }

    std::fs::write(
        path.join("wal.log"),
        "BEGIN 21\nOP 21 delete from p where id = 1\nCOMMIT 21\n",
    )
    .unwrap();

    {
        let mut db = Database::open(path.clone());
        assert_eq!(db.execute("select * from p").unwrap(), "id");
        assert_eq!(db.execute("select * from c").unwrap(), "id\tpid");
    }
}

#[test]
fn recovery_ignores_uncommitted_delete_with_on_delete_cascade() {
    let path = temp_dir("wal_fk_delete_cascade_uncommitted");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table p (id int primary key)").unwrap();
        db.execute("create table c (id int, pid int)").unwrap();
        db.execute("alter table c add foreign key(pid) references p(id) on delete cascade")
            .unwrap();
        db.execute("insert into p values (1)").unwrap();
        db.execute("insert into c values (10, 1)").unwrap();
    }

    std::fs::write(path.join("wal.log"), "BEGIN 22\nOP 22 delete from p where id = 1\n").unwrap();

    {
        let mut db = Database::open(path.clone());
        assert_eq!(db.execute("select * from p").unwrap(), "id\n1");
        assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n10\t1");
    }
}

#[test]
fn recovery_replays_committed_update_with_on_update_set_null() {
    let path = temp_dir("wal_fk_update_set_null_replay");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table p (id int primary key)").unwrap();
        db.execute("create table c (id int, pid int)").unwrap();
        db.execute("alter table c add foreign key(pid) references p(id) on update set null")
            .unwrap();
        db.execute("insert into p values (1)").unwrap();
        db.execute("insert into c values (10, 1)").unwrap();
    }

    std::fs::write(
        path.join("wal.log"),
        "BEGIN 23\nOP 23 update p set id = 2 where id = 1\nCOMMIT 23\n",
    )
    .unwrap();

    {
        let mut db = Database::open(path.clone());
        assert_eq!(db.execute("select * from p").unwrap(), "id\n2");
        assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n10\tnull");
    }
}

#[test]
fn recovery_replays_committed_update_with_on_update_no_action_when_fixed_in_tx() {
    let path = temp_dir("wal_fk_update_no_action_fixed");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table p (id int primary key)").unwrap();
        db.execute("create table c (id int, pid int)").unwrap();
        db.execute("alter table c add foreign key(pid) references p(id) on update no action")
            .unwrap();
        db.execute("insert into p values (1)").unwrap();
        db.execute("insert into c values (10, 1)").unwrap();
    }

    std::fs::write(
        path.join("wal.log"),
        concat!(
            "BEGIN 31\n",
            "OP 31 update p set id = 2 where id = 1\n",
            "OP 31 update c set pid = 2 where id = 10\n",
            "COMMIT 31\n",
        ),
    )
    .unwrap();

    {
        let mut db = Database::open(path.clone());
        assert_eq!(db.execute("select * from p").unwrap(), "id\n2");
        assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n10\t2");
    }
}

#[test]
fn recovery_skips_committed_update_with_on_update_no_action_when_still_violated() {
    let path = temp_dir("wal_fk_update_no_action_violated");
    {
        let mut db = Database::open(path.clone());
        db.execute("create table p (id int primary key)").unwrap();
        db.execute("create table c (id int, pid int)").unwrap();
        db.execute("alter table c add foreign key(pid) references p(id) on update no action")
            .unwrap();
        db.execute("insert into p values (1)").unwrap();
        db.execute("insert into c values (10, 1)").unwrap();
    }

    std::fs::write(
        path.join("wal.log"),
        "BEGIN 32\nOP 32 update p set id = 2 where id = 1\nCOMMIT 32\n",
    )
    .unwrap();

    {
        let mut db = Database::open(path.clone());
        // Invalid NO ACTION commit must not be applied during recovery.
        assert_eq!(db.execute("select * from p").unwrap(), "id\n1");
        assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n10\t1");
    }
}


