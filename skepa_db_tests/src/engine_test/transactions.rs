use super::*;

#[test]
fn test_transaction_commit_persists_changes() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    assert_transaction_result(db.execute("begin").unwrap(), "transaction started");
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    assert_transaction_result(db.execute("commit").unwrap(), "transaction committed");
    assert_select_result(
        db.execute("select * from users").unwrap(),
        &["id", "name"],
        vec![vec![Value::Int(1), Value::Text("ram".to_string())]],
    );
}

#[test]
fn test_transaction_rollback_discards_changes() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    assert_transaction_result(db.execute("begin").unwrap(), "transaction started");
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    assert_transaction_result(db.execute("rollback").unwrap(), "transaction rolled back");
    assert_select_result(
        db.execute("select * from users").unwrap(),
        &["id", "name"],
        vec![],
    );
}

#[test]
fn test_transaction_is_visible_inside_tx_before_commit() {
    let mut db = test_db();
    db.execute("create table users (id int, name text)")
        .unwrap();
    db.execute("begin").unwrap();
    db.execute(r#"insert into users values (1, "ram")"#)
        .unwrap();
    assert_select_result(
        db.execute("select * from users").unwrap(),
        &["id", "name"],
        vec![vec![Value::Int(1), Value::Text("ram".to_string())]],
    );
    assert_transaction_result(db.execute("rollback").unwrap(), "transaction rolled back");
}

#[test]
fn test_nested_begin_is_rejected() {
    let mut db = test_db();
    db.execute_legacy("begin").unwrap();
    let err = db.execute_legacy("begin").unwrap_err();
    assert!(err.to_lowercase().contains("already active"));
}

#[test]
fn test_commit_without_active_tx_errors() {
    let mut db = test_db();
    let err = db.execute_legacy("commit").unwrap_err();
    assert!(err.to_lowercase().contains("no active transaction"));
}

#[test]
fn test_rollback_without_active_tx_errors() {
    let mut db = test_db();
    let err = db.execute_legacy("rollback").unwrap_err();
    assert!(err.to_lowercase().contains("no active transaction"));
}

#[test]
fn test_create_inside_transaction_is_rejected() {
    let mut db = test_db();
    db.execute_legacy("begin").unwrap();
    let err = db.execute_legacy("create table t (id int)").unwrap_err();
    assert!(
        err.to_lowercase()
            .contains("cannot run inside an active transaction")
    );
}

#[test]
fn test_transaction_commit_persists_after_reopen() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_tx_commit_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open_legacy(path.clone());
        db.execute("create table users (id int, name text)")
            .unwrap();
        db.execute("begin").unwrap();
        db.execute(r#"insert into users values (1, "ram")"#)
            .unwrap();
        db.execute("commit").unwrap();
    }

    {
        let mut db = Database::open_legacy(path.clone());
        assert_select_result(
            db.execute("select * from users").unwrap(),
            &["id", "name"],
            vec![vec![Value::Int(1), Value::Text("ram".to_string())]],
        );
    }

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_transaction_rollback_not_persisted_after_reopen() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_tx_rollback_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut db = Database::open_legacy(path.clone());
        db.execute("create table users (id int, name text)")
            .unwrap();
        db.execute("begin").unwrap();
        db.execute(r#"insert into users values (1, "ram")"#)
            .unwrap();
        db.execute("rollback").unwrap();
    }

    {
        let mut db = Database::open_legacy(path.clone());
        assert_select_result(
            db.execute("select * from users").unwrap(),
            &["id", "name"],
            vec![],
        );
    }

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_transaction_commit_with_multiple_operations() {
    let mut db = test_db();
    db.execute_legacy("create table users (id int primary key, name text, age int)")
        .unwrap();
    db.execute_legacy("begin").unwrap();
    db.execute_legacy(r#"insert into users values (1, "a", 10)"#)
        .unwrap();
    db.execute_legacy(r#"insert into users values (2, "b", 20)"#)
        .unwrap();
    db.execute_legacy(r#"update users set age = 21 where id = 2"#)
        .unwrap();
    db.execute_legacy(r#"delete from users where name = "a""#)
        .unwrap();
    db.execute_legacy("commit").unwrap();
    assert_eq!(
        db.execute_legacy("select * from users").unwrap(),
        "id\tname\tage\n2\tb\t21"
    );
}

#[test]
fn test_transaction_commit_conflict_when_table_changed_externally() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_tx_conflict_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut setup = Database::open_legacy(path.clone());
        setup
            .execute_legacy("create table t (id int, v int)")
            .unwrap();
        setup
            .execute_legacy("insert into t values (1, 10)")
            .unwrap();
    }

    let mut tx_db = Database::open_legacy(path.clone());
    let mut other_db = Database::open_legacy(path.clone());

    tx_db.execute_legacy("begin").unwrap();
    tx_db
        .execute_legacy("update t set v = 11 where id = 1")
        .unwrap();
    std::thread::sleep(Duration::from_millis(5));
    other_db
        .execute_legacy("insert into t values (2, 20)")
        .unwrap();

    let err = tx_db.execute_legacy("commit").unwrap_err();
    assert!(err.to_lowercase().contains("transaction conflict"));

    // Instance should refresh from disk after conflict.
    let out = tx_db
        .execute_legacy("select * from t order by id asc")
        .unwrap();
    assert_eq!(out, "id\tv\n1\t10\n2\t20");

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_transaction_commit_no_conflict_when_other_table_changes() {
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_tx_no_conflict_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    {
        let mut setup = Database::open_legacy(path.clone());
        setup
            .execute_legacy("create table a (id int, v int)")
            .unwrap();
        setup
            .execute_legacy("create table b (id int, v int)")
            .unwrap();
        setup
            .execute_legacy("insert into a values (1, 10)")
            .unwrap();
        setup
            .execute_legacy("insert into b values (1, 100)")
            .unwrap();
    }

    let mut tx_db = Database::open_legacy(path.clone());
    let mut other_db = Database::open_legacy(path.clone());

    tx_db.execute_legacy("begin").unwrap();
    tx_db
        .execute_legacy("update a set v = 11 where id = 1")
        .unwrap();
    std::thread::sleep(Duration::from_millis(5));
    other_db
        .execute_legacy("update b set v = 101 where id = 1")
        .unwrap();

    assert_eq!(
        tx_db.execute_legacy("commit").unwrap(),
        "transaction committed"
    );
    assert_eq!(
        tx_db.execute_legacy("select * from a").unwrap(),
        "id\tv\n1\t11"
    );

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_transaction_rollback_reverts_update_and_delete() {
    let mut db = test_db();
    db.execute_legacy("create table users (id int, name text, age int)")
        .unwrap();
    db.execute_legacy(r#"insert into users values (1, "a", 10)"#)
        .unwrap();
    db.execute_legacy(r#"insert into users values (2, "b", 20)"#)
        .unwrap();

    db.execute_legacy("begin").unwrap();
    db.execute_legacy("update users set age = 99 where id = 1")
        .unwrap();
    db.execute_legacy(r#"delete from users where name = "b""#)
        .unwrap();
    db.execute_legacy("rollback").unwrap();

    assert_eq!(
        db.execute_legacy("select * from users").unwrap(),
        "id\tname\tage\n1\ta\t10\n2\tb\t20"
    );
}

#[test]
fn test_transaction_rollback_restores_after_constraint_error() {
    let mut db = test_db();
    db.execute_legacy("create table users (id int primary key, email text unique)")
        .unwrap();
    db.execute_legacy(r#"insert into users values (1, "a@x.com")"#)
        .unwrap();
    db.execute_legacy(r#"insert into users values (2, "b@x.com")"#)
        .unwrap();

    db.execute_legacy("begin").unwrap();
    let err = db
        .execute_legacy(r#"update users set email = "a@x.com" where id = 2"#)
        .unwrap_err();
    assert!(err.to_lowercase().contains("unique"));
    db.execute_legacy("rollback").unwrap();

    assert_eq!(
        db.execute_legacy("select * from users").unwrap(),
        "id\temail\n1\ta@x.com\n2\tb@x.com"
    );
}

#[test]
fn test_transaction_commit_without_mutations_is_allowed() {
    let mut db = test_db();
    db.execute_legacy("create table users (id int, name text)")
        .unwrap();
    db.execute_legacy("begin").unwrap();
    let out = db.execute_legacy("commit").unwrap();
    assert_eq!(out, "transaction committed");
    assert_eq!(
        db.execute_legacy("select * from users").unwrap(),
        "id\tname"
    );
}

#[test]
fn test_select_still_works_while_transaction_active() {
    let mut db = test_db();
    db.execute_legacy("create table t (id int, name text)")
        .unwrap();
    db.execute_legacy(r#"insert into t values (1, "a")"#)
        .unwrap();
    db.execute_legacy("begin").unwrap();
    db.execute_legacy(r#"insert into t values (2, "b")"#)
        .unwrap();
    assert_eq!(
        db.execute_legacy("select name from t where id = 1")
            .unwrap(),
        "name\na"
    );
    db.execute_legacy("rollback").unwrap();
}

#[test]
fn test_transaction_multiple_updates_then_rollback() {
    let mut db = test_db();
    db.execute_legacy("create table t (id int primary key, v int)")
        .unwrap();
    db.execute_legacy("insert into t values (1, 10)").unwrap();
    db.execute_legacy("insert into t values (2, 20)").unwrap();
    db.execute_legacy("begin").unwrap();
    db.execute_legacy("update t set v = 11 where id = 1")
        .unwrap();
    db.execute_legacy("update t set v = 22 where id = 2")
        .unwrap();
    db.execute_legacy("rollback").unwrap();
    assert_eq!(
        db.execute_legacy("select * from t").unwrap(),
        "id\tv\n1\t10\n2\t20"
    );
}

#[test]
fn test_transaction_multiple_deletes_then_rollback() {
    let mut db = test_db();
    db.execute_legacy("create table t (id int primary key, v int)")
        .unwrap();
    db.execute_legacy("insert into t values (1, 10)").unwrap();
    db.execute_legacy("insert into t values (2, 20)").unwrap();
    db.execute_legacy("begin").unwrap();
    db.execute_legacy("delete from t where id = 1").unwrap();
    db.execute_legacy("delete from t where id = 2").unwrap();
    db.execute_legacy("rollback").unwrap();
    assert_eq!(
        db.execute_legacy("select * from t").unwrap(),
        "id\tv\n1\t10\n2\t20"
    );
}

#[test]
fn test_transaction_commit_then_new_begin_allowed() {
    let mut db = test_db();
    db.execute_legacy("create table t (id int)").unwrap();
    db.execute_legacy("begin").unwrap();
    db.execute_legacy("commit").unwrap();
    let out = db.execute_legacy("begin").unwrap();
    assert_eq!(out, "transaction started");
}

#[test]
fn test_transaction_rollback_then_new_begin_allowed() {
    let mut db = test_db();
    db.execute_legacy("create table t (id int)").unwrap();
    db.execute_legacy("begin").unwrap();
    db.execute_legacy("rollback").unwrap();
    let out = db.execute_legacy("begin").unwrap();
    assert_eq!(out, "transaction started");
}

#[test]
fn test_alter_table_not_allowed_inside_transaction() {
    let mut db = test_db();
    db.execute_legacy("create table t (id int)").unwrap();
    db.execute_legacy("begin").unwrap();
    let err = db
        .execute_legacy("alter table t add unique(id)")
        .unwrap_err();
    assert!(err.to_lowercase().contains("auto-commit"));
    db.execute_legacy("rollback").unwrap();
}

#[test]
fn test_index_not_allowed_inside_transaction() {
    let mut db = test_db();
    db.execute_legacy("create table users (id int, email text)")
        .unwrap();
    db.execute_legacy("begin").unwrap();
    let err = db
        .execute_legacy("create index on users (email)")
        .unwrap_err();
    assert!(err.to_lowercase().contains("auto-commit"));
    db.execute_legacy("rollback").unwrap();
}

#[test]
fn test_transaction_rollback_reverts_on_delete_cascade_side_effects() {
    let mut db = test_db();
    db.execute_legacy("create table p (id int primary key)")
        .unwrap();
    db.execute_legacy("create table c (id int, pid int)")
        .unwrap();
    db.execute_legacy("insert into p values (1)").unwrap();
    db.execute_legacy("insert into c values (10, 1)").unwrap();
    db.execute_legacy("alter table c add foreign key(pid) references p(id) on delete cascade")
        .unwrap();

    db.execute_legacy("begin").unwrap();
    db.execute_legacy("delete from p where id = 1").unwrap();
    assert_eq!(db.execute_legacy("select * from c").unwrap(), "id\tpid");
    db.execute_legacy("rollback").unwrap();

    assert_eq!(db.execute_legacy("select * from p").unwrap(), "id\n1");
    assert_eq!(
        db.execute_legacy("select * from c").unwrap(),
        "id\tpid\n10\t1"
    );
}

#[test]
fn test_transaction_commit_persists_on_delete_cascade_side_effects() {
    let mut db = test_db();
    db.execute_legacy("create table p (id int primary key)")
        .unwrap();
    db.execute_legacy("create table c (id int, pid int)")
        .unwrap();
    db.execute_legacy("insert into p values (1)").unwrap();
    db.execute_legacy("insert into c values (10, 1)").unwrap();
    db.execute_legacy("alter table c add foreign key(pid) references p(id) on delete cascade")
        .unwrap();

    db.execute_legacy("begin").unwrap();
    db.execute_legacy("delete from p where id = 1").unwrap();
    db.execute_legacy("commit").unwrap();

    assert_eq!(db.execute_legacy("select * from p").unwrap(), "id");
    assert_eq!(db.execute_legacy("select * from c").unwrap(), "id\tpid");
}

#[test]
fn test_transaction_rollback_reverts_on_update_set_null_side_effects() {
    let mut db = test_db();
    db.execute_legacy("create table p (id int primary key)")
        .unwrap();
    db.execute_legacy("create table c (id int, pid int)")
        .unwrap();
    db.execute_legacy("insert into p values (1)").unwrap();
    db.execute_legacy("insert into c values (10, 1)").unwrap();
    db.execute_legacy("alter table c add foreign key(pid) references p(id) on update set null")
        .unwrap();

    db.execute_legacy("begin").unwrap();
    db.execute_legacy("update p set id = 2 where id = 1")
        .unwrap();
    assert_eq!(
        db.execute_legacy("select * from c").unwrap(),
        "id\tpid\n10\tnull"
    );
    db.execute_legacy("rollback").unwrap();

    assert_eq!(db.execute_legacy("select * from p").unwrap(), "id\n1");
    assert_eq!(
        db.execute_legacy("select * from c").unwrap(),
        "id\tpid\n10\t1"
    );
}

#[test]
fn test_transaction_commit_persists_on_update_set_null_side_effects() {
    let mut db = test_db();
    db.execute_legacy("create table p (id int primary key)")
        .unwrap();
    db.execute_legacy("create table c (id int, pid int)")
        .unwrap();
    db.execute_legacy("insert into p values (1)").unwrap();
    db.execute_legacy("insert into c values (10, 1)").unwrap();
    db.execute_legacy("alter table c add foreign key(pid) references p(id) on update set null")
        .unwrap();

    db.execute_legacy("begin").unwrap();
    db.execute_legacy("update p set id = 2 where id = 1")
        .unwrap();
    db.execute_legacy("commit").unwrap();

    assert_eq!(db.execute_legacy("select * from p").unwrap(), "id\n2");
    assert_eq!(
        db.execute_legacy("select * from c").unwrap(),
        "id\tpid\n10\tnull"
    );
}

#[test]
fn test_transaction_rollback_reverts_on_update_cascade_side_effects() {
    let mut db = test_db();
    db.execute_legacy("create table p (id int primary key)")
        .unwrap();
    db.execute_legacy("create table c (id int, pid int)")
        .unwrap();
    db.execute_legacy("insert into p values (1)").unwrap();
    db.execute_legacy("insert into c values (10, 1)").unwrap();
    db.execute_legacy("alter table c add foreign key(pid) references p(id) on update cascade")
        .unwrap();

    db.execute_legacy("begin").unwrap();
    db.execute_legacy("update p set id = 2 where id = 1")
        .unwrap();
    assert_eq!(
        db.execute_legacy("select * from c").unwrap(),
        "id\tpid\n10\t2"
    );
    db.execute_legacy("rollback").unwrap();

    assert_eq!(db.execute_legacy("select * from p").unwrap(), "id\n1");
    assert_eq!(
        db.execute_legacy("select * from c").unwrap(),
        "id\tpid\n10\t1"
    );
}

#[test]
fn test_transaction_commit_persists_on_update_cascade_side_effects() {
    let mut db = test_db();
    db.execute_legacy("create table p (id int primary key)")
        .unwrap();
    db.execute_legacy("create table c (id int, pid int)")
        .unwrap();
    db.execute_legacy("insert into p values (1)").unwrap();
    db.execute_legacy("insert into c values (10, 1)").unwrap();
    db.execute_legacy("alter table c add foreign key(pid) references p(id) on update cascade")
        .unwrap();

    db.execute_legacy("begin").unwrap();
    db.execute_legacy("update p set id = 2 where id = 1")
        .unwrap();
    db.execute_legacy("commit").unwrap();

    assert_eq!(db.execute_legacy("select * from p").unwrap(), "id\n2");
    assert_eq!(
        db.execute_legacy("select * from c").unwrap(),
        "id\tpid\n10\t2"
    );
}

#[test]
fn test_transaction_rollback_reverts_on_delete_set_null_side_effects() {
    let mut db = test_db();
    db.execute_legacy("create table p (id int primary key)")
        .unwrap();
    db.execute_legacy("create table c (id int, pid int)")
        .unwrap();
    db.execute_legacy("insert into p values (1)").unwrap();
    db.execute_legacy("insert into c values (10, 1)").unwrap();
    db.execute_legacy("alter table c add foreign key(pid) references p(id) on delete set null")
        .unwrap();

    db.execute_legacy("begin").unwrap();
    db.execute_legacy("delete from p where id = 1").unwrap();
    assert_eq!(
        db.execute_legacy("select * from c").unwrap(),
        "id\tpid\n10\tnull"
    );
    db.execute_legacy("rollback").unwrap();

    assert_eq!(db.execute_legacy("select * from p").unwrap(), "id\n1");
    assert_eq!(
        db.execute_legacy("select * from c").unwrap(),
        "id\tpid\n10\t1"
    );
}

#[test]
fn test_transaction_commit_persists_on_delete_set_null_side_effects() {
    let mut db = test_db();
    db.execute_legacy("create table p (id int primary key)")
        .unwrap();
    db.execute_legacy("create table c (id int, pid int)")
        .unwrap();
    db.execute_legacy("insert into p values (1)").unwrap();
    db.execute_legacy("insert into c values (10, 1)").unwrap();
    db.execute_legacy("alter table c add foreign key(pid) references p(id) on delete set null")
        .unwrap();

    db.execute_legacy("begin").unwrap();
    db.execute_legacy("delete from p where id = 1").unwrap();
    db.execute_legacy("commit").unwrap();

    assert_eq!(db.execute_legacy("select * from p").unwrap(), "id");
    assert_eq!(
        db.execute_legacy("select * from c").unwrap(),
        "id\tpid\n10\tnull"
    );
}

#[test]
fn test_alter_fk_on_update_no_action_deferred_commit_fails_when_violated() {
    let mut db = test_db();
    db.execute_legacy("create table p (id int primary key)")
        .unwrap();
    db.execute_legacy("create table c (id int, pid int)")
        .unwrap();
    db.execute_legacy("insert into p values (1)").unwrap();
    db.execute_legacy("insert into c values (10, 1)").unwrap();
    db.execute_legacy("alter table c add foreign key(pid) references p(id) on update no action")
        .unwrap();

    db.execute_legacy("begin").unwrap();
    db.execute_legacy("update p set id = 2 where id = 1")
        .unwrap();
    let err = db.execute_legacy("commit").unwrap_err();
    assert!(err.to_lowercase().contains("no action"));
    assert_eq!(db.execute_legacy("select * from p").unwrap(), "id\n1");
    assert_eq!(
        db.execute_legacy("select * from c").unwrap(),
        "id\tpid\n10\t1"
    );
}

#[test]
fn test_alter_fk_on_update_no_action_commit_succeeds_when_fixed_in_tx() {
    let mut db = test_db();
    db.execute_legacy("create table p (id int primary key)")
        .unwrap();
    db.execute_legacy("create table c (id int, pid int)")
        .unwrap();
    db.execute_legacy("insert into p values (1)").unwrap();
    db.execute_legacy("insert into c values (10, 1)").unwrap();
    db.execute_legacy("alter table c add foreign key(pid) references p(id) on update no action")
        .unwrap();

    db.execute_legacy("begin").unwrap();
    db.execute_legacy("update p set id = 2 where id = 1")
        .unwrap();
    db.execute_legacy("update c set pid = 2 where id = 10")
        .unwrap();
    assert_eq!(
        db.execute_legacy("commit").unwrap(),
        "transaction committed"
    );
    assert_eq!(db.execute_legacy("select * from p").unwrap(), "id\n2");
    assert_eq!(
        db.execute_legacy("select * from c").unwrap(),
        "id\tpid\n10\t2"
    );
}

#[test]
fn test_alter_fk_on_delete_no_action_deferred_commit_fails_when_violated() {
    let mut db = test_db();
    db.execute_legacy("create table p (id int primary key)")
        .unwrap();
    db.execute_legacy("create table c (id int, pid int)")
        .unwrap();
    db.execute_legacy("insert into p values (1)").unwrap();
    db.execute_legacy("insert into c values (10, 1)").unwrap();
    db.execute_legacy("alter table c add foreign key(pid) references p(id) on delete no action")
        .unwrap();

    db.execute_legacy("begin").unwrap();
    db.execute_legacy("delete from p where id = 1").unwrap();
    let err = db.execute_legacy("commit").unwrap_err();
    assert!(err.to_lowercase().contains("no action"));
    assert_eq!(db.execute_legacy("select * from p").unwrap(), "id\n1");
    assert_eq!(
        db.execute_legacy("select * from c").unwrap(),
        "id\tpid\n10\t1"
    );
}

#[test]
fn test_alter_fk_on_delete_no_action_commit_succeeds_when_fixed_in_tx() {
    let mut db = test_db();
    db.execute_legacy("create table p (id int primary key)")
        .unwrap();
    db.execute_legacy("create table c (id int, pid int)")
        .unwrap();
    db.execute_legacy("insert into p values (1)").unwrap();
    db.execute_legacy("insert into c values (10, 1)").unwrap();
    db.execute_legacy("alter table c add foreign key(pid) references p(id) on delete no action")
        .unwrap();

    db.execute_legacy("begin").unwrap();
    db.execute_legacy("delete from p where id = 1").unwrap();
    db.execute_legacy("delete from c where id = 10").unwrap();
    assert_eq!(
        db.execute_legacy("commit").unwrap(),
        "transaction committed"
    );
    assert_eq!(db.execute_legacy("select * from p").unwrap(), "id");
    assert_eq!(db.execute_legacy("select * from c").unwrap(), "id\tpid");
}
