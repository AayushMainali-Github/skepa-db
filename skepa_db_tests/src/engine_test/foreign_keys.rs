use super::*;

#[test]
fn test_foreign_key_insert_requires_parent_row() {
    let mut db = test_db();
    db.execute("create table users (id int primary key)").unwrap();
    db.execute(
        "create table orders (id int, user_id int, foreign key(user_id) references users(id))",
    )
    .unwrap();
    let err = db.execute("insert into orders values (1, 99)").unwrap_err();
    assert!(err.to_lowercase().contains("foreign key"));
}

#[test]
fn test_foreign_key_insert_succeeds_when_parent_exists() {
    let mut db = test_db();
    db.execute("create table users (id int primary key)").unwrap();
    db.execute(
        "create table orders (id int, user_id int, foreign key(user_id) references users(id))",
    )
    .unwrap();
    db.execute("insert into users values (1)").unwrap();
    db.execute("insert into orders values (1, 1)").unwrap();
    assert_eq!(db.execute("select * from orders").unwrap(), "id\tuser_id\n1\t1");
}

#[test]
fn test_foreign_key_insert_succeeds_when_parent_unique_exists() {
    let mut db = test_db();
    db.execute("create table users (id int, email text unique)").unwrap();
    db.execute(
        "create table orders (id int, user_email text, foreign key(user_email) references users(email))",
    )
    .unwrap();
    db.execute(r#"insert into users values (1, "a@x.com")"#).unwrap();
    db.execute(r#"insert into orders values (1, "a@x.com")"#).unwrap();
    assert_eq!(
        db.execute("select * from orders").unwrap(),
        "id\tuser_email\n1\ta@x.com"
    );
}

#[test]
fn test_foreign_key_restrict_blocks_parent_delete() {
    let mut db = test_db();
    db.execute("create table users (id int primary key)").unwrap();
    db.execute(
        "create table orders (id int, user_id int, foreign key(user_id) references users(id))",
    )
    .unwrap();
    db.execute("insert into users values (1)").unwrap();
    db.execute("insert into orders values (1, 1)").unwrap();
    let err = db.execute("delete from users where id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("foreign key restrict"));
}

#[test]
fn test_foreign_key_restrict_blocks_parent_delete_with_child_secondary_index() {
    let mut db = test_db();
    db.execute("create table users (id int primary key)").unwrap();
    db.execute(
        "create table orders (id int, user_id int, foreign key(user_id) references users(id))",
    )
    .unwrap();
    db.execute("create index on orders (user_id)").unwrap();
    db.execute("insert into users values (1)").unwrap();
    db.execute("insert into orders values (1, 1)").unwrap();
    let err = db.execute("delete from users where id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("foreign key restrict"));
}

#[test]
fn test_foreign_key_restrict_blocks_parent_update() {
    let mut db = test_db();
    db.execute("create table users (id int primary key)").unwrap();
    db.execute(
        "create table orders (id int, user_id int, foreign key(user_id) references users(id))",
    )
    .unwrap();
    db.execute("insert into users values (1)").unwrap();
    db.execute("insert into orders values (1, 1)").unwrap();
    let err = db.execute("update users set id = 2 where id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("foreign key restrict"));
}

#[test]
fn test_foreign_key_update_child_requires_parent() {
    let mut db = test_db();
    db.execute("create table users (id int primary key)").unwrap();
    db.execute(
        "create table orders (id int, user_id int, foreign key(user_id) references users(id))",
    )
    .unwrap();
    db.execute("insert into users values (1)").unwrap();
    db.execute("insert into orders values (1, 1)").unwrap();
    let err = db
        .execute("update orders set user_id = 2 where id = 1")
        .unwrap_err();
    assert!(err.to_lowercase().contains("foreign key"));
}

#[test]
fn test_composite_foreign_key_enforced() {
    let mut db = test_db();
    db.execute("create table parent (a int, b int, primary key(a,b))")
        .unwrap();
    db.execute(
        "create table child (id int, a int, b int, foreign key(a,b) references parent(a,b))",
    )
    .unwrap();
    db.execute("insert into parent values (1, 2)").unwrap();
    db.execute("insert into child values (1, 1, 2)").unwrap();
    let err = db.execute("insert into child values (2, 9, 9)").unwrap_err();
    assert!(err.to_lowercase().contains("foreign key"));
}

#[test]
fn test_foreign_key_on_delete_cascade_deletes_children() {
    let mut db = test_db();
    db.execute("create table users (id int primary key)").unwrap();
    db.execute(
        "create table orders (id int, user_id int, foreign key(user_id) references users(id) on delete cascade)",
    )
    .unwrap();
    db.execute("insert into users values (1)").unwrap();
    db.execute("insert into users values (2)").unwrap();
    db.execute("insert into orders values (1, 1)").unwrap();
    db.execute("insert into orders values (2, 2)").unwrap();

    db.execute("delete from users where id = 1").unwrap();
    assert_eq!(db.execute("select * from orders").unwrap(), "id\tuser_id\n2\t2");
}

#[test]
fn test_composite_foreign_key_on_delete_cascade() {
    let mut db = test_db();
    db.execute("create table parent (a int, b int, primary key(a,b))")
        .unwrap();
    db.execute(
        "create table child (id int, a int, b int, foreign key(a,b) references parent(a,b) on delete cascade)",
    )
    .unwrap();
    db.execute("insert into parent values (1, 2)").unwrap();
    db.execute("insert into parent values (3, 4)").unwrap();
    db.execute("insert into child values (1, 1, 2)").unwrap();
    db.execute("insert into child values (2, 3, 4)").unwrap();

    db.execute("delete from parent where a = 1").unwrap();
    assert_eq!(db.execute("select * from child").unwrap(), "id\ta\tb\n2\t3\t4");
}

#[test]
fn test_fk_create_rejects_unknown_parent_table() {
    let mut db = test_db();
    db.execute("create table child (id int, p int, foreign key(p) references parent(id))")
        .unwrap_err();
}

#[test]
fn test_fk_create_rejects_unknown_parent_column() {
    let mut db = test_db();
    db.execute("create table parent (id int primary key)").unwrap();
    let err = db
        .execute("create table child (id int, p int, foreign key(p) references parent(missing))")
        .unwrap_err();
    assert!(err.to_lowercase().contains("unknown parent column"));
}

#[test]
fn test_fk_create_rejects_unknown_child_column() {
    let mut db = test_db();
    db.execute("create table parent (id int primary key)").unwrap();
    let err = db
        .execute("create table child (id int, foreign key(missing) references parent(id))")
        .unwrap_err();
    assert!(err.to_lowercase().contains("unknown column"));
}

#[test]
fn test_fk_create_rejects_mismatched_column_count() {
    let mut db = test_db();
    db.execute("create table parent (a int, b int, primary key(a,b))")
        .unwrap();
    let err = db
        .execute("create table child (x int, foreign key(x) references parent(a,b))")
        .unwrap_err();
    assert!(err.to_lowercase().contains("column count"));
}

#[test]
fn test_fk_create_requires_parent_key_or_unique() {
    let mut db = test_db();
    db.execute("create table parent (id int, v int)").unwrap();
    let err = db
        .execute("create table child (id int, p int, foreign key(p) references parent(id))")
        .unwrap_err();
    assert!(err.to_lowercase().contains("primary key or unique"));
}

#[test]
fn test_fk_create_allows_parent_unique_reference() {
    let mut db = test_db();
    db.execute("create table parent (id int, code text unique)").unwrap();
    db.execute("create table child (id int, code text, foreign key(code) references parent(code))")
        .unwrap();
}

#[test]
fn test_fk_restrict_parent_delete_allowed_when_unreferenced() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id))")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into p values (2)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    let out = db.execute("delete from p where id = 2").unwrap();
    assert_eq!(out, "deleted 1 row(s) from p");
}

#[test]
fn test_fk_cascade_deletes_multiple_child_rows() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute(
        "create table c (id int, pid int, foreign key(pid) references p(id) on delete cascade)",
    )
    .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("insert into c values (2, 1)").unwrap();
    db.execute("delete from p where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid");
}

#[test]
fn test_fk_cascade_and_restrict_can_coexist() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c1 (id int, pid int, foreign key(pid) references p(id) on delete cascade)")
        .unwrap();
    db.execute("create table c2 (id int, pid int, foreign key(pid) references p(id))")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c1 values (1, 1)").unwrap();
    db.execute("insert into c2 values (1, 1)").unwrap();
    let err = db.execute("delete from p where id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("restrict"));
}

#[test]
fn test_fk_child_update_to_existing_parent_succeeds() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id))")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into p values (2)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("update c set pid = 2 where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\t2");
}

#[test]
fn test_fk_parent_update_non_key_column_allowed() {
    let mut db = test_db();
    db.execute("create table p (id int primary key, name text)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id))")
        .unwrap();
    db.execute(r#"insert into p values (1, "a")"#).unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute(r#"update p set name = "b" where id = 1"#).unwrap();
    assert_eq!(db.execute("select * from p").unwrap(), "id\tname\n1\tb");
}

#[test]
fn test_fk_composite_restrict_blocks_parent_delete() {
    let mut db = test_db();
    db.execute("create table p (a int, b int, primary key(a,b))").unwrap();
    db.execute("create table c (id int, a int, b int, foreign key(a,b) references p(a,b))")
        .unwrap();
    db.execute("insert into p values (1, 2)").unwrap();
    db.execute("insert into c values (1, 1, 2)").unwrap();
    let err = db.execute("delete from p where a = 1").unwrap_err();
    assert!(err.to_lowercase().contains("restrict"));
}

#[test]
fn test_foreign_key_on_update_restrict_blocks_parent_key_update() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on update restrict)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    let err = db.execute("update p set id = 2 where id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("restrict"));
}

#[test]
fn test_foreign_key_on_update_cascade_updates_child_rows() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on update cascade)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("update p set id = 2 where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\t2");
}

#[test]
fn test_composite_foreign_key_on_update_cascade() {
    let mut db = test_db();
    db.execute("create table p (a int, b int, primary key(a,b))").unwrap();
    db.execute(
        "create table c (id int, a int, b int, foreign key(a,b) references p(a,b) on update cascade)",
    )
    .unwrap();
    db.execute("insert into p values (1, 2)").unwrap();
    db.execute("insert into c values (1, 1, 2)").unwrap();
    db.execute("update p set a = 3 where a = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\ta\tb\n1\t3\t2");
}

#[test]
fn test_foreign_key_on_update_cascade_multiple_children_rows() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on update cascade)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("insert into c values (2, 1)").unwrap();
    db.execute("update p set id = 2 where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\t2\n2\t2");
}

#[test]
fn test_foreign_key_on_delete_set_null_sets_child_to_null() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on delete set null)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("delete from p where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\tnull");
}

#[test]
fn test_foreign_key_on_update_set_null_sets_child_to_null() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on update set null)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("update p set id = 2 where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\tnull");
}

#[test]
fn test_foreign_key_set_null_requires_nullable_child_columns() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    let err = db
        .execute("create table c (id int, pid int not null, foreign key(pid) references p(id) on delete set null)")
        .unwrap_err();
    assert!(err.to_lowercase().contains("set null requires nullable"));
}

#[test]
fn test_foreign_key_on_delete_no_action_behaves_like_restrict() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on delete no action)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    let err = db.execute("delete from p where id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("no action"));
}

#[test]
fn test_foreign_key_on_update_no_action_behaves_like_restrict() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on update no action)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    let err = db.execute("update p set id = 2 where id = 1").unwrap_err();
    assert!(err.to_lowercase().contains("no action"));
}

#[test]
fn test_foreign_key_no_action_deferred_until_commit_can_be_fixed_in_tx() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on update no action)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();

    db.execute("begin").unwrap();
    db.execute("update p set id = 2 where id = 1").unwrap();
    db.execute("update c set pid = 2 where id = 1").unwrap();
    let out = db.execute("commit").unwrap();
    assert_eq!(out, "transaction committed");
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\t2");
}

#[test]
fn test_foreign_key_no_action_commit_fails_if_still_violated() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on update no action)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();

    db.execute("begin").unwrap();
    db.execute("update p set id = 2 where id = 1").unwrap();
    let err = db.execute("commit").unwrap_err();
    assert!(err.to_lowercase().contains("no action"));
    assert_eq!(db.execute("select * from p").unwrap(), "id\n1");
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\t1");
}

#[test]
fn test_foreign_key_no_action_delete_deferred_until_commit_can_be_fixed_in_tx() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on delete no action)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();

    db.execute("begin").unwrap();
    db.execute("delete from p where id = 1").unwrap();
    db.execute("delete from c where id = 1").unwrap();
    let out = db.execute("commit").unwrap();
    assert_eq!(out, "transaction committed");
    assert_eq!(db.execute("select * from p").unwrap(), "id");
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid");
}

#[test]
fn test_foreign_key_no_action_delete_commit_fails_if_still_violated() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id) on delete no action)")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();

    db.execute("begin").unwrap();
    db.execute("delete from p where id = 1").unwrap();
    let err = db.execute("commit").unwrap_err();
    assert!(err.to_lowercase().contains("no action"));

    // Commit failure should restore previous consistent state.
    assert_eq!(db.execute("select * from p").unwrap(), "id\n1");
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\t1");
}

#[test]
fn test_foreign_key_insert_with_null_child_key_is_allowed() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id))")
        .unwrap();
    db.execute("insert into c values (1, null)").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\tnull");
}

#[test]
fn test_foreign_key_update_child_to_null_is_allowed() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute("create table c (id int, pid int, foreign key(pid) references p(id))")
        .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1, 1)").unwrap();
    db.execute("update c set pid = null where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n1\tnull");
}

#[test]
fn test_composite_foreign_key_insert_with_partial_null_is_allowed() {
    let mut db = test_db();
    db.execute("create table p (a int, b int, primary key(a,b))")
        .unwrap();
    db.execute("create table c (id int, a int, b int, foreign key(a,b) references p(a,b))")
        .unwrap();
    db.execute("insert into c values (1, null, 2)").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\ta\tb\n1\tnull\t2");
}

#[test]
fn test_foreign_key_on_delete_cascade_propagates_to_grandchildren() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute(
        "create table c (id int primary key, pid int, foreign key(pid) references p(id) on delete cascade)",
    )
    .unwrap();
    db.execute(
        "create table g (id int, cid int, foreign key(cid) references c(id) on delete cascade)",
    )
    .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (10, 1)").unwrap();
    db.execute("insert into g values (100, 10)").unwrap();

    db.execute("delete from p where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid");
    assert_eq!(db.execute("select * from g").unwrap(), "id\tcid");
}

#[test]
fn test_foreign_key_on_delete_set_null_then_child_cascade_propagates() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute(
        "create table c (id int primary key, pid int, foreign key(pid) references p(id) on delete set null)",
    )
    .unwrap();
    db.execute(
        "create table g (id int, cid int, foreign key(cid) references c(id) on delete cascade)",
    )
    .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (10, 1)").unwrap();
    db.execute("insert into g values (100, 10)").unwrap();

    db.execute("delete from p where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\tpid\n10\tnull");
    assert_eq!(db.execute("select * from g").unwrap(), "id\tcid\n100\t10");
}

#[test]
fn test_foreign_key_on_update_cascade_propagates_to_grandchildren() {
    let mut db = test_db();
    db.execute("create table p (id int primary key)").unwrap();
    db.execute(
        "create table c (id int primary key, foreign key(id) references p(id) on update cascade)",
    )
    .unwrap();
    db.execute(
        "create table g (id int, cid int, foreign key(cid) references c(id) on update cascade)",
    )
    .unwrap();
    db.execute("insert into p values (1)").unwrap();
    db.execute("insert into c values (1)").unwrap();
    db.execute("insert into g values (100, 1)").unwrap();

    db.execute("update p set id = 2 where id = 1").unwrap();
    assert_eq!(db.execute("select * from c").unwrap(), "id\n2");
    assert_eq!(db.execute("select * from g").unwrap(), "id\tcid\n100\t2");
}

