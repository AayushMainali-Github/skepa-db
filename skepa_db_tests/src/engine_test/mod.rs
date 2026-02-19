use skepa_db_core::Database;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

fn test_db() -> Database {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_test_{}_{}", std::process::id(), id));
    let _ = std::fs::remove_dir_all(&path);
    Database::open(path)
}

fn seed_users_3(db: &mut Database) {
    db.execute("create table users (id int, name text, age int)")
        .unwrap();
    db.execute(r#"insert into users values (1, "a", 30)"#)
        .unwrap();
    db.execute(r#"insert into users values (2, "b", 20)"#)
        .unwrap();
    db.execute(r#"insert into users values (3, "c", 10)"#)
        .unwrap();
}

mod aggregates;
mod basic;
mod constraints;
mod dml;
mod foreign_keys;
mod indexes;
mod joins;
mod misc;
mod persistence;
mod select;
mod transactions;
