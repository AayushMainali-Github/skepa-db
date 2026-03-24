use skepa_db_core::Database;
use skepa_db_core::query_result::QueryResult;
use skepa_db_core::types::value::Value;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

fn test_db() -> Database {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("skepa_db_test_{}_{}", std::process::id(), id));
    let _ = std::fs::remove_dir_all(&path);
    Database::open_legacy(path)
}

fn seed_users_3(db: &mut Database) {
    db.execute_legacy("create table users (id int, name text, age int)")
        .unwrap();
    db.execute_legacy(r#"insert into users values (1, "a", 30)"#)
        .unwrap();
    db.execute_legacy(r#"insert into users values (2, "b", 20)"#)
        .unwrap();
    db.execute_legacy(r#"insert into users values (3, "c", 10)"#)
        .unwrap();
}

fn assert_select_result(
    result: QueryResult,
    expected_columns: &[&str],
    expected_rows: Vec<Vec<Value>>,
) {
    match result {
        QueryResult::Select {
            schema,
            rows,
            stats,
        } => {
            let actual_columns = schema
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>();
            assert_eq!(actual_columns, expected_columns);
            assert_eq!(rows, expected_rows);
            assert_eq!(stats.rows_returned, Some(rows.len()));
            assert_eq!(stats.rows_affected, None);
        }
        other => panic!("expected select result, got {other:?}"),
    }
}

fn assert_mutation_result(
    result: QueryResult,
    expected_message: &str,
    expected_rows_affected: usize,
) {
    match result {
        QueryResult::Mutation {
            message,
            rows_affected,
            stats,
        } => {
            assert_eq!(message, expected_message);
            assert_eq!(rows_affected, expected_rows_affected);
            assert_eq!(stats.rows_returned, None);
            assert_eq!(stats.rows_affected, Some(expected_rows_affected));
        }
        other => panic!("expected mutation result, got {other:?}"),
    }
}

fn assert_schema_change_result(result: QueryResult, expected_message: &str) {
    match result {
        QueryResult::SchemaChange { message, stats } => {
            assert_eq!(message, expected_message);
            assert_eq!(stats.rows_returned, None);
            assert_eq!(stats.rows_affected, None);
        }
        other => panic!("expected schema change result, got {other:?}"),
    }
}

fn assert_transaction_result(result: QueryResult, expected_message: &str) {
    match result {
        QueryResult::Transaction { message, stats } => {
            assert_eq!(message, expected_message);
            assert_eq!(stats.rows_returned, None);
            assert_eq!(stats.rows_affected, None);
        }
        other => panic!("expected transaction result, got {other:?}"),
    }
}

mod aggregates;
mod api_json;
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
