use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use skepa_db_core::Database;
use skepa_db_core::config::DbConfig;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const ROW_COUNTS: &[usize] = &[100, 1_000];

fn bench_core_workloads(c: &mut Criterion) {
    let mut group = c.benchmark_group("core_workloads");

    for &row_count in ROW_COUNTS {
        group.bench_with_input(BenchmarkId::new("indexed_eq_select", row_count), &row_count, |b, &row_count| {
            let mut db = setup_users_db(row_count);
            b.iter(|| {
                db.execute("select name from users where id = 777")
                    .expect("indexed equality select should succeed");
            });
        });

        group.bench_with_input(BenchmarkId::new("full_scan_select", row_count), &row_count, |b, &row_count| {
            let mut db = setup_users_db(row_count);
            b.iter(|| {
                db.execute("select * from users where age >= 0")
                    .expect("full scan select should succeed");
            });
        });

        group.bench_with_input(BenchmarkId::new("indexed_update", row_count), &row_count, |b, &row_count| {
            let mut db = setup_users_db(row_count);
            let mut next_age = 1000_i64;
            b.iter(|| {
                let sql = format!("update users set age = {next_age} where id = 777");
                next_age += 1;
                db.execute(&sql)
                    .expect("indexed update should succeed");
            });
        });

        group.bench_with_input(BenchmarkId::new("indexed_delete_insert", row_count), &row_count, |b, &row_count| {
            let mut db = setup_users_db(row_count);
            let mut next_id = 10_000_i64;
            b.iter(|| {
                db.execute("delete from users where id = 777")
                    .expect("delete should succeed");
                let sql = format!(
                    "insert into users values ({next_id}, \"user-{next_id}\", 42)"
                );
                next_id += 1;
                db.execute(&sql)
                    .expect("insert should succeed");
            });
        });
    }

    group.finish();
}

fn setup_users_db(row_count: usize) -> Database {
    let path = unique_bench_path();
    let mut db = Database::open(DbConfig::new(path)).expect("benchmark db should open");
    db.execute("create table users (id int primary key, name text, age int)")
        .expect("create table should succeed");
    db.execute("create index on users (age)")
        .expect("create index should succeed");

    for id in 0..row_count {
        let sql = format!(
            "insert into users values ({id}, \"user-{id}\", {})",
            20 + (id % 50)
        );
        db.execute(&sql).expect("seed insert should succeed");
    }

    db
}

fn unique_bench_path() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "skepa-db-bench-{}-{nanos}-{id}",
        std::process::id()
    ))
}

criterion_group!(benches, bench_core_workloads);
criterion_main!(benches);
