#![allow(clippy::disallowed_methods)]

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use seerdb::btree::{BTree, LookupResult};
use seerdb::{DB, Options};
use std::time::Duration;
use tempfile::{TempDir, tempdir};

const DEFAULT_KEYS: usize = 1_000;

fn key(index: usize) -> String {
    format!("key-{index:08}")
}

fn value(index: usize) -> String {
    format!("value-{index:08}")
}

fn populated_btree(count: usize) -> BTree {
    let mut tree = BTree::new();
    for index in 0..count {
        let key = key(index);
        let value = value(index);
        tree.insert(key.as_bytes(), value.as_bytes()).unwrap();
    }
    tree
}

fn empty_db() -> (TempDir, DB) {
    let directory = tempdir().unwrap();
    let db = DB::open(directory.path().join("db"), Options::default()).unwrap();
    (directory, db)
}

fn populated_db(count: usize) -> (TempDir, DB) {
    let (directory, mut db) = empty_db();
    for index in 0..count {
        let key = key(index);
        let value = value(index);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.flush().unwrap();
    (directory, db)
}

fn configure_group<'a>(
    c: &'a mut Criterion,
    name: &str,
) -> criterion::BenchmarkGroup<'a, criterion::measurement::WallTime> {
    let mut group = c.benchmark_group(name);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(1));
    group
}

fn bench_btree_point_lookup(c: &mut Criterion) {
    let tree = populated_btree(DEFAULT_KEYS);
    let probe_key = key(DEFAULT_KEYS / 2);
    let probe_value = value(DEFAULT_KEYS / 2);
    let mut group = configure_group(c, "btree_point_lookup");
    group.bench_function(BenchmarkId::new("keys", DEFAULT_KEYS), |benchmark| {
        benchmark.iter(|| {
                let result = tree.lookup(black_box(probe_key.as_bytes())).unwrap();
            black_box(
                matches!(result, LookupResult::Found(value) if value == probe_value.as_bytes()),
            );
        });
    });
    group.finish();
}

fn bench_btree_range_scan(c: &mut Criterion) {
    let tree = populated_btree(DEFAULT_KEYS);
    let mut group = configure_group(c, "btree_range_scan");
    for scan_size in [100usize, 500, DEFAULT_KEYS] {
        group.throughput(Throughput::Elements(scan_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(scan_size),
            &scan_size,
            |benchmark, &scan_size| {
                benchmark.iter(|| {
                    let end = key(scan_size);
                    let count = tree
                        .range_scan(b"key-00000000", end.as_bytes())
                        .unwrap()
                        .map(Result::unwrap)
                        .count();
                    black_box(count);
                });
            },
        );
    }
    group.finish();
}

fn bench_db_point_lookup(c: &mut Criterion) {
    let (_directory, db) = populated_db(DEFAULT_KEYS);
    let probe_key = key(DEFAULT_KEYS / 2);
    let mut group = configure_group(c, "db_point_lookup");
    group.bench_function(BenchmarkId::new("keys", DEFAULT_KEYS), |benchmark| {
        benchmark.iter(|| black_box(db.get(black_box(probe_key.as_bytes())).unwrap()));
    });
    group.finish();
}

fn bench_db_range_scan(c: &mut Criterion) {
    let (_directory, db) = populated_db(DEFAULT_KEYS);
    let mut group = configure_group(c, "db_range_scan");
    for scan_size in [100usize, 500, DEFAULT_KEYS] {
        group.throughput(Throughput::Elements(scan_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(scan_size),
            &scan_size,
            |benchmark, &scan_size| {
                benchmark.iter(|| {
                    let end = key(scan_size);
                    black_box(db.range(b"key-00000000", end.as_bytes()).unwrap().len());
                });
            },
        );
    }
    group.finish();
}

fn bench_db_flush_batch(c: &mut Criterion) {
    let mut group = configure_group(c, "db_flush_batch");
    for batch_size in [100usize, 500] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |benchmark, &batch_size| {
                benchmark.iter_batched(
                    || {
                        let (directory, mut db) = empty_db();
                        for index in 0..batch_size {
                            let key = key(index);
                            let value = value(index);
                            db.put(key.as_bytes(), value.as_bytes()).unwrap();
                        }
                        (directory, db)
                    },
                    |(directory, mut db)| {
                        db.flush().unwrap();
                        drop(db);
                        drop(directory);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_db_mixed_workload(c: &mut Criterion) {
    const OPERATIONS: usize = 500;
    let mut group = configure_group(c, "db_mixed_workload");
    group.throughput(Throughput::Elements(OPERATIONS as u64));
    group.bench_function("500_ops", |benchmark| {
        benchmark.iter_batched(
            empty_db,
            |(directory, mut db)| {
                for index in 0..OPERATIONS {
                    let key = key(index);
                    match index % 10 {
                        0..=1 => {
                            black_box(db.get(key.as_bytes()).unwrap());
                        }
                        2 => {
                            db.delete(key.as_bytes()).unwrap();
                        }
                        _ => {
                            db.put(key.as_bytes(), value(index).as_bytes()).unwrap();
                        }
                    }
                }
                db.flush().unwrap();
                drop(db);
                drop(directory);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_db_blob_read(c: &mut Criterion) {
    let (directory, mut db) = empty_db();
    let large_value = vec![0x5Au8; 4 * 1024];
    db.put(b"large", &large_value).unwrap();
    db.flush().unwrap();

    let mut group = configure_group(c, "db_blob_read");
    group.throughput(Throughput::Bytes(large_value.len() as u64));
    group.bench_function("4kb", |benchmark| {
        benchmark.iter(|| black_box(db.get(black_box(b"large")).unwrap()));
    });
    group.finish();
    drop(db);
    drop(directory);
}

fn bench_db_reopen_lazy_point_read(c: &mut Criterion) {
    let (directory, mut db) = populated_db(DEFAULT_KEYS);
    let path = directory.path().join("db");
    let probe_key = key(DEFAULT_KEYS / 2);
    db.close().unwrap();

    let mut group = configure_group(c, "db_reopen_lazy_point_read");
    group.throughput(Throughput::Elements(1));
    group.bench_function(BenchmarkId::new("keys", DEFAULT_KEYS), |benchmark| {
        benchmark.iter(|| {
            let reopened = DB::open(&path, Options::default()).unwrap();
            black_box(reopened.get(black_box(probe_key.as_bytes())).unwrap());
        });
    });
    group.finish();
}

fn bench_db_reopen_verify(c: &mut Criterion) {
    let (directory, mut db) = populated_db(DEFAULT_KEYS);
    let path = directory.path().join("db");
    db.close().unwrap();

    let mut group = configure_group(c, "db_reopen_verify");
    group.bench_function(BenchmarkId::new("keys", DEFAULT_KEYS), |benchmark| {
        benchmark.iter(|| {
            let mut reopened = DB::open(&path, Options::default()).unwrap();
            black_box(reopened.verify().unwrap());
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_btree_point_lookup,
    bench_btree_range_scan,
    bench_db_point_lookup,
    bench_db_range_scan,
    bench_db_flush_batch,
    bench_db_mixed_workload,
    bench_db_blob_read,
    bench_db_reopen_lazy_point_read,
    bench_db_reopen_verify,
);
criterion_main!(benches);
