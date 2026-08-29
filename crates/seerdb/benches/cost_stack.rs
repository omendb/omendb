#![allow(clippy::disallowed_methods)]

use criterion::{
    BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use seerdb::btree::{BTree, LookupResult};
use seerdb::buffer::{BufferManager, GuardAccess};
use seerdb::{DB, Options, PAGE_SIZE, TransactionDatabase, TreeId};
use std::time::Duration;
use tempfile::{TempDir, tempdir};

const DEFAULT_KEYS: usize = 1_000;
const KEY_WIDTHS: [usize; 3] = [16, 64, 256];

fn key(index: usize) -> String {
    format!("key-{index:08}")
}

fn value(index: usize) -> String {
    format!("value-{index:08}")
}

fn fixed_width_key(index: usize, width: usize) -> Vec<u8> {
    let mut bytes = format!("{index:016x}").into_bytes();
    bytes.resize(width.max(bytes.len()), b'x');
    bytes
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

fn populated_btree_with_width(count: usize, width: usize) -> BTree {
    let mut tree = BTree::new();
    for index in 0..count {
        let key = fixed_width_key(index, width);
        tree.insert(&key, value(index).as_bytes()).unwrap();
    }
    tree
}

fn populated_db(count: usize) -> (TempDir, DB) {
    let directory = tempdir().unwrap();
    let mut db = DB::create(directory.path().join("db"), Options::default()).unwrap();
    for index in 0..count {
        db.put(key(index).as_bytes(), value(index).as_bytes()).unwrap();
    }
    db.flush().unwrap();
    (directory, db)
}

fn populated_transaction_db(count: usize) -> (TempDir, TransactionDatabase, TreeId) {
    let directory = tempdir().unwrap();
    let db = TransactionDatabase::create(directory.path().join("db"), Options::default()).unwrap();

    let tree = {
        let mut transaction = db.begin().unwrap();
        let tree = transaction.create_tree().unwrap();
        transaction.commit().unwrap();
        tree
    };

    let mut transaction = db.begin().unwrap();
    for index in 0..count {
        transaction
            .put(tree, key(index).as_bytes(), value(index).as_bytes())
            .unwrap();
    }
    transaction.commit().unwrap();

    (directory, db, tree)
}

/// Pure in-memory B-tree lookup. Varying key width makes node fanout and key
/// comparison cost visible before buffer or transaction machinery is involved.
fn bench_btree_point_lookup_by_key_width(c: &mut Criterion) {
    let mut group = configure_group(c, "cost_stack/btree_point_lookup");
    group.throughput(Throughput::Elements(1));

    for width in KEY_WIDTHS {
        let tree = populated_btree_with_width(DEFAULT_KEYS, width);
        let probe_key = fixed_width_key(DEFAULT_KEYS / 2, width);
        let probe_value = value(DEFAULT_KEYS / 2);
        group.bench_with_input(
            BenchmarkId::new("key_bytes", width),
            &width,
            |benchmark, _| {
                benchmark.iter(|| {
                    let result = tree.lookup(black_box(&probe_key)).unwrap();
                    black_box(
                        matches!(result, LookupResult::Found(found) if found == probe_value.as_bytes()),
                    );
                });
            },
        );
    }

    group.finish();
}

/// Resident buffer hit with no device I/O. This isolates PageCacheKey
/// translation plus frame/guard pinning from B-tree traversal.
fn bench_buffer_resident_hit(c: &mut Criterion) {
    let mut manager = BufferManager::new(PAGE_SIZE * 64);
    let page = [0xA5_u8; PAGE_SIZE];
    {
        let guard = manager
            .fetch(7, &page, GuardAccess::Read)
            .expect("initial resident page");
        black_box(manager.frame_data(&guard)[0]);
    }

    let mut group = configure_group(c, "cost_stack/buffer_resident_hit");
    group.throughput(Throughput::Elements(1));
    group.bench_function("hash_translation_and_guard", |benchmark| {
        benchmark.iter(|| {
            let guard = manager
                .fetch(black_box(7_u64), black_box(&page), GuardAccess::Read)
                .unwrap();
            black_box(manager.frame_data(&guard)[0]);
        });
    });
    group.finish();
}

/// Durable DB point read on a warm handle. Compare this with the pure B-tree
/// and resident-buffer groups to locate storage-engine read-path overhead.
fn bench_db_point_lookup(c: &mut Criterion) {
    let (_directory, db) = populated_db(DEFAULT_KEYS);
    let probe_key = key(DEFAULT_KEYS / 2);

    let mut group = configure_group(c, "cost_stack/db_point_lookup");
    group.throughput(Throughput::Elements(1));
    group.bench_function(BenchmarkId::new("keys", DEFAULT_KEYS), |benchmark| {
        benchmark.iter(|| black_box(db.get(black_box(probe_key.as_bytes())).unwrap()));
    });
    group.finish();
}

/// Repeated lookup inside one fixed snapshot. This adds logical MVCC
/// visibility and the transaction runtime's shared-state acquisition to the
/// durable DB point-read path without transaction begin/finish overhead.
fn bench_transaction_snapshot_point_lookup(c: &mut Criterion) {
    let (_directory, db, tree) = populated_transaction_db(DEFAULT_KEYS);
    let probe_key = key(DEFAULT_KEYS / 2);
    let transaction = db.begin().unwrap();

    let mut group = configure_group(c, "cost_stack/transaction_snapshot_point_lookup");
    group.throughput(Throughput::Elements(1));
    group.bench_function(BenchmarkId::new("keys", DEFAULT_KEYS), |benchmark| {
        benchmark.iter(|| {
            black_box(
                transaction
                    .get(tree, black_box(probe_key.as_bytes()))
                    .unwrap(),
            );
        });
    });
    group.finish();

    drop(transaction);
}

/// Begin + one point read + read-only commit. Read-only commit does not create
/// a durable publication, so the delta from the snapshot lookup primarily
/// exposes transaction lifecycle and snapshot-registry overhead.
fn bench_transaction_read_only_round_trip(c: &mut Criterion) {
    let (_directory, db, tree) = populated_transaction_db(DEFAULT_KEYS);
    let probe_key = key(DEFAULT_KEYS / 2);

    let mut group = configure_group(c, "cost_stack/transaction_read_only_round_trip");
    group.throughput(Throughput::Elements(1));
    group.bench_function(BenchmarkId::new("keys", DEFAULT_KEYS), |benchmark| {
        benchmark.iter(|| {
            let mut transaction = db.begin().unwrap();
            black_box(
                transaction
                    .get(tree, black_box(probe_key.as_bytes()))
                    .unwrap(),
            );
            black_box(transaction.commit().unwrap());
        });
    });
    group.finish();
}

/// One durable transactional point write. The fixture stays open so the
/// measured operation is begin/stage/commit rather than database creation or
/// reopen. Every iteration uses a fresh key to avoid conflict-path skew.
fn bench_transaction_durable_point_commit(c: &mut Criterion) {
    let (_directory, db, tree) = populated_transaction_db(DEFAULT_KEYS);
    let mut next_index = DEFAULT_KEYS;

    let mut group = c.benchmark_group("cost_stack/transaction_durable_point_commit");
    group.sample_size(10);
    group.measurement_time(Duration::from_millis(750));
    group.throughput(Throughput::Elements(1));
    group.bench_function("single_key", |benchmark| {
        benchmark.iter(|| {
            let index = next_index;
            next_index = next_index.checked_add(1).expect("benchmark key exhausted");
            let key = key(index);
            let value = value(index);
            let mut transaction = db.begin().unwrap();
            transaction
                .put(tree, black_box(key.as_bytes()), black_box(value.as_bytes()))
                .unwrap();
            black_box(transaction.commit().unwrap());
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_btree_point_lookup_by_key_width,
    bench_buffer_resident_hit,
    bench_db_point_lookup,
    bench_transaction_snapshot_point_lookup,
    bench_transaction_read_only_round_trip,
    bench_transaction_durable_point_commit,
);
criterion_main!(benches);
