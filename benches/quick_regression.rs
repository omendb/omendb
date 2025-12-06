//! Quick regression benchmark (~45 seconds)
//!
//! Covers key operations for fast iteration. Run with:
//!   cargo bench --bench quick_regression
//!
//! For comprehensive benchmarks, use:
//!   cargo bench --bench ycsb
//!   cargo bench --bench simd_search_comparison

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use seerdb::DBOptions;
use std::time::Duration;
use tempfile::TempDir;

fn quick_config(c: &mut Criterion) -> criterion::BenchmarkGroup<criterion::measurement::WallTime> {
    let mut group = c.benchmark_group("quick");
    group.sample_size(20); // Fewer samples
    group.measurement_time(Duration::from_millis(200)); // Shorter measurement
    group.warm_up_time(Duration::from_millis(100)); // Shorter warmup
    group
}

fn bench_db_operations(c: &mut Criterion) {
    let mut group = quick_config(c);

    let tmp = TempDir::new().unwrap();
    let db = DBOptions::default()
        .memtable_capacity(16 * 1024 * 1024) // 16MB
        .background_compaction(false)
        .open(tmp.path())
        .unwrap();

    // Pre-populate for reads
    for i in 0..10_000u64 {
        db.put(&i.to_be_bytes(), &[0u8; 100]).unwrap();
    }

    // Random read
    group.bench_function("get_100B", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i = (i + 7919) % 10_000; // Pseudo-random
            black_box(db.get(black_box(&i.to_be_bytes())).unwrap())
        });
    });

    // Sequential write
    group.bench_function("put_100B", |b| {
        let mut i = 10_000u64;
        b.iter(|| {
            i += 1;
            black_box(db.put(&i.to_be_bytes(), &[0u8; 100]).unwrap())
        });
    });

    // Range scan (10 keys)
    group.bench_function("scan_10", |b| {
        b.iter(|| {
            let scan = db.scan();
            let iter = scan.iter().unwrap();
            let mut count = 0;
            for _ in iter.take(10) {
                count += 1;
            }
            black_box(count)
        });
    });

    // Point delete
    group.bench_function("delete", |b| {
        let mut i = 20_000u64;
        b.iter(|| {
            i += 1;
            db.put(&i.to_be_bytes(), &[0u8; 10]).unwrap();
            black_box(db.delete(&i.to_be_bytes()).unwrap())
        });
    });

    group.finish();
}

fn bench_simd_key_ops(c: &mut Criterion) {
    use seerdb::simd::{compare_keys, decode_varint, shared_prefix_len};

    let mut group = quick_config(c);

    let key_a = b"user:12345:profile:settings";
    let key_b = b"user:12345:profile:settingz";
    let key_long = b"this_is_a_very_long_key_that_exceeds_32_bytes_for_simd";

    group.bench_function("compare_keys_28B", |b| {
        b.iter(|| black_box(compare_keys(black_box(key_a), black_box(key_b))))
    });

    group.bench_function("compare_keys_54B", |b| {
        b.iter(|| black_box(compare_keys(black_box(key_long), black_box(key_long))))
    });

    group.bench_function("shared_prefix", |b| {
        b.iter(|| black_box(shared_prefix_len(black_box(key_a), black_box(key_b))))
    });

    let mut buf = [0u8; 32];
    buf[0] = 0x85;
    buf[1] = 0x01;
    group.bench_function("decode_varint", |b| {
        b.iter(|| black_box(decode_varint(black_box(&buf))))
    });

    group.finish();
}

fn bench_value_sizes(c: &mut Criterion) {
    let mut group = quick_config(c);

    for size in [64, 1024, 4096] {
        let tmp = TempDir::new().unwrap();
        let db = DBOptions::default()
            .memtable_capacity(32 * 1024 * 1024)
            .background_compaction(false)
            .open(tmp.path())
            .unwrap();

        let value = vec![0u8; size];

        group.bench_with_input(BenchmarkId::new("put", size), &size, |b, _| {
            let mut i = 0u64;
            b.iter(|| {
                i += 1;
                black_box(db.put(&i.to_be_bytes(), &value).unwrap())
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_db_operations,
    bench_simd_key_ops,
    bench_value_sizes
);
criterion_main!(benches);
