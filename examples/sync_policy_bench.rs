//! Benchmark different WAL sync policies
//!
//! Run with: cargo run --release --example sync_policy_bench

use seerdb::wal::SyncPolicy;
use seerdb::DBOptions;
use std::time::Instant;
use tempfile::tempdir;

fn main() {
    println!("WAL Sync Policy Benchmark");
    println!("=========================\n");

    let iterations = 100u64;
    let value = vec![0u8; 100];

    for (name, policy) in [
        ("SyncAll ", SyncPolicy::SyncAll),
        ("SyncData", SyncPolicy::SyncData),
        ("Barrier ", SyncPolicy::Barrier),
        ("None    ", SyncPolicy::None),
    ] {
        let tmp = tempdir().unwrap();
        let db = DBOptions::default()
            .sync_policy(policy)
            .background_compaction(false)
            .open(tmp.path())
            .unwrap();

        // Warmup
        for i in 0u64..10 {
            db.put(&i.to_be_bytes(), &value).unwrap();
        }

        let start = Instant::now();
        for i in 10u64..(10 + iterations) {
            db.put(&i.to_be_bytes(), &value).unwrap();
        }
        let elapsed = start.elapsed();

        let ms_per_op = elapsed.as_secs_f64() * 1000.0 / iterations as f64;
        let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();

        println!(
            "{}: {:>8.3} ms/op  ({:>8.0} ops/sec)",
            name, ms_per_op, ops_per_sec
        );
    }

    println!("\nNote: Barrier uses F_BARRIERFSYNC on macOS (~10x faster than SyncData)");
    println!("      On Linux, Barrier falls back to SyncData (fdatasync is already fast)");
}
