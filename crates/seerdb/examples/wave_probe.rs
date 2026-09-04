//! Group-commit wave-formation probe: N threads each commit single-row
//! transactions; prints wave sizes and aggregate throughput. Run with:
//!
//! ```text
//! cargo run --release --example wave_probe -- [threads] [commits-per-thread] [--wal-first]
//! ```

use seerdb::{Options, TransactionDatabase};

fn main() {
    let threads: usize = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(8);
    let per_thread: usize = std::env::args()
        .nth(2)
        .and_then(|value| value.parse().ok())
        .unwrap_or(500);

    // WAL-first acks (PG-shaped: one WAL sync per wave, pages deferred to
    // the materialization bound) with --wal-first, so the probe measures
    // both durability postures from one binary.
    let wal_first = std::env::args().any(|arg| arg == "--wal-first");
    let options = Options {
        wal_first_commits: wal_first,
        ..Options::default()
    };
    let directory = tempfile::tempdir().expect("tempdir");
    let database = TransactionDatabase::create(directory.path().join("db"), options).expect("db");
    let database = std::sync::Arc::new(database);

    // One tree up front so the probe measures pure commit waves.
    let tree = {
        let mut transaction = database.begin().expect("begin");
        let tree = transaction.create_tree().expect("tree");
        transaction.commit().expect("seed");
        tree
    };

    let started = std::time::Instant::now();
    let mut handles = Vec::new();
    for worker in 0..threads {
        let database = std::sync::Arc::clone(&database);
        handles.push(std::thread::spawn(move || {
            for step in 0..per_thread {
                let key = format!("w{worker}-k{step}");
                let mut transaction = database.begin().expect("begin");
                transaction
                    .put(tree, key.as_bytes(), key.as_bytes())
                    .expect("put");
                transaction.commit().expect("commit");
            }
        }));
    }
    for handle in handles {
        handle.join().expect("worker");
    }
    let elapsed = started.elapsed();
    let total = threads * per_thread;
    println!(
        "{total} commits across {threads} threads in {elapsed:?}: {:.0} ops/s",
        total as f64 / elapsed.as_secs_f64()
    );
    println!(
        "db bytes: {}",
        std::fs::metadata(directory.path().join("db"))
            .map(|m| m.len())
            .unwrap_or(0)
    );
}
