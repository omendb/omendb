//! Mixed-workload probe: one writer commits point transactions in waves
//! while R reader threads issue snapshot point reads. Reader throughput
//! during waves is the metric — a publication wave's database-guard hold
//! must not serialize transaction reads.
//!
//! ```text
//! cargo run --release -p seerdb --example mixed_probe -- [readers] [writer-commits]
//! ```

use seerdb::{Options, TransactionDatabase};

fn main() {
    let readers: usize = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(7);
    let writer_commits: usize = std::env::args()
        .nth(2)
        .and_then(|value| value.parse().ok())
        .unwrap_or(2_000);

    let directory = tempfile::tempdir().expect("tempdir");
    let database =
        TransactionDatabase::create(directory.path().join("db"), Options::default()).expect("db");
    let database = std::sync::Arc::new(database);

    let tree = {
        let mut transaction = database.begin().expect("begin");
        let tree = transaction.create_tree().expect("tree");
        transaction
            .put(tree, b"hot", b"seed")
            .expect("seed hot key");
        transaction.commit().expect("commit");
        tree
    };

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let writer = {
        let database = std::sync::Arc::clone(&database);
        let stop = std::sync::Arc::clone(&stop);
        std::thread::spawn(move || {
            for step in 0..writer_commits {
                let mut transaction = database.begin().expect("begin");
                transaction
                    .put(tree, format!("w{step}").as_bytes(), b"value")
                    .expect("put");
                transaction.commit().expect("commit");
            }
            stop.store(true, std::sync::atomic::Ordering::Release);
        })
    };

    let reader_handles: Vec<_> = (0..readers)
        .map(|_| {
            let database = std::sync::Arc::clone(&database);
            let stop = std::sync::Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut reads: usize = 0;
                while !stop.load(std::sync::atomic::Ordering::Acquire) {
                    let mut transaction = database.begin().expect("begin");
                    let value = transaction.get(tree, b"hot").expect("get");
                    assert_eq!(value.as_deref(), Some(b"seed".as_slice()));
                    transaction.commit().expect("read-only commit");
                    reads += 1;
                }
                reads
            })
        })
        .collect();

    let started = std::time::Instant::now();
    writer.join().expect("writer");
    let elapsed = started.elapsed();
    let total_reads: usize = reader_handles
        .into_iter()
        .map(|handle| handle.join().expect("reader"))
        .sum();
    println!(
        "writer: {writer_commits} commits in {elapsed:?} ({:.0} commits/s); \
         readers: {total_reads} snapshot reads ({:.0} reads/s across {readers} readers)",
        writer_commits as f64 / elapsed.as_secs_f64(),
        total_reads as f64 / elapsed.as_secs_f64(),
    );
}
