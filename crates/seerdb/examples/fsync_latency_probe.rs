//! Measure raw fsync latency on this host with the same access pattern the
//! publication path produces: small writes followed by data-device syncs,
//! one file per durable artifact (data pages, PMT metadata, MVCC versions),
//! plus the single-file shape PostgreSQL uses (one WAL append + one sync).
//!
//! Run with:
//!   cargo run --release -p seerdb --example fsync_latency_probe -- [syncs]

#![allow(clippy::disallowed_methods)]

use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Instant;

fn main() {
    let syncs: usize = env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(300);

    let directory = tempfile::tempdir().expect("tempdir");

    // Shape A: three separate files, each written then synced, per
    // "commit" — the current publication cost structure.
    let data = OpenOptions::new()
        .create(true)
        .append(true)
        .open(directory.path().join("data"))
        .expect("open data");
    let pmt = OpenOptions::new()
        .create(true)
        .append(true)
        .open(directory.path().join("pmt"))
        .expect("open pmt");
    let versions = OpenOptions::new()
        .create(true)
        .append(true)
        .open(directory.path().join("versions"))
        .expect("open versions");

    let payload = vec![0u8; 4096];
    let started = Instant::now();
    let mut data = data;
    let mut pmt = pmt;
    let mut versions = versions;
    for _ in 0..syncs {
        data.write_all(&payload).expect("data write");
        data.sync_data().expect("data sync");
        pmt.write_all(&payload).expect("pmt write");
        pmt.sync_data().expect("pmt sync");
        versions.write_all(&payload).expect("versions write");
        versions.sync_data().expect("versions sync");
    }
    let three_file = started.elapsed();

    // Shape B: one file, one append + one sync per commit — PostgreSQL's
    // WAL shape and the collapse candidate.
    let single = OpenOptions::new()
        .create(true)
        .append(true)
        .open(directory.path().join("wal"))
        .expect("open wal");
    let started = Instant::now();
    let mut single = single;
    for _ in 0..syncs {
        single.write_all(&payload).expect("wal write");
        single.sync_data().expect("wal sync");
    }
    let one_file = started.elapsed();

    println!("== fsync latency probe: {syncs} commits ==");
    println!(
        "three-file shape:  {:>10.3} ms total ({:.3} ms/commit)",
        three_file.as_secs_f64() * 1000.0,
        three_file.as_secs_f64() * 1000.0 / syncs as f64
    );
    println!(
        "one-file shape:    {:>10.3} ms total ({:.3} ms/commit)",
        one_file.as_secs_f64() * 1000.0,
        one_file.as_secs_f64() * 1000.0 / syncs as f64
    );
}
