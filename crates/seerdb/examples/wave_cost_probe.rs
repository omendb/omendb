//! Attribute the transactional group-commit wave's wall-clock cost.
//!
//! The earlier hand measurement (pgbench differential era) estimated the
//! MVCC version-store sync at ~4.5 ms and `commit_group_at` at ~4.5 ms per
//! wave, against a 0.05 ms raw fsync on the same volume. This probe drives
//! single-statement write transactions through `TransactionDatabase`
//! (the shape pgbench produces: point read + writes, one txn per
//! execution) and diffs the recorded publication-phase counters, timing
//! the whole wave externally so the version-store sync gap is visible as
//! `wave_total - recorded phases`.
//!
//! Run with:
//!   cargo run --release -p seerdb --example wave_cost_probe -- [txns] [keyspace]

#![allow(clippy::disallowed_methods)]

use seerdb::{Options, TransactionDatabase};
use std::env;
use std::time::Instant;

fn main() {
    let txns: usize = env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(200);
    let keyspace: usize = env::args()
        .nth(2)
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000);

    // Third argument selects the sync class: "device" (default) or
    // "kernel". The probe exists to show the difference.
    let sync_class = match env::args().nth(3).as_deref() {
        Some("kernel") => seerdb::db::SyncClass::KernelBarrier,
        _ => seerdb::db::SyncClass::DeviceBarrier,
    };
    println!("sync class:       {sync_class:?}");

    let directory = tempfile::tempdir().expect("tempdir");
    let database = TransactionDatabase::create(
        directory.path().join("db"),
        Options {
            sync_class,
            ..Options::default()
        },
    )
    .expect("create database");

    let tree = {
        let mut transaction = database.begin().expect("begin");
        let tree = transaction.create_tree().expect("create tree");
        transaction.commit().expect("commit tree creation");
        tree
    };
    // Seed: every key starts with a version history.
    {
        let mut transaction = database.begin().expect("begin seed");
        for index in 0..keyspace {
            transaction
                .put(tree, &key(index), format!("value-{index}").as_bytes())
                .expect("seed put");
        }
        transaction.commit().expect("seed commit");
    }

    let before = database.metrics().expect("metrics before");
    let started = Instant::now();

    // One transaction per iteration: read a key, write another (the TPC-B
    // shape without the SQL tier overhead). Each commit stages, then the
    // wave publishes it — this measures the publish lane itself.
    for index in 0..txns {
        let mut transaction = database.begin().expect("begin");
        transaction.get(tree, &key(index % keyspace)).expect("read");
        transaction
            .put(tree, &key((index * 7) % keyspace), &value(index))
            .expect("write");
        transaction.commit().expect("commit");
    }

    let elapsed = started.elapsed();
    let after = database.metrics().expect("metrics after");

    println!("== wave cost probe: {txns} single-write txns, keyspace {keyspace} ==");
    println!(
        "total wall:      {:>10.3} ms ({:.0} us/txn)",
        elapsed.as_secs_f64() * 1000.0,
        elapsed.as_secs_f64() * 1_000_000.0 / txns as f64
    );
    let timing_after = after.publication_timing;
    let timing_before = before.publication_timing;
    let phase = |name: &str, before: u64, after: u64| {
        let delta = after.saturating_sub(before);
        println!(
            "{name:<18} {:>10.3} ms ({:.1}% of wall)",
            delta as f64 / 1_000_000.0,
            if elapsed.as_nanos() > 0 {
                delta as f64 / elapsed.as_nanos() as f64 * 100.0
            } else {
                0.0
            }
        );
    };
    let phases = [
        (
            "candidate_prepare",
            timing_before.candidate_prepare_ns,
            timing_after.candidate_prepare_ns,
        ),
        (
            "wal_write",
            timing_before.wal_write_ns,
            timing_after.wal_write_ns,
        ),
        (
            "admission",
            timing_before.admission_ns,
            timing_after.admission_ns,
        ),
        (
            "data_flush",
            timing_before.data_flush_ns,
            timing_after.data_flush_ns,
        ),
        (
            "metadata_write",
            timing_before.metadata_write_ns,
            timing_after.metadata_write_ns,
        ),
        (
            "blob_write",
            timing_before.blob_write_ns,
            timing_after.blob_write_ns,
        ),
        (
            "history_write",
            timing_before.history_write_ns,
            timing_after.history_write_ns,
        ),
        (
            "directory_sync",
            timing_before.directory_sync_ns,
            timing_after.directory_sync_ns,
        ),
        (
            "manifest_write",
            timing_before.manifest_write_ns,
            timing_after.manifest_write_ns,
        ),
        (
            "manifest_mirror",
            timing_before.manifest_mirror_ns,
            timing_after.manifest_mirror_ns,
        ),
        ("cleanup", timing_before.cleanup_ns, timing_after.cleanup_ns),
    ];
    let recorded_total: u64 = phases
        .iter()
        .map(|(_, before, after)| after.saturating_sub(*before))
        .sum();
    for (name, before, after) in phases {
        phase(name, before, after);
    }
    println!(
        "recorded phases:  {:>10.3} ms",
        recorded_total as f64 / 1_000_000.0
    );
    println!(
        "unattributed gap (version-store sync + queue + locks): {:>10.3} ms",
        elapsed.as_nanos().saturating_sub(recorded_total as u128) as f64 / 1_000_000.0
    );
    println!(
        "data-device syncs: {} (version-store syncs are not in this count)",
        after.storage.syncs.saturating_sub(before.storage.syncs)
    );
    let physical_writes = after
        .storage
        .physical_page_writes
        .saturating_sub(before.storage.physical_page_writes);
    let page_bytes = after
        .storage
        .page_bytes_written
        .saturating_sub(before.storage.page_bytes_written);
    let gen_flushes = after
        .storage
        .generation_flushes
        .saturating_sub(before.storage.generation_flushes);
    let logical_reads = after
        .storage
        .logical_page_reads
        .saturating_sub(before.storage.logical_page_reads);
    let physical_reads = after
        .storage
        .physical_page_reads
        .saturating_sub(before.storage.physical_page_reads);
    println!(
        "physical page writes: {physical_writes} ({:.2} pages/txn)",
        physical_writes as f64 / txns as f64
    );
    println!(
        "page bytes written:  {page_bytes} ({:.1} KiB/txn)",
        page_bytes as f64 / 1024.0 / txns as f64
    );
    println!("generation flushes:  {gen_flushes}");
    println!(
        "logical page reads:  {logical_reads} ({:.2}/txn)",
        logical_reads as f64 / txns as f64
    );
    println!(
        "physical page reads: {physical_reads} ({:.2}/txn)",
        physical_reads as f64 / txns as f64
    );
}

fn key(index: usize) -> Vec<u8> {
    format!("wave-key-{index:06}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    format!("payload-{index:06}").into_bytes()
}
