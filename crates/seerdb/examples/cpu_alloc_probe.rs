//! Attribute single-client commit cost across CPU, allocations, WAL bytes,
//! and durability barriers.
//!
//! This is the profiling half of the performance gate in
//! `docs/alpha-release-gates.md`: before an optimization is accepted, a
//! reproducible profile must identify the measured bottleneck. The probe
//! wraps a counting global allocator (allocation count and bytes per run),
//! samples `getrusage` CPU time, and diffs SeerDB's cumulative
//! publication-phase timings and byte counters over the same TPC-B-shaped
//! single-write transaction stream `wave_cost_probe` uses.
//!
//! Run with:
//!   cargo run --release -p seerdb --example cpu_alloc_probe -- [txns] [keyspace] [sync-class]
//!
//! The allocation counters are process-global: the diff window starts
//! after seeding, so the reported figures cover only the measured run.

#![allow(clippy::disallowed_methods, unsafe_code)]

use seerdb::{Options, TransactionDatabase};
use std::alloc::{GlobalAlloc, Layout, System};
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[cfg(unix)]
fn cpu_time_ns() -> (u128, u128) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `getrusage` writes the complete `rusage` structure for the
    // requested process and the pointer refers to valid writable storage.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return (0, 0);
    }
    // SAFETY: A successful `getrusage` call initialized every field.
    let usage = unsafe { usage.assume_init() };
    let to_ns = |value: libc::timeval| {
        (value.tv_sec as u128)
            .saturating_mul(1_000_000_000)
            .saturating_add((value.tv_usec as u128).saturating_mul(1_000))
    };
    (to_ns(usage.ru_utime), to_ns(usage.ru_stime))
}

#[cfg(not(unix))]
fn cpu_time_ns() -> (u128, u128) {
    (0, 0)
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:08}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    format!("value-{index:08}").into_bytes()
}

fn main() {
    let txns: usize = env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(200);
    let keyspace: usize = env::args()
        .nth(2)
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000);
    let sync_class = match env::args().nth(3).as_deref() {
        Some("device") => seerdb::db::SyncClass::DeviceBarrier,
        Some("kernel") => seerdb::db::SyncClass::KernelBarrier,
        _ => seerdb::db::SyncClass::DeviceBarrier,
    };

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
    {
        let mut transaction = database.begin().expect("begin seed");
        for index in 0..keyspace {
            transaction
                .put(tree, &key(index), &value(index))
                .expect("seed put");
        }
        transaction.commit().expect("seed commit");
    }

    let before = database.metrics().expect("metrics before");
    let (user_before, system_before) = cpu_time_ns();
    let allocations_before = ALLOC_COUNT.load(Ordering::Relaxed);
    let allocation_bytes_before = ALLOC_BYTES.load(Ordering::Relaxed);
    let started = Instant::now();

    // TPC-B shape at the engine tier: read one key, write another, one
    // transaction per iteration (exactly what wave_cost_probe drives).
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
    let (user_after, system_after) = cpu_time_ns();
    let allocations = ALLOC_COUNT.load(Ordering::Relaxed) - allocations_before;
    let allocation_bytes = ALLOC_BYTES.load(Ordering::Relaxed) - allocation_bytes_before;

    println!(
        "== cpu/alloc profile: {txns} single-write txns, keyspace {keyspace}, {sync_class:?} =="
    );
    println!(
        "wall:                 {:>10.3} ms ({:.1} us/txn)",
        elapsed.as_secs_f64() * 1000.0,
        elapsed.as_secs_f64() * 1_000_000.0 / txns as f64,
    );
    println!(
        "cpu user+system:      {:>10.3} ms ({:.1}% of wall)",
        (user_after - user_before + system_after - system_before) as f64 / 1_000_000.0,
        ((user_after - user_before) + (system_after - system_before)) as f64
            / elapsed.as_nanos() as f64
            * 100.0,
    );
    println!(
        "allocations:          {:>10} ({:.0}/txn, {:.0} bytes/txn)",
        allocations,
        allocations as f64 / txns as f64,
        allocation_bytes as f64 / txns as f64,
    );

    let timing_before = before.publication_timing;
    let timing_after = after.publication_timing;
    let phase = |name: &str, before: u64, after: u64| {
        let delta = after.saturating_sub(before);
        println!(
            "{name:<18}    {:>10.3} ms ({:.1}% of wall)",
            delta as f64 / 1_000_000.0,
            if elapsed.as_nanos() > 0 {
                delta as f64 / elapsed.as_nanos() as f64 * 100.0
            } else {
                0.0
            },
        );
    };
    phase(
        "candidate_prepare",
        timing_before.candidate_prepare_ns,
        timing_after.candidate_prepare_ns,
    );
    phase(
        "wal_write",
        timing_before.wal_write_ns,
        timing_after.wal_write_ns,
    );
    phase(
        "admission",
        timing_before.admission_ns,
        timing_after.admission_ns,
    );
    phase(
        "data_flush",
        timing_before.data_flush_ns,
        timing_after.data_flush_ns,
    );
    phase(
        "metadata_write",
        timing_before.metadata_write_ns,
        timing_after.metadata_write_ns,
    );
    phase(
        "history_write",
        timing_before.history_write_ns,
        timing_after.history_write_ns,
    );
    phase(
        "directory_sync",
        timing_before.directory_sync_ns,
        timing_after.directory_sync_ns,
    );
    phase(
        "manifest_write",
        timing_before.manifest_write_ns,
        timing_after.manifest_write_ns,
    );
    phase(
        "manifest_mirror",
        timing_before.manifest_mirror_ns,
        timing_after.manifest_mirror_ns,
    );
    phase("cleanup", timing_before.cleanup_ns, timing_after.cleanup_ns);

    let counters = |name: &str, before: u64, after: u64| {
        println!("{name:<24} {:>10} bytes", after.saturating_sub(before));
    };
    counters(
        "wal_bytes",
        before.publication.wal_bytes_written,
        after.publication.wal_bytes_written,
    );
    counters(
        "metadata_bytes",
        before.publication.metadata_bytes_written,
        after.publication.metadata_bytes_written,
    );
    counters(
        "history_bytes",
        before.publication.history_bytes_written,
        after.publication.history_bytes_written,
    );
    counters(
        "manifest_bytes",
        before.publication.manifest_bytes_written,
        after.publication.manifest_bytes_written,
    );
    database.close().expect("close");
}
