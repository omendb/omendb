//! Attribute one durable publication's wall-clock cost and page write
//! amplification across the recorded publication phases.
//!
//! This is a measurement harness for the publication-weight-reduction
//! contract (`ai/design/publication_performance_2026-08-21.md`). It runs the
//! common-KV batch-put shape (128 keys, 128-byte values, durable batch
//! commits), diffs the cumulative publication counters and phase timings
//! around every commit, and reports per-phase attribution plus reopen cost.
//! It records evidence; it is not a durability or performance claim.

#![allow(clippy::disallowed_methods)]

use seerdb::{BatchMutation, DB, Options};
use serde_json::json;
use std::env;
use std::error::Error;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_KEYS: usize = 128;
const DEFAULT_OPERATIONS: usize = 256;
const DEFAULT_BATCH_SIZE: usize = 16;
const DEFAULT_VALUE_BYTES: usize = 128;

#[derive(Default, Clone, Copy)]
struct PhaseDelta {
    elapsed_ns: u128,
    candidate_prepare_ns: u64,
    wal_write_ns: u64,
    admission_ns: u64,
    data_flush_ns: u64,
    metadata_write_ns: u64,
    blob_write_ns: u64,
    history_write_ns: u64,
    directory_sync_ns: u64,
    manifest_write_ns: u64,
    manifest_mirror_ns: u64,
    cleanup_ns: u64,
    wal_bytes_written: u64,
    metadata_bytes_written: u64,
    blob_bytes_written: u64,
    history_bytes_written: u64,
    manifest_bytes_written: u64,
    page_bytes_written: u64,
    physical_page_writes: u64,
    syncs: u64,
    durability_syncs: u64,
    logical_dirty_bytes: u64,
}

fn key(index: usize) -> Vec<u8> {
    format!("attrib-key-{index:06}").into_bytes()
}

fn value_bytes(size: usize) -> Vec<u8> {
    let pattern = b"0123456789abcdef";
    (0..size).map(|i| pattern[i % pattern.len()]).collect()
}

struct Args {
    keys: usize,
    operations: usize,
    batch_size: usize,
    value_bytes: usize,
    reopens: usize,
    path: Option<std::path::PathBuf>,
}

fn parse_args() -> Result<Args, Box<dyn Error>> {
    let mut args = Args {
        keys: DEFAULT_KEYS,
        operations: DEFAULT_OPERATIONS,
        batch_size: DEFAULT_BATCH_SIZE,
        value_bytes: DEFAULT_VALUE_BYTES,
        reopens: 3,
        path: None,
    };
    let mut argv = env::args().skip(1);
    let take =
        |argv: &mut dyn Iterator<Item = String>, name: &str| -> Result<usize, Box<dyn Error>> {
            argv.next()
                .ok_or_else(|| format!("{name} requires a value").into())
                .and_then(|v| v.parse().map_err(Into::into))
        };
    while let Some(flag) = argv.next() {
        match flag.as_str() {
            "--keys" => args.keys = take(&mut argv, "--keys")?,
            "--operations" => args.operations = take(&mut argv, "--operations")?,
            "--batch-size" => args.batch_size = take(&mut argv, "--batch-size")?,
            "--value-bytes" => args.value_bytes = take(&mut argv, "--value-bytes")?,
            "--reopens" => args.reopens = take(&mut argv, "--reopens")?,
            "--path" => {
                args.path = Some(
                    argv.next()
                        .ok_or_else(|| "--path requires a value".to_string())?
                        .into(),
                )
            }
            "--help" | "-h" => {
                println!(
                    "usage: seerdb_publication_attribution [--keys N] [--operations N] [--batch-size N] [--value-bytes N] [--reopens N] [--path DIR]"
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument {other}").into()),
        }
    }
    if args.keys == 0 || args.operations == 0 || args.batch_size == 0 || args.value_bytes == 0 {
        return Err("all counts must be positive".into());
    }
    Ok(args)
}

fn durability_syncs(db: &DB) -> u64 {
    db.durability_sync_count()
}

fn snapshot(
    db: &DB,
) -> seerdb::Result<(
    seerdb::StorageMetrics,
    seerdb::PublicationMetrics,
    seerdb::PublicationTimingMetrics,
)> {
    let metrics = db.metrics()?;
    Ok((
        metrics.storage,
        metrics.publication,
        metrics.publication_timing,
    ))
}

type PhaseField = fn(&PhaseDelta) -> u64;

#[allow(clippy::type_complexity)]
fn phase_names() -> [(&'static str, PhaseField); 11] {
    [
        ("candidate_prepare", |d| d.candidate_prepare_ns),
        ("wal_write", |d| d.wal_write_ns),
        ("admission", |d| d.admission_ns),
        ("manifest_mirror", |d| d.manifest_mirror_ns),
        ("data_flush", |d| d.data_flush_ns),
        ("metadata_write", |d| d.metadata_write_ns),
        ("blob_write", |d| d.blob_write_ns),
        ("history_write", |d| d.history_write_ns),
        ("directory_sync", |d| d.directory_sync_ns),
        ("manifest_write", |d| d.manifest_write_ns),
        ("cleanup", |d| d.cleanup_ns),
    ]
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;
    // Default to a temp dir, but allow pinning the database to a caller-
    // chosen filesystem: on Linux /tmp is frequently tmpfs, where syncs are
    // no-ops and fallocate zeroes pages, so attribution there does not
    // reflect device-backed publication cost.
    let _tempdir;
    let path = match &args.path {
        Some(dir) => {
            std::fs::create_dir_all(dir)?;
            dir.join("db")
        }
        None => {
            _tempdir = tempfile::tempdir()?;
            _tempdir.path().join("db")
        }
    };
    let value = value_bytes(args.value_bytes);

    let mut db = DB::create(&path, Options::default())?;

    // Bootstrap one generation so every measured publication is a delta
    // publication rather than first-root creation.
    db.commit_batch(
        &(0..args.keys)
            .map(|index| BatchMutation::Put {
                key: key(index),
                value: value.clone(),
            })
            .collect::<Vec<_>>(),
    )?;

    let mut deltas = Vec::new();
    let mut index = 0usize;
    while index < args.operations {
        let end = (index + args.batch_size).min(args.operations);
        let mutations: Vec<BatchMutation> = (index..end)
            .map(|i| {
                // Golden-ratio scatter so batch members spread across the key
                // space instead of clustering in one leaf neighborhood.
                let key_index =
                    ((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) % args.keys as u64) as usize;
                BatchMutation::Put {
                    key: key(key_index),
                    value: value.clone(),
                }
            })
            .collect();
        let logical_dirty: u64 = mutations
            .iter()
            .map(|m| match m {
                BatchMutation::Put { key, value } => (key.len() + value.len()) as u64,
                BatchMutation::Delete { key } => key.len() as u64,
            })
            .sum();

        let (storage_before, publication_before, timing_before) = snapshot(&db)?;
        let durability_syncs_before = durability_syncs(&db);
        let started = Instant::now();
        db.commit_batch(&mutations)?;
        let elapsed_ns = started.elapsed().as_nanos();
        let (storage_after, publication_after, timing_after) = snapshot(&db)?;
        let durability_syncs_after = durability_syncs(&db);

        deltas.push(PhaseDelta {
            elapsed_ns,
            candidate_prepare_ns: timing_after
                .candidate_prepare_ns
                .saturating_sub(timing_before.candidate_prepare_ns),
            wal_write_ns: timing_after
                .wal_write_ns
                .saturating_sub(timing_before.wal_write_ns),
            admission_ns: timing_after
                .admission_ns
                .saturating_sub(timing_before.admission_ns),
            data_flush_ns: timing_after
                .data_flush_ns
                .saturating_sub(timing_before.data_flush_ns),
            metadata_write_ns: timing_after
                .metadata_write_ns
                .saturating_sub(timing_before.metadata_write_ns),
            blob_write_ns: timing_after
                .blob_write_ns
                .saturating_sub(timing_before.blob_write_ns),
            history_write_ns: timing_after
                .history_write_ns
                .saturating_sub(timing_before.history_write_ns),
            directory_sync_ns: timing_after
                .directory_sync_ns
                .saturating_sub(timing_before.directory_sync_ns),
            manifest_write_ns: timing_after
                .manifest_write_ns
                .saturating_sub(timing_before.manifest_write_ns),
            manifest_mirror_ns: timing_after
                .manifest_mirror_ns
                .saturating_sub(timing_before.manifest_mirror_ns),
            cleanup_ns: timing_after
                .cleanup_ns
                .saturating_sub(timing_before.cleanup_ns),
            wal_bytes_written: publication_after
                .wal_bytes_written
                .saturating_sub(publication_before.wal_bytes_written),
            metadata_bytes_written: publication_after
                .metadata_bytes_written
                .saturating_sub(publication_before.metadata_bytes_written),
            blob_bytes_written: publication_after
                .blob_bytes_written
                .saturating_sub(publication_before.blob_bytes_written),
            history_bytes_written: publication_after
                .history_bytes_written
                .saturating_sub(publication_before.history_bytes_written),
            manifest_bytes_written: publication_after
                .manifest_bytes_written
                .saturating_sub(publication_before.manifest_bytes_written),
            page_bytes_written: storage_after
                .page_bytes_written
                .saturating_sub(storage_before.page_bytes_written),
            physical_page_writes: storage_after
                .physical_page_writes
                .saturating_sub(storage_before.physical_page_writes),
            syncs: storage_after.syncs.saturating_sub(storage_before.syncs),
            durability_syncs: durability_syncs_after.saturating_sub(durability_syncs_before),
            logical_dirty_bytes: logical_dirty,
        });
        index = end;
    }

    db.verify()?;
    db.close()?;
    drop(db);

    let mut reopen_ns_samples: Vec<u128> = Vec::new();
    for _ in 0..args.reopens {
        let started = Instant::now();
        let mut reopened = DB::open(&path, Options::default())?;
        reopen_ns_samples.push(started.elapsed().as_nanos());
        reopened.verify()?;
        reopened.close()?;
        drop(reopened);
    }
    reopen_ns_samples.sort_unstable();

    let publications = deltas.len() as u64;
    let total_ns: u128 = deltas.iter().map(|d| d.elapsed_ns).sum();
    let mut phase_totals: Vec<(String, u64)> = phase_names()
        .into_iter()
        .map(|(name, field)| (name.to_string(), deltas.iter().map(field).sum()))
        .collect();
    phase_totals.sort_by_key(|(_, ns)| std::cmp::Reverse(*ns));
    let phase_sum_ns: u64 = phase_totals.iter().map(|(_, ns)| ns).sum();

    let total_page_bytes: u64 = deltas.iter().map(|d| d.page_bytes_written).sum();
    let total_logical_dirty: u64 = deltas.iter().map(|d| d.logical_dirty_bytes).sum();
    let total_page_writes: u64 = deltas.iter().map(|d| d.physical_page_writes).sum();
    let total_syncs: u64 = deltas.iter().map(|d| d.syncs).sum();
    let total_durability_syncs: u64 = deltas.iter().map(|d| d.durability_syncs).sum();
    let total_wal_bytes: u64 = deltas.iter().map(|d| d.wal_bytes_written).sum();
    let total_metadata_bytes: u64 = deltas.iter().map(|d| d.metadata_bytes_written).sum();
    let total_history_bytes: u64 = deltas.iter().map(|d| d.history_bytes_written).sum();
    let total_manifest_bytes: u64 = deltas.iter().map(|d| d.manifest_bytes_written).sum();
    let total_blob_bytes: u64 = deltas.iter().map(|d| d.blob_bytes_written).sum();

    let mut elapsed_sorted: Vec<u128> = deltas.iter().map(|d| d.elapsed_ns).collect();
    elapsed_sorted.sort_unstable();
    let median = |samples: &Vec<u128>| samples[samples.len() / 2];
    let page_amp = total_page_bytes as f64 / total_logical_dirty.max(1) as f64;
    let pages_per_publication = total_page_writes as f64 / publications as f64;

    let report = json!({
        "schema": "seerdb-publication-attribution-v1",
        "evidence_class": "publication_phase_attribution_diagnostic",
        "hardware_benchmark": false,
        "recorded_at": SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        "workload": {
            "keys": args.keys,
            "operations": args.operations,
            "batch_size": args.batch_size,
            "value_bytes": args.value_bytes,
            "publications": publications,
            "durability": "durable",
        },
        "publication_cost_ns": {
            "total": total_ns,
            "median": median(&elapsed_sorted),
            "p90": elapsed_sorted[(elapsed_sorted.len() as f64 * 0.9) as usize % elapsed_sorted.len()],
            "mean": total_ns / publications as u128,
        },
        "phase_attribution": {
            "sum_ns": phase_sum_ns,
            "phases": phase_totals
                .iter()
                .map(|(name, ns)| json!({
                    "phase": name,
                    "total_ns": ns,
                    "share_of_phases": *ns as f64 / phase_sum_ns.max(1) as f64,
                    "per_publication_ns": ns / publications,
                }))
                .collect::<Vec<_>>(),
        },
        "write_volume_per_publication": {
            "page_bytes": total_page_bytes / publications,
            "page_writes": total_page_writes as f64 / publications as f64,
            "wal_bytes": total_wal_bytes / publications,
            "metadata_bytes": total_metadata_bytes / publications,
            "blob_bytes": total_blob_bytes / publications,
            "history_bytes": total_history_bytes / publications,
            "manifest_bytes": total_manifest_bytes / publications,
            "data_device_syncs": total_syncs as f64 / publications as f64,
            "durability_syncs": total_durability_syncs as f64 / publications as f64,
        },
        "write_amplification": {
            "page_bytes_per_logical_dirty_byte": page_amp,
            "pages_written_per_publication": pages_per_publication,
        },
        "reopen_ns_median": median(&reopen_ns_samples),
        "per_publication_samples_ns": deltas.iter().map(|d| d.elapsed_ns).collect::<Vec<_>>(),
        "per_publication_page_writes": deltas.iter().map(|d| d.physical_page_writes).collect::<Vec<_>>(),
        "per_publication_logical_dirty_bytes": deltas.iter().map(|d| d.logical_dirty_bytes).collect::<Vec<_>>(),
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
