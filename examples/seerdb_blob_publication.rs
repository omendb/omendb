//! Measure durable publication cost for blob-heavy workloads.
//!
//! This is a bounded qualification harness, not a throughput benchmark. It
//! makes the current whole-image blob publication cost visible while checking
//! replacement, reopen, and retained-root behavior. The result is useful for
//! deciding whether the opt-in immutable-segment/catalog design is justified
//! for a workload.

#![allow(clippy::disallowed_methods)]

use seerdb::{BatchMutation, BlobStorageMode, DB, Options, PublicationMetrics};
use serde_json::json;
use std::env;
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

const DEFAULT_RECORDS: usize = 64;
const DEFAULT_VALUE_BYTES: usize = 8 * 1024;
const DEFAULT_ROUNDS: usize = 4;
const DEFAULT_BATCH: usize = 1;

#[derive(Debug)]
struct Args {
    records: usize,
    value_bytes: usize,
    rounds: usize,
    batch: usize,
    segmented: bool,
    output: Option<PathBuf>,
}

fn parse_usize(value: &str, name: &str) -> Result<usize, Box<dyn Error>> {
    let parsed = value.parse::<usize>()?;
    if parsed == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

fn parse_args() -> Result<Args, Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let mut parsed = Args {
        records: DEFAULT_RECORDS,
        value_bytes: DEFAULT_VALUE_BYTES,
        rounds: DEFAULT_ROUNDS,
        batch: DEFAULT_BATCH,
        segmented: false,
        output: None,
    };
    while let Some(flag) = args.next() {
        if flag == "--segmented" {
            parsed.segmented = true;
            continue;
        }
        let value = args
            .next()
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag.as_str() {
            "--records" => parsed.records = parse_usize(&value, "--records")?,
            "--value-bytes" => parsed.value_bytes = parse_usize(&value, "--value-bytes")?,
            "--rounds" => parsed.rounds = parse_usize(&value, "--rounds")?,
            "--batch" => parsed.batch = parse_usize(&value, "--batch")?,
            "--output" => parsed.output = Some(PathBuf::from(value)),
            "--help" | "-h" => {
                println!(
                    "usage: seerdb_blob_publication [--records N] [--value-bytes N] [--rounds N] [--batch N] [--segmented] [--output PATH]"
                );
                std::process::exit(0);
            }
            _ => return Err(format!("unknown argument {flag}").into()),
        }
    }
    Ok(parsed)
}

fn key(index: usize) -> Vec<u8> {
    format!("blob-qualification-key-{index:08}").into_bytes()
}

fn value(index: usize, round: usize, length: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; length];
    let marker = format!("value-{index:08}-round-{round:08}").into_bytes();
    let copy_len = marker.len().min(bytes.len());
    bytes[..copy_len].copy_from_slice(&marker[..copy_len]);
    bytes
}

fn delta(before: PublicationMetrics, after: PublicationMetrics) -> PublicationMetrics {
    PublicationMetrics {
        metadata_bytes_written: after
            .metadata_bytes_written
            .saturating_sub(before.metadata_bytes_written),
        blob_bytes_written: after
            .blob_bytes_written
            .saturating_sub(before.blob_bytes_written),
        history_bytes_written: after
            .history_bytes_written
            .saturating_sub(before.history_bytes_written),
        manifest_bytes_written: after
            .manifest_bytes_written
            .saturating_sub(before.manifest_bytes_written),
    }
}

fn add(total: &mut PublicationMetrics, increment: PublicationMetrics) {
    total.metadata_bytes_written = total
        .metadata_bytes_written
        .saturating_add(increment.metadata_bytes_written);
    total.blob_bytes_written = total
        .blob_bytes_written
        .saturating_add(increment.blob_bytes_written);
    total.history_bytes_written = total
        .history_bytes_written
        .saturating_add(increment.history_bytes_written);
    total.manifest_bytes_written = total
        .manifest_bytes_written
        .saturating_add(increment.manifest_bytes_written);
}

fn json_publication(metrics: PublicationMetrics) -> serde_json::Value {
    json!({
        "metadata_bytes_written": metrics.metadata_bytes_written,
        "blob_bytes_written": metrics.blob_bytes_written,
        "history_bytes_written": metrics.history_bytes_written,
        "manifest_bytes_written": metrics.manifest_bytes_written,
    })
}

fn segment_bytes(path: &std::path::Path) -> Result<u64, Box<dyn Error>> {
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry
                .file_name()
                .to_string_lossy()
                .starts_with("seerdb.blob.segment.")
        {
            total = total.saturating_add(entry.metadata()?.len());
        }
    }
    Ok(total)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;
    let directory = tempdir()?;
    let path = directory.path().join("blob-qualification.db");
    let options = Options {
        blob_storage: if args.segmented {
            BlobStorageMode::Segmented
        } else {
            BlobStorageMode::WholeImage
        },
        ..Options::default()
    };
    let mut db = DB::open(&path, options)?;
    let mut publication = PublicationMetrics::default();
    let mut round_reports = Vec::with_capacity(args.rounds);

    for round in 0..args.rounds {
        let before = db.metrics()?.publication;
        for start in (0..args.records).step_by(args.batch) {
            let end = (start + args.batch).min(args.records);
            let mutations = (start..end)
                .map(|index| BatchMutation::Put {
                    key: key(index),
                    value: value(index, round, args.value_bytes),
                })
                .collect::<Vec<_>>();
            db.commit_batch(&mutations)?;
        }
        let after = db.metrics()?.publication;
        let report = delta(before, after);
        add(&mut publication, report);
        round_reports.push(json!({
            "round": round,
            "publication": json_publication(report),
            "blob_file_bytes": db.metrics()?.blob_bytes,
        }));
    }

    let final_commit = db.durability_status().commit_id;
    let snapshot = db.retain_commit(final_commit)?;
    let expected = value(0, args.rounds - 1, args.value_bytes);
    if db.get(&key(0))?.as_deref() != Some(expected.as_slice())
        || db.get_at(snapshot, &key(0))?.as_deref() != Some(expected.as_slice())
    {
        return Err("active or retained blob value diverged before reopen".into());
    }
    db.close()?;
    drop(db);

    let mut reopened = DB::open(&path, Options::default())?;
    if reopened.get(&key(0))?.as_deref() != Some(expected.as_slice())
        || reopened.get_at(snapshot, &key(0))?.as_deref() != Some(expected.as_slice())
    {
        return Err("active or retained blob value diverged after reopen".into());
    }
    reopened.verify()?;
    let report = json!({
        "schema": "seerdb-blob-publication-v1",
        "config": {
            "records": args.records,
            "value_bytes": args.value_bytes,
            "rounds": args.rounds,
            "batch": args.batch,
            "segmented": args.segmented,
        },
        "publication": json_publication(publication),
        "rounds": round_reports,
        "final_blob_file_bytes": fs::metadata(path.join("seerdb.blob"))?.len(),
        "final_blob_segment_bytes": segment_bytes(&path)?,
        "final_commit": final_commit.get(),
        "retained_snapshot": snapshot.get(),
    });
    let encoded = serde_json::to_string_pretty(&report)?;
    if let Some(output) = args.output {
        fs::write(output, &encoded)?;
    }
    println!("{encoded}");
    Ok(())
}
