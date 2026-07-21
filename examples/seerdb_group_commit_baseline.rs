//! Compare one-operation commits with the existing root-bound batch API.
//!
//! This is a measurement harness, not a new durability mode. Every run
//! reopens, verifies, and hashes the same final key/value state so grouping
//! can be evaluated without weakening the publication contract.

#![allow(clippy::disallowed_methods)]

use seerdb::{BatchMutation, DB, Options};
use serde_json::{Value, json};
use std::env;
use std::error::Error;
use std::time::Instant;
use tempfile::tempdir;

const DEFAULT_OPERATIONS: usize = 256;

#[derive(Debug)]
struct RunReport {
    label: String,
    operations: usize,
    commits: usize,
    elapsed_ns: u128,
    logical_bytes: u64,
    page_bytes_written: u64,
    physical_page_writes: u64,
    syncs: u64,
    digest: u64,
}

fn key(index: usize) -> Vec<u8> {
    format!("group-key-{index:08}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    format!("group-value-{index:08}-revision-0001").into_bytes()
}

fn parse_operations() -> Result<usize, Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let mut operations = DEFAULT_OPERATIONS;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--operations" => {
                operations = args
                    .next()
                    .ok_or("--operations requires a value")?
                    .parse()?;
            }
            "--help" | "-h" => {
                println!("usage: seerdb_group_commit_baseline [--operations N]");
                std::process::exit(0);
            }
            _ => return Err(format!("unknown argument {flag}").into()),
        }
    }
    if operations == 0 {
        return Err("--operations must be positive".into());
    }
    Ok(operations)
}

fn digest(db: &DB) -> seerdb::Result<u64> {
    let rows = db.range(&[], &[0xff; 64])?;
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for (key, value) in rows {
        for byte in key.iter().chain(value.iter()) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x1000_0000_01b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    Ok(hash)
}

fn run(
    label: &str,
    operations: usize,
    group_size: Option<usize>,
) -> Result<RunReport, Box<dyn Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("db");
    let mut db = DB::create(&path, Options::default())?;
    let before = db.metrics()?.storage;
    let started = Instant::now();
    let mut commits = 0;

    match group_size {
        None => {
            for index in 0..operations {
                db.commit_batch(&[BatchMutation::Put {
                    key: key(index),
                    value: value(index),
                }])?;
                commits += 1;
            }
        }
        Some(group_size) => {
            for start in (0..operations).step_by(group_size) {
                let end = (start + group_size).min(operations);
                let mut transaction = db.begin_batch_transaction()?;
                for index in start..end {
                    transaction.put(&key(index), &value(index))?;
                }
                transaction.commit(&mut db)?;
                commits += 1;
            }
        }
    }

    let elapsed_ns = started.elapsed().as_nanos();
    db.verify()?;
    let digest_before_reopen = digest(&db)?;
    let after = db.metrics()?.storage;
    let logical_bytes = (0..operations)
        .map(|index| (key(index).len() + value(index).len()) as u64)
        .sum();
    db.close()?;
    drop(db);

    let mut reopened = DB::open(&path, Options::default())?;
    reopened.verify()?;
    let digest_after_reopen = digest(&reopened)?;
    if digest_before_reopen != digest_after_reopen {
        return Err(format!(
            "{label} digest changed across reopen: {digest_before_reopen:016x} != {digest_after_reopen:016x}"
        )
        .into());
    }
    reopened.close()?;
    drop(reopened);
    drop(directory);

    Ok(RunReport {
        label: label.to_owned(),
        operations,
        commits,
        elapsed_ns,
        logical_bytes,
        page_bytes_written: after
            .page_bytes_written
            .saturating_sub(before.page_bytes_written),
        physical_page_writes: after
            .physical_page_writes
            .saturating_sub(before.physical_page_writes),
        syncs: after.syncs.saturating_sub(before.syncs),
        digest: digest_after_reopen,
    })
}

fn report_value(report: &RunReport) -> Value {
    json!({
        "label": report.label,
        "operations": report.operations,
        "commits": report.commits,
        "elapsed_ms": report.elapsed_ns as f64 / 1_000_000.0,
        "logical_bytes": report.logical_bytes,
        "page_bytes_written": report.page_bytes_written,
        "physical_page_writes": report.physical_page_writes,
        "syncs": report.syncs,
        "page_write_amplification": report.page_bytes_written as f64 / report.logical_bytes.max(1) as f64,
        "digest": format!("{:016x}", report.digest),
    })
}

fn main() -> Result<(), Box<dyn Error>> {
    let operations = parse_operations()?;
    let reports = [
        run("individual", operations, None)?,
        run("group-16", operations, Some(16))?,
        run("group-all", operations, Some(operations))?,
    ];
    let digest = reports[0].digest;
    if reports.iter().any(|report| report.digest != digest) {
        return Err("grouped runs produced different final state digests".into());
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema": "seerdb-group-commit-baseline-v1",
            "operations": operations,
            "runs": reports.iter().map(report_value).collect::<Vec<_>>(),
        }))?
    );
    Ok(())
}
