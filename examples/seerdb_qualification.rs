//! Run a deterministic, portable SeerDB qualification workload.
//!
//! This is an evidence harness rather than a performance claim. It exercises
//! the durable byte API, retained-root reads, bounded maintenance, close and
//! reopen, offline checking, and verification while recording latency
//! quantiles and physical counters in a machine-readable result.

#![allow(clippy::disallowed_methods)]

use seerdb::{BatchMutation, DB, Options};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::env;
use std::error::Error as StdError;
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::path::PathBuf;
use std::time::Instant;
use tempfile::tempdir;

type AnyResult<T> = Result<T, Box<dyn StdError>>;
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

const DEFAULT_KEYS: usize = 128;
const DEFAULT_OPERATIONS: usize = 512;
const DEFAULT_SEED: u64 = 0x005E_EDDB_2026;

#[derive(Debug)]
struct Args {
    keys: usize,
    operations: usize,
    seed: u64,
    db: Option<PathBuf>,
    output: Option<PathBuf>,
}

fn invalid(message: impl Into<String>) -> Box<dyn StdError> {
    Box::new(IoError::new(ErrorKind::InvalidInput, message.into()))
}

fn parse_usize(value: &str, flag: &str) -> AnyResult<usize> {
    value
        .parse()
        .map_err(|_| invalid(format!("{flag} must be a positive integer")))
}

fn parse_u64(value: &str, flag: &str) -> AnyResult<u64> {
    value
        .parse()
        .map_err(|_| invalid(format!("{flag} must be an unsigned integer")))
}

fn parse_args() -> AnyResult<Args> {
    let mut args = env::args().skip(1);
    let mut parsed = Args {
        keys: DEFAULT_KEYS,
        operations: DEFAULT_OPERATIONS,
        seed: DEFAULT_SEED,
        db: None,
        output: None,
    };

    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| invalid(format!("{flag} requires a value")))?;
        match flag.as_str() {
            "--keys" => parsed.keys = parse_usize(&value, &flag)?,
            "--operations" => parsed.operations = parse_usize(&value, &flag)?,
            "--seed" => parsed.seed = parse_u64(&value, &flag)?,
            "--db" => parsed.db = Some(PathBuf::from(value)),
            "--output" => parsed.output = Some(PathBuf::from(value)),
            "--help" | "-h" => {
                println!(
                    "usage: seerdb_qualification [--keys N] [--operations N] [--seed N] [--db PATH] [--output PATH]"
                );
                std::process::exit(0);
            }
            _ => return Err(invalid(format!("unknown argument {flag}"))),
        }
    }

    if parsed.keys == 0 || parsed.operations == 0 {
        return Err(invalid("--keys and --operations must be positive"));
    }
    Ok(parsed)
}

fn next_random(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

fn workload_key(index: usize) -> Vec<u8> {
    format!("qualification-key-{index:08}").into_bytes()
}

fn workload_value(index: usize, revision: usize) -> Vec<u8> {
    format!("qualification-value-{index:08}-revision-{revision:08}").into_bytes()
}

fn digest(model: &Model) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for (key, value) in model {
        for byte in key.iter().chain(value) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x1000_0000_01b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    format!("{hash:016x}")
}

fn quantiles(samples: &[u128]) -> Value {
    if samples.is_empty() {
        return json!({"count": 0});
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let percentile = |percent: usize| sorted[(sorted.len() - 1) * percent / 100];
    json!({
        "count": sorted.len(),
        "p50_ns": percentile(50),
        "p95_ns": percentile(95),
        "p99_ns": percentile(99),
        "max_ns": sorted[sorted.len() - 1],
    })
}

fn model_range(model: &Model, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    model
        .range(start.to_vec()..end.to_vec())
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn check_range(db: &DB, model: &Model, start: &[u8], end: &[u8]) -> AnyResult<()> {
    let actual = db.range(start, end)?;
    let expected = model_range(model, start, end);
    if actual != expected {
        return Err(invalid("qualification range diverged from reference model"));
    }
    Ok(())
}

fn logical_bytes(model: &Model) -> u64 {
    model
        .iter()
        .map(|(key, value)| (key.len() + value.len()) as u64)
        .sum()
}

fn main() -> AnyResult<()> {
    let args = parse_args()?;
    let (temporary, path) = match args.db.clone() {
        Some(path) => (None, path),
        None => {
            let directory = tempdir()?;
            let path = directory.path().join("qualification.db");
            (Some(directory), path)
        }
    };

    let mut db = DB::create(&path, Options::default())?;
    let initial: Model = (0..args.keys)
        .map(|index| (workload_key(index), workload_value(index, 0)))
        .collect();
    let initial_batch: Vec<_> = initial
        .iter()
        .map(|(key, value)| BatchMutation::Put {
            key: key.clone(),
            value: value.clone(),
        })
        .collect();
    db.commit_batch(&initial_batch)?;
    let mut model = initial;
    let mut retained = Some(db.retain_current()?);
    let retained_model = model.clone();
    let mut state = args.seed;
    let mut revisions = vec![0usize; args.keys + args.operations];
    let mut commit_latency = Vec::new();
    let mut get_latency = Vec::new();
    let mut range_latency = Vec::new();
    let mut maintenance_latency = Vec::new();
    let mut logical_write_bytes = 0u64;
    let mut maintenance_runs = 0u64;
    let mut reopen_runs = 0u64;
    let initial_metrics = db.metrics()?.storage;
    let mut handle_page_bytes_written = initial_metrics.page_bytes_written;
    let mut handle_physical_page_writes = initial_metrics.physical_page_writes;
    let mut handle_syncs = initial_metrics.syncs;
    let mut total_page_bytes_written = 0u64;
    let mut total_physical_page_writes = 0u64;
    let mut total_syncs = 0u64;

    for operation in 0..args.operations {
        let random = next_random(&mut state);
        let key_index = (random as usize) % (args.keys + args.operations);
        let key = workload_key(key_index);
        match random % 100 {
            0..=44 => {
                let started = Instant::now();
                let actual = db.get(&key)?;
                get_latency.push(started.elapsed().as_nanos());
                if actual != model.get(&key).cloned() {
                    return Err(invalid("qualification point read diverged from model"));
                }
            }
            45..=69 => {
                revisions[key_index] = revisions[key_index].saturating_add(1);
                let value = workload_value(key_index, revisions[key_index]);
                let started = Instant::now();
                db.commit_batch(&[BatchMutation::Put {
                    key: key.clone(),
                    value: value.clone(),
                }])?;
                commit_latency.push(started.elapsed().as_nanos());
                logical_write_bytes =
                    logical_write_bytes.saturating_add((key.len() + value.len()) as u64);
                model.insert(key, value);
            }
            70..=84 => {
                let started = Instant::now();
                db.commit_batch(&[BatchMutation::Delete { key: key.clone() }])?;
                commit_latency.push(started.elapsed().as_nanos());
                logical_write_bytes = logical_write_bytes.saturating_add(key.len() as u64);
                model.remove(&key);
            }
            _ => {
                let start_index = key_index.min(args.keys + args.operations - 1);
                let end_index = (start_index + 16).min(args.keys + args.operations);
                let start = workload_key(start_index);
                let end = workload_key(end_index);
                let started = Instant::now();
                check_range(&db, &model, &start, &end)?;
                range_latency.push(started.elapsed().as_nanos());
            }
        }

        if operation == args.operations / 2 {
            let snapshot = retained
                .as_ref()
                .ok_or_else(|| invalid("retained snapshot disappeared"))?;
            for (key, value) in retained_model.iter().take(8) {
                if snapshot.get(key)?.as_deref() != Some(value.as_slice()) {
                    return Err(invalid("retained snapshot changed after later commits"));
                }
            }
        }

        if (operation + 1) % (args.operations / 4).max(1) == 0 {
            let started = Instant::now();
            db.compact_with_limit(2)?;
            db.verify()?;
            maintenance_latency.push(started.elapsed().as_nanos());
            maintenance_runs = maintenance_runs.saturating_add(1);
            let current_metrics = db.metrics()?.storage;
            total_page_bytes_written = total_page_bytes_written.saturating_add(
                current_metrics
                    .page_bytes_written
                    .saturating_sub(handle_page_bytes_written),
            );
            total_physical_page_writes = total_physical_page_writes.saturating_add(
                current_metrics
                    .physical_page_writes
                    .saturating_sub(handle_physical_page_writes),
            );
            total_syncs =
                total_syncs.saturating_add(current_metrics.syncs.saturating_sub(handle_syncs));
            db.close()?;
            db = DB::open(&path, Options::default())?;
            handle_page_bytes_written = 0;
            handle_physical_page_writes = 0;
            handle_syncs = 0;
            db.verify()?;
            check_range(&db, &model, &[], &[u8::MAX; 64])?;
            reopen_runs = reopen_runs.saturating_add(1);
        }
    }

    let current_digest = digest(&model);
    let mut retained = retained
        .take()
        .ok_or_else(|| invalid("retained snapshot was already released"))?;
    retained.verify()?;
    retained.release()?;

    let started = Instant::now();
    let vacuum = db.vacuum()?;
    db.prune_history()?;
    maintenance_latency.push(started.elapsed().as_nanos());
    maintenance_runs = maintenance_runs.saturating_add(1);
    db.verify()?;
    check_range(&db, &model, &[], &[u8::MAX; 64])?;
    let after_maintenance = db.metrics()?;
    total_page_bytes_written = total_page_bytes_written.saturating_add(
        after_maintenance
            .storage
            .page_bytes_written
            .saturating_sub(handle_page_bytes_written),
    );
    total_physical_page_writes = total_physical_page_writes.saturating_add(
        after_maintenance
            .storage
            .physical_page_writes
            .saturating_sub(handle_physical_page_writes),
    );
    total_syncs =
        total_syncs.saturating_add(after_maintenance.storage.syncs.saturating_sub(handle_syncs));
    let final_space_bytes = after_maintenance
        .data_bytes
        .saturating_add(after_maintenance.blob_bytes)
        .saturating_add(after_maintenance.wal_bytes);
    let logical_bytes = logical_bytes(&model);

    let check_before_close = DB::check(&path, Options::default())?;
    db.close()?;
    drop(db);
    let mut reopened = DB::open(&path, Options::default())?;
    let reopened_report = reopened.verify()?;
    check_range(&reopened, &model, &[], &[u8::MAX; 64])?;
    let reopened_digest = digest(&model);
    if reopened_digest != current_digest {
        return Err(invalid("reopen digest changed"));
    }
    let status = reopened.durability_status();
    reopened.close()?;
    drop(reopened);
    drop(temporary);

    let report = json!({
        "schema": "seerdb-qualification-v1",
        "config": {
            "keys": args.keys,
            "operations": args.operations,
            "seed": args.seed,
            "path": path,
        },
        "correctness": {
            "digest": current_digest,
            "reopened_digest": reopened_digest,
            "check_wal_status": format!("{:?}", check_before_close.wal_status),
            "verified_pages_after_reopen": reopened_report.verified_pages,
            "commit_id": status.commit_id.get(),
            "generation_id": status.generation_id.get(),
            "vacuum_live_entries": vacuum.live_entries,
        },
        "latency": {
            "commit": quantiles(&commit_latency),
            "get": quantiles(&get_latency),
            "range": quantiles(&range_latency),
            "maintenance": quantiles(&maintenance_latency),
        },
        "work": {
            "maintenance_runs": maintenance_runs,
            "reopen_runs_during_workload": reopen_runs,
            "logical_write_bytes": logical_write_bytes,
            "page_bytes_written_after_workload_start": total_page_bytes_written,
            "page_write_amplification": if logical_write_bytes == 0 {
                Value::Null
            } else {
                json!(total_page_bytes_written as f64 / logical_write_bytes as f64)
            },
            "logical_live_bytes": logical_bytes,
            "final_space_bytes": final_space_bytes,
            "space_amplification": if logical_bytes == 0 {
                Value::Null
            } else {
                json!(final_space_bytes as f64 / logical_bytes as f64)
            },
            "reclaimable_pages": after_maintenance.reclaimable_pages,
            "physical_page_reads": after_maintenance.storage.physical_page_reads,
            "physical_page_writes": total_physical_page_writes,
            "syncs": total_syncs,
        },
    });
    let serialized = serde_json::to_string_pretty(&report)?;
    if let Some(output) = args.output {
        fs::write(output, format!("{serialized}\n"))?;
    } else {
        println!("{serialized}");
    }
    Ok(())
}
