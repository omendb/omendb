//! Run a deterministic, portable SeerDB qualification workload.
//!
//! This is an evidence harness rather than a performance claim. It exercises
//! the durable byte API, retained-root reads, bounded maintenance, close and
//! reopen, offline checking, and verification while recording latency
//! quantiles and physical counters in a machine-readable result.

#![allow(clippy::disallowed_methods)]

use seerdb::{BatchMutation, DB, Options, PublicationMetrics, StorageMetrics};
use serde_json::{Map, Value, json};
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

#[derive(Debug, Default)]
struct CounterTotals {
    page_bytes_written: u64,
    physical_page_writes: u64,
    syncs: u64,
}

#[derive(Debug, Default)]
struct PublicationTotals {
    wal_bytes_written: u64,
    metadata_bytes_written: u64,
    blob_bytes_written: u64,
    history_bytes_written: u64,
    manifest_bytes_written: u64,
}

impl PublicationTotals {
    fn add_delta(&mut self, before: PublicationMetrics, after: PublicationMetrics) {
        self.wal_bytes_written = self.wal_bytes_written.saturating_add(
            after
                .wal_bytes_written
                .saturating_sub(before.wal_bytes_written),
        );
        self.metadata_bytes_written = self.metadata_bytes_written.saturating_add(
            after
                .metadata_bytes_written
                .saturating_sub(before.metadata_bytes_written),
        );
        self.blob_bytes_written = self.blob_bytes_written.saturating_add(
            after
                .blob_bytes_written
                .saturating_sub(before.blob_bytes_written),
        );
        self.history_bytes_written = self.history_bytes_written.saturating_add(
            after
                .history_bytes_written
                .saturating_sub(before.history_bytes_written),
        );
        self.manifest_bytes_written = self.manifest_bytes_written.saturating_add(
            after
                .manifest_bytes_written
                .saturating_sub(before.manifest_bytes_written),
        );
    }
}

impl CounterTotals {
    fn add_delta(&mut self, before: StorageMetrics, after: StorageMetrics) {
        self.page_bytes_written = self.page_bytes_written.saturating_add(
            after
                .page_bytes_written
                .saturating_sub(before.page_bytes_written),
        );
        self.physical_page_writes = self.physical_page_writes.saturating_add(
            after
                .physical_page_writes
                .saturating_sub(before.physical_page_writes),
        );
        self.syncs = self
            .syncs
            .saturating_add(after.syncs.saturating_sub(before.syncs));
    }
}

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
    trace_output: Option<PathBuf>,
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
        trace_output: None,
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
            "--trace-output" => parsed.trace_output = Some(PathBuf::from(value)),
            "--help" | "-h" => {
                println!(
                    "usage: seerdb_qualification [--keys N] [--operations N] [--seed N] [--db PATH] [--output PATH] [--trace-output PATH]"
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

fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn trace_event(events: &mut Vec<Value>, next_seq: &mut u64, kind: &str, payload: Value) {
    let mut event = Map::new();
    event.insert("seq".to_owned(), json!(*next_seq));
    event.insert("kind".to_owned(), json!(kind));
    if let Value::Object(payload) = payload {
        event.extend(payload);
    }
    events.push(Value::Object(event));
    *next_seq = next_seq.saturating_add(1);
}

fn trace_pairs(pairs: &[(Vec<u8>, Vec<u8>)]) -> Value {
    Value::Array(
        pairs
            .iter()
            .map(|(key, value)| {
                json!({
                    "key_hex": hex_encode(key),
                    "value_hex": hex_encode(value),
                })
            })
            .collect(),
    )
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
    let mut trace_events = Vec::with_capacity(args.operations.saturating_add(args.keys) + 16);
    let mut next_trace_seq = 0;
    trace_event(
        &mut trace_events,
        &mut next_trace_seq,
        "header",
        json!({
            "schema": "seerdb-storage-conformance-v1",
            "keys": args.keys,
            "operations": args.operations,
            "seed": args.seed,
        }),
    );
    trace_event(
        &mut trace_events,
        &mut next_trace_seq,
        "batch",
        json!({
            "atomic": true,
            "mutations": initial.iter().map(|(key, value)| json!({
                "op": "put",
                "key_hex": hex_encode(key),
                "value_hex": hex_encode(value),
            })).collect::<Vec<_>>(),
        }),
    );
    db.commit_batch(&initial_batch)?;
    let mut model = initial;
    let mut retained = Some(db.retain_current()?);
    let retained_model = model.clone();
    trace_event(
        &mut trace_events,
        &mut next_trace_seq,
        "retain",
        json!({"snapshot": "initial"}),
    );
    let mut state = args.seed;
    let mut revisions = vec![0usize; args.keys + args.operations];
    let mut commit_latency = Vec::new();
    let mut get_latency = Vec::new();
    let mut range_latency = Vec::new();
    let mut maintenance_latency = Vec::new();
    let mut logical_write_bytes = 0u64;
    let mut maintenance_runs = 0u64;
    let mut reopen_runs = 0u64;
    let initial_metrics = db.metrics()?;
    let mut handle_metrics = initial_metrics.storage;
    let mut handle_publication = initial_metrics.publication;
    let mut commit_counters = CounterTotals::default();
    let mut maintenance_counters = CounterTotals::default();
    let mut commit_publication = PublicationTotals::default();
    let mut maintenance_publication = PublicationTotals::default();

    for operation in 0..args.operations {
        let random = next_random(&mut state);
        let key_index = (random as usize) % (args.keys + args.operations);
        let key = workload_key(key_index);
        match random % 100 {
            0..=44 => {
                trace_event(
                    &mut trace_events,
                    &mut next_trace_seq,
                    "get",
                    json!({
                        "key_hex": hex_encode(&key),
                        "expected_value_hex": model.get(&key).map(|value| hex_encode(value)),
                    }),
                );
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
                trace_event(
                    &mut trace_events,
                    &mut next_trace_seq,
                    "batch",
                    json!({
                        "atomic": true,
                        "mutations": [{
                            "op": "put",
                            "key_hex": hex_encode(&key),
                            "value_hex": hex_encode(&value),
                        }],
                    }),
                );
                let started = Instant::now();
                db.commit_batch(&[BatchMutation::Put {
                    key: key.clone(),
                    value: value.clone(),
                }])
                .map_err(|error| invalid(format!("operation {operation} put: {error}")))?;
                commit_latency.push(started.elapsed().as_nanos());
                logical_write_bytes =
                    logical_write_bytes.saturating_add((key.len() + value.len()) as u64);
                model.insert(key, value);
            }
            70..=84 => {
                trace_event(
                    &mut trace_events,
                    &mut next_trace_seq,
                    "batch",
                    json!({
                        "atomic": true,
                        "mutations": [{
                            "op": "delete",
                            "key_hex": hex_encode(&key),
                        }],
                    }),
                );
                let started = Instant::now();
                db.commit_batch(&[BatchMutation::Delete { key: key.clone() }])
                    .map_err(|error| invalid(format!("operation {operation} delete: {error}")))?;
                commit_latency.push(started.elapsed().as_nanos());
                logical_write_bytes = logical_write_bytes.saturating_add(key.len() as u64);
                model.remove(&key);
            }
            _ => {
                let start_index = key_index.min(args.keys + args.operations - 1);
                let end_index = (start_index + 16).min(args.keys + args.operations);
                let start = workload_key(start_index);
                let end = workload_key(end_index);
                let expected = model_range(&model, &start, &end);
                trace_event(
                    &mut trace_events,
                    &mut next_trace_seq,
                    "range",
                    json!({
                        "start_hex": hex_encode(&start),
                        "end_hex": hex_encode(&end),
                        "expected": trace_pairs(&expected),
                    }),
                );
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
                trace_event(
                    &mut trace_events,
                    &mut next_trace_seq,
                    "snapshot_get",
                    json!({
                        "snapshot": "initial",
                        "key_hex": hex_encode(key),
                        "expected_value_hex": hex_encode(value),
                    }),
                );
                if snapshot.get(key)?.as_deref() != Some(value.as_slice()) {
                    return Err(invalid("retained snapshot changed after later commits"));
                }
            }
        }

        if (operation + 1) % (args.operations / 4).max(1) == 0 {
            trace_event(
                &mut trace_events,
                &mut next_trace_seq,
                "maintenance",
                json!({"action": "compact", "limit": 2}),
            );
            let before_maintenance = db.metrics()?;
            commit_counters.add_delta(handle_metrics, before_maintenance.storage);
            commit_publication.add_delta(handle_publication, before_maintenance.publication);
            let started = Instant::now();
            db.compact_with_limit(2)?;
            db.verify()?;
            maintenance_latency.push(started.elapsed().as_nanos());
            maintenance_runs = maintenance_runs.saturating_add(1);
            let after_maintenance = db.metrics()?;
            maintenance_counters.add_delta(before_maintenance.storage, after_maintenance.storage);
            maintenance_publication.add_delta(
                before_maintenance.publication,
                after_maintenance.publication,
            );
            trace_event(
                &mut trace_events,
                &mut next_trace_seq,
                "verify",
                json!({"boundary": "maintenance"}),
            );
            db.close()?;
            trace_event(
                &mut trace_events,
                &mut next_trace_seq,
                "reopen",
                json!({"boundary": "maintenance"}),
            );
            db = DB::open(&path, Options::default())?;
            handle_metrics = StorageMetrics::default();
            handle_publication = PublicationMetrics::default();
            db.verify()?;
            check_range(&db, &model, &[], &[u8::MAX; 64])?;
            reopen_runs = reopen_runs.saturating_add(1);
        }
    }

    let current_digest = digest(&model);
    let mut retained = retained
        .take()
        .ok_or_else(|| invalid("retained snapshot was already released"))?;
    trace_event(
        &mut trace_events,
        &mut next_trace_seq,
        "snapshot_release",
        json!({"snapshot": "initial"}),
    );
    retained.verify()?;
    retained.release()?;

    let before_maintenance = db.metrics()?;
    commit_counters.add_delta(handle_metrics, before_maintenance.storage);
    commit_publication.add_delta(handle_publication, before_maintenance.publication);
    trace_event(
        &mut trace_events,
        &mut next_trace_seq,
        "maintenance",
        json!({"action": "vacuum_prune"}),
    );
    let started = Instant::now();
    let vacuum = db.vacuum()?;
    db.prune_history()?;
    maintenance_latency.push(started.elapsed().as_nanos());
    maintenance_runs = maintenance_runs.saturating_add(1);
    db.verify()?;
    check_range(&db, &model, &[], &[u8::MAX; 64])?;
    let after_maintenance = db.metrics()?;
    maintenance_counters.add_delta(before_maintenance.storage, after_maintenance.storage);
    maintenance_publication.add_delta(
        before_maintenance.publication,
        after_maintenance.publication,
    );
    let total_page_bytes_written = commit_counters
        .page_bytes_written
        .saturating_add(maintenance_counters.page_bytes_written);
    let total_physical_page_writes = commit_counters
        .physical_page_writes
        .saturating_add(maintenance_counters.physical_page_writes);
    let total_syncs = commit_counters
        .syncs
        .saturating_add(maintenance_counters.syncs);
    let final_space_bytes = after_maintenance
        .data_bytes
        .saturating_add(after_maintenance.blob_bytes)
        .saturating_add(after_maintenance.wal_bytes);
    let logical_bytes = logical_bytes(&model);

    trace_event(
        &mut trace_events,
        &mut next_trace_seq,
        "check",
        json!({"boundary": "pre-close"}),
    );
    let check_before_close = DB::check(&path, Options::default())?;
    trace_event(
        &mut trace_events,
        &mut next_trace_seq,
        "reopen",
        json!({"boundary": "final"}),
    );
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
    trace_event(
        &mut trace_events,
        &mut next_trace_seq,
        "expect_final",
        json!({
            "digest": current_digest,
            "commit_id": status.commit_id.get(),
        }),
    );
    reopened.close()?;
    drop(reopened);
    drop(temporary);

    let trace_text = trace_events
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?
        .join("\n")
        + "\n";
    let trace_crc32c = format!("{:08x}", crc32c::crc32c(trace_text.as_bytes()));
    if let Some(trace_output) = args.trace_output {
        fs::write(&trace_output, &trace_text)?;
    }

    let report = json!({
        "schema": "seerdb-qualification-v1",
        "config": {
            "keys": args.keys,
            "operations": args.operations,
            "seed": args.seed,
            "path": path,
        },
        "trace": {
            "schema": "seerdb-storage-conformance-v1",
            "events": trace_events.len(),
            "bytes": trace_text.len(),
            "crc32c": trace_crc32c,
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
            "page_bytes_written_by_commits": commit_counters.page_bytes_written,
            "page_bytes_written_by_maintenance": maintenance_counters.page_bytes_written,
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
            "physical_page_writes_by_commits": commit_counters.physical_page_writes,
            "physical_page_writes_by_maintenance": maintenance_counters.physical_page_writes,
            "syncs": total_syncs,
            "syncs_by_commits": commit_counters.syncs,
            "syncs_by_maintenance": maintenance_counters.syncs,
            "publication": {
                "wal_bytes_written_by_commits": commit_publication.wal_bytes_written,
                "wal_bytes_written_by_maintenance": maintenance_publication.wal_bytes_written,
                "metadata_bytes_written_by_commits": commit_publication.metadata_bytes_written,
                "metadata_bytes_written_by_maintenance": maintenance_publication.metadata_bytes_written,
                "blob_bytes_written_by_commits": commit_publication.blob_bytes_written,
                "blob_bytes_written_by_maintenance": maintenance_publication.blob_bytes_written,
                "history_bytes_written_by_commits": commit_publication.history_bytes_written,
                "history_bytes_written_by_maintenance": maintenance_publication.history_bytes_written,
                "manifest_bytes_written_by_commits": commit_publication.manifest_bytes_written,
                "manifest_bytes_written_by_maintenance": maintenance_publication.manifest_bytes_written,
            },
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
