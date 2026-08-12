//! Replay the shared DBNext R0 JSONL trace through SeerDB.
//!
//! This is deliberately a narrow adapter proof, not a replacement for the
//! DBNext transaction/index layer. Each JSONL commit is applied as one SeerDB
//! atomic mutation batch and closed at the explicit SeerDB checkpoint barrier.
//! The result is suitable for the DBNext workload tooling to compare with its
//! reference digest.

#![allow(clippy::disallowed_methods)]

use seerdb::{BatchMutation, DB, Options};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashSet};
use std::env;
use std::error::Error as StdError;
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;
use tempfile::TempDir;

type AnyResult<T> = std::result::Result<T, Box<dyn StdError>>;
type RecordKey = (u64, u64);

#[derive(Default)]
struct R0State {
    commit_id: u64,
    rows: BTreeMap<RecordKey, Option<String>>,
    checkpoints: BTreeMap<String, String>,
    idempotency_keys: HashSet<String>,
    last_acknowledged_commit_id: u64,
    ambiguous_commit_id: Option<u64>,
}

struct ReplayArgs {
    trace: PathBuf,
    db: Option<PathBuf>,
    output: Option<PathBuf>,
    manifest: Option<PathBuf>,
}

fn invalid(message: impl Into<String>) -> Box<dyn StdError> {
    Box::new(IoError::new(ErrorKind::InvalidData, message.into()))
}

fn object<'a>(value: &'a Value, context: &str) -> AnyResult<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| invalid(format!("{context} must be a JSON object")))
}

fn field<'a>(value: &'a Map<String, Value>, name: &str, context: &str) -> AnyResult<&'a Value> {
    value
        .get(name)
        .ok_or_else(|| invalid(format!("{context} is missing {name}")))
}

fn string_field(value: &Map<String, Value>, name: &str, context: &str) -> AnyResult<String> {
    field(value, name, context)?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid(format!("{context}.{name} must be a string")))
}

fn u64_field(value: &Map<String, Value>, name: &str, context: &str) -> AnyResult<u64> {
    field(value, name, context)?
        .as_u64()
        .ok_or_else(|| invalid(format!("{context}.{name} must be an unsigned integer")))
}

fn decode_hex(value: &str, context: &str) -> AnyResult<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(invalid(format!(
            "{context} must contain an even number of hex digits"
        )));
    }

    value
        .as_bytes()
        .chunks_exact(2)
        .enumerate()
        .map(|(index, pair)| {
            let high = hex_digit(pair[0]).ok_or_else(|| {
                invalid(format!(
                    "{context} contains an invalid hex digit at {index}"
                ))
            })?;
            let low = hex_digit(pair[1]).ok_or_else(|| {
                invalid(format!(
                    "{context} contains an invalid hex digit at {}",
                    index + 1
                ))
            })?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn record_key((tenant_id, record_id): RecordKey) -> Vec<u8> {
    let mut key = Vec::with_capacity(19);
    key.extend_from_slice(b"r0\0");
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&record_id.to_be_bytes());
    key
}

fn visible_digest(rows: &BTreeMap<RecordKey, Option<String>>) -> String {
    let mut input = String::new();
    for ((tenant_id, record_id), payload) in rows {
        if let Some(payload) = payload {
            input.push_str(&format!("{tenant_id}:{record_id}:{payload}\n"));
        }
    }
    sha256_hex(input.as_bytes())
}

// Minimal SHA-256 implementation for the R0 oracle's byte-identical digest.
// It is intentionally local to this evidence adapter; SeerDB's storage
// checksums remain CRC32C and do not depend on this workload digest.
fn sha256_hex(input: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut message = input.to_vec();
    let bit_len = (message.len() as u64) * 8;
    message.push(0x80);
    while (message.len() % 64) != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());

    let mut state: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    for chunk in message.chunks_exact(64) {
        let mut schedule = [0u32; 64];
        for (word, bytes) in schedule[..16].iter_mut().zip(chunk.chunks_exact(4)) {
            *word = u32::from_be_bytes(bytes.try_into().expect("four-byte SHA word"));
        }
        for index in 16..64 {
            let s0 = schedule[index - 15].rotate_right(7)
                ^ schedule[index - 15].rotate_right(18)
                ^ (schedule[index - 15] >> 3);
            let s1 = schedule[index - 2].rotate_right(17)
                ^ schedule[index - 2].rotate_right(19)
                ^ (schedule[index - 2] >> 10);
            schedule[index] = schedule[index - 16]
                .wrapping_add(s0)
                .wrapping_add(schedule[index - 7])
                .wrapping_add(s1);
        }

        let mut working = state;
        for index in 0..64 {
            let sigma1 = working[4].rotate_right(6)
                ^ working[4].rotate_right(11)
                ^ working[4].rotate_right(25);
            let choice = (working[4] & working[5]) ^ ((!working[4]) & working[6]);
            let temp1 = working[7]
                .wrapping_add(sigma1)
                .wrapping_add(choice)
                .wrapping_add(K[index])
                .wrapping_add(schedule[index]);
            let sigma0 = working[0].rotate_right(2)
                ^ working[0].rotate_right(13)
                ^ working[0].rotate_right(22);
            let majority =
                (working[0] & working[1]) ^ (working[0] & working[2]) ^ (working[1] & working[2]);
            let temp2 = sigma0.wrapping_add(majority);
            working[7] = working[6];
            working[6] = working[5];
            working[5] = working[4];
            working[4] = working[3].wrapping_add(temp1);
            working[3] = working[2];
            working[2] = working[1];
            working[1] = working[0];
            working[0] = temp1.wrapping_add(temp2);
        }
        for (destination, source) in state.iter_mut().zip(working) {
            *destination = (*destination).wrapping_add(source);
        }
    }

    let mut result = String::with_capacity(64);
    for word in state {
        result.push_str(&format!("{word:08x}"));
    }
    result
}

fn apply_commit(db: &mut DB, state: &mut R0State, event: &Map<String, Value>) -> AnyResult<()> {
    let context = "commit event";
    let _transaction_id = string_field(event, "transaction_id", context)?;
    let idempotency_key = string_field(event, "idempotency_key", context)?;
    let acknowledged = field(event, "acknowledged", context)?
        .as_bool()
        .ok_or_else(|| invalid("commit event.acknowledged must be a boolean"))?;
    let mutations = field(event, "mutations", context)?
        .as_array()
        .ok_or_else(|| invalid("commit event.mutations must be an array"))?;
    if mutations.is_empty() {
        return Err(invalid("commit event.mutations must not be empty"));
    }

    let next_commit = state.commit_id + 1;
    if state.idempotency_keys.contains(&idempotency_key) {
        return Err(invalid(format!(
            "duplicate idempotency key: {idempotency_key}"
        )));
    }
    // Validate the entire logical transaction before touching SeerDB. This
    // keeps the adapter's reference semantics atomic even though the current
    // public DB API exposes individual byte-key mutations.
    let mut candidate = state.rows.clone();
    let mut operations = Vec::with_capacity(mutations.len());
    for (index, mutation) in mutations.iter().enumerate() {
        let mutation = object(mutation, &format!("commit mutation {index}"))?;
        let operation = string_field(mutation, "op", "mutation")?;
        let tenant_id = u64_field(mutation, "tenant_id", "mutation")?;
        let record_id = u64_field(mutation, "record_id", "mutation")?;
        if tenant_id > u16::MAX as u64 {
            return Err(invalid("mutation.tenant_id exceeds the R0 u16 schema"));
        }
        let key = (tenant_id, record_id);
        match operation.as_str() {
            "put" | "update" => {
                let payload = string_field(mutation, "payload", "mutation")?;
                let bytes = decode_hex(&payload, "mutation.payload")?;
                if bytes.len() > 256 {
                    return Err(invalid("mutation.payload exceeds the R0 256-byte limit"));
                }
                if operation == "update" && candidate.get(&key).and_then(Option::as_ref).is_none() {
                    return Err(invalid(format!("cannot update absent record: {key:?}")));
                }
                candidate.insert(key, Some(payload));
                operations.push(BatchMutation::Put {
                    key: record_key(key),
                    value: bytes,
                });
            }
            "delete" => {
                if candidate.get(&key).and_then(Option::as_ref).is_none() {
                    return Err(invalid(format!("cannot delete absent record: {key:?}")));
                }
                candidate.insert(key, None);
                operations.push(BatchMutation::Delete {
                    key: record_key(key),
                });
            }
            other => return Err(invalid(format!("unknown mutation operation: {other}"))),
        }
    }

    let durability = db.commit_batch(&operations)?;
    if durability.commit_id.get() != next_commit
        || durability.pending_mutations != 0
        || durability.write_fenced
    {
        return Err(invalid(format!(
            "SeerDB publication did not close R0 commit {next_commit}: {durability:?}"
        )));
    }

    state.idempotency_keys.insert(idempotency_key);
    state.rows = candidate;
    state.commit_id = next_commit;
    if acknowledged {
        state.last_acknowledged_commit_id = next_commit;
    } else {
        state.ambiguous_commit_id = Some(next_commit);
    }
    Ok(())
}

fn apply_checkpoint(db: &mut DB, state: &mut R0State, event: &Map<String, Value>) -> AnyResult<()> {
    let name = string_field(event, "name", "checkpoint event")?;
    if name.is_empty() {
        return Err(invalid("checkpoint event.name must not be empty"));
    }
    let report = db.checkpoint()?;
    let durability = db.durability_status();
    if durability.commit_id.get() != state.commit_id || report.wal_bytes != 0 {
        return Err(invalid(
            "checkpoint did not verify a clean durable boundary",
        ));
    }
    state.checkpoints.insert(name, visible_digest(&state.rows));
    Ok(())
}

fn replay_trace(trace: &Path, db_path: &Path) -> AnyResult<Value> {
    let options = Options::default();
    let mut db = DB::open(db_path, options.clone())?;
    let trace_bytes = fs::read(trace)?;
    let trace_text = std::str::from_utf8(&trace_bytes)?;
    let replay_started = Instant::now();
    let mut state = R0State::default();
    let mut previous_seq = 0u64;
    let mut event_count = 0u64;
    let mut commit_count = 0u64;
    let mut checkpoint_count = 0u64;
    let mut commit_seconds = 0.0;
    let mut checkpoint_seconds = 0.0;

    for (line_number, line) in trace_text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let event: Value = serde_json::from_str(line)?;
        let event = object(&event, &format!("trace line {}", line_number + 1))?;
        let sequence = u64_field(event, "seq", "trace event")?;
        if sequence != previous_seq + 1 {
            return Err(invalid(format!(
                "trace line {} has non-monotonic seq {sequence} after {previous_seq}",
                line_number + 1
            )));
        }
        previous_seq = sequence;
        let kind = string_field(event, "kind", "trace event")?;
        let phase_started = Instant::now();
        match kind.as_str() {
            "commit" => {
                apply_commit(&mut db, &mut state, event)?;
                commit_count += 1;
                commit_seconds += phase_started.elapsed().as_secs_f64();
            }
            "checkpoint" => {
                apply_checkpoint(&mut db, &mut state, event)?;
                checkpoint_count += 1;
                checkpoint_seconds += phase_started.elapsed().as_secs_f64();
            }
            kind => return Err(invalid(format!("unsupported R0 event kind: {kind}"))),
        }
        event_count += 1;
    }

    let mut expected = Vec::new();
    for (key, payload) in &state.rows {
        if let Some(payload) = payload {
            expected.push((record_key(*key), decode_hex(payload, "state payload")?));
        }
    }
    let actual = db.range(b"r0\0", b"r0\xff")?;
    if actual != expected {
        return Err(invalid("SeerDB visible rows disagree with the R0 state"));
    }
    let verification = db.verify()?;
    let metrics = db.metrics()?;
    let durability = db.durability_status();
    if durability.commit_id.get() != state.commit_id || verification.wal_bytes != 0 {
        return Err(invalid(
            "final SeerDB state is not a clean durable boundary",
        ));
    }
    db.close()?;

    Ok(json!({
        "adapter": "seerdb-r0-v0",
        "workload": "r0-integrity-recovery",
        "trace": trace,
        "trace_bytes": trace_bytes.len(),
        "trace_sha256": sha256_hex(&trace_bytes),
        "events": event_count,
        "commit_id": state.commit_id,
        "acknowledged_commit_id": state.last_acknowledged_commit_id,
        "ambiguous_commit_id": state.ambiguous_commit_id,
        "state_digest": visible_digest(&state.rows),
        "checkpoints": state.checkpoints,
        "durability": {
            "database_id": database_id_hex(durability.database_id),
            "history_id": durability.history_id.get(),
            "generation_id": durability.generation_id.get(),
            "commit_id": durability.commit_id.get(),
        },
        "durability_settings": {
            "mode": "local-durable-serialized-prototype",
            "sync_writes": options.sync_writes,
            "use_odirect": options.use_odirect,
            "buffer_pool_size": options.buffer_pool_size,
            "page_size": seerdb::PAGE_SIZE,
            "blob_threshold": options.blob_threshold,
            "max_wal_bytes": options.max_wal_bytes,
        },
        "timing": {
            "replay_seconds": replay_started.elapsed().as_secs_f64(),
            "commit_seconds": commit_seconds,
            "checkpoint_seconds": checkpoint_seconds,
            "commit_count": commit_count,
            "checkpoint_count": checkpoint_count,
        },
        "verification": {
            "verified_pages": verification.verified_pages,
            "data_bytes": verification.data_bytes,
            "blob_bytes": verification.blob_bytes,
            "wal_bytes": verification.wal_bytes,
            "reclaimable_pages": verification.reclaimable_pages,
        },
        "metrics": {
            "storage": format_storage_metrics(metrics.storage),
            "buffer": format_buffer_stats(metrics.buffer),
            "wal_admission_failures": metrics.wal_admission_failures,
            "data_bytes": metrics.data_bytes,
            "blob_bytes": metrics.blob_bytes,
            "wal_bytes": metrics.wal_bytes,
            "wal_reserved_bytes": metrics.wal_reserved_bytes,
            "reclaimable_pages": metrics.reclaimable_pages,
        },
        "unsupported": [
            "secondary indexes and unique enforcement",
            "retained historical snapshots inside one database lineage",
            "scheduled fault execution; use the fault-injection integration suite"
        ]
    }))
}

fn database_id_hex(id: seerdb::storage::format::DatabaseId) -> String {
    id.as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn format_storage_metrics(metrics: seerdb::StorageMetrics) -> Value {
    json!({
        "logical_page_reads": metrics.logical_page_reads,
        "physical_page_reads": metrics.physical_page_reads,
        "physical_page_writes": metrics.physical_page_writes,
        "page_bytes_read": metrics.page_bytes_read,
        "page_bytes_written": metrics.page_bytes_written,
        "generation_flushes": metrics.generation_flushes,
        "syncs": metrics.syncs,
        "reclaimed_pages": metrics.reclaimed_pages,
        "reclaimed_bytes": metrics.reclaimed_bytes,
        "capacity_preflight_failures": metrics.capacity_preflight_failures,
    })
}

fn format_buffer_stats(stats: seerdb::buffer::BufferStats) -> Value {
    json!({
        "total_frames": stats.total_frames,
        "free_frames": stats.free_frames,
        "pinned_frames": stats.pinned_frames,
        "dirty_frames": stats.dirty_frames,
        "reads": stats.reads,
        "writes": stats.writes,
        "hits": stats.hits,
    })
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn absolute_path_string(path: &Path) -> String {
    if path.is_absolute() {
        return path_string(path);
    }
    std::env::current_dir()
        .map(|current| path_string(&current.join(path)))
        .unwrap_or_else(|_| path_string(path))
}

fn canonical_path_string(path: &Path) -> String {
    fs::canonicalize(path)
        .map(|path| path_string(&path))
        .unwrap_or_else(|_| absolute_path_string(path))
}

fn command_output(program: &str, arguments: &[&str]) -> Value {
    match Command::new(program).args(arguments).output() {
        Ok(output) if output.status.success() => {
            Value::String(String::from_utf8_lossy(&output.stdout).trim().to_owned())
        }
        _ => Value::Null,
    }
}

fn run_manifest(
    trace: &Path,
    db_path: &Path,
    output: Option<&Path>,
    manifest: Option<&Path>,
    result: &Value,
) -> Value {
    let parallelism = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .ok();
    json!({
        "manifest_version": "dbnext-run-manifest-v1",
        "target": "seerdb-rust",
        "workload": result["workload"],
        "trace": {
            "path": canonical_path_string(trace),
            "bytes": result["trace_bytes"],
            "events": result["events"],
            "sha256": result["trace_sha256"],
        },
        "schema": {
            "bundle": "r0-integrity-recovery-v0",
            "schema_version": 1,
            "relation": "r0_records",
        },
        "paths": {
            "trace": canonical_path_string(trace),
            "result": output.map(canonical_path_string),
            "manifest": manifest.map(canonical_path_string),
            "database_directory": canonical_path_string(db_path),
        },
        "host": {
            "os": std::env::consts::OS,
            "architecture": std::env::consts::ARCH,
            "cpu": {
                "logical": parallelism,
                "machine": std::env::consts::ARCH,
            },
        },
        "software": {
            "package": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "git_head": command_output("git", &["rev-parse", "HEAD"]),
            "rustc": command_output("rustc", &["--version"]),
            "cargo": command_output("cargo", &["--version"]),
        },
        "durability": result["durability_settings"],
        "timing": result["timing"],
        "correctness": {
            "state_digest": result["state_digest"],
            "commit_id": result["commit_id"],
            "acknowledged_commit_id": result["acknowledged_commit_id"],
            "checkpoints": result["checkpoints"],
        },
        "metrics": result["metrics"],
    })
}

fn parse_args() -> AnyResult<ReplayArgs> {
    let mut arguments = env::args_os().skip(1);
    let trace = arguments.next().map(PathBuf::from).ok_or_else(|| {
        invalid("usage: dbnext_r0_replay TRACE [--db PATH] [--output PATH] [--manifest PATH]")
    })?;
    let mut db = None;
    let mut output = None;
    let mut manifest = None;
    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--db") => {
                db = Some(PathBuf::from(
                    arguments
                        .next()
                        .ok_or_else(|| invalid("--db requires a path"))?,
                ));
            }
            Some("--output") => {
                output = Some(PathBuf::from(
                    arguments
                        .next()
                        .ok_or_else(|| invalid("--output requires a path"))?,
                ));
            }
            Some("--manifest") => {
                manifest = Some(PathBuf::from(
                    arguments
                        .next()
                        .ok_or_else(|| invalid("--manifest requires a path"))?,
                ));
            }
            Some(other) => return Err(invalid(format!("unknown argument: {other}"))),
            None => return Err(invalid("arguments must be valid UTF-8")),
        }
    }
    Ok(ReplayArgs {
        trace,
        db,
        output,
        manifest,
    })
}

fn main() -> AnyResult<()> {
    let ReplayArgs {
        trace,
        db,
        output,
        manifest,
    } = parse_args()?;
    let (db_path, _temporary): (PathBuf, Option<TempDir>) = match db {
        Some(path) => {
            if path.exists() {
                return Err(invalid(format!(
                    "refusing to replay into an existing database: {}",
                    path.display()
                )));
            }
            (path, None)
        }
        None => {
            let temporary = tempfile::tempdir()?;
            let path = temporary.path().join("db");
            (path, Some(temporary))
        }
    };
    if output.as_deref() == manifest.as_deref() && output.is_some() {
        return Err(invalid("--output and --manifest must be different paths"));
    }
    let mut result = replay_trace(&trace, &db_path)?;
    let manifest_value = run_manifest(
        &trace,
        &db_path,
        output.as_deref(),
        manifest.as_deref(),
        &result,
    );
    result["run_manifest"] = manifest_value.clone();
    let encoded = serde_json::to_string_pretty(&result)? + "\n";
    if let Some(output) = output.as_deref() {
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(output, encoded)?;
    } else {
        print!("{encoded}");
    }
    if let Some(manifest) = manifest.as_deref() {
        if let Some(parent) = manifest.parent() {
            fs::create_dir_all(parent)?;
        }
        let encoded_manifest = serde_json::to_string_pretty(&manifest_value)? + "\n";
        fs::write(manifest, encoded_manifest)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replays_a_multi_record_commit_and_checkpoint() {
        let root = tempfile::tempdir().unwrap();
        let trace = root.path().join("trace.jsonl");
        fs::write(
            &trace,
            concat!(
                r#"{"seq":1,"kind":"commit","transaction_id":"t1","idempotency_key":"k1","acknowledged":true,"mutations":[{"op":"put","tenant_id":1,"record_id":1,"payload":"616c706861"},{"op":"put","tenant_id":1,"record_id":2,"payload":"62657461"}]}"#, "\n",
                r#"{"seq":2,"kind":"checkpoint","name":"cp-1"}"#, "\n",
            ),
        )
        .unwrap();
        let result = replay_trace(&trace, &root.path().join("db")).unwrap();
        assert_eq!(result["events"], 2);
        assert_eq!(result["commit_id"], 1);
        assert_eq!(result["acknowledged_commit_id"], 1);
        assert_eq!(result["ambiguous_commit_id"], Value::Null);
        assert_eq!(result["checkpoints"]["cp-1"], result["state_digest"]);
        assert_eq!(
            result["trace_sha256"],
            sha256_hex(&fs::read(&trace).unwrap())
        );
        assert_eq!(result["trace_bytes"], fs::metadata(&trace).unwrap().len());
        assert_eq!(result["timing"]["commit_count"], 1);
        assert_eq!(result["timing"]["checkpoint_count"], 1);
        assert_eq!(result["durability_settings"]["sync_writes"], false);
    }
}
