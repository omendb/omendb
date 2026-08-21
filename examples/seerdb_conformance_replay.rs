//! Replay the SeerDB storage-conformance JSONL trace.
//!
//! The replay validates the trace contract against a fresh SeerDB directory:
//! batches update a byte-oriented reference model, reads compare with the
//! recorded oracle, and lifecycle events exercise retention, maintenance,
//! verification, checking, and reopen. This is a storage-engine artifact;
//! it is not SQL or DBNext compatibility evidence.

#![allow(clippy::disallowed_methods)]

use seerdb::{BatchMutation, DB, Options, RetainedSnapshot};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::env;
use std::error::Error as StdError;
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

type AnyResult<T> = Result<T, Box<dyn StdError>>;
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

struct RetainedEntry {
    snapshot: RetainedSnapshot,
    model: Model,
}

struct ReplayArgs {
    trace: PathBuf,
    db: Option<PathBuf>,
    output: Option<PathBuf>,
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

fn bool_field(value: &Map<String, Value>, name: &str, context: &str) -> AnyResult<bool> {
    field(value, name, context)?
        .as_bool()
        .ok_or_else(|| invalid(format!("{context}.{name} must be a boolean")))
}

fn decode_hex(value: &str, context: &str) -> AnyResult<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(invalid(format!(
            "{context} must contain an even number of hex digits"
        )));
    }
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
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

fn optional_hex_field(
    value: &Map<String, Value>,
    name: &str,
    context: &str,
) -> AnyResult<Option<Vec<u8>>> {
    let value = field(value, name, context)?;
    if value.is_null() {
        return Ok(None);
    }
    let encoded = value
        .as_str()
        .ok_or_else(|| invalid(format!("{context}.{name} must be a hex string or null")))?;
    decode_hex(encoded, &format!("{context}.{name}")).map(Some)
}

fn parse_mutations(value: &Value, context: &str) -> AnyResult<Vec<BatchMutation>> {
    let mutations = value
        .as_array()
        .ok_or_else(|| invalid(format!("{context}.mutations must be an array")))?;
    mutations
        .iter()
        .enumerate()
        .map(|(index, mutation)| {
            let context = format!("{context}.mutations[{index}]");
            let mutation = object(mutation, &context)?;
            let operation = string_field(mutation, "op", &context)?;
            let key = decode_hex(
                &string_field(mutation, "key_hex", &context)?,
                &format!("{context}.key_hex"),
            )?;
            match operation.as_str() {
                "put" => Ok(BatchMutation::Put {
                    key,
                    value: decode_hex(
                        &string_field(mutation, "value_hex", &context)?,
                        &format!("{context}.value_hex"),
                    )?,
                }),
                "delete" => Ok(BatchMutation::Delete { key }),
                other => Err(invalid(format!(
                    "{context}.op has unsupported value {other}"
                ))),
            }
        })
        .collect()
}

fn model_range(model: &Model, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    model
        .range(start.to_vec()..end.to_vec())
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn parse_pairs(value: &Value, context: &str) -> AnyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    value
        .as_array()
        .ok_or_else(|| invalid(format!("{context} must be an array")))?
        .iter()
        .enumerate()
        .map(|(index, pair)| {
            let context = format!("{context}[{index}]");
            let pair = object(pair, &context)?;
            let key = decode_hex(
                &string_field(pair, "key_hex", &context)?,
                &format!("{context}.key_hex"),
            )?;
            let value = decode_hex(
                &string_field(pair, "value_hex", &context)?,
                &format!("{context}.value_hex"),
            )?;
            Ok((key, value))
        })
        .collect()
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

fn scan_end(model: &Model) -> Vec<u8> {
    model
        .keys()
        .next_back()
        .map(|key| {
            let mut end = key.clone();
            end.push(0xff);
            end
        })
        .unwrap_or_else(|| vec![0xff])
}

fn compare_model(db: &DB, model: &Model, context: &str) -> AnyResult<()> {
    let end = scan_end(model);
    let actual = db.range(&[], &end)?;
    let expected = model_range(model, &[], &end);
    if actual != expected {
        return Err(invalid(format!(
            "{context}: database diverged from reference model"
        )));
    }
    Ok(())
}

fn apply_batch(db: &mut DB, model: &mut Model, event: &Map<String, Value>) -> AnyResult<()> {
    let context = "batch event";
    if !bool_field(event, "atomic", context)? {
        return Err(invalid("batch event.atomic must be true"));
    }
    let mutations = parse_mutations(field(event, "mutations", context)?, context)?;
    db.commit_batch(&mutations)?;
    for mutation in mutations {
        match mutation {
            BatchMutation::Put { key, value } => {
                model.insert(key, value);
            }
            BatchMutation::Delete { key } => {
                model.remove(&key);
            }
        }
    }
    Ok(())
}

fn parse_args() -> AnyResult<ReplayArgs> {
    let mut args = env::args().skip(1);
    let trace = args.next().ok_or_else(|| {
        invalid("usage: seerdb_conformance_replay TRACE [--db PATH] [--output PATH]")
    })?;
    let mut parsed = ReplayArgs {
        trace: PathBuf::from(trace),
        db: None,
        output: None,
    };
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| invalid(format!("{flag} requires a value")))?;
        match flag.as_str() {
            "--db" => parsed.db = Some(PathBuf::from(value)),
            "--output" => parsed.output = Some(PathBuf::from(value)),
            _ => return Err(invalid(format!("unknown argument {flag}"))),
        }
    }
    Ok(parsed)
}

fn replay_trace(trace: &Path, db_path: &Path) -> AnyResult<Value> {
    let trace_bytes = fs::read(trace)?;
    let trace_text = std::str::from_utf8(&trace_bytes)?;
    let mut db = DB::create(db_path, Options::default())?;
    let mut model = Model::new();
    let mut snapshots: BTreeMap<String, RetainedEntry> = BTreeMap::new();
    let mut header_seen = false;
    let mut final_seen = false;
    let mut expected_seq = 0u64;
    let mut event_count = 0u64;

    for (line_number, line) in trace_text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let context = format!("trace line {}", line_number + 1);
        let event = serde_json::from_str::<Value>(line)?;
        let event = object(&event, &context)?;
        let sequence = u64_field(event, "seq", &context)?;
        if sequence != expected_seq {
            return Err(invalid(format!(
                "{context} has seq {sequence}, expected {expected_seq}"
            )));
        }
        expected_seq = expected_seq.saturating_add(1);
        let kind = string_field(event, "kind", &context)?;
        match kind.as_str() {
            "header" => {
                if header_seen || event_count != 0 {
                    return Err(invalid(
                        "trace header must be the first event and appear once",
                    ));
                }
                let schema = string_field(event, "schema", &context)?;
                if schema != "seerdb-storage-conformance-v1" {
                    return Err(invalid(format!(
                        "unsupported storage trace schema {schema}"
                    )));
                }
                header_seen = true;
            }
            "batch" => apply_batch(&mut db, &mut model, event)?,
            "get" => {
                let key = decode_hex(
                    &string_field(event, "key_hex", &context)?,
                    &format!("{context}.key_hex"),
                )?;
                let expected = optional_hex_field(event, "expected_value_hex", &context)?;
                let actual = db.get(&key)?;
                if actual != expected {
                    return Err(invalid(format!(
                        "{context}: point read diverged from trace"
                    )));
                }
                if actual != model.get(&key).cloned() {
                    return Err(invalid(format!(
                        "{context}: point read diverged from mutation model"
                    )));
                }
            }
            "range" => {
                let start = decode_hex(
                    &string_field(event, "start_hex", &context)?,
                    &format!("{context}.start_hex"),
                )?;
                let end = decode_hex(
                    &string_field(event, "end_hex", &context)?,
                    &format!("{context}.end_hex"),
                )?;
                let expected = parse_pairs(field(event, "expected", &context)?, &context)?;
                let actual = db.range(&start, &end)?;
                if actual != expected {
                    return Err(invalid(format!("{context}: range diverged from trace")));
                }
                if model_range(&model, &start, &end) != expected {
                    return Err(invalid(format!(
                        "{context}: trace oracle diverged from mutation model"
                    )));
                }
            }
            "snapshot_get" => {
                let snapshot_name = string_field(event, "snapshot", &context)?;
                let retained = snapshots.get(&snapshot_name).ok_or_else(|| {
                    invalid(format!(
                        "{context} references unknown snapshot {snapshot_name}"
                    ))
                })?;
                let key = decode_hex(
                    &string_field(event, "key_hex", &context)?,
                    &format!("{context}.key_hex"),
                )?;
                let expected = optional_hex_field(event, "expected_value_hex", &context)?;
                let actual = retained.snapshot.get(&key)?;
                if actual != expected {
                    return Err(invalid(format!(
                        "{context}: retained snapshot read diverged from trace"
                    )));
                }
                if actual != retained.model.get(&key).cloned() {
                    return Err(invalid(format!(
                        "{context}: retained snapshot read diverged from snapshot model"
                    )));
                }
            }
            "retain" => {
                let snapshot_name = string_field(event, "snapshot", &context)?;
                if snapshots.contains_key(&snapshot_name) {
                    return Err(invalid(format!(
                        "{context} attempts to replace live snapshot {snapshot_name}"
                    )));
                }
                snapshots.insert(
                    snapshot_name,
                    RetainedEntry {
                        snapshot: db.retain_current()?,
                        model: model.clone(),
                    },
                );
            }
            "snapshot_release" => {
                let snapshot_name = string_field(event, "snapshot", &context)?;
                let RetainedEntry { mut snapshot, .. } =
                    snapshots.remove(&snapshot_name).ok_or_else(|| {
                        invalid(format!(
                            "{context} releases unknown snapshot {snapshot_name}"
                        ))
                    })?;
                snapshot.verify()?;
                snapshot.release()?;
            }
            "maintenance" => {
                let action = string_field(event, "action", &context)?;
                match action.as_str() {
                    "compact" => {
                        let limit = u64_field(event, "limit", &context)?;
                        let limit = usize::try_from(limit)
                            .map_err(|_| invalid(format!("{context}.limit exceeds usize")))?;
                        db.compact_with_limit(limit)?;
                    }
                    "vacuum_prune" => {
                        db.vacuum()?;
                        db.prune_history()?;
                    }
                    other => {
                        return Err(invalid(format!(
                            "{context}.action has unsupported value {other}"
                        )));
                    }
                }
                compare_model(&db, &model, &context)?;
            }
            "verify" => {
                db.verify()?;
            }
            "check" => {
                DB::check(db_path, Options::default())?;
            }
            "reopen" => {
                db.close()?;
                db = DB::open(db_path, Options::default())?;
                db.verify()?;
                compare_model(&db, &model, &context)?;
            }
            "expect_final" => {
                if final_seen {
                    return Err(invalid("trace contains more than one expect_final event"));
                }
                let expected_digest = string_field(event, "digest", &context)?;
                let expected_commit = u64_field(event, "commit_id", &context)?;
                compare_model(&db, &model, &context)?;
                let actual_digest = digest(&model);
                if actual_digest != expected_digest {
                    return Err(invalid(format!(
                        "{context}: digest {actual_digest} does not match trace {expected_digest}"
                    )));
                }
                let status = db.durability_status();
                if status.commit_id.get() != expected_commit {
                    return Err(invalid(format!(
                        "{context}: commit {} does not match trace {expected_commit}",
                        status.commit_id.get()
                    )));
                }
                db.verify()?;
                final_seen = true;
            }
            other => {
                return Err(invalid(format!(
                    "{context} has unsupported event kind {other}"
                )));
            }
        }
        event_count = event_count.saturating_add(1);
    }

    if !header_seen {
        return Err(invalid("trace has no header event"));
    }
    if !final_seen {
        return Err(invalid("trace has no expect_final event"));
    }
    if !snapshots.is_empty() {
        return Err(invalid("trace ended with live retained snapshots"));
    }
    let verification = db.verify()?;
    let status = db.durability_status();
    let result = json!({
        "schema": "seerdb-storage-conformance-replay-v1",
        "trace": {
            "schema": "seerdb-storage-conformance-v1",
            "path": trace,
            "events": event_count,
            "bytes": trace_bytes.len(),
            "crc32c": format!("{:08x}", crc32c::crc32c(&trace_bytes)),
        },
        "correctness": {
            "digest": digest(&model),
            "commit_id": status.commit_id.get(),
            "generation_id": status.generation_id.get(),
        },
        "verification": {
            "verified_pages": verification.verified_pages,
            "data_bytes": verification.data_bytes,
            "blob_bytes": verification.blob_bytes,
            "wal_bytes": verification.wal_bytes,
            "reclaimable_pages": verification.reclaimable_pages,
        },
        "scope": {
            "storage_contract": "ordered byte keys, atomic batches, retention, maintenance, verification, check, reopen",
            "not_proven": [
                "independent adapter equivalence",
                "scheduled fault execution",
                "SQL, transaction, or index compatibility",
                "comparative performance"
            ]
        }
    });
    db.close()?;
    Ok(result)
}

fn main() -> AnyResult<()> {
    let args = parse_args()?;
    let (db_path, _temporary): (PathBuf, Option<TempDir>) = match args.db {
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
    let result = replay_trace(&args.trace, &db_path)?;
    let encoded = serde_json::to_string_pretty(&result)? + "\n";
    if let Some(output) = args.output {
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(output, encoded)?;
    } else {
        print!("{encoded}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replays_storage_trace_lifecycle() {
        let root = tempfile::tempdir().expect("temporary root");
        let trace = root.path().join("trace.jsonl");
        let mut model = Model::new();
        model.insert(b"a".to_vec(), b"b".to_vec());
        let expected_digest = digest(&model);
        let events = vec![
            json!({"seq": 0, "kind": "header", "schema": "seerdb-storage-conformance-v1"}),
            json!({"seq": 1, "kind": "batch", "atomic": true, "mutations": [{"op": "put", "key_hex": "61", "value_hex": "62"}]}),
            json!({"seq": 2, "kind": "retain", "snapshot": "initial"}),
            json!({"seq": 3, "kind": "get", "key_hex": "61", "expected_value_hex": "62"}),
            json!({"seq": 4, "kind": "range", "start_hex": "", "end_hex": "ff", "expected": [{"key_hex": "61", "value_hex": "62"}]}),
            json!({"seq": 5, "kind": "snapshot_get", "snapshot": "initial", "key_hex": "61", "expected_value_hex": "62"}),
            json!({"seq": 6, "kind": "verify", "boundary": "test"}),
            json!({"seq": 7, "kind": "snapshot_release", "snapshot": "initial"}),
            json!({"seq": 8, "kind": "maintenance", "action": "vacuum_prune"}),
            json!({"seq": 9, "kind": "check", "boundary": "test"}),
            json!({"seq": 10, "kind": "reopen", "boundary": "test"}),
            json!({"seq": 11, "kind": "expect_final", "digest": expected_digest, "commit_id": 1}),
        ];
        let text = events
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .expect("serialize trace")
            .join("\n")
            + "\n";
        fs::write(&trace, text).expect("write trace");

        let result = replay_trace(&trace, &root.path().join("db")).expect("replay trace");
        assert_eq!(result["trace"]["events"], 12);
        assert_eq!(result["correctness"]["digest"], expected_digest);
        assert_eq!(result["correctness"]["commit_id"], 1);
    }
}
