use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use omendb::{Database, DatabaseConfig, Key, Mutation, NoFaults};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

mod support;

#[derive(Debug, Deserialize)]
struct TraceEvent {
    seq: u64,
    kind: String,
    name: Option<String>,
    mutations: Option<Vec<TraceMutation>>,
    acknowledged: Option<bool>,
    outcome: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TraceMutation {
    op: String,
    tenant_id: u64,
    record_id: u64,
    payload: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let trace_path = env::args()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: r0_replay <trace.jsonl>")?;
    let trace_bytes = fs::read(&trace_path)?;
    let trace_text = std::str::from_utf8(&trace_bytes)?;
    let directory = tempfile::tempdir()?;
    let config = DatabaseConfig {
        directory: directory.path().to_path_buf(),
    };
    let mut database = Database::create(config.clone())?;
    let checkpoint_each_commit = support::checkpoint_each_commit();
    let mut keys = BTreeSet::new();
    let mut expected_seq = 1_u64;
    let mut checkpoints = Vec::new();
    let mut expected_generation = 0_u64;
    let mut acknowledged_commit_id = 0_u64;
    let mut ambiguous_commit_ids = Vec::new();
    let mut metrics = support::ReplayMetrics::default();
    let replay_started = Instant::now();
    for line in trace_text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let event: TraceEvent = serde_json::from_str(line)?;
        if event.seq != expected_seq {
            return Err(format!("trace sequence {} != {}", event.seq, expected_seq).into());
        }
        expected_seq += 1;
        match event.kind.as_str() {
            "commit" => {
                let acknowledged = event
                    .acknowledged
                    .ok_or("commit has no acknowledged flag")?;
                let trace_mutations = event.mutations.ok_or("commit has no mutations")?;
                let mut mutations = Vec::with_capacity(trace_mutations.len());
                for trace_mutation in trace_mutations {
                    let key = Key::new(trace_mutation.tenant_id, trace_mutation.record_id);
                    keys.insert((trace_mutation.tenant_id, trace_mutation.record_id));
                    match trace_mutation.op.as_str() {
                        "put" | "update" => mutations.push(Mutation::Put {
                            key,
                            value: hex_decode(
                                trace_mutation
                                    .payload
                                    .as_deref()
                                    .ok_or("put/update mutation has no payload")?,
                            )?,
                        }),
                        "delete" => mutations.push(Mutation::Delete { key }),
                        operation => {
                            return Err(format!("unsupported mutation {operation}").into());
                        }
                    }
                }
                let started = Instant::now();
                let commit = database.commit(mutations, &mut NoFaults)?;
                metrics.record("commit", started);
                if checkpoint_each_commit {
                    let started = Instant::now();
                    database.checkpoint(&mut NoFaults)?;
                    metrics.record("commit_checkpoint", started);
                    expected_generation += 1;
                }
                if acknowledged {
                    acknowledged_commit_id = commit.0;
                } else {
                    if event.outcome.as_deref().is_none_or(str::is_empty) {
                        return Err(format!("ambiguous commit {} has no outcome", commit.0).into());
                    }
                    ambiguous_commit_ids.push(commit.0);
                }
            }
            "checkpoint" => {
                let name = event.name.ok_or("checkpoint has no name")?;
                let started = Instant::now();
                database.checkpoint(&mut NoFaults)?;
                metrics.record("checkpoint", started);
                expected_generation += 1;
                metrics.add_storage(database.metrics());
                let expected_commit = database.commit_id();
                database.close()?;
                let started = Instant::now();
                let reopened = Database::open(config.clone(), &mut NoFaults)?;
                metrics.record("recovery_reopen", started);
                if reopened.generation() != expected_generation
                    || reopened.commit_id() != expected_commit
                {
                    return Err(format!(
                        "checkpoint {name} reopened as generation {} at commit {}, expected generation {} at commit {}",
                        reopened.generation(),
                        reopened.commit_id().0,
                        expected_generation,
                        expected_commit.0
                    )
                    .into());
                }
                database = reopened;
                checkpoints.push(name);
            }
            kind => return Err(format!("unsupported trace event {kind}").into()),
        }
    }

    metrics.add_storage(database.metrics());
    let started = Instant::now();
    let digest = state_digest(&database, &keys)?;
    metrics.record("state_digest", started);
    metrics.record("replay_total", replay_started);
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "trace": trace_path.file_name().and_then(|name| name.to_str()),
            "trace_sha256": hex_encode(&Sha256::digest(&trace_bytes)),
            "events": expected_seq - 1,
            "commit_id": database.commit_id().0,
            "durable_commit_id": database.commit_id().0,
            "acknowledged_commit_id": acknowledged_commit_id,
            "ambiguous_commit_ids": ambiguous_commit_ids,
            "state_digest": digest,
            "commit_durability": if checkpoint_each_commit {
                "wal_sync_plus_checkpoint_each_commit"
            } else {
                "wal_sync_until_explicit_checkpoint"
            },
            "checkpoints": checkpoints,
            "metrics": metrics.json(),
        }))?
    );
    Ok(())
}

fn state_digest(
    database: &Database,
    keys: &BTreeSet<(u64, u64)>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut canonical = String::new();
    for (tenant_id, record_id) in keys {
        if let Some(value) = database.get(database.commit_id(), Key::new(*tenant_id, *record_id))? {
            canonical.push_str(&format!("{tenant_id}:{record_id}:{}\n", hex_encode(&value)));
        }
    }
    let mut digest = Sha256::new();
    digest.update(canonical.as_bytes());
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn hex_decode(value: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if !value.len().is_multiple_of(2) {
        return Err("hex payload has odd length".into());
    }
    (0..value.len())
        .step_by(2)
        .map(|offset| Ok(u8::from_str_radix(&value[offset..offset + 2], 16)?))
        .collect()
}

fn hex_encode(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}
