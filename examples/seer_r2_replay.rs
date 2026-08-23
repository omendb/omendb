//! Run the canonical R2 trace against the SeerDB-backed typed slice.
//!
//! This is an adoption gate, not a benchmark. It deliberately reuses the
//! existing R2 trace and digest contract while exercising SeerDB's catalog,
//! row/index batch, retained-root, compaction, and reopen paths.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use omendb::{
    ColumnDefinition, ColumnId, ColumnType, IndexDefinition, IndexId, Key, RelationalMutation, Row,
    SeerKernelConfig, SeerRelationalStore, SnapshotLease, TableDefinition, TableId, Value,
};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

#[path = "support/seer_metrics.rs"]
mod seer_metrics;
use seer_metrics::{PhaseStats, phase_json, storage_json};

const DOCUMENTS_TABLE: TableId = TableId(10);
const STATUS_INDEX: IndexId = IndexId(10);
const OWNER_INDEX: IndexId = IndexId(11);
const UPDATED_INDEX: IndexId = IndexId(12);

#[derive(Debug, Default, Deserialize)]
struct TraceEvent {
    seq: u64,
    kind: String,
    rows: Option<u64>,
    snapshot: Option<String>,
    name: Option<String>,
    expect: Option<String>,
    operations: Option<Vec<Operation>>,
}

#[derive(Debug, Deserialize)]
struct Operation {
    op: String,
    document_id: u64,
    status: Option<String>,
    owner_id: Option<u64>,
}

fn main() -> Result<()> {
    let mut arguments = env::args().skip(1);
    let trace_path = arguments
        .next()
        .map(PathBuf::from)
        .context("usage: seer_r2_replay <trace.jsonl> [--expected-digest <sha256>]")?;
    let mut expected_digest = None;
    while let Some(argument) = arguments.next() {
        if argument == "--expected-digest" {
            expected_digest = Some(
                arguments
                    .next()
                    .context("--expected-digest requires a SHA-256 value")?,
            );
        } else {
            bail!("unknown argument {argument}");
        }
    }

    let replay_started = Instant::now();
    let setup_started = Instant::now();
    let directory = tempfile::tempdir().context("create temporary SeerDB directory")?;
    let config = SeerKernelConfig::new(directory.path().join("seerdb"));
    let mut store = SeerRelationalStore::create(config.clone()).context("create SeerDB store")?;
    let mut phases = BTreeMap::new();
    phases
        .entry("setup")
        .or_insert_with(PhaseStats::default)
        .record(setup_started);
    let mut snapshots = BTreeMap::<String, SnapshotLease>::new();
    let mut expected_seq = 1_u64;
    let mut logical_commit_id = 0_u64;
    let mut initial_digest = None;
    let mut snapshot_digest_before_maintenance = None;
    let mut snapshot_digest_after_maintenance = None;

    for (line_number, line) in fs::read_to_string(&trace_path)
        .with_context(|| format!("read {}", trace_path.display()))?
        .lines()
        .enumerate()
    {
        if line.trim().is_empty() {
            continue;
        }
        let event: TraceEvent = serde_json::from_str(line)
            .with_context(|| format!("parse {} line {}", trace_path.display(), line_number + 1))?;
        if event.seq != expected_seq {
            bail!("trace sequence {} != {}", event.seq, expected_seq);
        }
        expected_seq += 1;
        match event.kind.as_str() {
            "seed" => {
                let started = Instant::now();
                logical_commit_id = seed(&mut store, event.rows.context("R2 seed has no rows")?)
                    .with_context(|| format!("R2 seed at seq {}", event.seq))?;
                let digest = state_digest_at(&store, store.commit_id())?;
                if event.expect.as_deref() == Some("state-digest-0") {
                    initial_digest = Some(digest);
                }
                phases
                    .entry("seed")
                    .or_insert_with(PhaseStats::default)
                    .record(started);
            }
            "retain" => {
                if event.snapshot.as_deref() != Some("current") {
                    bail!("R2 SeerDB runner only retains the current snapshot");
                }
                let name = event.name.context("R2 retain has no name")?;
                let snapshot = store.commit_id();
                snapshots.insert(name, store.retain(snapshot)?);
            }
            "commit" => {
                logical_commit_id = apply_commit(
                    &mut store,
                    event
                        .operations
                        .as_ref()
                        .context("R2 commit has no operations")?,
                    logical_commit_id,
                    &mut phases,
                )
                .with_context(|| format!("R2 commit at seq {}", event.seq))?;
            }
            "scan" => {
                let started = Instant::now();
                let name = event.snapshot.context("R2 scan has no snapshot")?;
                let lease = snapshots.get(&name).context("unknown R2 snapshot")?;
                let snapshot = lease.commit();
                let digest = state_digest_at(&store, snapshot)?;
                if event.expect.as_deref() == Some("state-digest-0")
                    && initial_digest.as_deref() != Some(digest.as_str())
                {
                    bail!("retained snapshot digest changed before maintenance");
                }
                snapshot_digest_before_maintenance = Some(digest);
                phases
                    .entry("scan")
                    .or_insert_with(PhaseStats::default)
                    .record(started);
            }
            "maintenance" => {
                let started = Instant::now();
                store
                    .compact()
                    .with_context(|| format!("SeerDB compaction at seq {}", event.seq))?;
                let name = snapshots
                    .keys()
                    .next()
                    .context("maintenance requires a retained snapshot")?;
                let snapshot = snapshots.get(name).expect("snapshot key exists").commit();
                snapshot_digest_after_maintenance = Some(state_digest_at(&store, snapshot)?);
                phases
                    .entry("maintenance")
                    .or_insert_with(PhaseStats::default)
                    .record(started);
            }
            "release" => {
                let name = event.snapshot.context("R2 release has no snapshot")?;
                let lease = snapshots.remove(&name).context("unknown R2 snapshot")?;
                store.release(lease)?;
            }
            kind => bail!("unsupported R2 event {kind}"),
        }
    }

    let current_snapshot = store.commit_id();
    let current_index_started = Instant::now();
    verify_indexes_at(&store, current_snapshot).context("verify current R2 indexes")?;
    phases
        .entry("current_index_verify")
        .or_insert_with(PhaseStats::default)
        .record(current_index_started);
    let current_digest_started = Instant::now();
    let digest = state_digest_at(&store, current_snapshot)?;
    phases
        .entry("current_state_digest")
        .or_insert_with(PhaseStats::default)
        .record(current_digest_started);
    if let Some(expected) = expected_digest
        && expected != digest
    {
        bail!("state digest {digest} != expected {expected}");
    }
    if snapshot_digest_before_maintenance != snapshot_digest_after_maintenance {
        bail!("maintenance changed the retained snapshot digest");
    }
    let checkpoint_started = Instant::now();
    store.checkpoint().context("SeerDB checkpoint")?;
    phases
        .entry("checkpoint")
        .or_insert_with(PhaseStats::default)
        .record(checkpoint_started);
    let verify_started = Instant::now();
    store.verify().context("SeerDB verification")?;
    phases
        .entry("verify")
        .or_insert_with(PhaseStats::default)
        .record(verify_started);
    let final_metrics = store.metrics().context("SeerDB metrics")?;
    drop(store);

    let reopen_started = Instant::now();
    let reopen_open_started = Instant::now();
    let reopened = SeerRelationalStore::open(config).context("reopen SeerDB store")?;
    phases
        .entry("reopen_open")
        .or_insert_with(PhaseStats::default)
        .record(reopen_open_started);
    let reopened_snapshot = reopened.commit_id();
    let reopen_index_started = Instant::now();
    verify_indexes_at(&reopened, reopened_snapshot).context("verify reopened R2 indexes")?;
    phases
        .entry("reopen_index_verify")
        .or_insert_with(PhaseStats::default)
        .record(reopen_index_started);
    let reopen_digest_started = Instant::now();
    let reopened_digest = state_digest_at(&reopened, reopened_snapshot)?;
    phases
        .entry("reopen_state_digest")
        .or_insert_with(PhaseStats::default)
        .record(reopen_digest_started);
    if reopened_digest != digest {
        bail!("reopened digest {reopened_digest} != committed digest {digest}");
    }
    let reopened_metrics = reopened.metrics().context("reopened SeerDB metrics")?;
    phases
        .entry("reopen_verify")
        .or_insert_with(PhaseStats::default)
        .record(reopen_started);
    phases
        .entry("replay_total")
        .or_insert_with(PhaseStats::default)
        .record(replay_started);

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "trace": trace_path.file_name().and_then(|name| name.to_str()),
            "events": expected_seq - 1,
            "logical_commit_id": logical_commit_id,
            "storage_commit_id": current_snapshot.0,
            "state_digest": digest,
            "reopened_digest": reopened_digest,
            "retained_snapshot_before_maintenance": snapshot_digest_before_maintenance,
            "retained_snapshot_after_maintenance": snapshot_digest_after_maintenance,
            "metrics": {
                "phases": phase_json(&phases),
                "storage": storage_json(&final_metrics),
                "reopen_storage": storage_json(&reopened_metrics),
            },
        }))?
    );
    Ok(())
}

fn seed(store: &mut SeerRelationalStore, rows: u64) -> Result<u64> {
    if rows == 0 {
        bail!("R2 seed must contain at least one row");
    }
    store.create_table(documents_table())?;
    for index in documents_indexes() {
        store.create_index(index)?;
    }
    let mutations = (1..=rows).map(|document_id| {
        let tenant_id = ((document_id - 1) % 100) + 1;
        let owner_id = document_id % 32;
        RelationalMutation::Insert {
            table: DOCUMENTS_TABLE,
            row: document_row(
                document_id,
                tenant_id,
                "new",
                owner_id,
                format!("payload-{document_id:08}"),
                1,
            ),
        }
    });
    store.commit_batch(mutations)?;
    Ok(1)
}

fn apply_commit(
    store: &mut SeerRelationalStore,
    operations: &[Operation],
    logical_commit_id: u64,
    phases: &mut BTreeMap<&'static str, PhaseStats>,
) -> Result<u64> {
    if operations.is_empty() {
        bail!("R2 commit has no operations");
    }
    let commit_started = Instant::now();
    let next_commit = logical_commit_id
        .checked_add(1)
        .context("R2 logical commit ID overflow")?;
    let snapshot = store.commit_id();
    let mut mutations = Vec::with_capacity(operations.len());
    for operation in operations {
        let key = document_key(operation.document_id);
        let previous = store
            .get(DOCUMENTS_TABLE, snapshot, key)?
            .context("R2 operation targets an absent document")?;
        match operation.op.as_str() {
            "update" => mutations.push(RelationalMutation::Update {
                table: DOCUMENTS_TABLE,
                row: document_row(
                    operation.document_id,
                    value_u64(&previous, 0)?,
                    operation.status.as_deref().context("missing status")?,
                    operation.owner_id.context("missing owner_id")?,
                    value_bytes(&previous, 4)?,
                    next_commit,
                ),
            }),
            "delete" => mutations.push(RelationalMutation::Delete {
                table: DOCUMENTS_TABLE,
                primary: key,
            }),
            operation => bail!("unsupported R2 operation {operation}"),
        }
    }
    phases
        .entry("commit_prepare")
        .or_default()
        .record(commit_started);
    let publish_started = Instant::now();
    store.commit_batch(mutations)?;
    phases
        .entry("commit_publish")
        .or_default()
        .record(publish_started);
    phases.entry("commit").or_default().record(commit_started);
    Ok(next_commit)
}

fn documents_table() -> TableDefinition {
    TableDefinition {
        id: DOCUMENTS_TABLE,
        name: "r2_documents".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "tenant_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "document_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "status".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(4),
                name: "owner_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(5),
                name: "payload".to_owned(),
                data_type: ColumnType::Bytes,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(6),
                name: "updated_commit".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
        ],
    }
}

fn documents_indexes() -> [IndexDefinition; 3] {
    [
        IndexDefinition {
            id: STATUS_INDEX,
            table: DOCUMENTS_TABLE,
            columns: vec![ColumnId(1), ColumnId(3)],
            unique: false,
        },
        IndexDefinition {
            id: OWNER_INDEX,
            table: DOCUMENTS_TABLE,
            columns: vec![ColumnId(1), ColumnId(4)],
            unique: false,
        },
        IndexDefinition {
            id: UPDATED_INDEX,
            table: DOCUMENTS_TABLE,
            columns: vec![ColumnId(1), ColumnId(6)],
            unique: false,
        },
    ]
}

fn document_key(document_id: u64) -> Key {
    Key::new(DOCUMENTS_TABLE.0, document_id)
}

fn document_row<P: AsRef<[u8]>>(
    document_id: u64,
    tenant_id: u64,
    status: &str,
    owner_id: u64,
    payload: P,
    updated_commit: u64,
) -> Row {
    Row {
        primary: document_key(document_id),
        values: vec![
            Value::U64(tenant_id),
            Value::U64(document_id),
            Value::Text(status.to_owned()),
            Value::U64(owner_id),
            Value::Bytes(payload.as_ref().to_vec()),
            Value::U64(updated_commit),
        ],
    }
}

fn state_digest_at(store: &SeerRelationalStore, snapshot: omendb::CommitId) -> Result<String> {
    let mut canonical = String::new();
    for row in store.scan(DOCUMENTS_TABLE, snapshot, usize::MAX)? {
        canonical.push_str(&format!(
            "{}|{}|{}|{}|{}\n",
            value_u64(&row, 1)?,
            value_text(&row, 2)?,
            value_u64(&row, 3)?,
            String::from_utf8(value_bytes(&row, 4)?)?,
            value_u64(&row, 5)?,
        ));
    }
    let mut digest = Sha256::new();
    digest.update(canonical.as_bytes());
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn verify_indexes_at(store: &SeerRelationalStore, snapshot: omendb::CommitId) -> Result<()> {
    let rows = store.scan(DOCUMENTS_TABLE, snapshot, usize::MAX)?;
    for index in [STATUS_INDEX, OWNER_INDEX, UPDATED_INDEX] {
        let mut expected = rows.iter().map(|row| row.primary).collect::<Vec<_>>();
        expected.sort_unstable();
        let mut actual = store
            .index_scan(DOCUMENTS_TABLE, snapshot, index, None, None, usize::MAX)?
            .into_iter()
            .map(|row| row.primary)
            .collect::<Vec<_>>();
        actual.sort_unstable();
        if actual != expected {
            bail!(
                "R2 index {index:?} diverged at snapshot {} (expected {}, actual {})",
                snapshot.0,
                expected.len(),
                actual.len()
            );
        }
        for row in &rows {
            let matches =
                store.index_get(DOCUMENTS_TABLE, snapshot, index, &index_values(row, index)?)?;
            if !matches
                .iter()
                .any(|candidate| candidate.primary == row.primary)
            {
                bail!("R2 index {index:?} missing row {:?}", row.primary);
            }
        }
    }
    Ok(())
}

fn index_values(row: &Row, index: IndexId) -> Result<Vec<Value>> {
    match index {
        STATUS_INDEX => Ok(vec![
            Value::U64(value_u64(row, 0)?),
            Value::Text(value_text(row, 2)?.to_owned()),
        ]),
        OWNER_INDEX => Ok(vec![
            Value::U64(value_u64(row, 0)?),
            Value::U64(value_u64(row, 3)?),
        ]),
        UPDATED_INDEX => Ok(vec![
            Value::U64(value_u64(row, 0)?),
            Value::U64(value_u64(row, 5)?),
        ]),
        _ => bail!("unsupported R2 index {index:?}"),
    }
}

fn value_u64(row: &Row, position: usize) -> Result<u64> {
    match row.values.get(position) {
        Some(Value::U64(value)) => Ok(*value),
        other => bail!("expected U64 at column {position}, got {other:?}"),
    }
}

fn value_text(row: &Row, position: usize) -> Result<&str> {
    match row.values.get(position) {
        Some(Value::Text(value)) => Ok(value),
        other => bail!("expected Text at column {position}, got {other:?}"),
    }
}

fn value_bytes(row: &Row, position: usize) -> Result<Vec<u8>> {
    match row.values.get(position) {
        Some(Value::Bytes(value)) => Ok(value.clone()),
        other => bail!("expected Bytes at column {position}, got {other:?}"),
    }
}
