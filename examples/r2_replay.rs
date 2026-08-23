use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use omendb::{
    ColumnDefinition, ColumnId, ColumnType, CompactionBudget, DatabaseConfig, IndexDefinition,
    IndexId, Key, NoFaults, RelationalMutation, RelationalStore, Row, TableDefinition, TableId,
    Value,
};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

mod support;

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
    budget: Option<Budget>,
}

#[derive(Debug, Deserialize)]
struct Operation {
    op: String,
    document_id: u64,
    status: Option<String>,
    owner_id: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct Budget {
    max_row_keys: usize,
    max_index_keys: usize,
}

fn main() -> Result<()> {
    let mut arguments = env::args().skip(1);
    let trace_path = arguments
        .next()
        .map(PathBuf::from)
        .context("usage: r2_replay <trace.jsonl> [--expected-digest <sha256>]")?;
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

    let directory = tempfile::tempdir().context("create temporary OmenDB directory")?;
    let mut store = RelationalStore::create(DatabaseConfig {
        directory: directory.path().to_path_buf(),
    })
    .context("create OmenDB relational store")?;
    let checkpoint_each_commit = support::checkpoint_each_commit();
    let mut faults = NoFaults;
    let mut metrics = support::ReplayMetrics::default();
    let replay_started = Instant::now();
    let mut snapshots = BTreeMap::new();
    let mut expected_seq = 1_u64;
    let mut logical_commit_id = 0_u64;
    let mut initial_digest = None;
    let mut snapshot_digest_before_maintenance = None;
    let mut snapshot_digest_after_maintenance = None;
    let mut maintenance_reports = Vec::new();

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
                logical_commit_id = seed(
                    &mut store,
                    &mut faults,
                    event.rows.context("R2 seed has no rows")?,
                    &mut metrics,
                    checkpoint_each_commit,
                )?;
                let started = Instant::now();
                let snapshot = store.commit_id();
                let digest = state_digest_at(&store, snapshot)?;
                metrics.record("seed_verify", started);
                if event.expect.as_deref() == Some("state-digest-0") {
                    initial_digest = Some(digest);
                }
            }
            "retain" => {
                if event.snapshot.as_deref() != Some("current") {
                    bail!("R2 smoke adapter only retains the current snapshot");
                }
                let name = event.name.context("R2 retain has no name")?;
                let snapshot = store.commit_id();
                store.retain(snapshot)?;
                snapshots.insert(name, snapshot);
            }
            "commit" => {
                logical_commit_id = apply_commit(
                    &mut store,
                    &mut faults,
                    event
                        .operations
                        .as_ref()
                        .context("R2 commit has no operations")?,
                    logical_commit_id,
                    &mut metrics,
                    checkpoint_each_commit,
                )?;
            }
            "scan" => {
                let name = event.snapshot.context("R2 scan has no snapshot")?;
                let snapshot = *snapshots.get(&name).context("unknown R2 snapshot")?;
                let started = Instant::now();
                let digest = state_digest_at(&store, snapshot)?;
                metrics.record("snapshot_scan", started);
                if event.expect.as_deref() == Some("state-digest-0")
                    && initial_digest.as_deref() != Some(digest.as_str())
                {
                    bail!("retained snapshot digest changed before maintenance");
                }
                snapshot_digest_before_maintenance = Some(digest);
            }
            "maintenance" => {
                let budget = event.budget.context("R2 maintenance has no budget")?;
                let started = Instant::now();
                let report = store.compact_with_budget(CompactionBudget {
                    max_row_keys: budget.max_row_keys,
                    max_index_keys: budget.max_index_keys,
                })?;
                metrics.record("compaction", started);
                maintenance_reports.push(json!({
                    "row_keys_considered": report.row_keys_considered,
                    "index_keys_considered": report.index_keys_considered,
                    "row_fragments_reclaimed": report.row_fragments_reclaimed,
                    "index_fragments_reclaimed": report.index_fragments_reclaimed,
                    "foreground_reserve_preserved": true,
                }));
                let name = snapshots
                    .keys()
                    .next()
                    .context("maintenance requires a retained snapshot")?;
                let snapshot = snapshots[name];
                let started = Instant::now();
                snapshot_digest_after_maintenance = Some(state_digest_at(&store, snapshot)?);
                metrics.record("snapshot_verify", started);
            }
            "release" => {
                let name = event.snapshot.context("R2 release has no snapshot")?;
                let snapshot = snapshots.remove(&name).context("unknown R2 snapshot")?;
                store.release(snapshot);
            }
            kind => bail!("unsupported R2 event {kind}"),
        }
    }

    metrics.add_storage(store.metrics());
    let started = Instant::now();
    let current_snapshot = store.commit_id();
    verify_indexes_at(&store, current_snapshot)?;
    metrics.record("index_verify", started);
    let started = Instant::now();
    let digest = state_digest_at(&store, current_snapshot)?;
    metrics.record("state_digest", started);
    metrics.record("replay_total", replay_started);
    if let Some(expected) = expected_digest
        && expected != digest
    {
        bail!("state digest {digest} != expected {expected}");
    }
    if snapshot_digest_before_maintenance != snapshot_digest_after_maintenance {
        bail!("maintenance changed the retained snapshot digest");
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "trace": trace_path.file_name().and_then(|name| name.to_str()),
            "events": expected_seq - 1,
            "commit_id": logical_commit_id,
            "initial_digest": initial_digest,
            "state_digest": digest,
            "retained_snapshot_before_maintenance": snapshot_digest_before_maintenance,
            "retained_snapshot_after_maintenance": snapshot_digest_after_maintenance,
            "commit_durability": if checkpoint_each_commit {
                "wal_sync_plus_checkpoint_each_commit"
            } else {
                "wal_sync_until_explicit_checkpoint"
            },
            "maintenance_reports": maintenance_reports,
            "metrics": metrics.json(),
        }))?
    );
    Ok(())
}

fn seed(
    store: &mut RelationalStore,
    faults: &mut NoFaults,
    rows: u64,
    metrics: &mut support::ReplayMetrics,
    checkpoint_each_commit: bool,
) -> Result<u64> {
    if rows == 0 {
        bail!("R2 seed must contain at least one row");
    }
    store.create_table(documents_table())?;
    for index in documents_indexes() {
        store.create_index(index, faults)?;
    }
    let mutations = (1..=rows)
        .map(|document_id| {
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
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    store
        .commit_batch(mutations, faults)
        .context("seed R2 rows")?;
    metrics.record("seed_commit", started);
    if checkpoint_each_commit {
        let started = Instant::now();
        store.checkpoint(faults).context("checkpoint R2 seed")?;
        metrics.record("seed_checkpoint", started);
    }
    Ok(1)
}

fn apply_commit(
    store: &mut RelationalStore,
    faults: &mut NoFaults,
    operations: &[Operation],
    logical_commit_id: u64,
    metrics: &mut support::ReplayMetrics,
    checkpoint_each_commit: bool,
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
            "update" => {
                let status = operation
                    .status
                    .as_deref()
                    .context("R2 update has no status")?;
                let owner_id = operation.owner_id.context("R2 update has no owner_id")?;
                mutations.push(RelationalMutation::Update {
                    table: DOCUMENTS_TABLE,
                    row: document_row(
                        operation.document_id,
                        value_u64(&previous, 0)?,
                        status,
                        owner_id,
                        value_bytes(&previous, 4)?,
                        next_commit,
                    ),
                });
            }
            "delete" => mutations.push(RelationalMutation::Delete {
                table: DOCUMENTS_TABLE,
                primary: key,
            }),
            operation => bail!("unsupported R2 operation {operation}"),
        }
    }
    metrics.record("commit_prepare", commit_started);
    let publish_started = Instant::now();
    store
        .commit_batch(mutations, faults)
        .context("commit R2 operations")?;
    metrics.record("commit_publish", publish_started);
    metrics.record("commit", commit_started);
    if checkpoint_each_commit {
        let started = Instant::now();
        store
            .checkpoint(faults)
            .context("checkpoint R2 operations")?;
        metrics.record("commit_checkpoint", started);
    }
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

fn state_digest_at(store: &RelationalStore, snapshot: omendb::CommitId) -> Result<String> {
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

fn verify_indexes_at(store: &RelationalStore, snapshot: omendb::CommitId) -> Result<()> {
    let rows = store.scan(DOCUMENTS_TABLE, snapshot, usize::MAX)?;
    for index in [STATUS_INDEX, OWNER_INDEX, UPDATED_INDEX] {
        let mut expected_primaries = rows.iter().map(|row| row.primary).collect::<Vec<_>>();
        expected_primaries.sort_unstable();
        let mut actual_primaries = store
            .index_scan(DOCUMENTS_TABLE, snapshot, index, None, None, usize::MAX)?
            .into_iter()
            .map(|row| row.primary)
            .collect::<Vec<_>>();
        actual_primaries.sort_unstable();
        if actual_primaries != expected_primaries {
            bail!("R2 index {index:?} diverged at snapshot {}", snapshot.0);
        }
        for row in &rows {
            let matches =
                store.index_get(DOCUMENTS_TABLE, snapshot, index, &index_values(row, index)?)?;
            if !matches
                .iter()
                .any(|candidate| candidate.primary == row.primary)
            {
                bail!(
                    "R2 index {index:?} is missing document {:?} at snapshot {}",
                    row.primary,
                    snapshot.0
                );
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
