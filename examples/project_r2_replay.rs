//! Run the generated R2 trace through OmenDB's project-facing API.
//!
//! The runner keeps an independent in-memory model and uses only
//! `RelationalDatabase` types. It is a bounded adoption harness, not a
//! publish-grade performance benchmark.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use omendb::{
    ColumnDefinition, ColumnId, ColumnType, CommitId, DatabaseConfig, IndexDefinition, IndexId,
    Key, RelationalBackendConfig, RelationalBackendKind, RelationalCompactionReport,
    RelationalDatabase, RelationalDatabaseTransaction, RelationalMetrics, RelationalSnapshotLease,
    Row, SeerKernelConfig, TableDefinition, TableId, Value,
};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};

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

#[derive(Clone, Debug, Eq, PartialEq)]
struct Document {
    tenant_id: u64,
    status: String,
    owner_id: u64,
    payload: Vec<u8>,
    updated_commit: u64,
}

type Model = BTreeMap<u64, Document>;

#[derive(Debug, Default)]
struct Phase {
    count: u64,
    total: Duration,
}

#[derive(Debug, Default)]
struct Phases {
    values: BTreeMap<&'static str, Phase>,
}

impl Phases {
    fn record(&mut self, name: &'static str, started: Instant) {
        let phase = self.values.entry(name).or_default();
        phase.count = phase.count.saturating_add(1);
        phase.total = phase.total.saturating_add(started.elapsed());
    }

    fn json(&self) -> JsonValue {
        self.values
            .iter()
            .map(|(name, phase)| {
                (
                    (*name).to_owned(),
                    json!({
                        "count": phase.count,
                        "total_seconds": phase.total.as_secs_f64(),
                    }),
                )
            })
            .collect::<serde_json::Map<String, JsonValue>>()
            .into()
    }
}

#[derive(Debug)]
struct RetainedSnapshot {
    commit: CommitId,
    lease: RelationalSnapshotLease,
}

fn main() -> Result<()> {
    let (trace_path, backend, expected_digest) = parse_arguments()?;
    let directory = tempfile::tempdir().context("create temporary OmenDB directory")?;
    let config = config(backend, &directory.path().join("database"));
    let mut database = RelationalDatabase::create(config.clone())
        .context("create project-facing OmenDB database")?;
    let mut phases = Phases::default();
    let replay_started = Instant::now();
    let mut model = Model::new();
    let mut retained = BTreeMap::<String, RetainedSnapshot>::new();
    let mut expected_seq = 1_u64;
    let mut logical_commit_id = 0_u64;
    let mut initial_digest = None;
    let mut retained_before_maintenance = None;
    let mut retained_after_maintenance = None;
    let mut maintenance_reports = Vec::new();

    let trace = fs::read_to_string(&trace_path)
        .with_context(|| format!("read {}", trace_path.display()))?;
    for (line_number, line) in trace.lines().enumerate() {
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
                logical_commit_id = seed(
                    &mut database,
                    event.rows.context("R2 seed has no rows")?,
                    &mut model,
                )?;
                phases.record("seed", started);
                let digest = digest_model(&model);
                if event.expect.as_deref() == Some("state-digest-0") {
                    initial_digest = Some(digest);
                }
                verify_model(&database, &model, database.commit_id())?;
            }
            "retain" => {
                if event.snapshot.as_deref() != Some("current") {
                    bail!("project R2 runner only retains the current snapshot");
                }
                let name = event.name.context("R2 retain has no name")?;
                let commit = database.commit_id();
                let lease = database.retain(commit)?;
                retained.insert(name, RetainedSnapshot { commit, lease });
            }
            "commit" => {
                let operations = event
                    .operations
                    .as_ref()
                    .context("R2 commit has no operations")?;
                logical_commit_id = apply_commit(
                    &mut database,
                    &mut model,
                    operations,
                    logical_commit_id,
                    &mut phases,
                )?;
            }
            "scan" => {
                let name = event.snapshot.context("R2 scan has no snapshot")?;
                let snapshot = retained.get(&name).context("unknown R2 snapshot")?;
                let started = Instant::now();
                let digest = digest_database(&database, snapshot.commit)?;
                phases.record("retained_scan", started);
                if event.expect.as_deref() == Some("state-digest-0")
                    && initial_digest.as_deref() != Some(digest.as_str())
                {
                    bail!("retained snapshot digest changed before maintenance");
                }
                retained_before_maintenance = Some(digest);
            }
            "maintenance" => {
                let started = Instant::now();
                let report = database.compact().context("R2 project compaction")?;
                phases.record("maintenance", started);
                maintenance_reports.push(compaction_json(&report));
                let snapshot = retained
                    .values()
                    .next()
                    .context("maintenance requires a retained snapshot")?;
                let started = Instant::now();
                retained_after_maintenance = Some(digest_database(&database, snapshot.commit)?);
                phases.record("retained_verify", started);
            }
            "release" => {
                let name = event.snapshot.context("R2 release has no snapshot")?;
                let snapshot = retained.remove(&name).context("unknown R2 snapshot")?;
                database.release(snapshot.lease)?;
            }
            kind => bail!("unsupported R2 event {kind}"),
        }
    }

    if !retained.is_empty() {
        bail!("R2 trace left retained snapshots unreleased");
    }
    if retained_before_maintenance != retained_after_maintenance {
        bail!("maintenance changed the retained snapshot digest");
    }

    let expected_model_digest = digest_model(&model);
    if let Some(expected) = expected_digest.as_deref()
        && expected != expected_model_digest
    {
        bail!("independent model digest {expected_model_digest} != expected {expected}");
    }
    let current_snapshot = database.commit_id();
    let started = Instant::now();
    verify_model(&database, &model, current_snapshot)?;
    phases.record("current_verify", started);
    let started = Instant::now();
    let verification = database.verify().context("project-facing verification")?;
    phases.record("verify", started);
    let started = Instant::now();
    let checkpoint = database.checkpoint().context("project-facing checkpoint")?;
    phases.record("checkpoint", started);
    let current_metrics = database.metrics().context("project-facing metrics")?;
    let started = Instant::now();
    database.close().context("close project-facing database")?;
    phases.record("close", started);

    let started = Instant::now();
    let mut reopened =
        RelationalDatabase::open(config).context("reopen project-facing database")?;
    phases.record("reopen", started);
    let reopened_snapshot = reopened.commit_id();
    if reopened_snapshot != current_snapshot {
        bail!(
            "reopened commit {} != committed commit {}",
            reopened_snapshot.0,
            current_snapshot.0
        );
    }
    let started = Instant::now();
    verify_model(&reopened, &model, reopened_snapshot)?;
    phases.record("reopen_verify", started);
    let reopened_digest = digest_database(&reopened, reopened_snapshot)?;
    let reopened_verification = reopened.verify().context("reopened project verification")?;
    let reopened_metrics = reopened.metrics().context("reopened project metrics")?;
    reopened
        .close()
        .context("close reopened project-facing database")?;
    phases.record("replay_total", replay_started);

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "trace": trace_path.file_name().and_then(|name| name.to_str()),
            "events": expected_seq - 1,
            "backend": format!("{backend:?}"),
            "public_facade": true,
            "logical_commit_id": logical_commit_id,
            "storage_commit_id": current_snapshot.0,
            "state_digest": expected_model_digest,
            "reopened_digest": reopened_digest,
            "retained_snapshot_before_maintenance": retained_before_maintenance,
            "retained_snapshot_after_maintenance": retained_after_maintenance,
            "maintenance_reports": maintenance_reports,
            "verification": verification_json(&verification),
            "reopened_verification": verification_json(&reopened_verification),
            "checkpoint": checkpoint_json(&checkpoint),
            "metrics": {
                "phases": phases.json(),
                "current": metrics_json(&current_metrics),
                "reopened": metrics_json(&reopened_metrics),
            },
        }))?
    );
    Ok(())
}

fn parse_arguments() -> Result<(PathBuf, RelationalBackendKind, Option<String>)> {
    let mut arguments = env::args().skip(1);
    let trace_path = arguments
        .next()
        .map(PathBuf::from)
        .context("usage: project_r2_replay <trace.jsonl> --backend <temporary|seer> [--expected-digest <sha256>]")?;
    let mut backend = None;
    let mut expected_digest = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--backend" => {
                let value = arguments.next().context("--backend requires a value")?;
                backend = Some(match value.as_str() {
                    "temporary" => RelationalBackendKind::Temporary,
                    "seer" => RelationalBackendKind::Seer,
                    _ => bail!("unsupported project backend {value}"),
                });
            }
            "--expected-digest" => {
                expected_digest = Some(
                    arguments
                        .next()
                        .context("--expected-digest requires a SHA-256 value")?,
                );
            }
            _ => bail!("unknown argument {argument}"),
        }
    }
    Ok((
        trace_path,
        backend.unwrap_or(RelationalBackendKind::Seer),
        expected_digest,
    ))
}

fn config(kind: RelationalBackendKind, directory: &Path) -> RelationalBackendConfig {
    match kind {
        RelationalBackendKind::Temporary => RelationalBackendConfig::Temporary(DatabaseConfig {
            directory: directory.to_owned(),
        }),
        RelationalBackendKind::Seer => {
            RelationalBackendConfig::Seer(SeerKernelConfig::new(directory.to_owned()))
        }
    }
}

fn seed(database: &mut RelationalDatabase, rows: u64, model: &mut Model) -> Result<u64> {
    if rows == 0 {
        bail!("R2 seed must contain at least one row");
    }
    database.create_table(documents_table())?;
    for index in documents_indexes() {
        database.create_index(index)?;
    }
    let ((), _storage_commit) = database.transaction(|database, transaction| {
        for document_id in 1..=rows {
            let document = Document {
                tenant_id: ((document_id - 1) % 100) + 1,
                status: "new".to_owned(),
                owner_id: document_id % 32,
                payload: format!("payload-{document_id:08}").into_bytes(),
                updated_commit: 1,
            };
            transaction.insert(
                database,
                DOCUMENTS_TABLE,
                document_row(document_id, &document),
            )?;
            model.insert(document_id, document);
        }
        Ok(())
    })?;
    Ok(1)
}

fn apply_commit(
    database: &mut RelationalDatabase,
    model: &mut Model,
    operations: &[Operation],
    logical_commit_id: u64,
    phases: &mut Phases,
) -> Result<u64> {
    if operations.is_empty() {
        bail!("R2 commit has no operations");
    }
    let next_logical_commit = logical_commit_id
        .checked_add(1)
        .context("R2 logical commit ID overflow")?;
    let mut candidate = model.clone();
    let started = Instant::now();
    let mut transaction = database.begin()?;
    for operation in operations {
        stage_operation(
            database,
            &mut transaction,
            &mut candidate,
            operation,
            next_logical_commit,
        )?;
    }
    phases.record("transaction_stage", started);
    let started = Instant::now();
    transaction.commit(database)?;
    phases.record("transaction_commit", started);
    *model = candidate;
    Ok(next_logical_commit)
}

fn stage_operation(
    database: &RelationalDatabase,
    transaction: &mut RelationalDatabaseTransaction,
    model: &mut Model,
    operation: &Operation,
    updated_commit: u64,
) -> Result<()> {
    let key = document_key(operation.document_id);
    let expected = model
        .get(&operation.document_id)
        .context("R2 operation targets an absent document")?;
    let actual = transaction.get(database, DOCUMENTS_TABLE, key)?;
    if actual.as_ref() != Some(&document_row(operation.document_id, expected)) {
        bail!("transaction read disagreed with independent R2 model");
    }
    match operation.op.as_str() {
        "update" => {
            let mut document = expected.clone();
            document.status = operation
                .status
                .clone()
                .context("R2 update has no status")?;
            document.owner_id = operation.owner_id.context("R2 update has no owner_id")?;
            document.updated_commit = updated_commit;
            transaction.update(
                database,
                DOCUMENTS_TABLE,
                document_row(operation.document_id, &document),
            )?;
            model.insert(operation.document_id, document);
        }
        "delete" => {
            model.remove(&operation.document_id);
            transaction.delete(database, DOCUMENTS_TABLE, key)?;
        }
        other => bail!("unsupported R2 operation {other}"),
    }
    Ok(())
}

fn verify_model(database: &RelationalDatabase, model: &Model, snapshot: CommitId) -> Result<()> {
    let expected_rows = model
        .iter()
        .map(|(document_id, document)| document_row(*document_id, document))
        .collect::<Vec<_>>();
    let actual_rows = database.scan(DOCUMENTS_TABLE, snapshot, usize::MAX)?;
    if actual_rows != expected_rows {
        bail!("R2 table state diverged at snapshot {}", snapshot.0);
    }
    let expected_digest = digest_model(model);
    let actual_digest = digest_rows(&actual_rows)?;
    if actual_digest != expected_digest {
        bail!("R2 database digest {actual_digest} != model digest {expected_digest}");
    }
    verify_indexes(database, snapshot, &actual_rows)
}

fn verify_indexes(database: &RelationalDatabase, snapshot: CommitId, rows: &[Row]) -> Result<()> {
    let mut expected_primaries = rows.iter().map(|row| row.primary).collect::<Vec<_>>();
    expected_primaries.sort_unstable();
    for index in documents_indexes().map(|definition| definition.id) {
        let mut actual_primaries = database
            .index_scan(DOCUMENTS_TABLE, snapshot, index, None, None, usize::MAX)?
            .into_iter()
            .map(|row| row.primary)
            .collect::<Vec<_>>();
        actual_primaries.sort_unstable();
        if actual_primaries != expected_primaries {
            bail!("R2 index {index:?} diverged at snapshot {}", snapshot.0);
        }
        for row in rows.iter().take(8) {
            let matches =
                database.index_get(DOCUMENTS_TABLE, snapshot, index, &index_values(row, index)?)?;
            if !matches
                .iter()
                .any(|candidate| candidate.primary == row.primary)
            {
                bail!("R2 index {index:?} point lookup missed {:?}", row.primary);
            }
        }
    }
    Ok(())
}

fn documents_table() -> TableDefinition {
    TableDefinition {
        id: DOCUMENTS_TABLE,
        name: "r2_documents".to_owned(),
        columns: vec![
            column(1, "tenant_id", ColumnType::U64),
            column(2, "document_id", ColumnType::U64),
            column(3, "status", ColumnType::Text),
            column(4, "owner_id", ColumnType::U64),
            column(5, "payload", ColumnType::Bytes),
            column(6, "updated_commit", ColumnType::U64),
        ],
    }
}

fn column(id: u16, name: &str, data_type: ColumnType) -> ColumnDefinition {
    ColumnDefinition {
        id: ColumnId(id),
        name: name.to_owned(),
        data_type,
        nullable: false,
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

fn document_row(document_id: u64, document: &Document) -> Row {
    Row {
        primary: document_key(document_id),
        values: vec![
            Value::U64(document.tenant_id),
            Value::U64(document_id),
            Value::Text(document.status.clone()),
            Value::U64(document.owner_id),
            Value::Bytes(document.payload.clone()),
            Value::U64(document.updated_commit),
        ],
    }
}

fn digest_model(model: &Model) -> String {
    let rows = model
        .iter()
        .map(|(document_id, document)| document_row(*document_id, document))
        .collect::<Vec<_>>();
    digest_rows(&rows).expect("model rows are valid")
}

fn digest_database(database: &RelationalDatabase, snapshot: CommitId) -> Result<String> {
    digest_rows(&database.scan(DOCUMENTS_TABLE, snapshot, usize::MAX)?)
}

fn digest_rows(rows: &[Row]) -> Result<String> {
    let mut canonical = String::new();
    for row in rows {
        canonical.push_str(&format!(
            "{}|{}|{}|{}|{}\n",
            value_u64(row, 1)?,
            value_text(row, 2)?,
            value_u64(row, 3)?,
            String::from_utf8(value_bytes(row, 4)?)?,
            value_u64(row, 5)?,
        ));
    }
    Ok(Sha256::digest(canonical.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
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

fn compaction_json(report: &RelationalCompactionReport) -> JsonValue {
    json!({
        "before_commit": report.before.commit.0,
        "after_commit": report.after.commit.0,
        "row_keys_considered": report.row_keys_considered,
        "index_keys_considered": report.index_keys_considered,
        "row_fragments_reclaimed": report.row_fragments_reclaimed,
        "index_fragments_reclaimed": report.index_fragments_reclaimed,
        "data_bytes_before": report.data_bytes_before,
        "data_bytes_after": report.data_bytes_after,
        "reclaimed_pages": report.reclaimed_pages,
        "relocated_pages": report.relocated_pages,
    })
}

fn metrics_json(metrics: &RelationalMetrics) -> JsonValue {
    json!({
        "backend": format!("{:?}", metrics.backend),
        "commit": metrics.commit.0,
        "wal_bytes": metrics.wal_bytes,
        "syncs": metrics.syncs,
        "logical_page_reads": metrics.logical_page_reads,
        "physical_page_reads": metrics.physical_page_reads,
        "physical_page_writes": metrics.physical_page_writes,
        "data_bytes": metrics.data_bytes,
        "blob_bytes": metrics.blob_bytes,
        "publication": metrics.publication.map(|publication| json!({
            "wal_bytes_written": publication.wal_bytes_written,
            "data_bytes_written": publication.data_bytes_written,
            "metadata_bytes_written": publication.metadata_bytes_written,
            "blob_bytes_written": publication.blob_bytes_written,
            "history_bytes_written": publication.history_bytes_written,
            "manifest_bytes_written": publication.manifest_bytes_written,
            "candidate_prepare_ns": publication.candidate_prepare_ns,
            "wal_write_ns": publication.wal_write_ns,
            "admission_ns": publication.admission_ns,
            "data_flush_ns": publication.data_flush_ns,
            "metadata_write_ns": publication.metadata_write_ns,
            "blob_write_ns": publication.blob_write_ns,
            "history_write_ns": publication.history_write_ns,
            "directory_sync_ns": publication.directory_sync_ns,
            "manifest_write_ns": publication.manifest_write_ns,
            "manifest_mirror_ns": publication.manifest_mirror_ns,
            "cleanup_ns": publication.cleanup_ns,
        })),
    })
}

fn checkpoint_json(report: &omendb::RelationalCheckpointReport) -> JsonValue {
    json!({
        "before_commit": report.before.commit.0,
        "after_commit": report.after.commit.0,
        "verified_physical_pages": report.verified_physical_pages,
        "data_bytes": report.data_bytes,
        "blob_bytes": report.blob_bytes,
        "wal_bytes": report.wal_bytes,
        "reclaimable_pages": report.reclaimable_pages,
    })
}

fn verification_json(report: &omendb::RelationalVerificationReport) -> JsonValue {
    json!({
        "backend": format!("{:?}", report.backend),
        "commit": report.commit.0,
        "catalog_generation": report.catalog_generation,
        "verified_tables": report.verified_tables,
        "verified_indexes": report.verified_indexes,
        "verified_rows": report.verified_rows,
        "verified_index_entries": report.verified_index_entries,
        "physical_pages": report.physical_pages,
        "data_bytes": report.data_bytes,
        "blob_bytes": report.blob_bytes,
        "wal_bytes": report.wal_bytes,
    })
}
