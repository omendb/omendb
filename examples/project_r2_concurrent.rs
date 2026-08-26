//! Exercise the project-facing session with concurrent callers using an
//! independent update/delete oracle.
//!
//! This runner qualifies concurrent callers, admission, and indexed reads.
//! The current OmenDB profile still gives writes one exclusive publication
//! lane; this is not evidence for parallel-writer isolation or a production
//! benchmark.

use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use omendb::{
    ColumnId, DbError, IndexDefinition, IndexId, Key, OperationControl, RelationalDatabaseConfig,
    RelationalDatabaseSession, Row, TableId, Value,
};
use serde_json::json;
use sha2::{Digest, Sha256};

#[path = "project_r2_concurrent/support.rs"]
mod support;

use support::{
    config, elapsed_nanos, latency_summary, next_random, parse_arguments, row, table, validate,
};

const TABLE: TableId = TableId(70);
const VALUE_INDEX: IndexId = IndexId(70);
const OWNER_INDEX: IndexId = IndexId(71);
const PAYLOAD_INDEX: IndexId = IndexId(72);

const DEFAULT_WORKERS: usize = 4;
const DEFAULT_OPERATIONS_PER_WORKER: usize = 128;
const DEFAULT_ROWS: u64 = 4_096;
const DEFAULT_HOT_ROWS: u64 = 256;
const DEFAULT_INDEXED_READ_LIMIT: usize = 64;
const DEFAULT_SEED: u64 = 0xDB0E_2026_0813;
const DEFAULT_ADMISSION_TIMEOUT_SECONDS: u64 = 300;

#[derive(Clone, Copy, Debug)]
struct WorkloadConfig {
    workers: usize,
    operations_per_worker: usize,
    rows: u64,
    hot_rows: u64,
    indexed_read_limit: usize,
    seed: u64,
    admission_timeout: Duration,
}

#[derive(Clone, Copy, Debug)]
struct Operation {
    kind: OperationKind,
    key: u64,
    delta: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OperationKind {
    Update,
    Delete,
    IndexedRead,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExpectedRow {
    value: u64,
    owner: u64,
    payload: u64,
}

#[derive(Debug, Default)]
struct WorkerStats {
    updates: u64,
    deletes: u64,
    indexed_reads: u64,
    read_latencies_ns: Vec<u64>,
    write_latencies_ns: Vec<u64>,
}

fn main() -> Result<()> {
    let workload = parse_arguments()?;
    validate(&workload)?;
    let operations = generate_operations(&workload);
    let expected = expected_rows(&workload, &operations);
    let operation_digest = digest_operations(&operations);
    let initial_digest = digest_expected_rows(&initial_rows(workload.rows));

    let directory = tempfile::tempdir().context("create concurrent workload directory")?;
    let database_directory = directory.path().join("database");
    let database_config = RelationalDatabaseConfig::new(config(&database_directory))
        .with_session_config(omendb::RelationalSessionConfig {
            max_in_flight: workload.workers.max(2),
            admission_timeout: workload.admission_timeout,
        });
    let session = RelationalDatabaseSession::create(database_config.clone())
        .context("create project-facing concurrent session")?;
    let session = Arc::new(session);
    let control = OperationControl::default();
    create_schema(&session, &control)?;
    seed(&session, workload.rows)?;

    let barrier = Arc::new(Barrier::new(workload.workers + 1));
    let started = Instant::now();
    let mut workers = Vec::with_capacity(workload.workers);
    for worker_operations in operations.iter() {
        workers.push(spawn_worker(
            Arc::clone(&session),
            Arc::clone(&barrier),
            workload,
            worker_operations.clone(),
        ));
    }
    barrier.wait();

    let mut stats = WorkerStats::default();
    let mut read_latencies_ns = Vec::new();
    let mut write_latencies_ns = Vec::new();
    let mut workload_error = None;
    for worker in workers {
        match worker.join() {
            Ok(Ok(worker_stats)) => {
                stats.updates += worker_stats.updates;
                stats.deletes += worker_stats.deletes;
                stats.indexed_reads += worker_stats.indexed_reads;
                read_latencies_ns.extend(worker_stats.read_latencies_ns);
                write_latencies_ns.extend(worker_stats.write_latencies_ns);
            }
            Ok(Err(error)) => {
                if workload_error.is_none() {
                    workload_error = Some(error);
                }
            }
            Err(_) => {
                if workload_error.is_none() {
                    workload_error = Some(anyhow::anyhow!("concurrent workload worker panicked"));
                }
            }
        }
    }
    if let Some(error) = workload_error {
        return Err(error);
    }
    let elapsed = started.elapsed();

    let final_commit = session
        .commit_id(&control)
        .context("read final concurrent commit frontier")?;
    let final_rows = session
        .scan(&control, TABLE, usize::MAX)
        .context("scan final concurrent state")?;
    assert_expected_state(&final_rows, &expected)?;
    if stats.updates != expected_update_count(&operations)
        || stats.deletes != expected_delete_count(&operations)
    {
        bail!(
            "successful mutation counts {} updates/{} deletes do not match independent trace {} updates/{} deletes",
            stats.updates,
            stats.deletes,
            expected_update_count(&operations),
            expected_delete_count(&operations)
        );
    }
    let admission = session
        .admission_status()
        .context("read final concurrent admission status")?;
    if admission.active_operations != 0 || admission.waiting_operations != 0 {
        bail!("session did not drain: {admission:?}");
    }

    let session = Arc::try_unwrap(session)
        .map_err(|_| anyhow::anyhow!("concurrent session still has worker references"))?;
    session
        .close()
        .context("close concurrent workload session")?;
    let reopened = RelationalDatabaseSession::open(database_config)
        .context("reopen concurrent workload session")?;
    let reopened_commit = reopened
        .commit_id(&control)
        .context("read reopened concurrent frontier")?;
    let reopened_rows = reopened
        .scan(&control, TABLE, usize::MAX)
        .context("scan reopened concurrent state")?;
    assert_expected_state(&reopened_rows, &expected)?;
    reopened
        .close()
        .context("close reopened concurrent session")?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "experiment": "omendb-project-r2-concurrent-v1",
            "evidence_class": "project_api_concurrent_workload",
            "hardware_benchmark": false,
            "parallel_writer_claim": false,
            "serialized_write_lane": true,
            "workers": workload.workers,
            "operations_per_worker": workload.operations_per_worker,
            "total_operation_attempts": total_operations(&operations),
            "rows": workload.rows,
            "hot_rows": workload.hot_rows,
            "indexed_read_limit": workload.indexed_read_limit,
            "seed": workload.seed,
            "operation_trace_sha256": operation_digest,
            "operation_mix": {
                "updates": expected_update_count(&operations),
                "deletes": expected_delete_count(&operations),
                "indexed_reads": expected_indexed_read_count(&operations),
            },
            "elapsed_seconds": elapsed.as_secs_f64(),
            "successful_operations": {
                "updates": stats.updates,
                "deletes": stats.deletes,
                "indexed_reads": stats.indexed_reads,
            },
            "expected": {
                "initial_digest": initial_digest,
                "final_digest": digest_expected_rows(&expected),
                "final_row_count": expected.len(),
            },
            "actual": {
                "final_commit": final_commit.0,
                "final_digest": digest_rows(&final_rows)?,
                "final_row_count": final_rows.len(),
                "reopened_commit": reopened_commit.0,
                "reopened_digest": digest_rows(&reopened_rows)?,
            },
            "latency": {
                "reads": latency_summary(&mut read_latencies_ns),
                "writes": latency_summary(&mut write_latencies_ns),
            },
            "admission": {
                "active_operations": admission.active_operations,
                "waiting_operations": admission.waiting_operations,
                "waiting_writers": admission.waiting_writers,
                "max_in_flight": admission.max_in_flight,
                "completed_operations": admission.completed_operations,
                "rejected_operations": admission.rejected_operations,
                "cancelled_operations": admission.cancelled_operations,
                "deadline_expired_operations": admission.deadline_expired_operations,
                "total_admission_wait_seconds": admission.total_admission_wait.as_secs_f64(),
                "max_admission_wait_seconds": admission.max_admission_wait.as_secs_f64(),
                "total_operation_seconds": admission.total_operation_time.as_secs_f64(),
                "max_operation_seconds": admission.max_operation_time.as_secs_f64(),
            },
        }))?
    );
    Ok(())
}

fn create_schema(session: &RelationalDatabaseSession, control: &OperationControl) -> Result<()> {
    session
        .create_table(control, table())
        .context("create R2 concurrent table")?;
    for index in [
        IndexDefinition {
            id: VALUE_INDEX,
            table: TABLE,
            columns: vec![ColumnId(1)],
            unique: false,
        },
        IndexDefinition {
            id: OWNER_INDEX,
            table: TABLE,
            columns: vec![ColumnId(2)],
            unique: false,
        },
        IndexDefinition {
            id: PAYLOAD_INDEX,
            table: TABLE,
            columns: vec![ColumnId(3)],
            unique: false,
        },
    ] {
        session
            .create_index(control, index)
            .context("create R2 concurrent index")?;
    }
    Ok(())
}

fn seed(session: &RelationalDatabaseSession, rows: u64) -> Result<()> {
    let control = OperationControl::default();
    session
        .transaction(&control, |database, transaction| {
            for key in 0..rows {
                transaction.insert(database, TABLE, row(key, 0, key % 32, key))?;
            }
            Ok(())
        })
        .context("seed R2 concurrent rows")?;
    Ok(())
}

fn spawn_worker(
    session: Arc<RelationalDatabaseSession>,
    barrier: Arc<Barrier>,
    workload: WorkloadConfig,
    operations: Vec<Operation>,
) -> thread::JoinHandle<Result<WorkerStats>> {
    thread::spawn(move || {
        barrier.wait();
        let mut stats = WorkerStats::default();
        for operation in operations.iter().copied() {
            let control = OperationControl::default();
            let started = Instant::now();
            match operation.kind {
                OperationKind::Update => {
                    update_one(&session, &control, operation)?;
                    stats.updates += 1;
                    stats.write_latencies_ns.push(elapsed_nanos(started));
                }
                OperationKind::Delete => {
                    session
                        .delete(&control, TABLE, Key::new(TABLE.0, operation.key))
                        .context("R2 concurrent delete")?;
                    stats.deletes += 1;
                    stats.write_latencies_ns.push(elapsed_nanos(started));
                }
                OperationKind::IndexedRead => {
                    let rows = session.index_scan(&control, TABLE, VALUE_INDEX)?;
                    let rows = &rows[..rows.len().min(workload.indexed_read_limit)];
                    validate_index_read(rows)?;
                    stats.indexed_reads += 1;
                    stats.read_latencies_ns.push(elapsed_nanos(started));
                }
            }
        }
        Ok(stats)
    })
}

fn update_one(
    session: &RelationalDatabaseSession,
    control: &OperationControl,
    operation: Operation,
) -> Result<()> {
    session
        .transaction(control, |database, transaction| {
            let primary = Key::new(TABLE.0, operation.key);
            let mut row = transaction
                .get(database, TABLE, primary)?
                .ok_or_else(|| DbError::InvalidState("R2 update target is missing".to_owned()))?;
            let [Value::U64(value), Value::U64(owner), Value::U64(payload)] = row.values.as_slice()
            else {
                return Err(DbError::InvalidState(
                    "R2 update target has unexpected shape".to_owned(),
                ));
            };
            row.values = vec![
                Value::U64(value.saturating_add(operation.delta)),
                Value::U64((owner.saturating_add(operation.delta)) % 32),
                Value::U64(payload.saturating_add(operation.delta.saturating_mul(3))),
            ];
            transaction.update(database, TABLE, row)?;
            Ok(())
        })
        .context("R2 concurrent update")?;
    Ok(())
}

fn generate_operations(workload: &WorkloadConfig) -> Vec<Vec<Operation>> {
    (0..workload.workers)
        .map(|worker_id| {
            let mut random =
                workload.seed ^ (worker_id as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            (0..workload.operations_per_worker)
                .enumerate()
                .map(|(operation_index, _)| {
                    let sample = next_random(&mut random) % 100;
                    if sample < 60 {
                        Operation {
                            kind: OperationKind::Update,
                            key: next_random(&mut random) % workload.hot_rows,
                            delta: 1 + next_random(&mut random) % 3,
                        }
                    } else if sample < 80 {
                        let global = worker_id * workload.operations_per_worker + operation_index;
                        Operation {
                            kind: OperationKind::Delete,
                            key: workload.rows
                                - workload.workers as u64 * workload.operations_per_worker as u64
                                + global as u64,
                            delta: 0,
                        }
                    } else {
                        Operation {
                            kind: OperationKind::IndexedRead,
                            key: 0,
                            delta: 0,
                        }
                    }
                })
                .collect()
        })
        .collect()
}

fn expected_rows(
    workload: &WorkloadConfig,
    operations: &[Vec<Operation>],
) -> BTreeMap<u64, ExpectedRow> {
    let mut rows = initial_rows(workload.rows);
    for worker in operations {
        for operation in worker {
            match operation.kind {
                OperationKind::Update => {
                    let row = rows
                        .get_mut(&operation.key)
                        .expect("generated update key is in the hot-row set");
                    row.value = row.value.saturating_add(operation.delta);
                    row.owner = (row.owner.saturating_add(operation.delta)) % 32;
                    row.payload = row
                        .payload
                        .saturating_add(operation.delta.saturating_mul(3));
                }
                OperationKind::Delete => {
                    rows.remove(&operation.key);
                }
                OperationKind::IndexedRead => {}
            }
        }
    }
    rows
}

fn initial_rows(rows: u64) -> BTreeMap<u64, ExpectedRow> {
    (0..rows)
        .map(|key| {
            (
                key,
                ExpectedRow {
                    value: 0,
                    owner: key % 32,
                    payload: key,
                },
            )
        })
        .collect()
}

fn assert_expected_state(rows: &[Row], expected: &BTreeMap<u64, ExpectedRow>) -> Result<()> {
    if rows.len() != expected.len() {
        bail!(
            "actual row count {} != expected {}",
            rows.len(),
            expected.len()
        );
    }
    for row in rows {
        let key = record_key(row.primary)?;
        let expected = expected
            .get(&key)
            .ok_or_else(|| anyhow::anyhow!("unexpected row key {key}"))?;
        if row.values
            != vec![
                Value::U64(expected.value),
                Value::U64(expected.owner),
                Value::U64(expected.payload),
            ]
        {
            bail!(
                "row {key} disagrees with independent oracle: {:?}",
                row.values
            );
        }
    }
    Ok(())
}

fn validate_index_read(rows: &[Row]) -> Result<()> {
    let mut keys = std::collections::BTreeSet::new();
    for row in rows {
        let key = record_key(row.primary)?;
        if !keys.insert(key) {
            bail!("indexed read returned duplicate row {key}");
        }
        if !matches!(row.values.first(), Some(Value::U64(_))) {
            bail!("indexed read returned malformed row {key}");
        }
    }
    Ok(())
}

fn expected_update_count(operations: &[Vec<Operation>]) -> u64 {
    count_kind(operations, OperationKind::Update)
}

fn expected_delete_count(operations: &[Vec<Operation>]) -> u64 {
    count_kind(operations, OperationKind::Delete)
}

fn expected_indexed_read_count(operations: &[Vec<Operation>]) -> u64 {
    count_kind(operations, OperationKind::IndexedRead)
}

fn count_kind(operations: &[Vec<Operation>], expected: OperationKind) -> u64 {
    operations
        .iter()
        .flatten()
        .filter(|operation| operation.kind == expected)
        .count() as u64
}

fn total_operations(operations: &[Vec<Operation>]) -> u64 {
    operations.iter().map(Vec::len).sum::<usize>() as u64
}

fn digest_expected_rows(rows: &BTreeMap<u64, ExpectedRow>) -> String {
    let mut digest = Sha256::new();
    for (key, row) in rows {
        digest.update(key.to_le_bytes());
        digest.update(row.value.to_le_bytes());
        digest.update(row.owner.to_le_bytes());
        digest.update(row.payload.to_le_bytes());
    }
    hex_digest(digest.finalize())
}

fn digest_rows(rows: &[Row]) -> Result<String> {
    let mut digest = Sha256::new();
    for row in rows {
        let key = record_key(row.primary)?;
        digest.update(key.to_le_bytes());
        let [Value::U64(value), Value::U64(owner), Value::U64(payload)] = row.values.as_slice()
        else {
            bail!("digest row {key} has unexpected shape");
        };
        digest.update(value.to_le_bytes());
        digest.update(owner.to_le_bytes());
        digest.update(payload.to_le_bytes());
    }
    Ok(hex_digest(digest.finalize()))
}

fn record_key(key: Key) -> Result<u64> {
    if key.0[..8] != TABLE.0.to_be_bytes() {
        bail!("row key has unexpected table prefix: {key:?}");
    }
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&key.0[8..]);
    Ok(u64::from_be_bytes(bytes))
}

fn digest_operations(operations: &[Vec<Operation>]) -> String {
    let mut digest = Sha256::new();
    for worker in operations {
        for operation in worker {
            digest.update([match operation.kind {
                OperationKind::Update => 0,
                OperationKind::Delete => 1,
                OperationKind::IndexedRead => 2,
            }]);
            digest.update(operation.key.to_le_bytes());
            digest.update(operation.delta.to_le_bytes());
        }
    }
    hex_digest(digest.finalize())
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        OperationKind, WorkloadConfig, digest_operations, expected_rows, generate_operations,
    };
    use std::time::Duration;

    #[test]
    fn generated_trace_is_stable_and_mutations_are_disjoint() {
        let workload = WorkloadConfig {
            workers: 4,
            operations_per_worker: 32,
            rows: 512,
            hot_rows: 32,
            indexed_read_limit: 8,
            seed: 7,
            admission_timeout: Duration::from_secs(1),
        };
        let first = generate_operations(&workload);
        let second = generate_operations(&workload);
        assert_eq!(digest_operations(&first), digest_operations(&second));
        let expected = expected_rows(&workload, &first);
        assert!(expected.len() < workload.rows as usize);
        assert!(
            first
                .iter()
                .flatten()
                .any(|operation| operation.kind == OperationKind::Update)
        );
        assert!(
            first
                .iter()
                .flatten()
                .any(|operation| operation.kind == OperationKind::Delete)
        );
    }
}
