//! Run a bounded hot-key/range workload through the project-facing session.
//!
//! This is a resource and ownership diagnostic, not a publish-grade benchmark.
//! Writer threads intentionally share the current serialized publication lane;
//! the runner measures the resulting admission and permit-hold behavior
//! without selecting a future parallel-writer protocol.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DatabaseConfig, DbError, IndexDefinition, IndexId, Key,
    OperationControl, RelationalBackendConfig, RelationalBackendKind, RelationalDatabaseConfig,
    RelationalDatabaseSession, RelationalSessionConfig, Row, SeerKernelConfig, TableDefinition,
    TableId, Value,
};
use serde_json::json;
use sha2::{Digest, Sha256};

const TABLE: TableId = TableId(50);
const VALUE_INDEX: IndexId = IndexId(50);
const DEFAULT_READERS: usize = 4;
const DEFAULT_WRITERS: usize = 4;
const DEFAULT_OPERATIONS: usize = 128;
const DEFAULT_BATCH_SIZE: usize = 1;
const DEFAULT_KEYS: u64 = 64;
const DEFAULT_HOT_KEYS: u64 = 4;
const DEFAULT_SEED: u64 = 0xDB0E_2026_0812;

#[derive(Clone, Copy, Debug)]
struct WorkloadConfig {
    backend: RelationalBackendKind,
    readers: usize,
    writers: usize,
    operations: usize,
    batch_size: usize,
    keys: u64,
    hot_keys: u64,
    seed: u64,
}

#[derive(Debug, Default)]
struct WorkerStats {
    point_reads: u64,
    range_reads: u64,
    writes: u64,
    read_latencies_ns: Vec<u64>,
    write_latencies_ns: Vec<u64>,
}

fn main() -> Result<()> {
    let workload = parse_arguments()?;
    validate(&workload)?;

    let directory = tempfile::tempdir().context("create temporary workload directory")?;
    let database_directory = directory.path().join("database");
    let session = RelationalDatabaseSession::create(
        RelationalDatabaseConfig::new(config(workload.backend, &database_directory))
            .with_session_config(RelationalSessionConfig {
                max_in_flight: workload.readers.max(2),
                admission_timeout: Duration::from_secs(60),
            }),
    )
    .context("create project-facing session")?;
    let session = Arc::new(session);
    let control = OperationControl::default();
    session
        .create_table(&control, table())
        .context("create contention table")?;
    session
        .create_index(
            &control,
            IndexDefinition {
                id: VALUE_INDEX,
                table: TABLE,
                columns: vec![ColumnId(1)],
                unique: false,
            },
        )
        .context("create value index")?;
    seed(&session, workload.keys)?;

    let started = Instant::now();
    let barrier = Arc::new(Barrier::new(workload.readers + workload.writers + 1));
    let mut workers = Vec::with_capacity(workload.readers + workload.writers);
    for worker_id in 0..workload.readers {
        workers.push(spawn_reader(
            Arc::clone(&session),
            Arc::clone(&barrier),
            worker_id,
            workload,
        ));
    }
    for worker_id in 0..workload.writers {
        workers.push(spawn_writer(
            Arc::clone(&session),
            Arc::clone(&barrier),
            worker_id,
            workload,
        ));
    }
    barrier.wait();

    let mut stats = WorkerStats::default();
    let mut read_latencies_ns = Vec::new();
    let mut write_latencies_ns = Vec::new();
    for worker in workers {
        let worker_stats = worker
            .join()
            .map_err(|_| anyhow::anyhow!("contention worker panicked"))??;
        stats.point_reads += worker_stats.point_reads;
        stats.range_reads += worker_stats.range_reads;
        stats.writes += worker_stats.writes;
        read_latencies_ns.extend(worker_stats.read_latencies_ns);
        write_latencies_ns.extend(worker_stats.write_latencies_ns);
    }
    let elapsed = started.elapsed();

    let final_commit = session
        .commit_id(&control)
        .context("read final commit frontier")?;
    let final_rows = session
        .read(&control, |database| {
            database.scan(TABLE, final_commit, usize::MAX)
        })
        .context("scan final contention state")?;
    if final_rows.len() != workload.keys as usize {
        bail!(
            "final row count {} != configured key count {}",
            final_rows.len(),
            workload.keys
        );
    }
    let final_digest = digest_rows(&final_rows)?;
    let final_sum = value_sum(&final_rows)?;
    if final_sum != stats.writes {
        bail!(
            "final value sum {} != successful writer operations {}",
            final_sum,
            stats.writes
        );
    }
    let status = session
        .admission_status()
        .context("read final admission status")?;
    if status.active_operations != 0 || status.waiting_operations != 0 {
        bail!("session did not drain: {status:?}");
    }
    if status.rejected_operations != 0 {
        bail!(
            "workload unexpectedly rejected {} operations",
            status.rejected_operations
        );
    }

    let session = Arc::try_unwrap(session)
        .map_err(|_| anyhow::anyhow!("contention session still has worker references"))?;
    session.close().context("close contention session")?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "experiment": "omendb-project-hot-key-range-v0",
            "evidence_class": "project_api_resource_diagnostic",
            "hardware_benchmark": false,
            "parallel_writer_claim": false,
            "backend": format!("{:?}", workload.backend),
            "readers": workload.readers,
            "writers": workload.writers,
            "operations_per_worker": workload.operations,
            "write_batch_size": workload.batch_size,
            "keys": workload.keys,
            "hot_keys": workload.hot_keys,
            "seed": workload.seed,
            "elapsed_seconds": elapsed.as_secs_f64(),
            "point_reads": stats.point_reads,
            "range_reads": stats.range_reads,
            "successful_writes": stats.writes,
            "final_commit": final_commit.0,
            "final_row_count": final_rows.len(),
            "final_value_sum": final_sum,
            "final_digest": final_digest,
            "latency": {
                "reads": latency_summary(&mut read_latencies_ns),
                "writes": latency_summary(&mut write_latencies_ns),
            },
            "admission": {
                "active_operations": status.active_operations,
                "waiting_operations": status.waiting_operations,
                "waiting_writers": status.waiting_writers,
                "max_in_flight": status.max_in_flight,
                "completed_operations": status.completed_operations,
                "rejected_operations": status.rejected_operations,
                "cancelled_operations": status.cancelled_operations,
                "deadline_expired_operations": status.deadline_expired_operations,
                "total_admission_wait_seconds": status.total_admission_wait.as_secs_f64(),
                "max_admission_wait_seconds": status.max_admission_wait.as_secs_f64(),
                "total_operation_seconds": status.total_operation_time.as_secs_f64(),
                "max_operation_seconds": status.max_operation_time.as_secs_f64(),
            },
        }))?
    );
    Ok(())
}

fn spawn_reader(
    session: Arc<RelationalDatabaseSession>,
    barrier: Arc<Barrier>,
    worker_id: usize,
    workload: WorkloadConfig,
) -> thread::JoinHandle<Result<WorkerStats>> {
    thread::spawn(move || {
        barrier.wait();
        let mut random = workload.seed ^ (worker_id as u64 + 1).wrapping_mul(0x9E37_79B9);
        let mut stats = WorkerStats::default();
        for operation in 0..workload.operations {
            let control = OperationControl::default();
            let key = choose_key(&mut random, workload.keys, workload.hot_keys);
            let started = Instant::now();
            if operation % 4 == 0 {
                let start = [Value::U64(0)];
                session
                    .read(&control, |database| {
                        let snapshot = database.commit_id();
                        database.index_scan(TABLE, snapshot, VALUE_INDEX, Some(&start), None, 16)
                    })
                    .context("indexed range read")?;
                stats.range_reads += 1;
            } else {
                session
                    .read(&control, |database| {
                        let snapshot = database.commit_id();
                        database.get(TABLE, snapshot, Key::new(TABLE.0, key))
                    })
                    .context("hot-key point read")?;
                stats.point_reads += 1;
            }
            stats.read_latencies_ns.push(elapsed_nanos(started));
        }
        Ok(stats)
    })
}

fn spawn_writer(
    session: Arc<RelationalDatabaseSession>,
    barrier: Arc<Barrier>,
    worker_id: usize,
    workload: WorkloadConfig,
) -> thread::JoinHandle<Result<WorkerStats>> {
    thread::spawn(move || {
        barrier.wait();
        let mut random =
            workload.seed ^ (worker_id as u64 + 101).wrapping_mul(0xD1B5_4A32_D192_ED03);
        let mut stats = WorkerStats::default();
        for batch_start in (0..workload.operations).step_by(workload.batch_size) {
            let batch_len = (workload.operations - batch_start).min(workload.batch_size);
            let control = OperationControl::default();
            let mut increments = BTreeMap::new();
            for _ in 0..batch_len {
                let key = choose_key(&mut random, workload.keys, workload.hot_keys);
                let entry = increments.entry(key).or_insert(0_u64);
                *entry = entry.saturating_add(1);
            }
            let started = Instant::now();
            session
                .transaction(&control, |database, transaction| {
                    for (key, increment) in &increments {
                        let primary = Key::new(TABLE.0, *key);
                        let mut row =
                            transaction.get(database, TABLE, primary)?.ok_or_else(|| {
                                DbError::InvalidState("writer target row missing".to_owned())
                            })?;
                        let value = match row.values.first() {
                            Some(Value::U64(value)) => *value,
                            other => {
                                return Err(DbError::InvalidState(format!(
                                    "writer target has unexpected value {other:?}"
                                )));
                            }
                        };
                        row.values[0] = Value::U64(value.saturating_add(*increment));
                        transaction.update(database, TABLE, row)?;
                    }
                    Ok(())
                })
                .context("hot-key update transaction")?;
            stats.writes += batch_len as u64;
            stats.write_latencies_ns.push(elapsed_nanos(started));
        }
        Ok(stats)
    })
}

fn seed(session: &RelationalDatabaseSession, keys: u64) -> Result<()> {
    let control = OperationControl::default();
    session
        .transaction(&control, |database, transaction| {
            for key in 0..keys {
                transaction.insert(database, TABLE, row(key, 0))?;
            }
            Ok(())
        })
        .context("seed contention rows")?;
    Ok(())
}

fn choose_key(random: &mut u64, keys: u64, hot_keys: u64) -> u64 {
    let sample = next_random(random) % 100;
    if sample < 80 {
        next_random(random) % hot_keys
    } else {
        hot_keys + next_random(random) % (keys - hot_keys)
    }
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 7;
    *state ^= *state >> 9;
    *state ^= *state << 8;
    *state
}

fn elapsed_nanos(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

fn percentile_nanos(sorted_samples: &[u64], numerator: usize, denominator: usize) -> u64 {
    if sorted_samples.is_empty() {
        return 0;
    }
    let rank = sorted_samples
        .len()
        .saturating_mul(numerator)
        .saturating_add(denominator.saturating_sub(1))
        / denominator;
    sorted_samples[rank.saturating_sub(1).min(sorted_samples.len() - 1)]
}

fn latency_summary(samples: &mut [u64]) -> serde_json::Value {
    samples.sort_unstable();
    let nanos_to_seconds = |nanos: u64| nanos as f64 / 1_000_000_000.0;
    json!({
        "successful_operations": samples.len(),
        "p50_seconds": nanos_to_seconds(percentile_nanos(samples, 50, 100)),
        "p95_seconds": nanos_to_seconds(percentile_nanos(samples, 95, 100)),
        "p99_seconds": nanos_to_seconds(percentile_nanos(samples, 99, 100)),
        "p99_9_seconds": nanos_to_seconds(percentile_nanos(samples, 999, 1000)),
        "max_seconds": nanos_to_seconds(samples.last().copied().unwrap_or_default()),
    })
}

fn table() -> TableDefinition {
    TableDefinition {
        id: TABLE,
        name: "contention_items".to_owned(),
        columns: vec![ColumnDefinition {
            id: ColumnId(1),
            name: "value".to_owned(),
            data_type: ColumnType::U64,
            nullable: false,
        }],
    }
}

fn row(key: u64, value: u64) -> Row {
    Row {
        primary: Key::new(TABLE.0, key),
        values: vec![Value::U64(value)],
    }
}

fn value_sum(rows: &[Row]) -> Result<u64> {
    rows.iter()
        .try_fold(0_u64, |sum, row| match row.values.first() {
            Some(Value::U64(value)) => sum.checked_add(*value).context("final value sum overflow"),
            other => bail!("final row has unexpected value {other:?}"),
        })
}

fn digest_rows(rows: &[Row]) -> Result<String> {
    let mut digest = Sha256::new();
    for row in rows {
        digest.update(row.primary.0);
        match row.values.first() {
            Some(Value::U64(value)) => digest.update(value.to_le_bytes()),
            other => bail!("digest row has unexpected value {other:?}"),
        }
    }
    let bytes = digest.finalize();
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
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

fn parse_arguments() -> Result<WorkloadConfig> {
    let mut workload = WorkloadConfig {
        backend: RelationalBackendKind::Seer,
        readers: DEFAULT_READERS,
        writers: DEFAULT_WRITERS,
        operations: DEFAULT_OPERATIONS,
        batch_size: DEFAULT_BATCH_SIZE,
        keys: DEFAULT_KEYS,
        hot_keys: DEFAULT_HOT_KEYS,
        seed: DEFAULT_SEED,
    };
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        let mut value = || {
            arguments
                .next()
                .with_context(|| format!("{argument} requires a value"))
        };
        match argument.as_str() {
            "--backend" => {
                workload.backend = match value()?.as_str() {
                    "temporary" => RelationalBackendKind::Temporary,
                    "seer" => RelationalBackendKind::Seer,
                    other => bail!("unsupported backend {other}"),
                };
            }
            "--readers" => workload.readers = value()?.parse().context("invalid --readers")?,
            "--writers" => workload.writers = value()?.parse().context("invalid --writers")?,
            "--operations" => {
                workload.operations = value()?.parse().context("invalid --operations")?
            }
            "--batch-size" => {
                workload.batch_size = value()?.parse().context("invalid --batch-size")?
            }
            "--keys" => workload.keys = value()?.parse().context("invalid --keys")?,
            "--hot-keys" => workload.hot_keys = value()?.parse().context("invalid --hot-keys")?,
            "--seed" => workload.seed = value()?.parse().context("invalid --seed")?,
            "--help" => {
                println!(
                    "usage: project_contention [--backend temporary|seer] [--readers N] [--writers N] [--operations N] [--batch-size N] [--keys N] [--hot-keys N] [--seed N]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument {other}"),
        }
    }
    Ok(workload)
}

fn validate(workload: &WorkloadConfig) -> Result<()> {
    if workload.readers == 0
        || workload.writers == 0
        || workload.operations == 0
        || workload.batch_size == 0
    {
        bail!("readers, writers, operations, and batch-size must be positive");
    }
    if workload.keys == 0 || workload.hot_keys == 0 || workload.hot_keys > workload.keys {
        bail!("require 0 < hot-keys <= keys");
    }
    if workload.hot_keys == workload.keys {
        bail!("keys must exceed hot-keys so the cold range is non-empty");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::percentile_nanos;

    #[test]
    fn percentile_uses_nearest_rank_for_tail_samples() {
        let samples = [1, 2, 3, 4];
        assert_eq!(percentile_nanos(&samples, 50, 100), 2);
        assert_eq!(percentile_nanos(&samples, 95, 100), 4);
        assert_eq!(percentile_nanos(&samples, 999, 1000), 4);
        assert_eq!(percentile_nanos(&[], 99, 100), 0);
    }
}
