//! Exercise contended allocation with ordinary relational primitives.
//!
//! Each request selects available rows through a secondary index and marks
//! them reserved in one transaction. The first wave deliberately prepares
//! against the same snapshot; one publication wins and the others retry from
//! a fresh snapshot after an explicit serialization conflict. This is a
//! resource/correctness diagnostic, not a reservation-specific API or a
//! parallel-writer isolation claim.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DbError, IndexDefinition, IndexId, Key,
    OperationControl, RelationalBackendConfig, RelationalDatabaseConfig, RelationalDatabaseSession,
    RelationalSessionConfig, Row, TableDefinition, TableId, Value,
};
use serde_json::json;
use sha2::{Digest, Sha256};

const TABLE: TableId = TableId(80);
const STATE_INDEX: IndexId = IndexId(80);
const AVAILABLE: u64 = 0;
const RESERVED: u64 = 1;
const DEFAULT_WORKERS: usize = 8;
const DEFAULT_OPERATIONS_PER_WORKER: usize = 16;
const DEFAULT_MAX_QUANTITY: u64 = 2;
const DEFAULT_MAX_RETRIES: usize = 64;
const DEFAULT_SEED: u64 = 0xDB0E_2026_0814;

#[derive(Clone, Copy, Debug)]
struct WorkloadConfig {
    workers: usize,
    operations_per_worker: usize,
    max_quantity: u64,
    max_retries: usize,
    retry_backoff_micros: u64,
    retry_backoff_max_micros: u64,
    seed: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AllocationRequest {
    reservation_id: u64,
    quantity: u64,
}

#[derive(Debug, Default)]
struct WorkerStats {
    successful_operations: u64,
    allocated_units: u64,
    attempts: u64,
    serialization_conflicts: u64,
    latencies_ns: Vec<u64>,
}

fn main() -> Result<()> {
    let workload = parse_arguments()?;
    validate(&workload)?;
    let requests = generate_requests(&workload)?;
    let expected_allocations = expected_allocations(&requests);
    let expected_units = expected_allocations.values().sum::<u64>();
    let trace_digest = digest_requests(&requests);
    let allocation_digest = digest_allocations(&expected_allocations);

    let directory = tempfile::tempdir().context("create allocation workload directory")?;
    let database_directory = directory.path().join("database");
    let session = RelationalDatabaseSession::create(
        RelationalDatabaseConfig::new(config(&database_directory)).with_session_config(
            RelationalSessionConfig {
                max_in_flight: workload.workers,
                admission_timeout: Duration::from_secs(300),
            },
        ),
    )
    .context("create allocation session")?;
    let session = Arc::new(session);
    let control = OperationControl::default();
    session
        .create_table(&control, table())
        .context("create allocation table")?;
    session
        .create_index(
            &control,
            IndexDefinition {
                id: STATE_INDEX,
                table: TABLE,
                columns: vec![ColumnId(2)],
                unique: false,
            },
        )
        .context("create allocation state index")?;
    seed(&session, expected_units)?;

    let first_wave = Arc::new(Barrier::new(workload.workers + 1));
    let started = Instant::now();
    let mut workers = Vec::with_capacity(workload.workers);
    for (worker_id, worker_requests) in requests.iter().enumerate() {
        workers.push(spawn_worker(
            Arc::clone(&session),
            Arc::clone(&first_wave),
            worker_id,
            workload,
            worker_requests.clone(),
        ));
    }
    first_wave.wait();

    let mut stats = WorkerStats::default();
    let mut workload_error = None;
    for worker in workers {
        match worker.join() {
            Ok(Ok(worker_stats)) => {
                stats.successful_operations += worker_stats.successful_operations;
                stats.allocated_units += worker_stats.allocated_units;
                stats.attempts += worker_stats.attempts;
                stats.serialization_conflicts += worker_stats.serialization_conflicts;
                stats.latencies_ns.extend(worker_stats.latencies_ns);
            }
            Ok(Err(error)) => {
                if workload_error.is_none() {
                    workload_error = Some(error);
                }
            }
            Err(_) => {
                if workload_error.is_none() {
                    workload_error = Some(anyhow::anyhow!("allocation worker panicked"));
                }
            }
        }
    }
    if let Some(error) = workload_error {
        return Err(error);
    }
    let elapsed = started.elapsed();

    let control = OperationControl::default();
    let final_commit = session
        .commit_id(&control)
        .context("read allocation commit frontier")?;
    let final_rows = session
        .scan(&control, TABLE, usize::MAX)
        .context("scan final allocation state")?;
    let actual_allocations = validate_final_state(&final_rows, expected_units)?;
    let actual_allocation_digest = digest_allocations(&actual_allocations);
    if actual_allocations != expected_allocations {
        bail!(
            "allocation oracle mismatch: expected {:?}, got {:?}",
            expected_allocations,
            actual_allocations
        );
    }
    if actual_allocation_digest != allocation_digest {
        bail!("allocation digest mismatch after final-state validation");
    }
    if stats.successful_operations != requests.iter().map(Vec::len).sum::<usize>() as u64
        || stats.allocated_units != expected_units
    {
        bail!(
            "successful work {} operations/{} units does not match expected {} operations/{} units",
            stats.successful_operations,
            stats.allocated_units,
            requests.iter().map(Vec::len).sum::<usize>(),
            expected_units
        );
    }

    let status = session
        .admission_status()
        .context("read final allocation admission status")?;
    if status.active_operations != 0 || status.waiting_operations != 0 {
        bail!("allocation session did not drain: {status:?}");
    }
    if status.rejected_operations != 0 {
        bail!(
            "allocation workload unexpectedly rejected {} operations",
            status.rejected_operations
        );
    }

    let session = Arc::try_unwrap(session)
        .map_err(|_| anyhow::anyhow!("allocation session still has worker references"))?;
    session.close().context("close allocation session")?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "experiment": "omendb-project-contended-allocation-v1",
            "evidence_class": "project_api_contended_allocation_diagnostic",
            "hardware_benchmark": false,
            "parallel_writer_claim": false,
            "workers": workload.workers,
            "operations_per_worker": workload.operations_per_worker,
            "max_quantity": workload.max_quantity,
            "max_retries": workload.max_retries,
            "retry_backoff_micros": workload.retry_backoff_micros,
            "retry_backoff_max_micros": workload.retry_backoff_max_micros,
            "seed": workload.seed,
            "units": expected_units,
            "requests": requests.iter().map(Vec::len).sum::<usize>(),
            "trace_sha256": trace_digest,
            "expected_allocation_digest": allocation_digest,
            "actual_allocation_digest": actual_allocation_digest,
            "elapsed_seconds": elapsed.as_secs_f64(),
            "successful_operations": stats.successful_operations,
            "successful_units": stats.allocated_units,
            "attempts": stats.attempts,
            "serialization_conflicts": stats.serialization_conflicts,
            "retries": stats.serialization_conflicts,
            "final_commit": final_commit.0,
            "final_row_count": final_rows.len(),
            "latency": latency_summary(&mut stats.latencies_ns),
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

fn spawn_worker(
    session: Arc<RelationalDatabaseSession>,
    first_wave: Arc<Barrier>,
    worker_id: usize,
    workload: WorkloadConfig,
    requests: Vec<AllocationRequest>,
) -> thread::JoinHandle<Result<WorkerStats>> {
    thread::spawn(move || {
        let mut stats = WorkerStats::default();
        let mut first_wave_complete = false;
        for (operation, request) in requests.into_iter().enumerate() {
            let started = Instant::now();
            let mut attempts = 0;
            loop {
                attempts += 1;
                let synchronize_first_wave = !first_wave_complete && attempts == 1;
                let control = OperationControl::default();
                let wave = Arc::clone(&first_wave);
                let result = session.transaction(&control, move |database, transaction| {
                    let start = [Value::U64(AVAILABLE)];
                    let end = [Value::U64(RESERVED)];
                    let _ = (&start, &end);
                    let selected = transaction.index_scan(database, TABLE, STATE_INDEX);
                    if synchronize_first_wave {
                        wave.wait();
                    }
                    let selected = selected?;
                    if selected.len() != request.quantity as usize {
                        return Err(DbError::InvalidState(format!(
                            "allocation pool exhausted for reservation {}: needed {}, found {}",
                            request.reservation_id,
                            request.quantity,
                            selected.len()
                        )));
                    }
                    for mut row in selected {
                        if row.values.get(1) != Some(&Value::U64(AVAILABLE)) {
                            return Err(DbError::InvalidState(
                                "state index returned a non-available row".to_owned(),
                            ));
                        }
                        row.values[1] = Value::U64(RESERVED);
                        row.values[2] = Value::U64(request.reservation_id);
                        row.values[3] = Value::U64(worker_id as u64);
                        transaction.update(database, TABLE, row)?;
                    }
                    Ok(request.quantity)
                });
                stats.attempts += 1;
                first_wave_complete = true;
                match result {
                    Ok((allocated, _commit)) => {
                        stats.successful_operations += 1;
                        stats.allocated_units += allocated;
                        stats.latencies_ns.push(elapsed_nanos(started));
                        break;
                    }
                    Err(DbError::SerializationConflict { .. }) => {
                        stats.serialization_conflicts += 1;
                        if attempts >= workload.max_retries {
                            bail!(
                                "reservation {} exceeded {} attempts",
                                request.reservation_id,
                                workload.max_retries
                            );
                        }
                        let delay = retry_backoff(attempts, workload);
                        if !delay.is_zero() {
                            thread::sleep(delay);
                        }
                    }
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!(
                                "allocate reservation {} at worker {} operation {}",
                                request.reservation_id, worker_id, operation
                            )
                        });
                    }
                }
            }
        }
        Ok(stats)
    })
}

fn seed(session: &RelationalDatabaseSession, units: u64) -> Result<()> {
    let control = OperationControl::default();
    session
        .transaction(&control, |database, transaction| {
            for unit in 0..units {
                transaction.insert(database, TABLE, unit_row(unit))?;
            }
            Ok(())
        })
        .context("seed available allocation units")?;
    Ok(())
}

fn generate_requests(workload: &WorkloadConfig) -> Result<Vec<Vec<AllocationRequest>>> {
    let mut requests = Vec::with_capacity(workload.workers);
    for worker in 0..workload.workers {
        let mut random = workload.seed ^ (worker as u64 + 1).wrapping_mul(0x9E37_79B9);
        let mut worker_requests = Vec::with_capacity(workload.operations_per_worker);
        for operation in 0..workload.operations_per_worker {
            let reservation_id = (worker as u64)
                .checked_mul(workload.operations_per_worker as u64)
                .and_then(|value| value.checked_add(operation as u64 + 1))
                .ok_or_else(|| anyhow::anyhow!("reservation ID overflow"))?;
            let quantity = next_random(&mut random) % workload.max_quantity + 1;
            worker_requests.push(AllocationRequest {
                reservation_id,
                quantity,
            });
        }
        requests.push(worker_requests);
    }
    Ok(requests)
}

fn expected_allocations(requests: &[Vec<AllocationRequest>]) -> BTreeMap<u64, u64> {
    requests
        .iter()
        .flatten()
        .map(|request| (request.reservation_id, request.quantity))
        .collect()
}

fn validate_final_state(rows: &[Row], expected_units: u64) -> Result<BTreeMap<u64, u64>> {
    if rows.len() as u64 != expected_units {
        bail!(
            "final row count {} does not equal seeded unit count {}",
            rows.len(),
            expected_units
        );
    }
    let mut allocations = BTreeMap::new();
    let mut units = BTreeSet::new();
    for row in rows {
        let [
            Value::U64(_unit_id),
            Value::U64(state),
            Value::U64(reservation),
            Value::U64(_worker),
        ] = row.values.as_slice()
        else {
            bail!("final allocation row has an unexpected shape: {row:?}");
        };
        if *state != RESERVED || *reservation == 0 {
            bail!("final allocation row is not reserved: {row:?}");
        }
        if !units.insert(row.primary) {
            bail!("final allocation state repeated unit {:?}", row.primary);
        }
        *allocations.entry(*reservation).or_insert(0) += 1;
    }
    Ok(allocations)
}

fn digest_requests(requests: &[Vec<AllocationRequest>]) -> String {
    let mut digest = Sha256::new();
    for request in requests.iter().flatten() {
        digest.update(request.reservation_id.to_le_bytes());
        digest.update(request.quantity.to_le_bytes());
    }
    hex_digest(digest.finalize())
}

fn digest_allocations(allocations: &BTreeMap<u64, u64>) -> String {
    let mut digest = Sha256::new();
    for (reservation, quantity) in allocations {
        digest.update(reservation.to_le_bytes());
        digest.update(quantity.to_le_bytes());
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

fn latency_summary(samples: &mut [u64]) -> serde_json::Value {
    samples.sort_unstable();
    let seconds = |nanos: u64| nanos as f64 / 1_000_000_000.0;
    json!({
        "successful_operations": samples.len(),
        "p50_seconds": seconds(percentile(samples, 50, 100)),
        "p95_seconds": seconds(percentile(samples, 95, 100)),
        "p99_seconds": seconds(percentile(samples, 99, 100)),
        "p99_9_seconds": seconds(percentile(samples, 999, 1000)),
        "max_seconds": seconds(samples.last().copied().unwrap_or_default()),
    })
}

fn percentile(samples: &[u64], numerator: usize, denominator: usize) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let rank = samples
        .len()
        .saturating_mul(numerator)
        .saturating_add(denominator.saturating_sub(1))
        / denominator;
    samples[rank.saturating_sub(1).min(samples.len() - 1)]
}

fn table() -> TableDefinition {
    TableDefinition {
        id: TABLE,
        name: "allocation_units".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "unit_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "state".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "reservation_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(4),
                name: "worker_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
        ],
    }
}

fn unit_row(unit: u64) -> Row {
    Row {
        primary: Key::new(TABLE.0, unit),
        values: vec![
            Value::U64(unit),
            Value::U64(AVAILABLE),
            Value::U64(0),
            Value::U64(0),
        ],
    }
}

fn config(directory: &Path) -> RelationalBackendConfig {
    RelationalBackendConfig::new(directory.to_owned())
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

fn retry_backoff(attempt: usize, workload: WorkloadConfig) -> Duration {
    if workload.retry_backoff_micros == 0 || workload.retry_backoff_max_micros == 0 {
        return Duration::ZERO;
    }
    let shift = attempt.saturating_sub(1).min(20) as u32;
    let multiplier = 1_u64 << shift;
    let micros = workload
        .retry_backoff_micros
        .saturating_mul(multiplier)
        .min(workload.retry_backoff_max_micros);
    Duration::from_micros(micros)
}

fn parse_arguments() -> Result<WorkloadConfig> {
    let mut workload = WorkloadConfig {
        workers: DEFAULT_WORKERS,
        operations_per_worker: DEFAULT_OPERATIONS_PER_WORKER,
        max_quantity: DEFAULT_MAX_QUANTITY,
        max_retries: DEFAULT_MAX_RETRIES,
        retry_backoff_micros: 0,
        retry_backoff_max_micros: 0,
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
            "--workers" => workload.workers = value()?.parse().context("invalid --workers")?,
            "--operations" => {
                workload.operations_per_worker = value()?.parse().context("invalid --operations")?
            }
            "--max-quantity" => {
                workload.max_quantity = value()?.parse().context("invalid --max-quantity")?
            }
            "--max-retries" => {
                workload.max_retries = value()?.parse().context("invalid --max-retries")?
            }
            "--retry-backoff-micros" => {
                workload.retry_backoff_micros =
                    value()?.parse().context("invalid --retry-backoff-micros")?
            }
            "--retry-backoff-max-micros" => {
                workload.retry_backoff_max_micros = value()?
                    .parse()
                    .context("invalid --retry-backoff-max-micros")?
            }
            "--seed" => workload.seed = value()?.parse().context("invalid --seed")?,
            "--help" => {
                println!(
                    "usage: project_allocation [--workers N] [--operations N] [--max-quantity N] [--max-retries N] [--retry-backoff-micros N] [--retry-backoff-max-micros N] [--seed N]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument {other}"),
        }
    }
    Ok(workload)
}

fn validate(workload: &WorkloadConfig) -> Result<()> {
    if workload.workers < 2
        || workload.operations_per_worker == 0
        || workload.max_quantity == 0
        || workload.max_retries == 0
        || (workload.retry_backoff_micros > 0 && workload.retry_backoff_max_micros == 0)
        || workload.retry_backoff_micros > workload.retry_backoff_max_micros
    {
        bail!(
            "workers must be at least 2, all workload bounds must be positive, and retry backoff must not exceed its positive cap"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_requests_and_oracle_are_stable() {
        let workload = WorkloadConfig {
            workers: 3,
            operations_per_worker: 4,
            max_quantity: 2,
            max_retries: 8,
            retry_backoff_micros: 0,
            retry_backoff_max_micros: 0,
            seed: 7,
        };
        let requests = generate_requests(&workload).expect("generate requests");
        assert_eq!(digest_requests(&requests), digest_requests(&requests));
        let allocations = expected_allocations(&requests);
        assert_eq!(allocations.len(), 12);
        assert_eq!(allocations.values().sum::<u64>(), 18);
        assert_eq!(
            digest_allocations(&allocations),
            digest_allocations(&allocations)
        );
    }

    #[test]
    fn percentile_uses_nearest_rank_for_tail_samples() {
        let samples = [1, 2, 3, 4];
        assert_eq!(percentile(&samples, 50, 100), 2);
        assert_eq!(percentile(&samples, 95, 100), 4);
        assert_eq!(percentile(&samples, 999, 1000), 4);
        assert_eq!(percentile(&[], 99, 100), 0);
    }

    #[test]
    fn retry_backoff_is_exponential_and_capped() {
        let workload = WorkloadConfig {
            workers: 2,
            operations_per_worker: 1,
            max_quantity: 1,
            max_retries: 4,
            retry_backoff_micros: 100,
            retry_backoff_max_micros: 250,
            seed: 7,
        };
        assert_eq!(retry_backoff(1, workload), Duration::from_micros(100));
        assert_eq!(retry_backoff(2, workload), Duration::from_micros(200));
        assert_eq!(retry_backoff(3, workload), Duration::from_micros(250));
        assert_eq!(retry_backoff(4, workload), Duration::from_micros(250));
    }
}
