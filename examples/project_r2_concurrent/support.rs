use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use omendb::{
    ColumnDefinition, ColumnId, ColumnType, Key, RelationalBackendConfig, Row, TableDefinition,
    Value,
};
use serde_json::json;

use super::{
    DEFAULT_ADMISSION_TIMEOUT_SECONDS, DEFAULT_HOT_ROWS, DEFAULT_INDEXED_READ_LIMIT,
    DEFAULT_OPERATIONS_PER_WORKER, DEFAULT_ROWS, DEFAULT_SEED, DEFAULT_WORKERS, TABLE,
    WorkloadConfig,
};

pub(super) fn table() -> TableDefinition {
    TableDefinition {
        id: TABLE,
        name: "r2_documents".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "value".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "owner".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "payload".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
        ],
    }
}

pub(super) fn row(key: u64, value: u64, owner: u64, payload: u64) -> Row {
    Row {
        primary: Key::new(TABLE.0, key),
        values: vec![Value::U64(value), Value::U64(owner), Value::U64(payload)],
    }
}

pub(super) fn config(directory: &Path) -> RelationalBackendConfig {
    RelationalBackendConfig::new(directory.to_owned())
}

pub(super) fn parse_arguments() -> Result<WorkloadConfig> {
    let mut workload = WorkloadConfig {
        workers: DEFAULT_WORKERS,
        operations_per_worker: DEFAULT_OPERATIONS_PER_WORKER,
        rows: DEFAULT_ROWS,
        hot_rows: DEFAULT_HOT_ROWS,
        indexed_read_limit: DEFAULT_INDEXED_READ_LIMIT,
        seed: DEFAULT_SEED,
        admission_timeout: Duration::from_secs(DEFAULT_ADMISSION_TIMEOUT_SECONDS),
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
            "--operations-per-worker" => {
                workload.operations_per_worker = value()?.parse().context("invalid operations")?
            }
            "--rows" => workload.rows = value()?.parse().context("invalid --rows")?,
            "--hot-rows" => workload.hot_rows = value()?.parse().context("invalid --hot-rows")?,
            "--indexed-read-limit" => {
                workload.indexed_read_limit =
                    value()?.parse().context("invalid indexed read limit")?
            }
            "--seed" => workload.seed = value()?.parse().context("invalid --seed")?,
            "--admission-timeout-seconds" => {
                workload.admission_timeout =
                    Duration::from_secs(value()?.parse().context("invalid admission timeout")?)
            }
            "--help" => {
                println!(
                    "usage: project_r2_concurrent [--workers N] [--operations-per-worker N] [--rows N] [--hot-rows N] [--indexed-read-limit N] [--seed N] [--admission-timeout-seconds N]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument {other}"),
        }
    }
    Ok(workload)
}

pub(super) fn validate(workload: &WorkloadConfig) -> Result<()> {
    if workload.workers == 0
        || workload.operations_per_worker == 0
        || workload.rows == 0
        || workload.hot_rows == 0
        || workload.hot_rows > workload.rows
        || workload.indexed_read_limit == 0
    {
        bail!("workers, operations, rows, hot-rows, and read limits must be positive");
    }
    let total = workload.workers as u64 * workload.operations_per_worker as u64;
    if total >= workload.rows.saturating_sub(workload.hot_rows) {
        bail!("rows must leave a disjoint delete range for the operation trace");
    }
    Ok(())
}

pub(super) fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 7;
    *state ^= *state >> 9;
    *state ^= *state << 8;
    *state
}

pub(super) fn elapsed_nanos(started: Instant) -> u64 {
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

pub(super) fn latency_summary(samples: &mut [u64]) -> serde_json::Value {
    samples.sort_unstable();
    let seconds = |nanos: u64| nanos as f64 / 1_000_000_000.0;
    json!({
        "successful_operations": samples.len(),
        "p50_seconds": seconds(percentile_nanos(samples, 50, 100)),
        "p95_seconds": seconds(percentile_nanos(samples, 95, 100)),
        "p99_seconds": seconds(percentile_nanos(samples, 99, 100)),
        "p99_9_seconds": seconds(percentile_nanos(samples, 999, 1000)),
        "max_seconds": seconds(samples.last().copied().unwrap_or_default()),
    })
}
