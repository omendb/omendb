//! Reproducible single-client OLTP baseline for the relational alpha.
//!
//! The workload is intentionally small and transparent: point reads and
//! primary-key updates over one table, with a deterministic key stream and an
//! independent final-state oracle. It measures the public SQL path, including
//! parsing and transaction admission. It is a baseline harness, not a claim
//! that OmenDB is competitive until the workload is run and profiled on the
//! target hardware.
//!
//! Usage:
//! ```text
//! cargo run --release --example alpha_oltp -- --backend all --rows 512 --operations 1000
//! ```

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use omendb::{
    DatabaseConfig, DbError, RelationalBackendConfig, RelationalDatabase, SeerKernelConfig, Value,
};
use rusqlite::{Connection, params};
use serde_json::json;

const DEFAULT_ROWS: usize = 512;
const DEFAULT_OPERATIONS: usize = 1_000;
const DEFAULT_READ_PERCENT: u64 = 80;
const DEFAULT_BATCH_SIZE: usize = 1;
const DEFAULT_SEED: u64 = 0x4F4C_5450_414C_5048;
const CREATE_TABLE: &str = "CREATE TABLE accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)";
const OMENDB_SELECT: &str = "SELECT balance FROM accounts WHERE id = $1";
const OMENDB_UPDATE: &str = "UPDATE accounts SET balance = $2 WHERE id = $1";
const SQLITE_SELECT: &str = "SELECT balance FROM accounts WHERE id = ?1";
const SQLITE_UPDATE: &str = "UPDATE accounts SET balance = ?2 WHERE id = ?1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Backend {
    Temporary,
    Seer,
    Sqlite,
}

impl Backend {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "temporary" => Ok(Self::Temporary),
            "seer" => Ok(Self::Seer),
            "sqlite" => Ok(Self::Sqlite),
            other => bail!("unsupported backend {other}; use temporary, seer, sqlite, or all"),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Temporary => "temporary",
            Self::Seer => "seer",
            Self::Sqlite => "sqlite",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Workload {
    rows: usize,
    operations: usize,
    read_percent: u64,
    batch_size: usize,
    seed: u64,
}

#[derive(Debug)]
struct RunResult {
    backend: Backend,
    workload: Workload,
    reads: usize,
    writes: usize,
    elapsed_nanos: u128,
    latencies_nanos: Vec<u64>,
    final_checksum: i128,
    database_bytes: u64,
}

fn main() -> Result<()> {
    let (backend, workload) = parse_arguments()?;
    validate(workload)?;
    let temp = tempfile::tempdir().context("create benchmark directory")?;

    let backends = match backend {
        None => vec![Backend::Temporary, Backend::Seer, Backend::Sqlite],
        Some(backend) => vec![backend],
    };
    for (index, backend) in backends.iter().copied().enumerate() {
        let result = match backend {
            Backend::Temporary => run_omendb(
                Backend::Temporary,
                RelationalBackendConfig::Temporary(DatabaseConfig {
                    directory: temp.path().join("temporary"),
                }),
                temp.path().join("temporary"),
                workload,
            )?,
            Backend::Seer => run_omendb(
                Backend::Seer,
                RelationalBackendConfig::Seer(SeerKernelConfig::new(temp.path().join("seer"))),
                temp.path().join("seer"),
                workload,
            )?,
            Backend::Sqlite => run_sqlite(temp.path().join("sqlite.db"), workload)?,
        };
        println!("{}", serde_json::to_string_pretty(&result_json(&result))?);
        if index + 1 != backends.len() {
            println!();
        }
    }
    Ok(())
}

fn run_omendb(
    backend: Backend,
    config: RelationalBackendConfig,
    database_directory: impl AsRef<Path>,
    workload: Workload,
) -> Result<RunResult> {
    let mut database = RelationalDatabase::create(config).context("create OmenDB database")?;
    database
        .execute_sql(CREATE_TABLE)
        .context("create benchmark table")?;
    seed_omendb(&mut database, workload.rows)?;

    let mut expected = vec![0_i64; workload.rows];
    let mut random = workload.seed;
    let mut latencies_nanos = Vec::with_capacity(workload.operations / workload.batch_size + 1);
    let mut reads = 0;
    let mut writes = 0;
    let started = Instant::now();
    for batch_start in (0..workload.operations).step_by(workload.batch_size) {
        let batch_end = (batch_start + workload.batch_size).min(workload.operations);
        let batch_started = Instant::now();
        let (batch_reads, batch_writes, pending) = database
            .transaction(|database, transaction| {
                let mut pending = BTreeMap::new();
                let mut batch_reads = 0;
                let mut batch_writes = 0;
                for operation in batch_start..batch_end {
                    let id = (next_random(&mut random) % workload.rows as u64) as usize;
                    if next_random(&mut random) % 100 < workload.read_percent {
                        let result = transaction
                            .execute_sql_with_params(
                                database,
                                OMENDB_SELECT,
                                &[Value::I64(id as i64)],
                            )
                            .map_err(|error| {
                                DbError::InvalidState(format!("read account {id}: {error}"))
                            })?;
                        let [row] = result.rows.as_slice() else {
                            return Err(DbError::InvalidState(format!(
                                "read account {id} returned {} rows",
                                result.rows.len()
                            )));
                        };
                        let expected_value = pending.get(&id).copied().unwrap_or(expected[id]);
                        if row.as_slice() != [Value::I64(expected_value)] {
                            return Err(DbError::InvalidState(format!(
                                "read account {id} returned {row:?}, expected {expected_value}"
                            )));
                        }
                        batch_reads += 1;
                    } else {
                        let value = operation as i64 + 1;
                        transaction
                            .execute_sql_with_params(
                                database,
                                OMENDB_UPDATE,
                                &[Value::I64(id as i64), Value::I64(value)],
                            )
                            .map_err(|error| {
                                DbError::InvalidState(format!("update account {id}: {error}"))
                            })?;
                        pending.insert(id, value);
                        batch_writes += 1;
                    }
                }
                Ok((batch_reads, batch_writes, pending))
            })
            .map(|(value, _)| value)
            .context("run OmenDB workload transaction")?;
        for (id, value) in pending {
            expected[id] = value;
        }
        reads += batch_reads;
        writes += batch_writes;
        latencies_nanos.push(elapsed_nanos(batch_started));
    }
    let elapsed_nanos = started.elapsed().as_nanos();
    let final_checksum = verify_omendb(&mut database, &expected)?;
    let database_bytes = directory_size(database_directory);
    database.close().context("close OmenDB benchmark")?;

    Ok(RunResult {
        backend,
        workload,
        reads,
        writes,
        elapsed_nanos,
        latencies_nanos,
        final_checksum,
        database_bytes,
    })
}

fn directory_size(path: impl AsRef<Path>) -> u64 {
    let path = path.as_ref();
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => return metadata.len(),
        _ => {}
    }
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            if metadata.is_dir() {
                total += directory_size(entry.path());
            } else {
                total += metadata.len();
            }
        }
    }
    total
}

fn run_sqlite(path: impl AsRef<Path>, workload: Workload) -> Result<RunResult> {
    if path.as_ref().exists() {
        std::fs::remove_file(&path).context("remove stale SQLite benchmark file")?;
    }
    let mut connection =
        Connection::open(path.as_ref()).context("open SQLite benchmark database")?;
    connection
        .execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             PRAGMA foreign_keys = ON;
             CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL);",
        )
        .context("create SQLite benchmark table")?;
    {
        let transaction = connection
            .transaction()
            .context("begin SQLite seed transaction")?;
        for id in 0..workload.rows {
            transaction
                .execute(
                    "INSERT INTO accounts (id, balance) VALUES (?1, ?2)",
                    params![id as i64, 0_i64],
                )
                .with_context(|| format!("seed SQLite account {id}"))?;
        }
        transaction.commit().context("commit SQLite seed")?;
    }

    let mut expected = vec![0_i64; workload.rows];
    let mut random = workload.seed;
    let mut latencies_nanos = Vec::with_capacity(workload.operations / workload.batch_size + 1);
    let mut reads = 0;
    let mut writes = 0;
    let started = Instant::now();
    for batch_start in (0..workload.operations).step_by(workload.batch_size) {
        let batch_end = (batch_start + workload.batch_size).min(workload.operations);
        let batch_started = Instant::now();
        let transaction = connection
            .transaction()
            .context("begin SQLite workload transaction")?;
        let mut pending = BTreeMap::new();
        let mut batch_reads = 0;
        let mut batch_writes = 0;
        for operation in batch_start..batch_end {
            let id = (next_random(&mut random) % workload.rows as u64) as usize;
            if next_random(&mut random) % 100 < workload.read_percent {
                let value: i64 = transaction
                    .query_row(SQLITE_SELECT, params![id as i64], |row| row.get(0))
                    .with_context(|| format!("read SQLite account {id}"))?;
                let expected_value = pending.get(&id).copied().unwrap_or(expected[id]);
                if value != expected_value {
                    bail!("read SQLite account {id} returned {value}, expected {expected_value}");
                }
                batch_reads += 1;
            } else {
                let value = operation as i64 + 1;
                let affected = transaction
                    .execute(SQLITE_UPDATE, params![id as i64, value])
                    .with_context(|| format!("update SQLite account {id}"))?;
                if affected != 1 {
                    bail!("update SQLite account {id} affected {affected} rows");
                }
                pending.insert(id, value);
                batch_writes += 1;
            }
        }
        transaction
            .commit()
            .context("commit SQLite workload transaction")?;
        for (id, value) in pending {
            expected[id] = value;
        }
        reads += batch_reads;
        writes += batch_writes;
        latencies_nanos.push(elapsed_nanos(batch_started));
    }
    let elapsed_nanos = started.elapsed().as_nanos();
    let final_checksum = verify_sqlite(&connection, &expected)?;
    if let Err((connection, error)) = connection.close() {
        drop(connection);
        return Err(error).context("close SQLite benchmark");
    }

    Ok(RunResult {
        backend: Backend::Sqlite,
        workload,
        reads,
        writes,
        elapsed_nanos,
        latencies_nanos,
        final_checksum,
        database_bytes: directory_size(&path),
    })
}

fn seed_omendb(database: &mut RelationalDatabase, rows: usize) -> Result<()> {
    const CHUNK: usize = 128;
    for start in (0..rows).step_by(CHUNK) {
        let end = (start + CHUNK).min(rows);
        let mut sql = String::from("INSERT INTO accounts VALUES ");
        for id in start..end {
            if id != start {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({}, 0)", id));
        }
        database
            .execute_sql(&sql)
            .with_context(|| format!("seed OmenDB accounts {start}..{end}"))?;
    }
    Ok(())
}

fn verify_omendb(database: &mut RelationalDatabase, expected: &[i64]) -> Result<i128> {
    let rows = database
        .execute_sql("SELECT id, balance FROM accounts ORDER BY id")
        .context("verify OmenDB final state")?
        .rows;
    if rows.len() != expected.len() {
        bail!(
            "OmenDB final row count {} != {}",
            rows.len(),
            expected.len()
        );
    }
    let mut checksum = 0_i128;
    for (position, row) in rows.iter().enumerate() {
        let [Value::I64(id), Value::I64(balance)] = row.as_slice() else {
            bail!("unexpected OmenDB final row {row:?}");
        };
        if *id != position as i64 || *balance != expected[position] {
            bail!("unexpected OmenDB final row at {position}: {row:?}");
        }
        checksum += i128::from(*balance);
    }
    Ok(checksum)
}

fn verify_sqlite(connection: &Connection, expected: &[i64]) -> Result<i128> {
    let mut statement = connection
        .prepare("SELECT id, balance FROM accounts ORDER BY id")
        .context("prepare SQLite final-state query")?;
    let rows = statement
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .context("query SQLite final state")?;
    let mut count = 0;
    let mut checksum = 0_i128;
    for row in rows {
        let (id, balance) = row.context("read SQLite final row")?;
        let position = count;
        if position >= expected.len() || id != position as i64 || balance != expected[position] {
            bail!("unexpected SQLite final row at {position}: ({id}, {balance})");
        }
        checksum += i128::from(balance);
        count += 1;
    }
    if count != expected.len() {
        bail!("SQLite final row count {count} != {}", expected.len());
    }
    Ok(checksum)
}

fn result_json(result: &RunResult) -> serde_json::Value {
    let mut latencies = result.latencies_nanos.clone();
    latencies.sort_unstable();
    let elapsed_seconds = result.elapsed_nanos as f64 / 1_000_000_000.0;
    json!({
        "experiment": "omendb-relational-alpha-oltp-v0",
        "evidence_class": "reproducible_single_client_sql_baseline",
        "competitive_claim": false,
        "platform": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        },
        "backend": result.backend.label(),
        "rows": result.workload.rows,
        "operations": result.workload.operations,
        "transactions": result.latencies_nanos.len(),
        "batch_size": result.workload.batch_size,
        "latency_sample_unit": "transaction",
        "reads": result.reads,
        "writes": result.writes,
        "read_percent": result.workload.read_percent,
        "seed": result.workload.seed,
        "elapsed_seconds": elapsed_seconds,
        "throughput_operations_per_second": result.workload.operations as f64 / elapsed_seconds.max(f64::MIN_POSITIVE),
        "final_checksum": result.final_checksum,
        "database_bytes": result.database_bytes,
        "peak_rss_kib": peak_rss_kib(),
        "latency": {
            "p50_seconds": nanos_to_seconds(percentile(&latencies, 50, 100)),
            "p95_seconds": nanos_to_seconds(percentile(&latencies, 95, 100)),
            "p99_seconds": nanos_to_seconds(percentile(&latencies, 99, 100)),
            "max_seconds": nanos_to_seconds(latencies.last().copied().unwrap_or_default()),
        },
    })
}

fn parse_arguments() -> Result<(Option<Backend>, Workload)> {
    let mut backend = None;
    let mut workload = Workload {
        rows: DEFAULT_ROWS,
        operations: DEFAULT_OPERATIONS,
        read_percent: DEFAULT_READ_PERCENT,
        batch_size: DEFAULT_BATCH_SIZE,
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
                let value = value()?;
                backend = if value == "all" {
                    None
                } else {
                    Some(Backend::parse(&value)?)
                };
            }
            "--rows" => workload.rows = value()?.parse().context("invalid --rows")?,
            "--operations" => {
                workload.operations = value()?.parse().context("invalid --operations")?
            }
            "--read-percent" => {
                workload.read_percent = value()?.parse().context("invalid --read-percent")?
            }
            "--batch-size" => {
                workload.batch_size = value()?.parse().context("invalid --batch-size")?
            }
            "--seed" => workload.seed = value()?.parse().context("invalid --seed")?,
            "--help" => {
                println!(
                    "usage: alpha_oltp [--backend all|temporary|seer|sqlite] [--rows N] [--operations N] [--read-percent N] [--batch-size N] [--seed N]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument {other}"),
        }
    }
    Ok((backend, workload))
}

fn validate(workload: Workload) -> Result<()> {
    if workload.rows == 0
        || workload.operations == 0
        || workload.batch_size == 0
        || workload.read_percent > 100
    {
        bail!("rows, operations, and batch-size must be positive; read-percent must be 0..=100");
    }
    Ok(())
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

fn nanos_to_seconds(nanos: u64) -> f64 {
    nanos as f64 / 1_000_000_000.0
}

/// Peak resident set size of this process so far, in KiB. Reported as
/// process-wide max RSS; it bounds the workload's footprint from above and
/// includes setup/teardown allocations.
fn peak_rss_kib() -> u64 {
    let mut usage = std::mem::MaybeUninit::uninit();
    // SAFETY: rusage is a plain C struct with no invariants on input.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return 0;
    }
    let usage = unsafe { usage.assume_init() };
    let rss = usage.ru_maxrss;
    // macOS reports bytes; Linux reports KiB.
    if cfg!(target_os = "macos") {
        (rss / 1024) as u64
    } else {
        rss as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_argument_bounds_are_checked() {
        assert!(
            validate(Workload {
                rows: 0,
                operations: 1,
                read_percent: 80,
                batch_size: 1,
                seed: 1,
            })
            .is_err()
        );
        assert!(
            validate(Workload {
                rows: 1,
                operations: 1,
                read_percent: 101,
                batch_size: 1,
                seed: 1,
            })
            .is_err()
        );
        assert!(
            validate(Workload {
                rows: 1,
                operations: 1,
                read_percent: 100,
                batch_size: 1,
                seed: 1,
            })
            .is_ok()
        );
    }

    #[test]
    fn percentile_uses_nearest_rank() {
        assert_eq!(percentile(&[1, 2, 3, 4], 50, 100), 2);
        assert_eq!(percentile(&[1, 2, 3, 4], 95, 100), 4);
        assert_eq!(percentile(&[], 99, 100), 0);
    }
}
