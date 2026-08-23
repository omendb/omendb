//! Multi-client certifier pressure harness over the PostgreSQL wire
//! protocol. Exploratory tooling, not part of the test suite; the
//! correctness bound (no lost updates) lives in
//! `tests/project_concurrency_stress.rs`.
//!
//! Workload: C concurrent clients hammer K hot keys with mixed point
//! reads and read-modify-write transactions. RMW conflicts surface at
//! COMMIT as 40001 and are retried with capped backoff, so latency is
//! end-to-end from the client's perspective (including retries).
//!
//! Usage:
//!   cargo run --features pgwire --example concurrency_stress -- \
//!     [--clients 8] [--keys 8] [--ops 500] [--rw-pct 50] [--json out.json]

#![allow(clippy::disallowed_methods)]

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use omendb::pgwire_server;
use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DatabaseConfig, RelationalBackendConfig,
    RelationalDatabase, TableDefinition, TableId,
};
use tokio_postgres::NoTls;
use tokio_postgres::error::SqlState;

#[derive(Clone)]
struct Config {
    clients: usize,
    keys: u64,
    ops: usize,
    rw_pct: u64,
    json: Option<String>,
}

fn parse_args() -> Config {
    let mut config = Config {
        clients: 8,
        keys: 8,
        ops: 500,
        rw_pct: 50,
        json: None,
    };
    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        let mut value = |name: &str| -> u64 {
            argv.next()
                .unwrap_or_else(|| panic!("{name} needs a value"))
                .parse()
                .unwrap_or_else(|error| panic!("{name}: {error}"))
        };
        match arg.as_str() {
            "--clients" => config.clients = value("--clients") as usize,
            "--keys" => config.keys = value("--keys"),
            "--ops" => config.ops = value("--ops") as usize,
            "--rw-pct" => config.rw_pct = value("--rw-pct").min(100),
            "--json" => {
                config.json = Some(argv.next().unwrap_or_else(|| panic!("--json needs a path")))
            }
            other => panic!("unknown argument {other}"),
        }
    }
    config
}

/// Deterministic per-task LCG; distribution quality is irrelevant here,
/// reproducibility across runs matters.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 16
    }
}

struct ClientStats {
    reads: usize,
    commits: usize,
    retries: usize,
    exhausted: usize,
    latencies_us: Vec<u64>,
}

fn is_certifier_retry(error: &tokio_postgres::Error) -> bool {
    matches!(
        error.code(),
        Some(&SqlState::T_R_SERIALIZATION_FAILURE) | Some(&SqlState::IN_FAILED_SQL_TRANSACTION)
    )
}

async fn connect(addr: std::net::SocketAddr) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=stress dbname=stress",
            addr.port()
        ),
        NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move { connection.await.expect("connection") });
    client
}

async fn run_client(addr: std::net::SocketAddr, config: Config, seed: u64) -> ClientStats {
    let mut client = connect(addr).await;
    let mut rng = Lcg(seed);
    let mut stats = ClientStats {
        reads: 0,
        commits: 0,
        retries: 0,
        exhausted: 0,
        latencies_us: Vec::new(),
    };
    for _ in 0..config.ops {
        let key = (rng.next() % config.keys.max(1)) as i64;
        let started = Instant::now();
        if (rng.next() % 100) >= config.rw_pct {
            let _ = client
                .query_one("SELECT value FROM counters WHERE id = $1", &[&key])
                .await
                .expect("read");
            stats.reads += 1;
            stats
                .latencies_us
                .push(started.elapsed().as_micros() as u64);
            continue;
        }
        let mut committed = false;
        for attempt in 0..200u32 {
            let transaction = client
                .transaction()
                .await
                .unwrap_or_else(|error| panic!("begin failed: {error}"));
            let row = transaction
                .query_one("SELECT value FROM counters WHERE id = $1", &[&key])
                .await
                .expect("rmw select");
            let value: i64 = row.get(0);
            if let Err(error) = transaction
                .execute(
                    "UPDATE counters SET value = $1 WHERE id = $2",
                    &[&(value + 1), &key],
                )
                .await
            {
                if is_certifier_retry(&error) && attempt < 199 {
                    stats.retries += 1;
                    tokio::time::sleep(
                        Duration::from_millis((attempt as u64) + 1).min(Duration::from_millis(20)),
                    )
                    .await;
                    continue;
                }
                panic!("rmw update failed terminally: {error}");
            }
            match transaction.commit().await {
                Ok(()) => {
                    stats.commits += 1;
                    committed = true;
                    break;
                }
                Err(error) if is_certifier_retry(&error) && attempt < 199 => {
                    stats.retries += 1;
                    tokio::time::sleep(
                        Duration::from_millis((attempt as u64) + 1).min(Duration::from_millis(20)),
                    )
                    .await;
                }
                Err(error) => panic!("commit failed terminally: {error}"),
            }
        }
        if !committed {
            stats.exhausted += 1;
        }
        stats
            .latencies_us
            .push(started.elapsed().as_micros() as u64);
    }
    stats
}

fn percentile(sorted: &[u64], fraction: f64) -> u64 {
    sorted
        .get(((sorted.len() as f64) * fraction).min(sorted.len() as f64 - 1.0) as usize)
        .copied()
        .unwrap_or(0)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = parse_args();
    let directory = tempfile::tempdir()?;
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::Temporary(DatabaseConfig {
            directory: directory.path().to_path_buf(),
        }))?;
    database.create_table(TableDefinition {
        id: TableId(9),
        name: "counters".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "value".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
        ],
    })?;
    for key in 0..config.keys {
        database.execute_sql(&format!(
            "INSERT INTO counters (id, value) VALUES ({key}, 0)"
        ))?;
    }
    let shared: Arc<RwLock<RelationalDatabase>> = Arc::new(RwLock::new(database));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(pgwire_server::serve(Arc::clone(&shared), listener));
    println!(
        "concurrency stress: {} clients x {} ops over {} keys, {}% rmw",
        config.clients, config.ops, config.keys, config.rw_pct
    );

    let started = Instant::now();
    let mut handles = Vec::new();
    for index in 0..config.clients {
        handles.push(tokio::spawn(run_client(
            addr,
            config.clone(),
            0x5EED_0000 + index as u64,
        )));
    }
    let mut totals = ClientStats {
        reads: 0,
        commits: 0,
        retries: 0,
        exhausted: 0,
        latencies_us: Vec::new(),
    };
    for handle in handles {
        let stats = handle.await?;
        totals.reads += stats.reads;
        totals.commits += stats.commits;
        totals.retries += stats.retries;
        totals.exhausted += stats.exhausted;
        totals.latencies_us.extend(stats.latencies_us);
    }
    let wall = started.elapsed();
    totals.latencies_us.sort_unstable();

    let attempts = totals.commits + totals.retries;
    println!(
        "wall {wall:?} | reads {} rmw-commits {} retries {} starved {} | conflict-rate {:.2}% | p50 {}us p95 {}us p99 {}us max {}us | throughput {:.0} ops/s",
        totals.reads,
        totals.commits,
        totals.retries,
        totals.exhausted,
        if attempts == 0 {
            0.0
        } else {
            (totals.retries as f64) / (attempts as f64) * 100.0
        },
        percentile(&totals.latencies_us, 0.50),
        percentile(&totals.latencies_us, 0.95),
        percentile(&totals.latencies_us, 0.99),
        totals.latencies_us.last().copied().unwrap_or(0),
        (totals.reads + totals.commits) as f64 / wall.as_secs_f64(),
    );

    // Final counter sum must equal successful increments; the equality
    // assertion lives in tests/project_concurrency_stress.rs.
    let mut db = shared.write().expect("lock");
    let sum = db
        .execute_sql("SELECT sum(value) FROM counters")?
        .rows
        .first()
        .expect("sum row")
        .clone();
    drop(db);
    println!("counter sum: {sum:?}");

    if let Some(path) = config.json {
        let summary = serde_json::json!({
            "label": "exploratory",
            "harness": "concurrency_stress",
            "clients": config.clients,
            "keys": config.keys,
            "ops_per_client": config.ops,
            "rw_pct": config.rw_pct,
            "reads": totals.reads,
            "rmw_commits": totals.commits,
            "retries": totals.retries,
            "starved": totals.exhausted,
            "latency_us": {
                "p50": percentile(&totals.latencies_us, 0.50),
                "p95": percentile(&totals.latencies_us, 0.95),
                "p99": percentile(&totals.latencies_us, 0.99),
                "max": totals.latencies_us.last().copied().unwrap_or(0),
            },
            "throughput_ops_per_s": (totals.reads + totals.commits) as f64 / wall.as_secs_f64(),
        });
        std::fs::write(&path, serde_json::to_string_pretty(&summary)?)?;
        println!("wrote {path}");
    }

    server.abort();
    Ok(())
}
