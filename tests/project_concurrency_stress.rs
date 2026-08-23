//! Certifier correctness bound under wire-level concurrency: concurrent
//! read-modify-write transactions on hot keys must never lose an update.
//! Every committed RMW increments its counter exactly once, so after the
//! run SUM(counters) equals the number of successful RMW commits. The
//! full measurement harness lives in examples/concurrency_stress.rs;
//! this test keeps the invariant enforced in CI at smoke scale.

#![allow(clippy::disallowed_methods)]

use std::sync::{Arc, RwLock};
use std::time::Duration;

use omendb::pgwire_server;
use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DatabaseConfig, RelationalBackendConfig,
    RelationalDatabase, TableDefinition, TableId,
};
use tokio_postgres::{error::SqlState, NoTls};

const CLIENTS: u64 = 4;
const KEYS: u64 = 4;
const RMW_OPS_PER_CLIENT: usize = 150;

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

fn retryable(error: &tokio_postgres::Error) -> bool {
    matches!(
        error.code(),
        Some(&SqlState::T_R_SERIALIZATION_FAILURE) | Some(&SqlState::IN_FAILED_SQL_TRANSACTION)
    )
}

async fn connect(addr: std::net::SocketAddr) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=stress dbname=stress", addr.port()),
        NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move { connection.await.expect("connection") });
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_rmw_transactions_never_lose_updates() {
    let directory = tempfile::tempdir().expect("tempdir");
    let mut database = RelationalDatabase::create(RelationalBackendConfig::Temporary(
        DatabaseConfig {
            directory: directory.path().to_path_buf(),
        },
    ))
    .expect("database");
    database
        .create_table(TableDefinition {
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
        })
        .expect("create counters");
    for key in 0..KEYS {
        database
            .execute_sql(&format!("INSERT INTO counters (id, value) VALUES ({key}, 0)"))
            .expect("seed counter");
    }
    let shared: Arc<RwLock<RelationalDatabase>> = Arc::new(RwLock::new(database));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(pgwire_server::serve(Arc::clone(&shared), listener));

    let mut handles = Vec::new();
    for index in 0..CLIENTS {
        handles.push(tokio::spawn(run_client(addr, 0xC0FFEE + index)));
    }
    let mut total_commits = 0usize;
    for handle in handles {
        total_commits += handle.await.expect("client task");
    }

    let mut db = shared.write().expect("lock");
    let rows = db.execute_sql("SELECT id, value FROM counters").expect("sum").rows;
    drop(db);
    let sum: u64 = rows
        .iter()
        .map(|row| match &row[1] {
            omendb::Value::U64(value) => value,
            other => panic!("unexpected value {other:?}"),
        })
        .sum();
    assert_eq!(
        sum as usize, total_commits,
        "lost updates detected: counters moved {sum}, commits recorded {total_commits}"
    );

    server.abort();
}

async fn run_client(addr: std::net::SocketAddr, seed: u64) -> usize {
    let mut client = connect(addr).await;
    let mut rng = Lcg(seed);
    let mut commits = 0usize;
    for _ in 0..RMW_OPS_PER_CLIENT {
        let key = (rng.next() % KEYS) as i64;
        for attempt in 0..200u32 {
            let transaction = client.transaction().await.expect("begin");
            let row = transaction
                .query_one("SELECT value FROM counters WHERE id = $1", &[&key])
                .await
                .expect("rmw select");
            let value: i64 = row.get(0);
            transaction
                .execute(
                    "UPDATE counters SET value = $1 WHERE id = $2",
                    &[&(value + 1), &key],
                )
                .await
                .expect("rmw update");
            match transaction.commit().await {
                Ok(()) => {
                    commits += 1;
                    break;
                }
                Err(error) if retryable(&error) => {
                    tokio::time::sleep(Duration::from_millis((attempt as u64 % 10) + 1)).await;
                }
                Err(error) => panic!("commit failed terminally: {error}"),
            }
        }
    }
    commits
}
