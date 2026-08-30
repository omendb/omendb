//! Live PostgreSQL differential checks for OmenDB's documented wire subset.
//!
//! The ordinary pgwire suite proves OmenDB behavior and negative paths. This
//! test adds an independent PostgreSQL oracle for representative semantics that
//! OmenDB already claims to support. It is ignored by the ordinary test matrix
//! because it requires a live PostgreSQL server; CI runs it in a dedicated job.

#![cfg(feature = "pgwire")]

use std::env;

use anyhow::{Context, Result, bail};
use omendb::pgwire_server;
use tempfile::tempdir;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, NoTls, Row};

const POSTGRES_ORACLE_URL: &str = "POSTGRES_ORACLE_URL";

#[derive(Debug, Eq, PartialEq)]
struct QuerySnapshot {
    columns: Vec<(String, u32)>,
    rows: Vec<Vec<Cell>>,
}

#[derive(Debug, Eq, PartialEq)]
enum Cell {
    Null,
    Bool(bool),
    I64(i64),
    Text(String),
    Bytes(Vec<u8>),
}

async fn connect(
    config: &str,
) -> Result<(
    Client,
    tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>,
)> {
    let (client, connection) = tokio_postgres::connect(config, NoTls)
        .await
        .with_context(|| format!("connect with {config}"))?;
    let task = tokio::spawn(connection);
    Ok((client, task))
}

async fn seed(client: &Client) -> Result<()> {
    client
        .batch_execute(
            "CREATE TABLE oracle_groups (
                 id BIGINT PRIMARY KEY,
                 label TEXT NOT NULL
             );
             CREATE TABLE oracle_accounts (
                 id BIGINT PRIMARY KEY,
                 group_id BIGINT NOT NULL,
                 balance BIGINT NOT NULL,
                 state TEXT
             );
             INSERT INTO oracle_groups (id, label)
             VALUES (10, 'retail'), (20, 'business');
             INSERT INTO oracle_accounts (id, group_id, balance, state)
             VALUES
                 (1, 10, 100, 'open'),
                 (2, 10, 40, 'open'),
                 (3, 20, 10, 'closed'),
                 (4, 20, 0, NULL);",
        )
        .await
        .context("seed oracle schema")?;
    Ok(())
}

async fn snapshot(
    client: &Client,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<QuerySnapshot> {
    let statement = client
        .prepare(sql)
        .await
        .with_context(|| format!("prepare oracle query: {sql}"))?;
    let columns = statement
        .columns()
        .iter()
        .map(|column| (column.name().to_owned(), column.type_().oid()))
        .collect();
    let rows = client
        .query(&statement, params)
        .await
        .with_context(|| format!("execute oracle query: {sql}"))?;
    let rows = rows.iter().map(decode_row).collect::<Result<Vec<_>>>()?;
    Ok(QuerySnapshot { columns, rows })
}

fn decode_row(row: &Row) -> Result<Vec<Cell>> {
    row.columns()
        .iter()
        .enumerate()
        .map(|(index, column)| decode_cell(row, index, column.type_()))
        .collect()
}

fn decode_cell(row: &Row, index: usize, data_type: &Type) -> Result<Cell> {
    if data_type == &Type::BOOL {
        return Ok(row
            .try_get::<_, Option<bool>>(index)?
            .map_or(Cell::Null, Cell::Bool));
    }
    if data_type == &Type::INT2 {
        return Ok(row
            .try_get::<_, Option<i16>>(index)?
            .map_or(Cell::Null, |value| Cell::I64(i64::from(value))));
    }
    if data_type == &Type::INT4 {
        return Ok(row
            .try_get::<_, Option<i32>>(index)?
            .map_or(Cell::Null, |value| Cell::I64(i64::from(value))));
    }
    if data_type == &Type::INT8 {
        return Ok(row
            .try_get::<_, Option<i64>>(index)?
            .map_or(Cell::Null, Cell::I64));
    }
    if data_type == &Type::TEXT || data_type == &Type::VARCHAR {
        return Ok(row
            .try_get::<_, Option<String>>(index)?
            .map_or(Cell::Null, Cell::Text));
    }
    if data_type == &Type::BYTEA {
        return Ok(row
            .try_get::<_, Option<Vec<u8>>>(index)?
            .map_or(Cell::Null, Cell::Bytes));
    }
    bail!(
        "live PostgreSQL oracle returned unsupported comparison type {} (OID {})",
        data_type,
        data_type.oid()
    )
}

async fn compare_query(
    omendb: &Client,
    postgres: &Client,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<()> {
    let actual = snapshot(omendb, sql, params).await?;
    let expected = snapshot(postgres, sql, params).await?;
    if actual != expected {
        bail!(
            "PostgreSQL oracle mismatch for {sql}:\nPostgreSQL: {expected:#?}\nOmenDB: {actual:#?}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL oracle"]
async fn documented_wire_subset_matches_live_postgresql() -> Result<()> {
    let postgres_url = env::var(POSTGRES_ORACLE_URL).with_context(|| {
        format!("{POSTGRES_ORACLE_URL} must point to the live PostgreSQL oracle")
    })?;

    let directory = tempdir().context("create OmenDB oracle directory")?;
    let server = pgwire_server::RunningServer::start(pgwire_server::ServerConfig::new(
        directory.path().join("db"),
        "127.0.0.1:0".parse().expect("loopback address"),
    ))
    .await
    .context("start OmenDB oracle server")?;

    let omen_config = format!(
        "host=127.0.0.1 port={} user=omendb",
        server.local_addr().port()
    );
    let (mut omendb, omen_connection) = connect(&omen_config).await?;
    let (mut postgres, postgres_connection) = connect(&postgres_url).await?;

    postgres
        .batch_execute(
            "DROP TABLE IF EXISTS oracle_accounts;
             DROP TABLE IF EXISTS oracle_groups;",
        )
        .await
        .context("reset PostgreSQL oracle schema")?;
    seed(&omendb).await.context("seed OmenDB")?;
    seed(&postgres).await.context("seed PostgreSQL")?;

    compare_query(
        &omendb,
        &postgres,
        "SELECT id, group_id, balance, state FROM oracle_accounts ORDER BY id",
        &[],
    )
    .await?;

    let low = 10_i64;
    let high = 100_i64;
    compare_query(
        &omendb,
        &postgres,
        "SELECT id, state FROM oracle_accounts WHERE balance BETWEEN $1 AND $2 ORDER BY id",
        &[&low, &high],
    )
    .await?;

    compare_query(
        &omendb,
        &postgres,
        "SELECT a.id, g.label, a.state
         FROM oracle_accounts AS a
         JOIN oracle_groups AS g ON a.group_id = g.id
         ORDER BY a.id",
        &[],
    )
    .await?;

    compare_query(
        &omendb,
        &postgres,
        "SELECT state, count(*) AS row_count
         FROM oracle_accounts
         GROUP BY state
         ORDER BY state ASC NULLS FIRST",
        &[],
    )
    .await?;

    let updated_balance = 55_i64;
    let updated_id = 2_i64;
    compare_query(
        &omendb,
        &postgres,
        "UPDATE oracle_accounts
         SET balance = $1
         WHERE id = $2
         RETURNING id, balance, state",
        &[&updated_balance, &updated_id],
    )
    .await?;

    let inserted_id = 5_i64;
    let inserted_group = 20_i64;
    let inserted_balance = 77_i64;
    let inserted_state = "open";
    compare_query(
        &omendb,
        &postgres,
        "INSERT INTO oracle_accounts (id, group_id, balance, state)
         VALUES ($1, $2, $3, $4)
         RETURNING id, group_id, balance, state",
        &[
            &inserted_id,
            &inserted_group,
            &inserted_balance,
            &inserted_state,
        ],
    )
    .await?;

    let deleted_id = 3_i64;
    compare_query(
        &omendb,
        &postgres,
        "DELETE FROM oracle_accounts WHERE id = $1 RETURNING id, balance, state",
        &[&deleted_id],
    )
    .await?;

    {
        let omen_transaction = omendb
            .transaction()
            .await
            .context("begin OmenDB rollback")?;
        omen_transaction
            .execute(
                "INSERT INTO oracle_accounts (id, group_id, balance, state)
                 VALUES (99, 10, 1, 'rollback')",
                &[],
            )
            .await
            .context("stage OmenDB rollback row")?;
        omen_transaction
            .rollback()
            .await
            .context("rollback OmenDB transaction")?;
    }
    {
        let postgres_transaction = postgres
            .transaction()
            .await
            .context("begin PostgreSQL rollback")?;
        postgres_transaction
            .execute(
                "INSERT INTO oracle_accounts (id, group_id, balance, state)
                 VALUES (99, 10, 1, 'rollback')",
                &[],
            )
            .await
            .context("stage PostgreSQL rollback row")?;
        postgres_transaction
            .rollback()
            .await
            .context("rollback PostgreSQL transaction")?;
    }

    compare_query(
        &omendb,
        &postgres,
        "SELECT id, group_id, balance, state FROM oracle_accounts ORDER BY id",
        &[],
    )
    .await?;

    drop(omendb);
    drop(postgres);
    server.shutdown().await.context("shutdown OmenDB oracle")?;
    omen_connection
        .await
        .context("join OmenDB wire connection")??;
    postgres_connection
        .await
        .context("join PostgreSQL oracle connection")??;
    Ok(())
}
