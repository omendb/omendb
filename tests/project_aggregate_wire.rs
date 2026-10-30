//! Aggregate errors must remain clean, rollback-only statement failures.
#![cfg(feature = "pgwire")]

#[allow(dead_code)]
#[path = "common/aggregate_sum.rs"]
mod aggregate_sum;
#[path = "common/aggregate_sum_wire.rs"]
mod aggregate_sum_wire;

use anyhow::Result;
use omendb::pgwire_server::{RunningServer, ServerConfig};
use tempfile::tempdir;
use tokio_postgres::NoTls;

#[tokio::test]
async fn bounded_integer_sums_fail_cleanly_without_committing_prior_work() -> Result<()> {
    let directory = tempdir()?;
    let server = RunningServer::start(ServerConfig::new(
        directory.path().join("db"),
        "127.0.0.1:0".parse()?,
    ))
    .await?;
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=omendb",
            server.local_addr().port()
        ),
        NoTls,
    )
    .await?;
    let connection = tokio::spawn(connection);
    aggregate_sum_wire::exercise(&client, false).await?;
    drop(client);
    server.shutdown().await?;
    connection.await??;
    Ok(())
}
