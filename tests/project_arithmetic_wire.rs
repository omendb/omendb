//! Scalar arithmetic errors must abort the statement and all earlier staged work.
#![cfg(feature = "pgwire")]

#[path = "common/numeric_text.rs"]
mod numeric_text;
#[path = "common/scalar_arithmetic_wire.rs"]
mod scalar_arithmetic_wire;

use anyhow::Result;
use omendb::pgwire_server::{RunningServer, ServerConfig};
use omendb::{RelationalBackendConfig, RelationalDatabase, Value};
use tempfile::tempdir;
use tokio_postgres::NoTls;

#[tokio::test]
async fn scalar_arithmetic_keeps_types_values_and_rollback_boundaries() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("db");
    let server =
        RunningServer::start(ServerConfig::new(path.clone(), "127.0.0.1:0".parse()?)).await?;
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=omendb",
            server.local_addr().port()
        ),
        NoTls,
    )
    .await?;
    let connection = tokio::spawn(connection);
    scalar_arithmetic_wire::exercise(&client, false).await?;
    drop(client);
    connection.await??;
    server.shutdown().await?;

    let mut reopened = RelationalDatabase::open(RelationalBackendConfig::new(path))?;
    assert!(
        reopened
            .execute_sql("SELECT amount FROM oracle_scalar_saved")?
            .rows
            .is_empty()
    );
    assert_eq!(
        reopened
            .execute_sql("SELECT id, n FROM oracle_scalar_numbers ORDER BY id")?
            .rows,
        vec![
            vec![Value::I64(1), Value::I64(i64::MAX)],
            vec![Value::I64(2), Value::Null]
        ]
    );
    assert_eq!(
        reopened
            .execute_sql("SELECT id FROM oracle_scalar_numbers WHERE d = 1")?
            .rows,
        vec![vec![Value::I64(1)]]
    );
    reopened.close()?;
    Ok(())
}
