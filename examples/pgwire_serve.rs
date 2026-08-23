//! Serve a throwaway seeded database over PostgreSQL wire protocol for
//! external client compatibility checks (psycopg, node-pg). Exploratory
//! tooling, not part of the test suite.
//!
//! Usage: cargo run --features pgwire --example pgwire_serve -- [port]
//! Set PGWIRE_USER + PGWIRE_PASSWORD to provision a SCRAM wire user and
//! switch the server into authenticated mode.

#![allow(clippy::disallowed_methods)]

use std::sync::{Arc, RwLock};

use omendb::pgwire_server;
use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DatabaseConfig, RelationalBackendConfig,
    RelationalDatabase, TableDefinition, TableId,
};

fn main() -> anyhow::Result<()> {
    let port: u16 = std::env::args().nth(1).unwrap_or_default().parse()?;
    let directory = tempfile::tempdir()?;
    let mut database = RelationalDatabase::create(RelationalBackendConfig::Temporary(
        DatabaseConfig {
            directory: directory.path().to_path_buf(),
        },
    ))?;
    database.create_table(TableDefinition {
        id: TableId(7),
        name: "users".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "email".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    })?;
    database.execute_sql("INSERT INTO users (id, email) VALUES (1, 'alice@example.com')")?;
    if let Ok(user) = std::env::var("PGWIRE_USER")
        && let Ok(password) = std::env::var("PGWIRE_PASSWORD")
    {
        omendb::pgwire_server::provision_wire_user(&mut database, &user, &password)?;
        println!("provisioned user {user}");
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let listener = tokio_net_listener(port).await?;
        println!("pgwire_serve ready on {}", listener.local_addr()?);
        let shared: Arc<RwLock<RelationalDatabase>> = Arc::new(RwLock::new(database));
        pgwire_server::serve(Arc::clone(&shared), listener).await?;
        Ok::<_, anyhow::Error>(())
    })?;
    drop(directory);
    Ok(())
}

async fn tokio_net_listener(
    port: u16,
) -> std::io::Result<tokio::net::TcpListener> {
    tokio::net::TcpListener::bind(("127.0.0.1", port)).await
}
