//! Wire-protocol spike conformance: real PostgreSQL clients (tokio-postgres)
//! against the bounded SQL tier, per TECH_SPEC "Optional Protocol Spike".
#![cfg(feature = "pgwire")]

use std::io::BufRead;
use std::sync::{Arc, RwLock};

use omendb::pgwire_server;
use omendb::{
    ColumnDefinition, ColumnId, ColumnType, RelationalBackendConfig, RelationalDatabase,
    TableDefinition, TableId,
};
use tempfile::tempdir;
use tokio_postgres::error::SqlState;

fn seed_database(directory: &std::path::Path) -> Arc<RwLock<RelationalDatabase>> {
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.join("db")))
            .expect("create database");
    database
        .create_table(TableDefinition {
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
        })
        .expect("create table");
    database
        .execute_sql("INSERT INTO users (id, email) VALUES (1, 'alice@example.com')")
        .expect("seed");
    Arc::new(RwLock::new(database))
}

#[tokio::test]
async fn persistent_server_reopens_durable_database_after_shutdown() {
    let directory = tempdir().expect("tempdir");
    let database_path = directory.path().join("db");
    let mut database = RelationalDatabase::create(RelationalBackendConfig::new(&database_path))
        .expect("create database");
    database
        .create_table(TableDefinition {
            id: TableId(7),
            name: "users".to_owned(),
            columns: vec![ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            }],
        })
        .expect("create table");
    database
        .execute_sql("INSERT INTO users (id) VALUES (1)")
        .expect("seed");
    database.close().expect("close seed database");

    let config =
        pgwire_server::ServerConfig::new(&database_path, "127.0.0.1:0".parse().expect("addr"))
            .with_create_if_missing(false);
    let server = pgwire_server::RunningServer::start(config.clone())
        .await
        .expect("start persistent server");
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=omendb",
            server.local_addr().port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    let connection_task = tokio::spawn(connection);
    let row = client
        .query_one("SELECT count(*) FROM users", &[])
        .await
        .expect("read persisted row");
    assert_eq!(row.get::<_, i64>(0), 1);
    let status = server.status();
    assert!(status.completed_operations >= 1);
    assert_eq!(status.failed_operations, 0);
    client
        .query("SELECT * FROM nonexistent_table", &[])
        .await
        .expect_err("unsupported query should fail");
    assert!(server.status().failed_operations >= 1);
    client.batch_execute("BEGIN").await.expect("begin block");
    client
        .batch_execute("INSERT INTO users (id) VALUES (2)")
        .await
        .expect("stage block write");
    assert_eq!(server.status().max_connections, 128);
    assert!(server.status().accepted_connections >= 1);
    drop(client);
    server.shutdown().await.expect("shutdown server");
    let _ = connection_task.await;

    let reopened = pgwire_server::RunningServer::start(config)
        .await
        .expect("reopen persistent server");
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=omendb",
            reopened.local_addr().port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("reconnect");
    let connection_task = tokio::spawn(connection);
    let row = client
        .query_one("SELECT count(*) FROM users", &[])
        .await
        .expect("read after reopen");
    assert_eq!(row.get::<_, i64>(0), 1);
    drop(client);
    reopened.shutdown().await.expect("shutdown reopened server");
    let _ = connection_task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_server_shutdown_cancels_query_before_database_close() {
    let directory = tempdir().expect("tempdir");
    let database_path = directory.path().join("db");
    let mut database = RelationalDatabase::create(RelationalBackendConfig::new(&database_path))
        .expect("create database");
    database
        .create_table(TableDefinition {
            id: TableId(7),
            name: "items".to_owned(),
            columns: vec![ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            }],
        })
        .expect("create table");
    let values = (1..=5_000)
        .map(|id| format!("({id})"))
        .collect::<Vec<_>>()
        .join(", ");
    database
        .execute_sql(&format!("INSERT INTO items (id) VALUES {values}"))
        .expect("seed rows");
    database.close().expect("close seed database");

    let config =
        pgwire_server::ServerConfig::new(&database_path, "127.0.0.1:0".parse().expect("addr"))
            .with_create_if_missing(false);
    let server = pgwire_server::RunningServer::start(config.clone())
        .await
        .expect("start persistent server");
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=omendb",
            server.local_addr().port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    let connection_task = tokio::spawn(connection);
    let query = tokio::spawn(async move {
        client
            .query(
                "SELECT lhs.id FROM items AS lhs CROSS JOIN items AS rhs LIMIT 1",
                &[],
            )
            .await
    });

    // The nested-loop query is deliberately larger than the result window:
    // shutdown must cancel its tracked worker rather than wait for all pairs.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    tokio::time::timeout(std::time::Duration::from_secs(10), server.shutdown())
        .await
        .expect("shutdown completed")
        .expect("shutdown cleanly");
    let query_result = tokio::time::timeout(std::time::Duration::from_secs(5), query)
        .await
        .expect("query task completed")
        .expect("query join completed");
    assert!(query_result.is_err(), "shutdown query must not succeed");
    let _ = connection_task.await;

    let reopened = pgwire_server::RunningServer::start(config)
        .await
        .expect("reopen after shutdown cancellation");
    let (check, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=omendb",
            reopened.local_addr().port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("reconnect");
    let connection_task = tokio::spawn(connection);
    let row = check
        .query_one("SELECT count(*) FROM items", &[])
        .await
        .expect("read after shutdown cancellation");
    assert_eq!(row.get::<_, i64>(0), 5_000);
    drop(check);
    reopened.shutdown().await.expect("shutdown reopened server");
    let _ = connection_task.await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn omendbd_process_kill_reopens_durable_database() {
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Child, Command, Stdio};

    struct ChildGuard(Option<Child>);

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            if let Some(child) = &mut self.0 {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    let directory = tempdir().expect("tempdir");
    let database_path = directory.path().join("db");
    let mut database = RelationalDatabase::create(RelationalBackendConfig::new(&database_path))
        .expect("create database");
    database
        .create_table(TableDefinition {
            id: TableId(7),
            name: "items".to_owned(),
            columns: vec![ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            }],
        })
        .expect("create table");
    database
        .execute_sql("INSERT INTO items (id) VALUES (1)")
        .expect("seed row");
    database.close().expect("close seed database");

    let mut child = ChildGuard(Some(
        Command::new(env!("CARGO_BIN_EXE_omendbd"))
            .args([
                "--path",
                database_path.to_str().expect("database path"),
                "--bind",
                "127.0.0.1:0",
            ])
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn omendbd"),
    ));
    let stdout = child
        .0
        .as_mut()
        .expect("daemon child")
        .stdout
        .take()
        .expect("daemon stdout");
    let banner = tokio::task::spawn_blocking(move || {
        let mut line = String::new();
        std::io::BufReader::new(stdout).read_line(&mut line)?;
        Ok::<_, std::io::Error>(line)
    })
    .await
    .expect("banner reader task")
    .expect("daemon banner");
    assert!(
        banner.starts_with("omendbd listening on "),
        "banner: {banner:?}"
    );

    let pid = i32::try_from(child.0.as_ref().expect("daemon child").id()).expect("daemon pid");
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
    let mut daemon = child.0.take().expect("daemon child");
    let status = tokio::task::spawn_blocking(move || daemon.wait())
        .await
        .expect("daemon wait task")
        .expect("daemon wait");
    assert_eq!(status.signal(), Some(libc::SIGKILL));

    let config =
        pgwire_server::ServerConfig::new(&database_path, "127.0.0.1:0".parse().expect("addr"))
            .with_create_if_missing(false);
    let reopened = pgwire_server::RunningServer::start(config)
        .await
        .expect("reopen after daemon kill");
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=omendb",
            reopened.local_addr().port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect after daemon kill");
    let connection_task = tokio::spawn(connection);
    let row = client
        .query_one("SELECT count(*) FROM items", &[])
        .await
        .expect("read durable row after daemon kill");
    assert_eq!(row.get::<_, i64>(0), 1);
    drop(client);
    reopened.shutdown().await.expect("shutdown reopened server");
    let _ = connection_task.await;
}

#[tokio::test]
async fn persistent_server_rejects_unbounded_connection_configuration() {
    let directory = tempdir().expect("tempdir");
    let error = match pgwire_server::RunningServer::start(
        pgwire_server::ServerConfig::new(
            directory.path().join("db"),
            "127.0.0.1:0".parse().expect("addr"),
        )
        .with_max_connections(0),
    )
    .await
    {
        Ok(_) => panic!("zero connection bound must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        pgwire_server::ServerError::InvalidConfiguration(_)
    ));
}

#[tokio::test]
async fn wire_client_selects_seeds_and_reads_typed_rows() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());

    let (addr, _server) =
        pgwire_server::spawn(database.clone(), "127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("spawn server");

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=spike dbname=spike",
            addr.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move { connection.await.expect("connection") });

    let one = client.query_one("SELECT 1", &[]).await.expect("SELECT 1");
    assert_eq!(one.get::<_, i64>(0), 1);

    let rows = client
        .query("SELECT id, email FROM users", &[])
        .await
        .expect("typed select");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "alice@example.com");

    let inserted = client
        .execute(
            "INSERT INTO users (id, email) VALUES (2, 'bob@example.com')",
            &[],
        )
        .await
        .expect("insert over the wire");
    assert_eq!(inserted, 1);

    let count = client
        .query("SELECT id FROM users", &[])
        .await
        .expect("post-insert select");
    assert_eq!(count.len(), 2);
}

#[tokio::test]
async fn wire_client_gets_clean_error_for_unsupported_sql() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    let (addr, _server) = pgwire_server::spawn(database, "127.0.0.1:0".parse().expect("addr"))
        .await
        .expect("spawn server");

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=spike dbname=spike",
            addr.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move { connection.await.expect("connection") });

    let error = client
        .query("SELECT * FROM nonexistent_table", &[])
        .await
        .expect_err("unsupported query errors cleanly");
    // A clean PostgreSQL-style error with a mapped SQLSTATE - the connection
    // stays usable rather than dying on an unhandled failure.
    assert!(
        matches!(
            error.code(),
            Some(&tokio_postgres::error::SqlState::INTERNAL_ERROR)
        ),
        "unexpected error shape: {error:?}"
    );
    let recovered = client
        .query_one("SELECT 1", &[])
        .await
        .expect("connection survives a failed statement");
    assert_eq!(recovered.get::<_, i64>(0), 1);
}

async fn wire_client(database: Arc<RwLock<RelationalDatabase>>) -> tokio_postgres::Client {
    let (addr, _server) =
        pgwire_server::spawn(database.clone(), "127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("spawn server");
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=omendb", addr.port()),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move { connection.await.expect("connection") });
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_cancel_request_aborts_lock_wait_before_publication() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    let (addr, server) =
        pgwire_server::spawn(database.clone(), "127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("spawn server");
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=omendb", addr.port()),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let cancel_token = client.cancel_token();

    let (release_lock, wait_for_release) = std::sync::mpsc::channel();
    let database_for_lock = database.clone();
    let lock_thread = std::thread::spawn(move || {
        let _database_guard = database_for_lock.write().expect("database lock");
        wait_for_release.recv().expect("release publication lock");
    });

    let query = tokio::spawn(async move {
        client
            .execute(
                "INSERT INTO users (id, email) VALUES (2, 'cancelled@example.com')",
                &[],
            )
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    cancel_token
        .cancel_query(tokio_postgres::NoTls)
        .await
        .expect("send cancel request");

    let error = tokio::time::timeout(std::time::Duration::from_secs(5), query)
        .await
        .expect("cancelled query completed")
        .expect("query task completed")
        .expect_err("cancelled query must fail");
    assert_eq!(error.code(), Some(&SqlState::QUERY_CANCELED));

    release_lock.send(()).expect("release publication lock");
    lock_thread.join().expect("lock thread");
    let (check, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=omendb", addr.port()),
        tokio_postgres::NoTls,
    )
    .await
    .expect("reconnect after cancellation");
    tokio::spawn(async move { connection.await.expect("connection") });
    let row = check
        .query_one("SELECT count(*) FROM users", &[])
        .await
        .expect("read after cancellation");
    assert_eq!(row.get::<_, i64>(0), 1);
    server.abort();
}

#[tokio::test]
async fn wire_transaction_block_rollback_discards_writes() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    let client = wire_client(database.clone()).await;

    // Simple-protocol block: BEGIN, write, ROLLBACK - nothing durable.
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("INSERT INTO users (id, email) VALUES (10, 'tx@example.com')")
        .await
        .expect("in-block insert");
    let visible_inside = client
        .query_one("SELECT count(*) FROM users", &[])
        .await
        .expect("read inside block");
    assert_eq!(visible_inside.get::<_, i64>(0), 2, "seed + in-block insert");
    client.batch_execute("ROLLBACK").await.expect("rollback");

    let after = client
        .query_one("SELECT count(*) FROM users", &[])
        .await
        .expect("read after rollback");
    assert_eq!(after.get::<_, i64>(0), 1, "back to seed-only");
}

#[tokio::test]
async fn wire_transaction_block_commit_persists_and_crosses_connections() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    let mut writer = wire_client(database.clone()).await;

    // Extended-protocol block via tokio-postgres' transaction handle: it
    // issues BEGIN over the wire and ROLLBACKs on drop unless committed.
    {
        let transaction = writer.transaction().await.expect("begin");
        transaction
            .execute(
                "INSERT INTO users (id, email) VALUES ($1, $2)",
                &[&11i64, &"committed@example.com"],
            )
            .await
            .expect("parameterized in-block insert");
        transaction.commit().await.expect("commit");
    }

    // A second independent connection observes the published writes.
    let reader = wire_client(database).await;
    let row = reader
        .query_one("SELECT email FROM users WHERE id = $1", &[&11i64])
        .await
        .expect("cross-connection read");
    assert_eq!(row.get::<_, &str>(0), "committed@example.com");

    // Dropped (uncommitted) blocks roll back through the same path.
    {
        let transaction = writer.transaction().await.expect("begin second");
        transaction
            .execute(
                "INSERT INTO users (id, email) VALUES ($1, $2)",
                &[&12i64, &"dropped@example.com"],
            )
            .await
            .expect("second in-block insert");
    } // dropped here -> implicit rollback
    let after = reader
        .query_one("SELECT count(*) FROM users", &[])
        .await
        .expect("count after drop");
    assert_eq!(
        after.get::<_, i64>(0),
        2,
        "seed + committed 11; dropped 12 gone"
    );
}

#[tokio::test]
async fn wire_aborted_block_rejects_until_rollback() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    let client = wire_client(database.clone()).await;

    client.batch_execute("BEGIN").await.expect("begin");
    let failure = client
        .batch_execute("SELECT * FROM nonexistent_table")
        .await
        .expect_err("statement inside the block fails");
    drop(failure);

    // Aborted state: ordinary statements fail with 25P02.
    let aborted = client
        .batch_execute("INSERT INTO users (id, email) VALUES (20, 'ignored@example.com')")
        .await
        .expect_err("aborted block rejects statements");
    assert!(
        matches!(
            aborted.code(),
            Some(&tokio_postgres::error::SqlState::IN_FAILED_SQL_TRANSACTION)
        ),
        "expected 25P02 aborted-block error, got: {:?}",
        aborted.as_db_error()
    );

    client.batch_execute("ROLLBACK").await.expect("rollback");
    // Connection fully usable again; nothing from the failed block landed.
    let after = client
        .query_one("SELECT count(*) FROM users", &[])
        .await
        .expect("usable after rollback");
    assert_eq!(after.get::<_, i64>(0), 1, "only the seed row remains");
}

#[tokio::test]
async fn wire_scram_auth_accepts_provisioned_user_and_rejects_bad_password() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    omendb::pgwire_server::provision_wire_user(
        &mut database.write().expect("lock"),
        "alice",
        "wonderland",
    )
    .expect("provision user");

    let (addr, _server) =
        pgwire_server::spawn(database.clone(), "127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("spawn server");
    let dsn = |user: &str, password: &str| {
        format!(
            "host=127.0.0.1 port={} user={user} password={password}",
            addr.port()
        )
    };

    // Correct password authenticates and the connection is fully usable.
    let (client, connection) =
        tokio_postgres::connect(&dsn("alice", "wonderland"), tokio_postgres::NoTls)
            .await
            .expect("authenticated connect");
    tokio::spawn(async move { connection.await.expect("connection") });
    let row = client
        .query_one("SELECT count(*) FROM users", &[])
        .await
        .expect("authenticated query");
    assert_eq!(row.get::<_, i64>(0), 1);

    // Wrong password and unknown user both fail with 28P01.
    for (user, password) in [("alice", "wrong"), ("mallory", "wonderland")] {
        let error = match tokio_postgres::connect(&dsn(user, password), tokio_postgres::NoTls).await
        {
            Err(error) => error,
            Ok(_) => panic!("must reject {user}"),
        };
        assert!(
            error
                .as_db_error()
                .map(|db| db.code().code() == "28P01")
                .unwrap_or(false),
            "expected 28P01 for {user}, got {error:?}"
        );
    }
}

#[test]
fn pgwire_sasl_initial_response_decode_probe() {
    use pgwire::messages::PgWireFrontendMessage;
    let client_first = format!("n,,n=alice,r={}", "abcdefghijklmnopqrstuv");
    let mech = b"SCRAM-SHA-256";
    let mut buf = bytes::BytesMut::new();
    buf.extend_from_slice(b"p");
    let body_len = mech.len() + 1 + 4 + client_first.len();
    buf.extend_from_slice(&((body_len + 4) as i32).to_be_bytes());
    buf.extend_from_slice(mech);
    buf.extend_from_slice(&[0]);
    buf.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
    buf.extend_from_slice(client_first.as_bytes());

    let mut ctx = pgwire::messages::DecodeContext::default();
    ctx.awaiting_frontend_ssl = false;
    ctx.awaiting_frontend_startup = false;
    let decoded = PgWireFrontendMessage::decode(&mut buf, &ctx)
        .expect("decode ok")
        .expect("complete message");
    let msg = match decoded {
        PgWireFrontendMessage::PasswordMessageFamily(m) => m,
        other => panic!("unexpected message {other:?}"),
    };
    let sasl = msg.into_sasl_initial_response().expect("coerce");
    assert_eq!(sasl.auth_method, "SCRAM-SHA-256");
    assert_eq!(
        std::str::from_utf8(sasl.data.as_ref().expect("data")).expect("utf8"),
        client_first
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn wire_concurrent_reads_publish_and_read_without_error() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        for id in 2..=200u64 {
            db.execute_sql(&format!(
                "INSERT INTO users (id, email) VALUES ({id}, 'user{id}@example.com')"
            ))
            .expect("seed rows");
        }
    }

    let (addr, _server) =
        pgwire_server::spawn(database.clone(), "127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("spawn server");
    let port = addr.port();

    async fn connect_client(port: u16, user: &str) -> tokio_postgres::Client {
        let dsn = format!("host=127.0.0.1 port={port} user={user}");
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move { connection.await.expect("connection") });
        client
    }

    const READERS: usize = 6;
    const QUERIES_PER_READER: usize = 40;
    let mut reader_handles = Vec::new();
    for reader in 0..READERS {
        let handle = tokio::spawn(async move {
            let client = connect_client(port, "reader").await;
            for step in 0..QUERIES_PER_READER {
                let id = 1 + ((reader * QUERIES_PER_READER + step) % 200) as i64;
                let rows = client
                    .query("SELECT email FROM users WHERE id = $1", &[&id])
                    .await
                    .unwrap_or_else(|error| panic!("concurrent read id={id}: {error}"));
                assert_eq!(rows.len(), 1, "id={id}");
                assert!(!rows[0].get::<_, &str>(0).is_empty());
            }
        });
        reader_handles.push(handle);
    }

    // One writer publishes blocks while the readers run; neither side may
    // observe errors or lost visibility.
    let writer = connect_client(port, "writer").await;
    for id in 1000..1020i64 {
        writer
            .batch_execute(&format!(
                "BEGIN; INSERT INTO users (id, email) VALUES ({id}, 'w{id}@example.com'); COMMIT;"
            ))
            .await
            .expect("writer block");
    }

    for handle in reader_handles {
        handle.await.expect("reader task");
    }

    // Correctness first: every published write is visible.
    let verifier = connect_client(port, "verifier").await;
    let total = verifier
        .query_one("SELECT count(*) FROM users", &[])
        .await
        .expect("final count");
    assert_eq!(total.get::<_, i64>(0), 220);
}

/// Timing gate: concurrent readers must overlap rather than serialize.
/// The wall-clock ratio is load-sensitive, so this runs explicitly (like
/// `project_group_commit_gate`), not in default CI where parallel suites
/// invalidate the baseline comparison.
#[ignore = "timing gate: run explicitly with --ignored"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn wire_concurrent_reads_scale_while_writes_publish() {
    use std::time::Instant;

    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        for id in 2..=200u64 {
            db.execute_sql(&format!(
                "INSERT INTO users (id, email) VALUES ({id}, 'user{id}@example.com')"
            ))
            .expect("seed rows");
        }
    }

    let (addr, _server) =
        pgwire_server::spawn(database.clone(), "127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("spawn server");
    let port = addr.port();
    let connect = move |user: &str| {
        let dsn = format!("host=127.0.0.1 port={port} user={user}");
        async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect");
            tokio::spawn(async move { connection.await.expect("connection") });
            client
        }
    };

    const READERS: usize = 6;
    const QUERIES_PER_READER: usize = 40;

    // Serial baseline: one reader alone establishes the uncontended wall
    // time for QUERIES_PER_READER queries.
    {
        let baseline_client = connect("baseline").await;
        let baseline_started = Instant::now();
        for id in 1..=QUERIES_PER_READER as i64 {
            baseline_client
                .query("SELECT email FROM users WHERE id = $1", &[&id])
                .await
                .expect("baseline read");
        }
        let base = baseline_started.elapsed().as_secs_f64();
        eprintln!("CONCURRENCY baseline_40={:.3}s", base);

        // Concurrent phase: READERS x QUERIES_PER_READER queries at once.
        // Serialized execution forces each reader to wait behind the
        // others (~READERS x base); genuine overlap keeps every reader
        // near the baseline. The bound sits between with slack for
        // scheduler noise and writer publications.
        let mut reader_handles = Vec::new();
        for reader in 0..READERS {
            let handle = tokio::spawn(async move {
                let client = connect("reader").await;
                let started = Instant::now();
                for step in 0..QUERIES_PER_READER {
                    let id = 1 + ((reader * QUERIES_PER_READER + step) % 200) as i64;
                    let rows = client
                        .query("SELECT email FROM users WHERE id = $1", &[&id])
                        .await
                        .unwrap_or_else(|error| panic!("concurrent read id={id}: {error}"));
                    assert_eq!(rows.len(), 1, "id={id}");
                    assert!(!rows[0].get::<_, &str>(0).is_empty());
                }
                started.elapsed()
            });
            reader_handles.push(handle);
        }

        // One writer publishes blocks while the readers run.
        let writer_started = Instant::now();
        let writer = connect("writer").await;
        for id in 1000..1020i64 {
            writer
                .batch_execute(&format!(
                    "BEGIN; INSERT INTO users (id, email) VALUES ({id}, 'w{id}@example.com'); COMMIT;"
                ))
                .await
                .expect("writer block");
        }
        eprintln!("writer20={:.3}s", writer_started.elapsed().as_secs_f64());

        let read_elapsed: Vec<std::time::Duration> = futures::future::join_all(reader_handles)
            .await
            .into_iter()
            .map(|r| r.expect("reader task"))
            .collect();

        let max_reader = read_elapsed.iter().max().expect("readers").as_secs_f64();
        eprintln!(
            "CONCURRENCY readers={} baseline_40={:.3}s max_concurrent_40={:.3}s ratio={:.2}x",
            READERS,
            base,
            max_reader,
            max_reader / base
        );
        assert!(
            max_reader < base * 4.5,
            "readers serialized: concurrent wall {:.3}s vs serial baseline {:.3}s (ratio {:.1}x, expected < 4.5x)",
            max_reader,
            base,
            max_reader / base
        );
    }

    // Correctness first: every published write is visible.
    let verifier = connect("verifier").await;
    let total = verifier
        .query_one("SELECT count(*) FROM users", &[])
        .await
        .expect("final count");
    assert_eq!(total.get::<_, i64>(0), 220);
}

#[tokio::test]
async fn wire_seeded_ids_visible_serially() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        for id in 2..=200u64 {
            db.execute_sql(&format!(
                "INSERT INTO users (id, email) VALUES ({id}, 'user{id}@example.com')"
            ))
            .expect("seed rows");
        }
    }
    let client = wire_client(database.clone()).await;
    let mut missing = Vec::new();
    for id in 1..=200i64 {
        let rows = client
            .query("SELECT email FROM users WHERE id = $1", &[&id])
            .await
            .expect("lookup");
        if rows.is_empty() {
            missing.push(id);
        }
    }
    assert!(missing.is_empty(), "missing ids: {missing:?}");
    let count = client
        .query_one("SELECT count(*) FROM users", &[])
        .await
        .expect("count");
    assert_eq!(count.get::<_, i64>(0), 200);
}

#[test]
fn facade_sequential_autocommit_inserts_all_visible() {
    let directory = tempdir().expect("tempdir");
    let database = std::sync::Arc::new(std::sync::RwLock::new(
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
            .expect("create"),
    ));
    let database = &mut *database.write().expect("lock");
    database
        .create_table(TableDefinition {
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
        })
        .expect("create table");
    for id in 1..=10u64 {
        database
            .execute_sql(&format!(
                "INSERT INTO users (id, email) VALUES ({id}, 'u{id}@x.com')"
            ))
            .expect("insert");
    }
    let result = database
        .execute_sql("SELECT count(*) FROM users")
        .expect("count");
    assert_eq!(
        result.rows[0][0],
        omendb::Value::U64(10),
        "all 10 rows visible"
    );
    // Now publish a second schema change, like serve() bootstrapping
    // pgwire_auth after user data exists.
    database
        .create_table(TableDefinition {
            id: TableId(u64::MAX - 1),
            name: "pgwire_auth".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(u16::MAX - 1),
                    name: "username".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(u16::MAX),
                    name: "secret".to_owned(),
                    data_type: ColumnType::Bytes,
                    nullable: false,
                },
            ],
        })
        .expect("create auth table");
    let result = database
        .execute_sql("SELECT count(*) FROM users")
        .expect("count after DDL");
    assert_eq!(
        result.rows[0][0],
        omendb::Value::U64(10),
        "rows survive an unrelated DDL publication"
    );
}

#[tokio::test]
async fn wire_inner_join_matches_and_filters() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')")
            .expect("seed bob");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "orders".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "item".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create orders");
        for (order_id, user_id, item) in [(1u64, 1u64, "book"), (2, 1, "pen"), (3, 2, "lamp")] {
            db.execute_sql(&format!(
                "INSERT INTO orders (order_id, user_id, item) VALUES ({order_id}, {user_id}, '{item}')"
            ))
            .expect("insert order");
        }
    }

    let client = wire_client(database.clone()).await;

    // Unfiltered join: alice has two orders, bob one.
    let rows = client
        .query(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
            &[],
        )
        .await
        .expect("join query");
    assert_eq!(rows.len(), 3, "expected 3 joined rows");
    let names: Vec<&str> = rows[0]
        .columns()
        .iter()
        .map(|column| column.name())
        .collect();
    assert_eq!(names[0], "users.id");
    assert_eq!(names.last().copied().expect("columns"), "orders.item");

    // WHERE over the combined schema filters to one user's rows.
    let filtered = client
        .query(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE id = 1",
            &[],
        )
        .await
        .expect("filtered join");
    assert_eq!(filtered.len(), 2);
    assert_eq!(
        filtered[0].get::<_, &str>("users.email"),
        "alice@example.com"
    );

    // Join key with no match contributes nothing.
    let empty = client
        .query(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE order_id = 99",
            &[],
        )
        .await
        .expect("empty join");
    assert!(empty.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_hot_statement_repeats_across_params() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        for id in 2..=50u64 {
            db.execute_sql(&format!(
                "INSERT INTO users (id, email) VALUES ({id}, 'user{id}@example.com')"
            ))
            .expect("seed rows");
        }
    }
    let client = wire_client(database.clone()).await;
    // Same source text, many different parameters: every execute after the
    // first resolves through the parse cache and must bind the right row.
    for round in 0..3 {
        for id in 1..=50i64 {
            let row = client
                .query_one("SELECT email FROM users WHERE id = $1", &[&id])
                .await
                .unwrap_or_else(|error| panic!("round={round} id={id}: {error}"));
            let expected: String = if id == 1 {
                "alice@example.com".to_owned()
            } else {
                format!("user{id}@example.com")
            };
            assert_eq!(row.get::<_, &str>(0), expected.as_str());
        }
    }
}

#[tokio::test]
async fn wire_inner_join_projection_alias_order_by() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')")
            .expect("seed bob");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "orders".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "item".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create orders");
        for (oid, uid, item) in [(1u64, 1u64, "book"), (2, 1, "pen"), (3, 2, "lamp")] {
            db.execute_sql(&format!(
                "INSERT INTO orders (order_id, user_id, item) VALUES ({oid}, {uid}, '{item}')"
            ))
            .expect("insert order");
        }
    }
    let client = wire_client(database.clone()).await;

    // Qualified projection narrows the output and renames via alias.
    let rows = client
        .query(
            "SELECT u.email AS who, o.item FROM users u JOIN orders o ON u.id = o.user_id ORDER BY order_id DESC",
            &[],
        )
        .await
        .expect("projected aliased ordered join");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].columns()[0].name(), "who");
    assert_eq!(rows[0].columns()[1].name(), "item");
    assert_eq!(
        rows[0].get::<_, &str>(1),
        "lamp",
        "ORDER BY order_id DESC first"
    );

    // Unqualified unique column resolves; literal projects.
    let filtered = client
        .query(
            "SELECT item, 42 FROM users JOIN orders ON users.id = user_id WHERE user_id = 2",
            &[],
        )
        .await
        .expect("filtered projected join");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].get::<_, &str>(0), "lamp");
    assert_eq!(filtered[0].get::<_, i64>(1), 42);
}

#[tokio::test]
async fn wire_three_way_join() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')")
            .expect("seed bob");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "orders".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "item".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create orders");
        db.create_table(TableDefinition {
            id: TableId(10),
            name: "shipments".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "shipment_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "carrier".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create shipments");
        for (oid, uid, item) in [(1i64, 1i64, "book"), (2, 2, "lamp")] {
            db.execute_sql(&format!(
                "INSERT INTO orders (order_id, user_id, item) VALUES ({oid}, {uid}, '{item}')"
            ))
            .expect("order");
        }
        for (sid, oid, carrier) in [(10i64, 1i64, "ups"), (11, 2, "fedex")] {
            db.execute_sql(&format!(
                "INSERT INTO shipments (shipment_id, order_id, carrier) VALUES ({sid}, {oid}, '{carrier}')"
            ))
            .expect("shipment");
        }
    }
    let client = wire_client(database.clone()).await;
    let rows = client
        .query(
            "SELECT email, item, carrier FROM users u JOIN orders o ON u.id = o.user_id JOIN shipments s ON o.order_id = s.order_id ORDER BY email",
            &[],
        )
        .await
        .expect("three-way join");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, &str>(0), "alice@example.com");
    assert_eq!(rows[0].get::<_, &str>(1), "book");
    assert_eq!(rows[0].get::<_, &str>(2), "ups");
    assert_eq!(rows[1].get::<_, &str>(2), "fedex");
}

#[tokio::test]
async fn wire_join_aggregates() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')")
            .expect("seed bob");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "orders".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "amount".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
            ],
        })
        .expect("create orders");
        for (oid, uid, amount) in [(1u64, 1u64, 10u64), (2, 1, 20), (3, 2, 5)] {
            db.execute_sql(&format!(
                "INSERT INTO orders (order_id, user_id, amount) VALUES ({oid}, {uid}, {amount})"
            ))
            .expect("insert order");
        }
    }
    let client = wire_client(database.clone()).await;
    let row = client
        .query_one(
            "SELECT count(*), sum(amount), min(amount), max(amount) FROM users JOIN orders ON users.id = orders.user_id",
            &[],
        )
        .await
        .expect("join aggregates");
    assert_eq!(row.get::<_, i64>(0), 3);
    assert_eq!(row.get::<_, i64>(1), 35);
    assert_eq!(row.get::<_, i64>(2), 5);
    assert_eq!(row.get::<_, i64>(3), 20);

    // Aggregates compose with WHERE.
    let filtered = client
        .query_one(
            "SELECT count(*), sum(amount) FROM users JOIN orders ON users.id = orders.user_id WHERE user_id = 1",
            &[],
        )
        .await
        .expect("filtered join aggregates");
    assert_eq!(filtered.get::<_, i64>(0), 2);
    assert_eq!(filtered.get::<_, i64>(1), 30);
}

#[tokio::test]
async fn wire_non_equi_join() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.create_table(TableDefinition {
            id: TableId(11),
            name: "ranges".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "range_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "low".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "high".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
            ],
        })
        .expect("create ranges");
        for (rid, low, high) in [(1u64, 0u64, 10u64), (2, 20, 30)] {
            db.execute_sql(&format!(
                "INSERT INTO ranges (range_id, low, high) VALUES ({rid}, {low}, {high})"
            ))
            .expect("insert range");
        }
    }
    let client = wire_client(database.clone()).await;
    // users(id=1,2) against ranges [0,10] and [20,30]: id falls in range 1 only for id=1.
    let rows = client
        .query(
            "SELECT email, range_id FROM users JOIN ranges ON users.id >= ranges.low AND users.id <= ranges.high",
            &[],
        )
        .await
        .expect("non-equi join");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "alice@example.com");
    assert_eq!(rows[0].get::<_, i64>(1), 1);
}

#[tokio::test]
async fn wire_join_group_by() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')")
            .expect("seed bob");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "orders".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "amount".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
            ],
        })
        .expect("create orders");
        for (oid, uid, amount) in [(1u64, 1u64, 10u64), (2, 1, 20), (3, 2, 5)] {
            db.execute_sql(&format!(
                "INSERT INTO orders (order_id, user_id, amount) VALUES ({oid}, {uid}, {amount})"
            ))
            .expect("insert order");
        }
    }
    let client = wire_client(database.clone()).await;
    let rows = client
        .query(
            "SELECT email, sum(amount) AS total FROM users JOIN orders ON users.id = orders.user_id GROUP BY email ORDER BY email",
            &[],
        )
        .await
        .expect("grouped join");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].columns()[0].name(), "email");
    assert_eq!(rows[0].columns()[1].name(), "total");
    assert_eq!(rows[0].get::<_, &str>(0), "alice@example.com");
    assert_eq!(rows[0].get::<_, i64>(1), 30);
    assert_eq!(rows[1].get::<_, &str>(0), "bob@example.com");
    assert_eq!(rows[1].get::<_, i64>(1), 5);

    // ORDER BY an aggregate alias sorts post-aggregation.
    let ranked = client
        .query(
            "SELECT email, sum(amount) AS total FROM users JOIN orders ON users.id = orders.user_id GROUP BY email ORDER BY total DESC",
            &[],
        )
        .await
        .expect("aggregate-ordered join");
    assert_eq!(ranked.len(), 2);
    assert_eq!(ranked[0].get::<_, i64>(1), 30, "alice first by total DESC");
    assert_eq!(ranked[1].get::<_, i64>(1), 5);

    // HAVING filters groups by their aggregate result.
    let big = client
        .query(
            "SELECT email, sum(amount) AS total FROM users JOIN orders ON users.id = orders.user_id GROUP BY email HAVING sum(amount) > 10",
            &[],
        )
        .await
        .expect("having join");
    assert_eq!(big.len(), 1, "only alice's group sums above 10");
    assert_eq!(big[0].get::<_, &str>(0), "alice@example.com");
    assert_eq!(big[0].get::<_, i64>(1), 30);
}

#[tokio::test]
async fn wire_join_order_by_qualified_and_ambiguous() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')")
            .expect("seed bob");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "orders".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "item".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create orders");
        for (oid, uid, item) in [(1u64, 1u64, "pen"), (2, 2, "lamp")] {
            db.execute_sql(&format!(
                "INSERT INTO orders (order_id, user_id, item) VALUES ({oid}, {uid}, '{item}')"
            ))
            .expect("insert order");
        }
    }
    let client = wire_client(database.clone()).await;
    let rows = client
        .query(
            "SELECT email FROM users u JOIN orders o ON u.id = o.user_id ORDER BY u.email DESC",
            &[],
        )
        .await
        .expect("qualified order by");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, &str>(0), "bob@example.com");

    // A bare name matching nothing errors cleanly.
    let missing = client
        .query(
            "SELECT email FROM users u JOIN orders o ON u.id = o.user_id ORDER BY nonexistent",
            &[],
        )
        .await;
    assert!(missing.is_err());
}

#[tokio::test]
async fn wire_left_outer_join_null_extension() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql(
            "INSERT INTO users (id, email) VALUES (2, 'bob@example.com'), (3, 'carol@example.com')",
        )
        .expect("seed more users");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "orders".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "item".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create orders");
        // Only alice and bob have orders; carol has none.
        for (oid, uid, item) in [(1u64, 1u64, "book"), (2, 2, "lamp")] {
            db.execute_sql(&format!(
                "INSERT INTO orders (order_id, user_id, item) VALUES ({oid}, {uid}, '{item}')"
            ))
            .expect("insert order");
        }
    }
    let client = wire_client(database.clone()).await;

    let rows = client
        .query(
            "SELECT email, item FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id ORDER BY email",
            &[],
        )
        .await
        .expect("left outer join");
    assert_eq!(rows.len(), 3, "alice 1 + bob 1 + carol null-extended");
    assert_eq!(rows[0].get::<_, &str>(0), "alice@example.com");
    assert_eq!(rows[0].get::<_, &str>(1), "book");
    assert_eq!(rows[2].get::<_, &str>(0), "carol@example.com");
    assert!(
        rows[2].try_get::<_, &str>(1).is_err(),
        "item is NULL for carol"
    );

    // Bare JOIN spelling of LEFT OUTER also parses.
    let bare = client
        .query(
            "SELECT count(*) FROM users LEFT JOIN orders ON users.id = orders.user_id",
            &[],
        )
        .await
        .expect("bare left join");
    assert_eq!(bare[0].get::<_, i64>(0), 3);
}

#[tokio::test]
async fn wire_using_and_natural_joins() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "orders".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "item".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create orders");
        for (oid, uid, item) in [(1u64, 1u64, "book"), (2, 2, "lamp")] {
            db.execute_sql(&format!(
                "INSERT INTO orders (order_id, user_id, item) VALUES ({oid}, {uid}, '{item}')"
            ))
            .expect("insert order");
        }
        db.create_table(TableDefinition {
            id: TableId(10),
            name: "order_items".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "price".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
            ],
        })
        .expect("create order_items");
        for (oid, price) in [(1u64, 999u64), (2, 499)] {
            db.execute_sql(&format!(
                "INSERT INTO order_items (order_id, price) VALUES ({oid}, {price})"
            ))
            .expect("insert item");
        }
    }
    let client = wire_client(database.clone()).await;

    // USING(order_id): both sides carry the column; the incoming
    // duplicate is dropped so order_id appears exactly once.
    let rows = client
        .query(
            "SELECT item, price FROM orders JOIN order_items USING (order_id) ORDER BY price",
            &[],
        )
        .await
        .expect("using join");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, &str>(0), "lamp");
    assert_eq!(rows[0].get::<_, i64>(1), 499);
    assert_eq!(rows[1].get::<_, &str>(0), "book");

    // Wildcard over USING shows the shared column exactly once.
    let starred = client
        .query(
            "SELECT * FROM orders JOIN order_items USING (order_id) ORDER BY order_id",
            &[],
        )
        .await
        .expect("starred using join");
    let names: Vec<&str> = starred[0].columns().iter().map(|c| c.name()).collect();
    assert_eq!(
        names.iter().filter(|n| n.ends_with("order_id")).count(),
        1,
        "USING keeps exactly one order_id column, got {names:?}"
    );

    // NATURAL joins on shared column names; users/orders share none,
    // so NATURAL must refuse rather than cross-join silently.
    let natural = client
        .query(
            "SELECT item FROM users NATURAL JOIN orders WHERE users.id = 2",
            &[],
        )
        .await;
    assert!(natural.is_err());
}

#[tokio::test]
async fn wire_right_and_full_outer_joins() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        // carol (id=3) has no orders; order 3 references a missing user.
        db.execute_sql(
            "INSERT INTO users (id, email) VALUES (2, 'bob@example.com'), (3, 'carol@example.com')",
        )
        .expect("seed users");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "orders".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "item".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create orders");
        for (oid, uid, item) in [(1u64, 1u64, "book"), (2, 2, "lamp"), (3, 99, "ghost")] {
            db.execute_sql(&format!(
                "INSERT INTO orders (order_id, user_id, item) VALUES ({oid}, {uid}, '{item}')"
            ))
            .expect("insert order");
        }
    }
    let client = wire_client(database.clone()).await;

    // RIGHT JOIN: the ghost order survives with NULL user columns.
    let rows = client
        .query(
            "SELECT email, item FROM users RIGHT JOIN orders ON users.id = orders.user_id ORDER BY item",
            &[],
        )
        .await
        .expect("right join");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, &str>(1), "book");
    assert_eq!(rows[0].get::<_, &str>(0), "alice@example.com");
    let ghost = &rows[1]; // item sort: book, ghost, lamp
    assert_eq!(ghost.get::<_, &str>(1), "ghost");
    assert!(
        ghost.try_get::<_, &str>(0).is_err(),
        "ghost order has NULL email"
    );

    // FULL OUTER: both unmatched users and unmatched orders survive.
    let full = client
        .query(
            "SELECT email, item FROM users FULL OUTER JOIN orders ON users.id = orders.user_id",
            &[],
        )
        .await
        .expect("full outer join");
    assert_eq!(
        full.len(),
        4,
        "alice + bob + carol(null item) + ghost(null email)"
    );
}

#[tokio::test]
async fn wire_cross_join_pairs_everything() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')")
            .expect("seed bob");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "colors".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "color_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "name".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create colors");
        for (cid, name) in [(1u64, "red"), (2, "blue"), (3, "green")] {
            db.execute_sql(&format!(
                "INSERT INTO colors (color_id, name) VALUES ({cid}, '{name}')"
            ))
            .expect("insert color");
        }
    }
    let client = wire_client(database.clone()).await;
    let rows = client
        .query(
            "SELECT email, name FROM users CROSS JOIN colors ORDER BY color_id",
            &[],
        )
        .await
        .expect("cross join");
    assert_eq!(rows.len(), 6, "2 users x 3 colors");
    assert_eq!(rows[0].get::<_, &str>(1), "red");
}

#[tokio::test]
async fn wire_update_delete_accept_table_alias() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')")
            .expect("seed bob");
    }
    let client = wire_client(database.clone()).await;

    let updated = client
        .execute(
            "UPDATE users u SET email = $1 WHERE u.id = $2",
            &[&"robert@example.com", &2i64],
        )
        .await
        .expect("aliased update");
    assert_eq!(updated, 1);
    let row = client
        .query_one("SELECT email FROM users WHERE id = 2", &[])
        .await
        .expect("read back");
    assert_eq!(row.get::<_, &str>(0), "robert@example.com");

    let deleted = client
        .execute("DELETE FROM users u WHERE u.id = $1", &[&2i64])
        .await
        .expect("aliased delete");
    assert_eq!(deleted, 1);
    let remaining = client
        .query_one("SELECT count(*) FROM users", &[])
        .await
        .expect("count after delete");
    assert_eq!(remaining.get::<_, i64>(0), 1);
}

#[tokio::test]
async fn wire_insert_returning() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    let client = wire_client(database.clone()).await;

    // Single-row RETURNING with explicit columns.
    let rows = client
        .query(
            "INSERT INTO users (id, email) VALUES (7, 'greg@example.com') RETURNING id, email",
            &[],
        )
        .await
        .expect("insert returning");
    assert_eq!(rows.len(), 1);
    let names: Vec<&str> = rows[0].columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, vec!["id", "email"]);
    assert_eq!(rows[0].get::<_, i64>(0), 7);
    assert_eq!(rows[0].get::<_, &str>(1), "greg@example.com");

    // Multi-row RETURNING * spans every column in schema order.
    let all = client
        .query(
            "INSERT INTO users (id, email) VALUES (8, 'hana@example.com'), (9, 'ivan@example.com') RETURNING *",
            &[],
        )
        .await
        .expect("multi-row insert returning");
    assert_eq!(all.len(), 2);
    let names: Vec<&str> = all[0].columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, vec!["id", "email"]);
    assert_eq!(all[1].get::<_, i64>(0), 9);
}

#[tokio::test]
async fn wire_update_delete_returning() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql(
            "INSERT INTO users (id, email) VALUES (2, 'bob@example.com'), (3, 'carol@example.com')",
        )
        .expect("seed users");
    }
    let client = wire_client(database.clone()).await;

    // UPDATE RETURNING shows post-update values.
    let updated = client
        .query(
            "UPDATE users SET email = 'robert@example.com' WHERE id = 2 RETURNING id, email",
            &[],
        )
        .await
        .expect("update returning");
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].get::<_, i64>(0), 2);
    assert_eq!(updated[0].get::<_, &str>(1), "robert@example.com");

    // DELETE RETURNING shows the removed rows.
    let removed = client
        .query("DELETE FROM users WHERE id >= 2 RETURNING *", &[])
        .await
        .expect("delete returning");
    assert_eq!(removed.len(), 2);
    assert_eq!(removed[1].get::<_, i64>(0), 3);
    assert_eq!(removed[1].get::<_, &str>(1), "carol@example.com");

    let remaining = client
        .query_one("SELECT count(*) FROM users", &[])
        .await
        .expect("count after deletes");
    assert_eq!(remaining.get::<_, i64>(0), 1);
}

#[tokio::test]
async fn wire_distinct_single_table_and_join() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')")
            .expect("seed bob");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "orders".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "item".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create orders");
        // Two users, three orders - user_id duplicates across rows.
        for (oid, uid) in [(1u64, 1u64), (2, 1), (3, 2)] {
            db.execute_sql(&format!(
                "INSERT INTO orders (order_id, user_id, item) VALUES ({oid}, {uid}, 'x')"
            ))
            .expect("insert order");
        }
    }
    let client = wire_client(database.clone()).await;

    // DISTINCT collapses duplicate projected values.
    let distinct = client
        .query("SELECT DISTINCT user_id FROM orders ORDER BY user_id", &[])
        .await
        .expect("distinct single table");
    assert_eq!(distinct.len(), 2, "user_id 1 and 2 after dedup");
    assert_eq!(distinct[0].get::<_, i64>(0), 1);

    // DISTINCT over a join output.
    let joined = client
        .query(
            "SELECT DISTINCT email FROM users JOIN orders ON users.id = orders.user_id",
            &[],
        )
        .await
        .expect("distinct join");
    assert_eq!(joined.len(), 2);

    // On DISTINCT ON variant refuses.
    let on_variant = client
        .query("SELECT DISTINCT ON (user_id) user_id FROM orders", &[])
        .await;
    assert!(on_variant.is_err());
}

#[tokio::test]
async fn wire_update_from_cross_table() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        // alice starts on the free tier; pricing lives in a second table.
        db.execute_sql("UPDATE users SET email = 'alice@free.example.com' WHERE id = 1")
            .expect("seed email");
        db.execute_sql("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')")
            .expect("seed bob");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "tiers".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "tier".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create tiers");
        for (uid, tier) in [(1u64, "pro"), (2u64, "enterprise")] {
            db.execute_sql(&format!(
                "INSERT INTO tiers (user_id, tier) VALUES ({uid}, '{tier}')"
            ))
            .expect("insert tier");
        }
    }
    let client = wire_client(database.clone()).await;

    // The tiers table decides WHICH user gets upgraded; alice matches.
    let updated = client
        .execute(
            "UPDATE users SET email = 'alice@pro.example.com' FROM tiers WHERE users.id = tiers.user_id AND tier = 'pro'",
            &[],
        )
        .await
        .expect("update from");
    assert_eq!(updated, 1);
    let row = client
        .query_one("SELECT email FROM users WHERE id = 1", &[])
        .await
        .expect("read back");
    assert_eq!(row.get::<_, &str>(0), "alice@pro.example.com");

    // Bob's enterprise tier did not match the predicate.
    let bob = client
        .query_one("SELECT email FROM users WHERE id = 2", &[])
        .await
        .expect("read bob");
    assert_eq!(bob.get::<_, &str>(0), "bob@example.com");
}

#[tokio::test]
async fn wire_delete_using_cross_table() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql(
            "INSERT INTO users (id, email) VALUES (2, 'bob@example.com'), (3, 'carol@example.com')",
        )
        .expect("seed users");
        // A ban list decides which users to purge.
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "banned".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "reason".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create banned");
        for (uid, reason) in [(2u64, "spam"), (99u64, "unknown")] {
            db.execute_sql(&format!(
                "INSERT INTO banned (user_id, reason) VALUES ({uid}, '{reason}')"
            ))
            .expect("insert ban");
        }
    }
    let client = wire_client(database.clone()).await;

    let deleted = client
        .execute(
            "DELETE FROM users USING banned WHERE users.id = banned.user_id",
            &[],
        )
        .await
        .expect("delete using");
    assert_eq!(deleted, 1, "only bob matches a ban entry");

    let remaining = client
        .query("SELECT email FROM users ORDER BY id", &[])
        .await
        .expect("remaining users");
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[1].get::<_, &str>(0), "carol@example.com");
}

#[tokio::test]
async fn wire_schema_qualified_names() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    let client = wire_client(database.clone()).await;

    // public.users resolves; a non-public schema refuses cleanly.
    let rows = client
        .query("SELECT id FROM public.users", &[])
        .await
        .expect("qualified select");
    assert_eq!(rows.len(), 1);

    let other = client.query("SELECT * FROM private.secrets", &[]).await;
    assert!(other.is_err(), "non-public schemas must refuse");
}

#[tokio::test]
async fn wire_in_list_and_between_predicates() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql(
            "INSERT INTO users (id, email) VALUES (2, 'bob@example.com'), (3, 'carol@example.com')",
        )
        .expect("seed users");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "orders".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "item".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create orders");
        for (oid, uid) in [(1u64, 1u64), (2, 3)] {
            db.execute_sql(&format!(
                "INSERT INTO orders (order_id, user_id, item) VALUES ({oid}, {uid}, 'x')"
            ))
            .expect("insert order");
        }
    }
    let client = wire_client(database.clone()).await;

    let rows = client
        .query("SELECT id FROM users WHERE id IN (1, 3) ORDER BY id", &[])
        .await
        .expect("IN list");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i64>(0), 1);

    let not_in = client
        .query_one("SELECT count(*) FROM users WHERE id NOT IN (1)", &[])
        .await
        .expect("NOT IN");
    assert_eq!(not_in.get::<_, i64>(0), 2);

    let between = client
        .query("SELECT id FROM users WHERE id BETWEEN 2 AND 3", &[])
        .await
        .expect("between");
    assert_eq!(between.len(), 2);

    // IN composes with a join's cross-table WHERE.
    let joined = client
        .query(
            "SELECT email FROM users JOIN orders ON users.id = orders.user_id WHERE order_id IN (1, 3)",
            &[],
        )
        .await
        .expect("in-list join");
    assert_eq!(joined.len(), 1, "only alice holds an order with id 1 or 3");
    assert_eq!(joined[0].get::<_, &str>(0), "alice@example.com");
}

#[tokio::test]
async fn wire_in_subquery() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql(
            "INSERT INTO users (id, email) VALUES (2, 'bob@example.com'), (3, 'carol@example.com')",
        )
        .expect("seed users");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "orders".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "item".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create orders");
        // Only alice and bob have orders; carol does not.
        for (oid, uid) in [(1u64, 1u64), (2, 2)] {
            db.execute_sql(&format!(
                "INSERT INTO orders (order_id, user_id, item) VALUES ({oid}, {uid}, 'x')"
            ))
            .expect("insert order");
        }
    }
    let client = wire_client(database.clone()).await;

    // Uncorrelated IN subquery resolves once and filters correctly.
    let rows = client
        .query(
            "SELECT email FROM users WHERE id IN (SELECT user_id FROM orders) ORDER BY id",
            &[],
        )
        .await
        .expect("in subquery");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, &str>(0), "alice@example.com");
    assert_eq!(rows[1].get::<_, &str>(0), "bob@example.com");

    // NOT IN excludes the matched set.
    let excluded = client
        .query(
            "SELECT email FROM users WHERE id NOT IN (SELECT user_id FROM orders)",
            &[],
        )
        .await
        .expect("not in subquery");
    assert_eq!(excluded.len(), 1);
    assert_eq!(excluded[0].get::<_, &str>(0), "carol@example.com");

    // Multi-column subqueries refuse with guidance.
    let multi = client
        .query(
            "SELECT email FROM users WHERE id IN (SELECT user_id, item FROM orders)",
            &[],
        )
        .await;
    assert!(multi.is_err());
}

#[tokio::test]
async fn wire_exists_subquery() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')")
            .expect("seed bob");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "orders".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "item".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create orders");
        // Only alice has an order.
        db.execute_sql("INSERT INTO orders (order_id, user_id, item) VALUES (1, 1, 'x')")
            .expect("insert order");
    }
    let client = wire_client(database.clone()).await;

    // EXISTS gates on the subquery's row presence.
    let customers = client
        .query(
            "SELECT email FROM users WHERE EXISTS (SELECT 1 FROM orders)",
            &[],
        )
        .await
        .expect("exists true");
    assert_eq!(customers.len(), 2);

    let none = client
        .query(
            "SELECT email FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE order_id > 99)",
            &[],
        )
        .await
        .expect("exists false");
    assert!(none.is_empty());

    // NOT EXISTS flips it.
    let not_exists = client
        .query(
            "SELECT email FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE order_id > 99) AND users.id = 2",
            &[],
        )
        .await
        .expect("not exists");
    assert_eq!(not_exists.len(), 1);
    assert_eq!(not_exists[0].get::<_, &str>(0), "bob@example.com");
}

#[tokio::test]
async fn wire_scalar_subquery_projection() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')")
            .expect("seed bob");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "orders".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "order_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "item".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create orders");
        for (oid, uid, item) in [(1u64, 1u64, "book"), (2, 2, "lamp")] {
            db.execute_sql(&format!(
                "INSERT INTO orders (order_id, user_id, item) VALUES ({oid}, {uid}, '{item}')"
            ))
            .expect("insert order");
        }
    }
    let client = wire_client(database.clone()).await;

    // Scalar subquery alongside regular columns, aliased.
    let rows = client
        .query(
            "SELECT email, (SELECT count(*) FROM orders) AS order_total FROM users ORDER BY id",
            &[],
        )
        .await
        .expect("scalar subquery projection");
    assert_eq!(rows.len(), 2);
    let names: Vec<&str> = rows[0].columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, vec!["email", "order_total"]);
    assert_eq!(
        rows[0].get::<_, i64>(1),
        2,
        "count resolves once for every row"
    );

    // Multi-row scalars refuse.
    let multi = client
        .query("SELECT (SELECT user_id FROM orders) FROM users", &[])
        .await;
    assert!(multi.is_err());
}

#[tokio::test]
async fn wire_aggregate_order_by_alias() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        // Three users share two emails domains; group by domain and rank.
        for (id, email) in [(2u64, "bob@example.com"), (3, "carol@other.net")] {
            db.execute_sql(&format!(
                "INSERT INTO users (id, email) VALUES ({id}, '{email}')"
            ))
            .expect("seed user");
        }
    }
    let client = wire_client(database.clone()).await;

    let rows = client
        .query(
            "SELECT email, count(*) AS n FROM users GROUP BY email ORDER BY n DESC",
            &[],
        )
        .await
        .expect("aggregate order by alias");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, i64>(1), 1);
    // Deterministic ordering by the aggregate alias is the point: with
    // three singleton groups any order satisfies DESC, so also check
    // ascending flips it against a stable sort of names.
    let asc = client
        .query(
            "SELECT email, count(*) AS n FROM users GROUP BY email ORDER BY n ASC, email ASC",
            &[],
        )
        .await
        .expect("multi-term aggregate order");
    assert_eq!(asc[0].get::<_, &str>(0), "alice@example.com");

    // Unknown output columns error instead of being ignored.
    let missing = client
        .query(
            "SELECT email, count(*) AS n FROM users GROUP BY email ORDER BY nonexistent",
            &[],
        )
        .await;
    assert!(missing.is_err(), "ORDER BY must not be silently ignored");
}

#[tokio::test]
async fn wire_insert_select() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        // Archive table mirrors users rows for user 1+.
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "archived_users".to_owned(),
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
        })
        .expect("create archive");
    }
    let client = wire_client(database.clone()).await;

    let inserted = client
        .execute(
            "INSERT INTO archived_users SELECT id, email FROM users WHERE id = 1",
            &[],
        )
        .await
        .expect("insert select");
    assert_eq!(inserted, 1);

    let count = client
        .query_one("SELECT count(*) FROM archived_users", &[])
        .await
        .expect("count archived");
    assert_eq!(count.get::<_, i64>(0), 1);

    // Column-count mismatch refuses cleanly.
    let mismatch = client
        .execute(
            "INSERT INTO archived_users SELECT id, email, email FROM users",
            &[],
        )
        .await;
    assert!(mismatch.is_err());
}

#[tokio::test]
async fn wire_projection_arithmetic() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        // archived_users from earlier tests is per-test; create fresh.
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "line_items".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "item_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "quantity".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(3),
                    name: "unit_price".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
            ],
        })
        .expect("create line_items");
        for (iid, qty, price) in [(1u64, 2u64, 500u64), (2, 3, 200)] {
            db.execute_sql(&format!(
                "INSERT INTO line_items (item_id, quantity, unit_price) VALUES ({iid}, {qty}, {price})"
            ))
            .expect("insert item");
        }
    }
    let client = wire_client(database.clone()).await;

    // Arithmetic over two columns plus a literal offset.
    let rows = client
        .query(
            "SELECT item_id, quantity * unit_price AS total, quantity * unit_price + 100 AS total_with_fee FROM line_items ORDER BY item_id",
            &[],
        )
        .await
        .expect("arithmetic projection");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i64>(1), 1000);
    assert_eq!(rows[0].get::<_, i64>(2), 1100);
    assert_eq!(rows[1].get::<_, i64>(1), 600);

    // Division and modulo with zero-guard errors.
    let divide_by_zero = client
        .query("SELECT quantity / 0 FROM line_items", &[])
        .await;
    assert!(divide_by_zero.is_err());

    // WHERE arithmetic composes (predicate side uses literal comparison).
    let filtered = client
        .query(
            "SELECT item_id FROM line_items WHERE unit_price * 2 > 400 ORDER BY item_id",
            &[],
        )
        .await
        .expect("filtered");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].get::<_, i64>(0), 1);
}

#[tokio::test]
async fn wire_constants_beside_aggregates() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.execute_sql("INSERT INTO users (id, email) VALUES (2, 'bob@other.net')")
            .expect("seed bob");
    }
    let client = wire_client(database.clone()).await;

    // A constant is group-invariant: it projects beside aggregates
    // without GROUP BY.
    let row = client
        .query_one("SELECT count(*), 'snapshot' AS label FROM users", &[])
        .await
        .expect("constant beside global aggregate");
    assert_eq!(row.get::<_, i64>(0), 2);
    assert_eq!(row.get::<_, &str>(1), "snapshot");
    assert_eq!(row.columns()[1].name(), "label");

    // And inside grouped output, same value repeated per group.
    let rows = client
        .query(
            "SELECT email, count(*), 'grouped' FROM users GROUP BY email ORDER BY email",
            &[],
        )
        .await
        .expect("constant beside grouped aggregates");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, &str>(2), "grouped");
    assert_eq!(rows[1].get::<_, &str>(2), "grouped");
}

#[tokio::test]
async fn wire_set_operations() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    let client = wire_client(database.clone()).await;

    // UNION dedupes; UNION ALL keeps both copies.
    let union = client
        .query(
            "SELECT id FROM users UNION SELECT id FROM users ORDER BY id",
            &[],
        )
        .await
        .expect("union");
    assert_eq!(union.len(), 1);

    let all = client
        .query("SELECT id FROM users UNION ALL SELECT id FROM users", &[])
        .await
        .expect("union all");
    assert_eq!(all.len(), 2);

    // INTERSECT keeps only shared rows.
    let intersect = client
        .query(
            "SELECT id FROM users WHERE id = 1 INTERSECT SELECT id FROM users WHERE id > 0",
            &[],
        )
        .await
        .expect("intersect");
    assert_eq!(intersect.len(), 1);

    // EXCEPT removes right-side rows from the left set.
    let except = client
        .query(
            "SELECT id FROM users EXCEPT SELECT id FROM users WHERE id = 1",
            &[],
        )
        .await
        .expect("except");
    assert!(except.is_empty());

    // Column-count mismatch refuses cleanly.
    let mismatch = client
        .query(
            "SELECT id, email FROM users UNION SELECT id FROM users",
            &[],
        )
        .await;
    assert!(mismatch.is_err());
}

#[tokio::test]
async fn wire_update_from_returning_post_update_values() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "tiers".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "user_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "tier".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create tiers");
        db.execute_sql("INSERT INTO tiers (user_id, tier) VALUES (1, 'pro')")
            .expect("insert tier");
    }
    let client = wire_client(database.clone()).await;

    // RETURNING over UPDATE FROM must show POST-update values.
    let rows = client
        .query(
            "UPDATE users SET email = 'alice@pro.example.com' FROM tiers WHERE users.id = tiers.user_id AND tier = 'pro' RETURNING email",
            &[],
        )
        .await
        .expect("update from returning");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<_, &str>(0),
        "alice@pro.example.com",
        "RETURNING must reflect the updated value"
    );
}

#[tokio::test]
async fn wire_trust_mode_refuses_non_loopback_listener() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    // No users provisioned -> trust mode -> non-loopback bind must refuse.
    // spawn() binds successfully; the refusal surfaces when serve runs.
    let (_addr, handle) =
        pgwire_server::spawn(database.clone(), "0.0.0.0:0".parse().expect("addr"))
            .await
            .expect("bind succeeds");
    let outcome = handle
        .await
        .expect("server task completes")
        .expect_err("trust mode must not serve a public interface");
    let message = outcome.to_string();
    assert!(message.contains("loopback"), "got: {message}");
}

#[tokio::test]
async fn wire_auth_failure_delays_repeat_attempts() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    omendb::pgwire_server::provision_wire_user(
        &mut database.write().expect("lock"),
        "alice",
        "wonderland",
    )
    .expect("provision user");
    let (addr, _server) =
        pgwire_server::spawn(database.clone(), "127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("spawn server");
    let bad_dsn = format!(
        "host=127.0.0.1 port={} user=alice password=wrong",
        addr.port()
    );

    // First failure is immediate; the second carries the base delay.
    let first_started = std::time::Instant::now();
    assert!(
        tokio_postgres::connect(&bad_dsn, tokio_postgres::NoTls)
            .await
            .is_err()
    );
    let first_elapsed = first_started.elapsed();

    let second_started = std::time::Instant::now();
    assert!(
        tokio_postgres::connect(&bad_dsn, tokio_postgres::NoTls)
            .await
            .is_err()
    );
    let second_elapsed = second_started.elapsed();

    assert!(
        second_elapsed >= first_elapsed + std::time::Duration::from_millis(80),
        "second attempt ({second_elapsed:?}) should be delayed past the first ({first_elapsed:?})"
    );

    // The correct password still works after failures (delay applies to
    // the failed exchange only, and counters do not lock the account).
    let good_dsn = format!(
        "host=127.0.0.1 port={} user=alice password=wonderland",
        addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&good_dsn, tokio_postgres::NoTls)
        .await
        .expect("good credentials still authenticate");
    tokio::spawn(async move { connection.await.expect("connection") });
    client
        .query_one("SELECT 1", &[])
        .await
        .expect("authenticated query");
}

#[tokio::test]
async fn wire_grant_enforcement_reader_writer_admin() {
    let directory = tempdir().expect("tempdir");
    let database = seed_database(directory.path());
    {
        let mut db = database.write().expect("lock");
        omendb::pgwire_server::provision_wire_user(&mut db, "reader", "rpass")
            .expect("provision reader");
        omendb::pgwire_server::provision_wire_user(&mut db, "writer", "wpass")
            .expect("provision writer");
        db.create_table(TableDefinition {
            id: TableId(8),
            name: "notes".to_owned(),
            columns: vec![
                ColumnDefinition {
                    id: ColumnId(1),
                    name: "note_id".to_owned(),
                    data_type: ColumnType::U64,
                    nullable: false,
                },
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "body".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create notes");
        db.execute_sql("INSERT INTO notes (note_id, body) VALUES (1, 'hello')")
            .expect("seed note");

        omendb::pgwire_server::provision_wire_grant(&mut db, "reader", "notes", true, false)
            .expect("grant reader read");
        omendb::pgwire_server::provision_wire_grant(&mut db, "writer", "*", false, true)
            .expect("grant writer admin");
    }

    let (addr, _server) =
        pgwire_server::spawn(database.clone(), "127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("spawn server");
    let dsn = |user: &str, password: &str| {
        format!(
            "host=127.0.0.1 port={} user={user} password={password}",
            addr.port()
        )
    };
    let connect = |user: &str, password: &str| {
        let dsn = dsn(user, password);
        async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await?;
            tokio::spawn(async move { connection.await.expect("connection") });
            Ok::<_, tokio_postgres::Error>(client)
        }
    };

    // Reader: SELECT allowed, INSERT refused with 42501.
    let reader = connect("reader", "rpass").await.expect("reader connect");
    let rows = reader
        .query("SELECT body FROM notes", &[])
        .await
        .expect("reader select granted");
    assert_eq!(rows.len(), 1);
    let denied = reader
        .execute("INSERT INTO notes (note_id, body) VALUES (2, 'nope')", &[])
        .await;
    let error = denied.expect_err("read-only role must not insert");
    assert_eq!(error.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));

    // Writer-admin: INSERT allowed, DDL allowed.
    let writer = connect("writer", "wpass").await.expect("writer connect");
    writer
        .execute(
            "INSERT INTO notes (note_id, body) VALUES (3, 'from writer')",
            &[],
        )
        .await
        .expect("writer insert granted");
    writer
        .batch_execute("CREATE TABLE scratch (id INT PRIMARY KEY)")
        .await
        .expect("writer admin DDL");

    // Ungranted table defaults to deny even for reads.
    let ungranted = reader.query("SELECT * FROM users", &[]).await;
    assert!(ungranted.is_err(), "ungranted tables default to deny");
}
