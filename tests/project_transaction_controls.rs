//! Transaction-control boundaries through real simple and extended wire clients.
#![cfg(feature = "pgwire")]

use omendb::pgwire_server::{RunningServer, ServerConfig};
use tempfile::{TempDir, tempdir};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage, error::SqlState};

struct Fixture {
    _directory: TempDir,
    server: RunningServer,
    client: Client,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_result_limit(None).await
    }

    async fn with_result_limit(max_result_bytes: Option<usize>) -> Self {
        let directory = tempdir().expect("tempdir");
        let mut config =
            ServerConfig::new(directory.path().join("db"), "127.0.0.1:0".parse().unwrap());
        config.max_result_bytes = max_result_bytes;
        let server = RunningServer::start(config).await.expect("start server");
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=omendb",
                server.local_addr().port()
            ),
            NoTls,
        )
        .await
        .expect("connect");
        tokio::spawn(async move { connection.await.expect("connection") });
        client
            .batch_execute("CREATE TABLE items (id BIGINT PRIMARY KEY)")
            .await
            .unwrap();
        Self {
            _directory: directory,
            server,
            client,
        }
    }

    async fn count(&self) -> i64 {
        self.client
            .query_one("SELECT count(*) FROM items", &[])
            .await
            .unwrap()
            .get(0)
    }

    async fn close(self) {
        drop(self.client);
        self.server.shutdown().await.unwrap();
    }
}

async fn execute(client: &Client, sql: &str, extended: bool) -> Result<(), tokio_postgres::Error> {
    if extended {
        client.execute(sql, &[]).await.map(|_| ())
    } else {
        client.batch_execute(sql).await
    }
}

/// A small protocol peer exposes ReadyForQuery and command tags, which the
/// high-level client does not expose. All SQL still runs on the real server.
struct Wire(TcpStream);

impl Wire {
    async fn connect(server: &RunningServer) -> Self {
        let mut wire = Self(TcpStream::connect(server.local_addr()).await.unwrap());
        let mut startup = 196608_i32.to_be_bytes().to_vec();
        startup.extend_from_slice(b"user\0omendb\0\0");
        wire.0
            .write_i32(i32::try_from(startup.len() + 4).unwrap())
            .await
            .unwrap();
        wire.0.write_all(&startup).await.unwrap();
        assert_eq!(wire.ready().await.status, b'I');
        wire
    }

    async fn message(&mut self, kind: u8, body: &[u8]) {
        self.0.write_u8(kind).await.unwrap();
        self.0
            .write_i32(i32::try_from(body.len() + 4).unwrap())
            .await
            .unwrap();
        self.0.write_all(body).await.unwrap();
    }

    async fn query(&mut self, sql: &str, extended: bool) -> Outcome {
        if extended {
            let mut parse = vec![0];
            parse.extend_from_slice(sql.as_bytes());
            parse.extend_from_slice(&[0, 0, 0]);
            self.message(b'P', &parse).await;
            self.message(b'B', &[0; 8]).await;
            self.message(b'E', &[0; 5]).await;
            self.message(b'S', &[]).await;
        } else {
            let mut body = sql.as_bytes().to_vec();
            body.push(0);
            self.message(b'Q', &body).await;
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), self.ready())
            .await
            .expect("ReadyForQuery")
    }

    async fn ready(&mut self) -> Outcome {
        let mut outcome = Outcome::default();
        loop {
            let kind = self.0.read_u8().await.unwrap();
            let length = self.0.read_i32().await.unwrap();
            let mut body = vec![0; usize::try_from(length - 4).unwrap()];
            self.0.read_exact(&mut body).await.unwrap();
            match kind {
                b'Z' => {
                    outcome.status = body[0];
                    return outcome;
                }
                b'C' => outcome
                    .tags
                    .push(String::from_utf8(body[..body.len() - 1].to_vec()).unwrap()),
                b'E' => {
                    let mut fields = body.as_slice();
                    while fields[0] != 0 {
                        let end = fields[1..].iter().position(|byte| *byte == 0).unwrap() + 1;
                        if fields[0] == b'C' {
                            outcome
                                .errors
                                .push(String::from_utf8(fields[1..end].to_vec()).unwrap());
                        }
                        fields = &fields[end + 1..];
                    }
                }
                _ => {}
            }
        }
    }
}

#[derive(Debug, Default)]
struct Outcome {
    status: u8,
    tags: Vec<String>,
    errors: Vec<String>,
}

#[tokio::test]
async fn wire_status_and_tags_follow_failure_and_terminal_commit_ownership() {
    let fixture = Fixture::new().await;
    let mut wire = Wire::connect(&fixture.server).await;
    for extended in [false, true] {
        let begin = wire.query("BEGIN", extended).await;
        assert_eq!((begin.status, begin.tags), (b'T', vec!["BEGIN".to_owned()]));
        assert_eq!(
            wire.query("INSERT INTO items VALUES (1)", extended)
                .await
                .status,
            b'T'
        );
        let error = wire.query("ROLLBACK TO SAVEPOINT missing", extended).await;
        assert_eq!(
            (error.status, error.errors),
            (b'E', vec!["0A000".to_owned()])
        );
        let error = wire.query("INSERT INTO items VALUES (2)", extended).await;
        assert_eq!(
            (error.status, error.errors),
            (b'E', vec!["25P02".to_owned()])
        );
        let commit = wire.query("COMMIT", extended).await;
        assert_eq!(
            (commit.status, commit.tags),
            (b'I', vec!["ROLLBACK".to_owned()])
        );
        assert_eq!(fixture.count().await, 0);

        // A prepare-time syntax error also poisons the open block.
        assert_eq!(wire.query("BEGIN", extended).await.status, b'T');
        let error = wire.query("ROLLBACK nonsense", extended).await;
        assert_eq!(
            (error.status, error.errors),
            (b'E', vec!["42601".to_owned()])
        );
        assert_eq!(wire.query("COMMIT", extended).await.tags, ["ROLLBACK"]);

        // Both writers stage a key absent in their snapshot. The second commit
        // must fail certification; commit consumes that transaction on error.
        assert_eq!(wire.query("BEGIN", extended).await.status, b'T');
        assert!(
            wire.query("INSERT INTO items VALUES (1)", extended)
                .await
                .errors
                .is_empty()
        );
        fixture
            .client
            .batch_execute("INSERT INTO items VALUES (1)")
            .await
            .unwrap();
        let commit = wire.query("COMMIT", extended).await;
        assert_eq!(commit.errors, ["40001"]);
        assert_eq!(
            commit.status, b'I',
            "a consumed transaction must not advertise an aborted block"
        );
        assert!(commit.tags.is_empty());
        assert_eq!(
            wire.query("INSERT INTO items VALUES (2)", extended)
                .await
                .status,
            b'I'
        );
        assert_eq!(fixture.count().await, 2);
        fixture
            .client
            .batch_execute("DELETE FROM items")
            .await
            .unwrap();
    }
    drop(wire);
    fixture.close().await;
}

#[tokio::test]
async fn unsupported_controls_preserve_outer_rollback_boundary() {
    let fixture = Fixture::new().await;
    for extended in [false, true] {
        for sql in [
            "ROLLBACK TO SAVEPOINT missing",
            "ROLLBACK /*!;*/ TO missing",
            "ROLLBACK WORK TO missing",
            "COMMIT AND CHAIN",
            "END AND CHAIN",
            "ROLLBACK AND CHAIN",
            "BEGIN READ ONLY",
            "START TRANSACTION ISOLATION LEVEL READ COMMITTED",
            "BEGIN ISOLATION LEVEL SERIALIZABLE",
            "BEGIN IMMEDIATE",
            "SAVEPOINT s",
            "RELEASE SAVEPOINT s",
            "SET TRANSACTION READ ONLY",
            "ROLLBACK nonsense",
            "COMMIT nonsense",
            "COMMIT END AND CHAIN",
            "ROLLBACK END nonsense",
            "BEGIN END nonsense",
            "ABORT AND CHAIN",
            "ABORT TO missing",
            "BEGIN nonsense",
            "START nonsense",
        ] {
            fixture.client.batch_execute("BEGIN").await.unwrap();
            fixture
                .client
                .batch_execute("INSERT INTO items VALUES (1)")
                .await
                .unwrap();
            let error = execute(&fixture.client, sql, extended)
                .await
                .expect_err(sql);
            assert!(
                matches!(
                    error.code(),
                    Some(&SqlState::FEATURE_NOT_SUPPORTED | &SqlState::SYNTAX_ERROR)
                ),
                "{sql}: {error}"
            );
            let error = fixture
                .client
                .batch_execute("INSERT INTO items VALUES (2)")
                .await
                .expect_err(sql);
            assert_eq!(
                error.code(),
                Some(&SqlState::IN_FAILED_SQL_TRANSACTION),
                "{sql}, extended={extended}"
            );
            fixture.client.batch_execute("ROLLBACK").await.unwrap();
            assert_eq!(fixture.count().await, 0, "{sql}, extended={extended}");
        }
    }
    fixture.close().await;
}

#[tokio::test]
async fn result_admission_failure_makes_prior_writes_rollback_only() {
    let fixture = Fixture::with_result_limit(Some(32)).await;
    for extended in [false, true] {
        fixture
            .client
            .batch_execute("BEGIN; INSERT INTO items VALUES (1)")
            .await
            .unwrap();
        let error = execute(
            &fixture.client,
            "SELECT 'this payload deliberately exceeds the thirty-two byte response admission bound'",
            extended,
        )
        .await
        .expect_err("result bound");
        assert_eq!(error.code(), Some(&SqlState::PROGRAM_LIMIT_EXCEEDED));
        fixture.client.batch_execute("COMMIT").await.unwrap();
        assert_eq!(fixture.count().await, 0);
    }
    fixture.close().await;
}

#[tokio::test]
async fn requested_read_only_transaction_is_rejected_before_writes() {
    let mut fixture = Fixture::new().await;
    let error = fixture
        .client
        .build_transaction()
        .read_only(true)
        .start()
        .await
        .err()
        .expect("read-only mode is unsupported");
    assert_eq!(error.code(), Some(&SqlState::FEATURE_NOT_SUPPORTED));
    assert_eq!(fixture.count().await, 0);
    fixture.close().await;
}

#[tokio::test]
async fn plain_controls_and_lexical_boundaries_are_preserved() {
    let fixture = Fixture::new().await;
    for extended in [false, true] {
        for (begin, rollback) in [
            ("BEGIN", "ROLLBACK"),
            ("START TRANSACTION", "ABORT"),
            (
                "/* begin; comment */ BEGIN WORK",
                "-- rollback; comment\nROLLBACK WORK",
            ),
            ("BEGIN TRANSACTION", "ABORT TRANSACTION"),
            ("BEGIN", "ROLLBACK AND NO CHAIN"),
        ] {
            execute(&fixture.client, begin, extended).await.unwrap();
            fixture
                .client
                .batch_execute("INSERT INTO items VALUES (1)")
                .await
                .unwrap();
            execute(&fixture.client, rollback, extended).await.unwrap();
            assert_eq!(fixture.count().await, 0);
        }
    }
    fixture
        .client
        .batch_execute("BEGIN; INSERT INTO items VALUES (1); COMMIT WORK AND NO CHAIN")
        .await
        .unwrap();
    assert_eq!(fixture.count().await, 1);
    fixture
        .client
        .batch_execute("BEGIN; INSERT INTO items VALUES (2); END")
        .await
        .unwrap();
    assert_eq!(fixture.count().await, 2);
    let rows = fixture
        .client
        .simple_query("SELECT 'hé;''llo' AS \"col;name\"; -- comment;\r\nSELECT ';'")
        .await
        .unwrap();
    let values: Vec<_> = rows
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0),
            _ => None,
        })
        .collect();
    assert_eq!(values, ["hé;'llo", ";"]);
    fixture.close().await;
}

#[tokio::test]
async fn parse_failure_in_batch_keeps_prior_writes_rollback_only() {
    let fixture = Fixture::new().await;
    fixture
        .client
        .batch_execute("BEGIN; INSERT INTO items VALUES (1); ROLLBACK nonsense")
        .await
        .expect_err("malformed rollback");
    let error = fixture
        .client
        .batch_execute("INSERT INTO items VALUES (2)")
        .await
        .expect_err("failed block");
    assert_eq!(error.code(), Some(&SqlState::IN_FAILED_SQL_TRANSACTION));
    let result = fixture.client.simple_query("COMMIT").await.unwrap();
    assert!(matches!(
        result.as_slice(),
        [SimpleQueryMessage::CommandComplete(0)]
    ));
    assert_eq!(fixture.count().await, 0);
    fixture.close().await;
}
