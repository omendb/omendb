use std::path::Path;

use omendb::{
    DatabaseConfig, OperationControl, RelationalBackendConfig, RelationalBackendKind,
    RelationalDatabaseConfig, RelationalDatabaseSession, RelationalSessionConfig, SeerKernelConfig,
    Value,
};
use tempfile::tempdir;

fn config(kind: RelationalBackendKind, directory: &Path) -> RelationalDatabaseConfig {
    let backend = match kind {
        RelationalBackendKind::Temporary => RelationalBackendConfig::Temporary(DatabaseConfig {
            directory: directory.to_owned(),
        }),
        RelationalBackendKind::Seer => {
            RelationalBackendConfig::Seer(SeerKernelConfig::new(directory.to_owned()))
        }
    };
    RelationalDatabaseConfig::new(backend).with_session_config(RelationalSessionConfig {
        max_in_flight: 2,
        ..RelationalSessionConfig::default()
    })
}

fn exercise_session_sql(kind: RelationalBackendKind, directory: &Path) -> Vec<Vec<Value>> {
    let database_config = config(kind, directory);
    let session = RelationalDatabaseSession::create(database_config.clone()).expect("create");
    let control = OperationControl::default();

    let created = session
        .execute_sql(
            &control,
            "CREATE TABLE accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL, state TEXT)",
        )
        .expect("create table");
    assert!(created.commit.is_some());

    assert_eq!(
        session
            .execute_sql_with_params(
                &control,
                "INSERT INTO accounts VALUES ($1, $2, $3)",
                &[
                    Value::I64(1),
                    Value::I64(100),
                    Value::Text("open".to_owned()),
                ],
            )
            .expect("parameterized insert")
            .affected_rows,
        1
    );
    session
        .execute_sql(
            &control,
            "INSERT INTO accounts VALUES (2, 40, 'open'), (3, 10, NULL)",
        )
        .expect("insert rows");

    let selected = session
        .execute_sql_with_params(
            &control,
            "SELECT id, balance, state FROM accounts WHERE id = $1",
            &[Value::I64(1)],
        )
        .expect("parameterized query");
    assert_eq!(
        selected.rows,
        vec![vec![
            Value::I64(1),
            Value::I64(100),
            Value::Text("open".to_owned()),
        ]]
    );

    let (transaction_rows, transaction_commit) = session
        .transaction(&control, |database, transaction| {
            transaction.execute_sql(database, "UPDATE accounts SET balance = 110 WHERE id = 1")?;
            let result = transaction
                .execute_sql(database, "SELECT id, balance FROM accounts WHERE id = 1")?;
            Ok::<_, omendb::DbError>(result.rows)
        })
        .expect("transaction SQL");
    assert_eq!(transaction_rows, vec![vec![Value::I64(1), Value::I64(110)]]);
    assert!(transaction_commit > created.commit.expect("schema commit"));

    let batch = session
        .execute_sql_batch(
            &control,
            &[
                "UPDATE accounts SET balance = 110 WHERE id = 1",
                "UPDATE accounts SET balance = 40 WHERE id = 2",
            ],
        )
        .expect("batch SQL transaction");
    assert_eq!(
        batch
            .iter()
            .map(|result| result.affected_rows)
            .collect::<Vec<_>>(),
        vec![1, 1]
    );
    let oversized = vec!["SELECT 1"; omendb::RELATIONAL_SQL_BATCH_LIMIT + 1];
    assert!(matches!(
        session.execute_sql_batch(&control, &oversized),
        Err(omendb::DbError::ResourceLimitExceeded(_))
    ));

    let rows = session
        .execute_sql(
            &control,
            "SELECT id, balance, state FROM accounts ORDER BY id",
        )
        .expect("select after transaction")
        .rows;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1], Value::I64(110));
    assert_eq!(rows[2][2], Value::Null);

    session.close().expect("close");

    let reopened = RelationalDatabaseSession::open(database_config).expect("reopen");
    let reopened_rows = reopened
        .execute_sql(
            &control,
            "SELECT id, balance, state FROM accounts ORDER BY id",
        )
        .expect("select after reopen")
        .rows;
    reopened.close().expect("close reopened");
    assert_eq!(reopened_rows, rows);
    rows
}

#[test]
fn public_session_sql_matches_across_backends_and_reopens() {
    let temporary = tempdir().expect("temporary directory");
    let temporary_rows = exercise_session_sql(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    );

    let seer = tempdir().expect("seer directory");
    let seer_rows = exercise_session_sql(RelationalBackendKind::Seer, &seer.path().join("seer"));

    assert_eq!(temporary_rows, seer_rows);
}
