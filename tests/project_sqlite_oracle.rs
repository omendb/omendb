//! Differential checks for the bounded SQL subset against SQLite.
//!
//! The statements intentionally stay inside the documented overlap. SQLite
//! is an independent semantic oracle here; the test compares result columns,
//! values, and affected-row counts on both OmenDB backends.

use std::path::Path;

use anyhow::{Context, Result, bail};
use omendb::{
    DatabaseConfig, RelationalBackendConfig, RelationalBackendKind, RelationalDatabase,
    SeerKernelConfig, Value,
};
use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::{Connection, params_from_iter};
use tempfile::tempdir;

fn config(kind: RelationalBackendKind, directory: &Path) -> RelationalBackendConfig {
    match kind {
        RelationalBackendKind::Temporary => RelationalBackendConfig::Temporary(DatabaseConfig {
            directory: directory.to_owned(),
        }),
        RelationalBackendKind::Seer => {
            RelationalBackendConfig::Seer(SeerKernelConfig::new(directory.to_owned()))
        }
    }
}

fn sqlite_value(value: &Value) -> rusqlite::types::Value {
    match value {
        Value::Null => rusqlite::types::Value::Null,
        Value::Bool(value) => rusqlite::types::Value::Integer(i64::from(*value)),
        Value::I64(value) => rusqlite::types::Value::Integer(*value),
        Value::U64(value) => rusqlite::types::Value::Integer(
            i64::try_from(*value).expect("oracle workload uses i64-sized integers"),
        ),
        Value::Text(value) => rusqlite::types::Value::Text(value.clone()),
        Value::Bytes(value) => rusqlite::types::Value::Blob(value.clone()),
    }
}

fn sqlite_params(values: &[Value]) -> Vec<ToSqlOutput<'static>> {
    values
        .iter()
        .map(|value| ToSqlOutput::Owned(sqlite_value(value)))
        .collect()
}

fn sqlite_query(
    connection: &Connection,
    sql: &str,
    values: &[Value],
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let params = sqlite_params(values);
    let mut statement = connection.prepare(sql).context("prepare SQLite query")?;
    let columns = statement
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mut rows = statement.query(params_from_iter(params.iter()))?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        let mut values = Vec::with_capacity(columns.len());
        for index in 0..columns.len() {
            values.push(match row.get_ref(index)? {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(value) => Value::I64(value),
                ValueRef::Real(value) => {
                    bail!("oracle workload unexpectedly returned REAL {value}")
                }
                ValueRef::Text(value) => Value::Text(String::from_utf8(value.to_vec())?),
                ValueRef::Blob(value) => Value::Bytes(value.to_vec()),
            });
        }
        result.push(values);
    }
    Ok((columns, result))
}

fn compare_query(
    database: &mut RelationalDatabase,
    connection: &Connection,
    sql: &str,
    values: &[Value],
) -> Result<()> {
    let (expected_columns, expected_rows) = sqlite_query(connection, sql, values)?;
    let actual = database
        .execute_sql_with_params(sql, values)
        .with_context(|| format!("execute OmenDB query {sql}"))?;
    let columns = actual
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let actual_rows = actual.rows;
    if columns != expected_columns || actual_rows != expected_rows {
        bail!(
            "SQLite oracle mismatch for {sql}: expected ({expected_columns:?}, {expected_rows:?}), got ({columns:?}, {actual_rows:?})"
        );
    }
    Ok(())
}

fn compare_command(
    database: &mut RelationalDatabase,
    connection: &Connection,
    sql: &str,
    values: &[Value],
) -> Result<()> {
    let params = sqlite_params(values);
    let expected = connection
        .execute(sql, params_from_iter(params.iter()))
        .with_context(|| format!("execute SQLite command {sql}"))?;
    let actual = database
        .execute_sql_with_params(sql, values)
        .with_context(|| format!("execute OmenDB command {sql}"))?;
    let statement = sql.trim_start().to_ascii_uppercase();
    if (statement.starts_with("INSERT")
        || statement.starts_with("UPDATE")
        || statement.starts_with("DELETE"))
        && actual.affected_rows != expected
    {
        bail!(
            "SQLite oracle affected-row mismatch for {sql}: expected {expected}, got {}",
            actual.affected_rows
        );
    }
    Ok(())
}

fn exercise(kind: RelationalBackendKind) -> Result<()> {
    let directory = tempdir().context("create oracle directory")?;
    let sqlite_path = directory.path().join("oracle.sqlite");
    let connection = Connection::open(&sqlite_path).context("open SQLite oracle")?;
    let mut database = RelationalDatabase::create(config(kind, &directory.path().join("omendb")))
        .context("create OmenDB oracle subject")?;

    let schema =
        "CREATE TABLE accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL, state TEXT)";
    compare_command(&mut database, &connection, schema, &[])?;
    compare_command(
        &mut database,
        &connection,
        "INSERT INTO accounts VALUES (1, 100, 'open'), (2, 40, 'open'), (3, 10, 'closed'), (4, 0, NULL)",
        &[],
    )?;
    compare_command(
        &mut database,
        &connection,
        "CREATE INDEX accounts_state_idx ON accounts (state)",
        &[],
    )?;

    compare_query(
        &mut database,
        &connection,
        "SELECT id, balance, state FROM accounts ORDER BY id",
        &[],
    )?;
    compare_query(
        &mut database,
        &connection,
        "SELECT DISTINCT state FROM accounts ORDER BY state ASC NULLS FIRST LIMIT 2",
        &[],
    )?;
    compare_query(
        &mut database,
        &connection,
        "SELECT DISTINCT state FROM accounts ORDER BY state ASC NULLS FIRST LIMIT 2 OFFSET 1",
        &[],
    )?;
    compare_query(
        &mut database,
        &connection,
        "SELECT id FROM accounts WHERE state IN (NULL, ?1) ORDER BY id",
        &[Value::Text("open".to_owned())],
    )?;
    compare_query(
        &mut database,
        &connection,
        "SELECT id FROM accounts WHERE state NOT IN (NULL, ?1) ORDER BY id",
        &[Value::Text("open".to_owned())],
    )?;
    compare_query(
        &mut database,
        &connection,
        "SELECT id FROM accounts WHERE balance BETWEEN ?1 AND ?2 ORDER BY id",
        &[Value::I64(10), Value::I64(100)],
    )?;
    compare_query(
        &mut database,
        &connection,
        "SELECT id FROM accounts WHERE state IS NULL ORDER BY id",
        &[],
    )?;

    compare_command(
        &mut database,
        &connection,
        "UPDATE accounts SET balance = ?2 WHERE id = ?1",
        &[Value::I64(2), Value::I64(55)],
    )?;
    compare_command(
        &mut database,
        &connection,
        "DELETE FROM accounts WHERE state IS NULL",
        &[],
    )?;
    compare_query(
        &mut database,
        &connection,
        "SELECT id, balance FROM accounts ORDER BY id",
        &[],
    )?;

    database.close().context("close OmenDB oracle subject")?;
    let mut reopened = RelationalDatabase::open(config(kind, &directory.path().join("omendb")))
        .context("reopen OmenDB oracle subject")?;
    compare_query(
        &mut reopened,
        &connection,
        "SELECT id, balance FROM accounts ORDER BY id",
        &[],
    )?;
    reopened.close().context("close reopened OmenDB oracle")?;
    Ok(())
}

#[test]
fn bounded_sql_matches_sqlite_on_temporary_backend() -> Result<()> {
    exercise(RelationalBackendKind::Temporary)
}

#[test]
fn bounded_sql_matches_sqlite_on_seer_backend() -> Result<()> {
    exercise(RelationalBackendKind::Seer)
}
