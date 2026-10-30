use anyhow::Result;
use tokio_postgres::{Client, error::SqlState, types::Type};

use super::aggregate_sum;

pub async fn exercise(client: &Client, postgres_reference: bool) -> Result<()> {
    for sql in aggregate_sum::SCHEMA {
        client.batch_execute(sql).await?;
    }
    for all_null in [false, true] {
        if all_null {
            client
                .batch_execute("INSERT INTO aggregate_items VALUES (1, 1, NULL), (2, 1, NULL)")
                .await?;
        }
        for &(name, omen_sql, postgres_sql) in aggregate_sum::CASES {
            let sql = if postgres_reference {
                postgres_sql
            } else {
                omen_sql
            };
            let statement = client.prepare(sql).await?;
            assert!(
                statement
                    .columns()
                    .iter()
                    .all(|column| column.type_() == &Type::INT8),
                "{name}"
            );
            let rows = client.query(&statement, &[]).await?;
            let expected_rows = match name {
                "global" | "joined" => 1,
                "window" if all_null => 2,
                _ => usize::from(all_null),
            };
            assert_eq!(rows.len(), expected_rows, "{name}; all_null={all_null}");
            for row in rows {
                assert_eq!(
                    row.try_get::<_, Option<i64>>(row.len() - 1)?,
                    None,
                    "{name}"
                );
            }
        }
    }
    for boundary in [i64::MAX, i64::MIN] {
        client.batch_execute("DELETE FROM aggregate_items").await?;
        client
            .execute(
                "INSERT INTO aggregate_items VALUES (1, 1, $1), (2, 1, 0)",
                &[&boundary],
            )
            .await?;
        for &(name, omen_sql, postgres_sql) in aggregate_sum::CASES {
            let sql = if postgres_reference {
                postgres_sql
            } else {
                omen_sql
            };
            for row in client.query(sql, &[]).await? {
                assert_eq!(row.try_get::<_, i64>(row.len() - 1)?, boundary, "{name}");
            }
        }
    }
    for (first, second) in [(i64::MAX, 1i64), (i64::MIN, -1i64)] {
        for &(name, omen_sql, postgres_sql) in aggregate_sum::CASES {
            client.batch_execute("DELETE FROM aggregate_items").await?;
            client
                .execute(
                    "INSERT INTO aggregate_items VALUES (1, 1, $1), (2, 1, $2)",
                    &[&first, &second],
                )
                .await?;
            let sql = if postgres_reference {
                postgres_sql
            } else {
                omen_sql
            };
            // Describe must not execute the overflowing aggregate.
            let statement = client.prepare(sql).await?;
            client
                .batch_execute("BEGIN; INSERT INTO aggregate_items VALUES (3, 2, 7)")
                .await?;
            let error = client.query(&statement, &[]).await.unwrap_err();
            assert_eq!(
                error.code(),
                Some(&SqlState::NUMERIC_VALUE_OUT_OF_RANGE),
                "{name}"
            );
            let error = client.simple_query("SELECT 1").await.unwrap_err();
            assert_eq!(
                error.code(),
                Some(&SqlState::IN_FAILED_SQL_TRANSACTION),
                "{name}"
            );
            client.batch_execute("COMMIT").await?;
            let ids: Vec<i64> = client
                .query("SELECT id FROM aggregate_items ORDER BY id", &[])
                .await?
                .iter()
                .map(|row| row.get(0))
                .collect();
            assert_eq!(ids, [1, 2], "{name}: prior work must roll back");
        }
    }
    // A simple-message prefix response must not turn an aggregate error into
    // a committed prefix, or allow execution of its tail.
    let joined = aggregate_sum::CASES[2];
    let query = if postgres_reference {
        joined.2
    } else {
        joined.1
    };
    let error = client.simple_query(&format!(
        "INSERT INTO aggregate_saved VALUES (7); {query}; INSERT INTO aggregate_saved VALUES (9)"
    )).await.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::NUMERIC_VALUE_OUT_OF_RANGE));
    assert!(
        client
            .query("SELECT amount FROM aggregate_saved", &[])
            .await?
            .is_empty()
    );

    let sql = if postgres_reference {
        "INSERT INTO aggregate_saved SELECT CAST(sum(amount) AS BIGINT) FROM aggregate_items"
    } else {
        "INSERT INTO aggregate_saved SELECT sum(amount) FROM aggregate_items"
    };
    let error = client.execute(sql, &[]).await.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::NUMERIC_VALUE_OUT_OF_RANGE));
    assert!(
        client
            .query("SELECT amount FROM aggregate_saved", &[])
            .await?
            .is_empty()
    );
    client.batch_execute("DELETE FROM aggregate_items").await?;
    Ok(())
}
