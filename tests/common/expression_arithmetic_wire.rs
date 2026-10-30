use anyhow::Result;
use tokio_postgres::{Client, error::SqlState};

pub async fn exercise(client: &Client) -> Result<()> {
    client.batch_execute("CREATE TABLE oracle_expression_numbers (id BIGINT PRIMARY KEY, n BIGINT, v BIGINT, f DOUBLE PRECISION, d NUMERIC)").await?;
    client
        .batch_execute("CREATE TABLE oracle_expression_saved (id BIGINT)")
        .await?;
    client.batch_execute("INSERT INTO oracle_expression_numbers VALUES (1, 10, 2, 2, 1.25), (2, NULL, 0, 0, NULL)").await?;
    for (name, expression) in [
        ("integer", "n / v"),
        ("float", "n / f"),
        ("decimal", "d * v"),
    ] {
        client
            .batch_execute(&format!(
                "CREATE INDEX oracle_expression_{name} ON oracle_expression_numbers (({expression}))"
            ))
            .await?;
    }
    assert_eq!(
        client
            .query_one(
                "SELECT id FROM oracle_expression_numbers WHERE n / f = 5",
                &[]
            )
            .await?
            .get::<_, i64>(0),
        1
    );
    for (sql, params, code) in [
        (
            "INSERT INTO oracle_expression_numbers VALUES (3, 8, 2, 2, 1), (4, 7, 0, 1, 1)",
            vec![],
            SqlState::DIVISION_BY_ZERO,
        ),
        (
            "UPDATE oracle_expression_numbers SET v = 0 WHERE id = 1",
            vec![],
            SqlState::DIVISION_BY_ZERO,
        ),
        (
            "INSERT INTO oracle_expression_numbers VALUES (3, $1, -1, 1, 1)",
            vec![i64::MIN],
            SqlState::NUMERIC_VALUE_OUT_OF_RANGE,
        ),
    ] {
        client
            .batch_execute("BEGIN; INSERT INTO oracle_expression_saved VALUES (7)")
            .await?;
        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|n| n as _).collect();
        let error = client.execute(sql, &params).await.unwrap_err();
        assert_eq!(error.code(), Some(&code), "{sql}");
        let error = client.simple_query("SELECT 1").await.unwrap_err();
        assert_eq!(error.code(), Some(&SqlState::IN_FAILED_SQL_TRANSACTION));
        client.batch_execute("COMMIT").await?;
        assert!(
            client
                .query("SELECT id FROM oracle_expression_saved", &[])
                .await?
                .is_empty()
        );
        let rows = client
            .query("SELECT id FROM oracle_expression_numbers ORDER BY id", &[])
            .await?;
        assert_eq!(
            rows.iter()
                .map(|row| row.get::<_, i64>(0))
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(
            client
                .query_one("SELECT v FROM oracle_expression_numbers WHERE id = 1", &[])
                .await?
                .get::<_, i64>(0),
            2
        );
    }
    Ok(())
}
