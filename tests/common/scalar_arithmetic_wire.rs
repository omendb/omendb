use super::numeric_text::NumericText;
use anyhow::Result;
use tokio_postgres::{Client, error::SqlState, types::Type};

pub async fn exercise(client: &Client, postgres_reference: bool) -> Result<()> {
    for sql in [
        "CREATE TABLE oracle_scalar_numbers (id BIGINT PRIMARY KEY, n BIGINT, d NUMERIC, f DOUBLE PRECISION)",
        "CREATE UNIQUE INDEX oracle_scalar_decimal ON oracle_scalar_numbers (d)",
        "CREATE TABLE oracle_scalar_saved (amount BIGINT)",
        "INSERT INTO oracle_scalar_numbers VALUES (1, -7, 1.25, 1.0), (2, NULL, NULL, NULL)",
    ] {
        client.batch_execute(sql).await?;
    }
    let sql = "SELECT n + 2, n - 2, n * 2, n / 3, n % 3, n / -3, n % -3 FROM oracle_scalar_numbers WHERE id = 1";
    let statement = client.prepare(sql).await?;
    assert!(statement.columns().iter().all(|c| c.type_() == &Type::INT8));
    let row = client.query_one(&statement, &[]).await?;
    let values: Vec<i64> = (0..row.len()).map(|i| row.get(i)).collect();
    assert_eq!(values, [-5, -9, -14, -2, -1, 2, -1]);
    assert_eq!(
        client
            .query_one("SELECT id FROM oracle_scalar_numbers WHERE n + 2 < 0", &[])
            .await?
            .get::<_, i64>(0),
        1
    );

    client
        .execute(
            "INSERT INTO oracle_scalar_numbers VALUES (3, $1, NULL, NULL)",
            &[&i64::MIN],
        )
        .await?;
    client
        .batch_execute("UPDATE oracle_scalar_numbers SET n = n - n WHERE id = 3")
        .await?;
    assert_eq!(
        client
            .query_one("SELECT n FROM oracle_scalar_numbers WHERE id = 3", &[])
            .await?
            .get::<_, i64>(0),
        0
    );
    client
        .batch_execute("DELETE FROM oracle_scalar_numbers WHERE id = 3")
        .await?;

    client
        .batch_execute(
            "UPDATE oracle_scalar_numbers SET n = n + 1, d = d - 1, f = f + 1 WHERE id = 2",
        )
        .await?;
    let row = client
        .query_one(
            "SELECT n, d + 1, f * 2 FROM oracle_scalar_numbers WHERE id = 2",
            &[],
        )
        .await?;
    assert_eq!(row.get::<_, Option<i64>>(0), None);
    assert!(row.get::<_, Option<NumericText>>(1).is_none());
    assert_eq!(row.get::<_, Option<f64>>(2), None);

    client
        .execute(
            "UPDATE oracle_scalar_numbers SET n = $1 WHERE id = 1",
            &[&9007199254740993_i64],
        )
        .await?;
    let statement = client
        .prepare("SELECT d + n FROM oracle_scalar_numbers WHERE id = 1")
        .await?;
    assert_eq!(statement.columns()[0].type_(), &Type::NUMERIC);
    client
        .batch_execute("UPDATE oracle_scalar_numbers SET d = d + n WHERE id = 1")
        .await?;
    let row = client
        .query_one(
            "SELECT id, d FROM oracle_scalar_numbers WHERE d = 9007199254740994.25",
            &[],
        )
        .await?;
    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, NumericText>(1).0, "9007199254740994.25");

    client
        .batch_execute("UPDATE oracle_scalar_numbers SET d = 1.25 WHERE id = 1")
        .await?;
    assert_eq!(client.query_one("SELECT d * 100000000000000000000000000000000 FROM oracle_scalar_numbers WHERE id = 1", &[]).await?.get::<_, NumericText>(0).0, "125000000000000000000000000000000.00");
    client
        .batch_execute("UPDATE oracle_scalar_numbers SET d = 1 WHERE id = 1")
        .await?;
    for (divisor, expected) in [(1, "1.00000"), (-3, "-0.33333"), (-6, "-0.16667")] {
        // Compare the declared bounded quotient, not PostgreSQL's default
        // arbitrary-precision numeric scale. OmenDB still ignores typmods.
        let quotient = if postgres_reference {
            format!("CAST(d / {divisor} AS NUMERIC(20,5))")
        } else {
            format!("d / {divisor}")
        };
        assert_eq!(
            client
                .query_one(
                    &format!("SELECT {quotient} FROM oracle_scalar_numbers WHERE id = 1"),
                    &[]
                )
                .await?
                .get::<_, NumericText>(0)
                .0,
            expected
        );
    }

    // NaN is the PostgreSQL exception to float division-by-zero errors.
    client
        .batch_execute("UPDATE oracle_scalar_numbers SET f = 'NaN' WHERE id = 1")
        .await?;
    assert!(
        client
            .query_one(
                "SELECT f / 0.0 FROM oracle_scalar_numbers WHERE id = 1",
                &[]
            )
            .await?
            .get::<_, f64>(0)
            .is_nan()
    );
    client
        .batch_execute("UPDATE oracle_scalar_numbers SET f = 1.0 WHERE id = 1")
        .await?;
    client
        .execute(
            "UPDATE oracle_scalar_numbers SET n = $1 WHERE id = 1",
            &[&i64::MAX],
        )
        .await?;
    for (sql, code) in [
        (
            "SELECT n + 1 FROM oracle_scalar_numbers WHERE id = 1",
            SqlState::NUMERIC_VALUE_OUT_OF_RANGE,
        ),
        (
            "SELECT n * 2 FROM oracle_scalar_numbers WHERE id = 1",
            SqlState::NUMERIC_VALUE_OUT_OF_RANGE,
        ),
        (
            "UPDATE oracle_scalar_numbers SET n = n + 1 WHERE id = 1",
            SqlState::NUMERIC_VALUE_OUT_OF_RANGE,
        ),
        (
            "SELECT id FROM oracle_scalar_numbers WHERE n + 1 > 0",
            SqlState::NUMERIC_VALUE_OUT_OF_RANGE,
        ),
        (
            "INSERT INTO oracle_scalar_saved SELECT id FROM oracle_scalar_numbers WHERE n + 1 > 0",
            SqlState::NUMERIC_VALUE_OUT_OF_RANGE,
        ),
        (
            "SELECT n / 0 FROM oracle_scalar_numbers WHERE id = 1",
            SqlState::DIVISION_BY_ZERO,
        ),
        (
            "SELECT d / 0 FROM oracle_scalar_numbers WHERE id = 1",
            SqlState::DIVISION_BY_ZERO,
        ),
        (
            "SELECT f / 0.0 FROM oracle_scalar_numbers WHERE id = 1",
            SqlState::DIVISION_BY_ZERO,
        ),
    ] {
        // Prepare/Describe must preserve metadata without running arithmetic.
        let statement = client.prepare(sql).await?;
        if sql.starts_with("SELECT n ") {
            assert_eq!(statement.columns()[0].type_(), &Type::INT8);
        }
        client
            .batch_execute("BEGIN; INSERT INTO oracle_scalar_saved VALUES (7)")
            .await?;
        let error = client.query(&statement, &[]).await.unwrap_err();
        assert_eq!(error.code(), Some(&code), "{sql}");
        let error = client.simple_query("SELECT 1").await.unwrap_err();
        assert_eq!(
            error.code(),
            Some(&SqlState::IN_FAILED_SQL_TRANSACTION),
            "{sql}"
        );
        client.batch_execute("COMMIT").await?;
        assert!(
            client
                .query("SELECT amount FROM oracle_scalar_saved", &[])
                .await?
                .is_empty(),
            "{sql}"
        );
        assert_eq!(
            client
                .query_one("SELECT n FROM oracle_scalar_numbers WHERE id = 1", &[])
                .await?
                .get::<_, i64>(0),
            i64::MAX
        );
    }
    let error = client.simple_query("INSERT INTO oracle_scalar_saved VALUES (7); SELECT n + 1 FROM oracle_scalar_numbers WHERE id = 1; INSERT INTO oracle_scalar_saved VALUES (9)").await.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::NUMERIC_VALUE_OUT_OF_RANGE));
    assert!(
        client
            .query("SELECT amount FROM oracle_scalar_saved", &[])
            .await?
            .is_empty()
    );
    Ok(())
}
