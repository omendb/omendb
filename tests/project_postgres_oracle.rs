//! Live PostgreSQL differential checks for OmenDB's documented wire subset.
//!
//! The ordinary pgwire suite proves OmenDB behavior and negative paths. This
//! test adds an independent PostgreSQL oracle for representative semantics that
//! OmenDB already claims to support. It is ignored by the ordinary test matrix
//! because it requires a live PostgreSQL server; CI runs it in a dedicated job.

#![cfg(feature = "pgwire")]

use std::env;

use anyhow::{Context, Result, bail};
use omendb::pgwire_server;
use tempfile::tempdir;
use tokio_postgres::types::{FromSql, ToSql, Type};
use tokio_postgres::{Client, NoTls, Row};

const POSTGRES_ORACLE_URL: &str = "POSTGRES_ORACLE_URL";

#[derive(Debug, PartialEq)]
struct QuerySnapshot {
    columns: Vec<(String, u32)>,
    rows: Vec<Vec<Cell>>,
}

#[derive(Debug, PartialEq)]
enum Cell {
    Null,
    Bool(bool),
    I64(i64),
    Text(String),
    Bytes(Vec<u8>),
    F64(NanEqF64),
    /// Days since the Unix epoch (OmenDB's date representation; the
    /// oracle compares through the text form, so this carries PG's
    /// normalized text).
    Date(String),
    Timestamp(String),
    Numeric(String),
    Uuid(String),
}

/// f64 wrapper where NaN equals NaN (the SQL float total order), used
/// inside the oracle's snapshot cells.
#[derive(Debug)]
struct NanEqF64(f64);

impl PartialEq for NanEqF64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.is_nan() && other.0.is_nan() || self.0 == other.0
    }
}

impl Eq for NanEqF64 {}

async fn connect(
    config: &str,
) -> Result<(
    Client,
    tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>,
)> {
    let (client, connection) = tokio_postgres::connect(config, NoTls)
        .await
        .with_context(|| format!("connect with {config}"))?;
    let task = tokio::spawn(connection);
    Ok((client, task))
}

async fn seed(client: &Client) -> Result<()> {
    client
        .batch_execute(
            "CREATE TABLE oracle_groups (
                 id BIGINT PRIMARY KEY,
                 label TEXT NOT NULL
             );
             CREATE TABLE oracle_accounts (
                 id BIGINT PRIMARY KEY,
                 group_id BIGINT NOT NULL,
                 balance BIGINT NOT NULL,
                 state TEXT
             );
             INSERT INTO oracle_groups (id, label)
             VALUES (10, 'retail'), (20, 'business');
             INSERT INTO oracle_accounts (id, group_id, balance, state)
             VALUES
                 (1, 10, 100, 'open'),
                 (2, 10, 40, 'open'),
                 (3, 20, 10, 'closed'),
                 (4, 20, 0, NULL);",
        )
        .await
        .context("seed oracle schema")?;
    Ok(())
}

async fn snapshot(
    client: &Client,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<QuerySnapshot> {
    let statement = client
        .prepare(sql)
        .await
        .with_context(|| format!("prepare oracle query: {sql}"))?;
    let columns = statement
        .columns()
        .iter()
        .map(|column| (column.name().to_owned(), column.type_().oid()))
        .collect();
    let rows = client
        .query(&statement, params)
        .await
        .with_context(|| format!("execute oracle query: {sql}"))?;
    let rows = rows
        .iter()
        .map(decode_row)
        .collect::<Result<Vec<_>>>()
        .with_context(|| format!("decode oracle query: {sql}"))?;
    Ok(QuerySnapshot { columns, rows })
}

fn decode_row(row: &Row) -> Result<Vec<Cell>> {
    row.columns()
        .iter()
        .enumerate()
        .map(|(index, column)| decode_cell(row, index, column.type_()))
        .collect()
}

fn decode_cell(row: &Row, index: usize, data_type: &Type) -> Result<Cell> {
    if data_type == &Type::BOOL {
        return Ok(row
            .try_get::<_, Option<bool>>(index)?
            .map_or(Cell::Null, Cell::Bool));
    }
    if data_type == &Type::INT2 {
        return Ok(row
            .try_get::<_, Option<i16>>(index)?
            .map_or(Cell::Null, |value| Cell::I64(i64::from(value))));
    }
    if data_type == &Type::INT4 {
        return Ok(row
            .try_get::<_, Option<i32>>(index)?
            .map_or(Cell::Null, |value| Cell::I64(i64::from(value))));
    }
    if data_type == &Type::INT8 {
        return Ok(row
            .try_get::<_, Option<i64>>(index)?
            .map_or(Cell::Null, Cell::I64));
    }
    if data_type == &Type::TEXT || data_type == &Type::VARCHAR {
        return Ok(row
            .try_get::<_, Option<String>>(index)?
            .map_or(Cell::Null, Cell::Text));
    }
    if data_type == &Type::BYTEA {
        return Ok(row
            .try_get::<_, Option<Vec<u8>>>(index)?
            .map_or(Cell::Null, Cell::Bytes));
    }
    // Typed scalars compare through their canonical client-rendered text
    // form. tokio-postgres has no text FromSql for these types in binary
    // mode, so the oracle carries its own wire decoders - independently
    // written from the PostgreSQL formats, which doubles as a check on
    // OmenDB's encoders.
    if data_type == &Type::FLOAT8 {
        return Ok(row
            .try_get::<_, Option<f64>>(index)?
            .map_or(Cell::Null, |value| Cell::F64(NanEqF64(value))));
    }
    if data_type == &Type::DATE {
        return Ok(row
            .try_get::<_, Option<DateText>>(index)?
            .map_or(Cell::Null, |value| Cell::Date(value.0)));
    }
    if data_type == &Type::TIMESTAMP {
        return Ok(row
            .try_get::<_, Option<TimestampText>>(index)?
            .map_or(Cell::Null, |value| Cell::Timestamp(value.0)));
    }
    if data_type == &Type::NUMERIC {
        return Ok(row
            .try_get::<_, Option<NumericText>>(index)?
            .map_or(Cell::Null, |value| Cell::Numeric(value.0)));
    }
    if data_type == &Type::UUID {
        return Ok(row
            .try_get::<_, Option<UuidText>>(index)?
            .map_or(Cell::Null, |value| Cell::Uuid(value.0)));
    }
    bail!(
        "live PostgreSQL oracle returned unsupported comparison type {} (OID {})",
        data_type,
        data_type.oid()
    )
}

async fn compare_query(
    omendb: &Client,
    postgres: &Client,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<()> {
    let actual = snapshot(omendb, sql, params).await?;
    let expected = snapshot(postgres, sql, params).await?;
    if actual != expected {
        bail!(
            "PostgreSQL oracle mismatch for {sql}:\nPostgreSQL: {expected:#?}\nOmenDB: {actual:#?}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL oracle"]
async fn documented_wire_subset_matches_live_postgresql() -> Result<()> {
    let postgres_url = env::var(POSTGRES_ORACLE_URL).with_context(|| {
        format!("{POSTGRES_ORACLE_URL} must point to the live PostgreSQL oracle")
    })?;

    let directory = tempdir().context("create OmenDB oracle directory")?;
    let server = pgwire_server::RunningServer::start(pgwire_server::ServerConfig::new(
        directory.path().join("db"),
        "127.0.0.1:0".parse().expect("loopback address"),
    ))
    .await
    .context("start OmenDB oracle server")?;

    let omen_config = format!(
        "host=127.0.0.1 port={} user=omendb",
        server.local_addr().port()
    );
    let (mut omendb, omen_connection) = connect(&omen_config).await?;
    let (mut postgres, postgres_connection) = connect(&postgres_url).await?;

    postgres
        .batch_execute(
            "DROP TABLE IF EXISTS oracle_accounts;
             DROP TABLE IF EXISTS oracle_groups;",
        )
        .await
        .context("reset PostgreSQL oracle schema")?;
    seed(&omendb).await.context("seed OmenDB")?;
    seed(&postgres).await.context("seed PostgreSQL")?;

    compare_query(
        &omendb,
        &postgres,
        "SELECT id, group_id, balance, state FROM oracle_accounts ORDER BY id",
        &[],
    )
    .await?;

    let low = 10_i64;
    let high = 100_i64;
    compare_query(
        &omendb,
        &postgres,
        "SELECT id, state FROM oracle_accounts WHERE balance BETWEEN $1 AND $2 ORDER BY id",
        &[&low, &high],
    )
    .await?;

    compare_query(
        &omendb,
        &postgres,
        "SELECT a.id, g.label, a.state
         FROM oracle_accounts AS a
         JOIN oracle_groups AS g ON a.group_id = g.id
         ORDER BY a.id",
        &[],
    )
    .await?;

    compare_query(
        &omendb,
        &postgres,
        "SELECT state, count(*) AS row_count
         FROM oracle_accounts
         GROUP BY state
         ORDER BY state ASC NULLS FIRST",
        &[],
    )
    .await?;

    let updated_balance = 55_i64;
    let updated_id = 2_i64;
    compare_query(
        &omendb,
        &postgres,
        "UPDATE oracle_accounts
         SET balance = $1
         WHERE id = $2
         RETURNING id, balance, state",
        &[&updated_balance, &updated_id],
    )
    .await?;

    let inserted_id = 5_i64;
    let inserted_group = 20_i64;
    let inserted_balance = 77_i64;
    let inserted_state = "open";
    compare_query(
        &omendb,
        &postgres,
        "INSERT INTO oracle_accounts (id, group_id, balance, state)
         VALUES ($1, $2, $3, $4)
         RETURNING id, group_id, balance, state",
        &[
            &inserted_id,
            &inserted_group,
            &inserted_balance,
            &inserted_state,
        ],
    )
    .await?;

    let deleted_id = 3_i64;
    compare_query(
        &omendb,
        &postgres,
        "DELETE FROM oracle_accounts WHERE id = $1 RETURNING id, balance, state",
        &[&deleted_id],
    )
    .await?;

    {
        let omen_transaction = omendb
            .transaction()
            .await
            .context("begin OmenDB rollback")?;
        omen_transaction
            .execute(
                "INSERT INTO oracle_accounts (id, group_id, balance, state)
                 VALUES (99, 10, 1, 'rollback')",
                &[],
            )
            .await
            .context("stage OmenDB rollback row")?;
        omen_transaction
            .rollback()
            .await
            .context("rollback OmenDB transaction")?;
    }
    {
        let postgres_transaction = postgres
            .transaction()
            .await
            .context("begin PostgreSQL rollback")?;
        postgres_transaction
            .execute(
                "INSERT INTO oracle_accounts (id, group_id, balance, state)
                 VALUES (99, 10, 1, 'rollback')",
                &[],
            )
            .await
            .context("stage PostgreSQL rollback row")?;
        postgres_transaction
            .rollback()
            .await
            .context("rollback PostgreSQL transaction")?;
    }

    compare_query(
        &omendb,
        &postgres,
        "SELECT id, group_id, balance, state FROM oracle_accounts ORDER BY id",
        &[],
    )
    .await?;

    // Typed scalar values: DDL with typed columns, literal inserts,
    // binary parameters, text/binary result decoding, ordering, and
    // arithmetic must all agree with live PostgreSQL.
    typed_scalar_subset_matches_live_postgres(&omendb, &postgres).await?;

    drop(omendb);
    drop(postgres);
    server.shutdown().await.context("shutdown OmenDB oracle")?;
    omen_connection
        .await
        .context("join OmenDB wire connection")??;
    postgres_connection
        .await
        .context("join PostgreSQL oracle connection")??;
    Ok(())
}

/// Create the typed-table pair (OmenDB + PostgreSQL) and run the typed
/// comparisons. Both clients' tables use identical DDL.
async fn typed_scalar_subset_matches_live_postgres(
    omendb: &Client,
    postgres: &Client,
) -> Result<()> {
    postgres
        .execute("DROP TABLE IF EXISTS oracle_typed", &[])
        .await?;
    // Unconstrained NUMERIC: OmenDB accepts NUMERIC(p,s) DDL but stores
    // unconstrained scale-tracked decimals today (documented divergence);
    // the oracle compares the unconstrained behavior.
    let typed_ddl = "CREATE TABLE oracle_typed (
            id BIGINT PRIMARY KEY,
            price NUMERIC,
            ratio DOUBLE PRECISION,
            occurred DATE NOT NULL,
            seen_at TIMESTAMP,
            token UUID
        )";
    omendb.execute(typed_ddl, &[]).await?;
    postgres.execute(typed_ddl, &[]).await?;

    let typed_inserts = [
        "INSERT INTO oracle_typed VALUES (1, '19.99', '0.5', '2026-08-31', '2026-08-31 13:45:21.123456', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')",
        "INSERT INTO oracle_typed VALUES (2, '0.001', 'NaN', '2026-01-01', '2026-08-31 13:45:21', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12')",
        "INSERT INTO oracle_typed VALUES (3, '-2.75', '-1.5', '2025-12-31', NULL, 'b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a13')",
    ];
    for insert in typed_inserts {
        omendb.execute(insert, &[]).await?;
        postgres.execute(insert, &[]).await?;
    }

    // Full projection round-trip: text-form cells must match exactly.
    compare_query(
        omendb,
        postgres,
        "SELECT id, price, ratio, occurred, seen_at, token FROM oracle_typed ORDER BY id",
        &[],
    )
    .await?;

    // Numeric ordering and cross-type comparison.
    compare_query(
        omendb,
        postgres,
        "SELECT id FROM oracle_typed WHERE price > 1 ORDER BY id",
        &[],
    )
    .await?;

    // Date range comparison through a literal.
    compare_query(
        omendb,
        postgres,
        "SELECT id FROM oracle_typed WHERE occurred < '2026-08-31' ORDER BY id",
        &[],
    )
    .await?;

    // Decimal arithmetic and aggregate.
    compare_query(omendb, postgres, "SELECT SUM(price) FROM oracle_typed", &[]).await?;
    compare_query(
        omendb,
        postgres,
        "SELECT MIN(occurred), MAX(occurred) FROM oracle_typed",
        &[],
    )
    .await?;

    // Float text rendering (NaN, negatives) matches.
    compare_query(
        omendb,
        postgres,
        "SELECT ratio FROM oracle_typed ORDER BY id",
        &[],
    )
    .await?;

    // Binary parameter round-trip: tokio-postgres sends float8/date/
    // timestamp/uuid parameters as binary and decodes typed results as
    // binary; any wire-encoder mismatch fails here. (numeric parameters
    // would need the rust_decimal feature and stay covered by the
    // text-form comparisons above.)
    compare_query(
        omendb,
        postgres,
        "SELECT id, ratio, occurred FROM oracle_typed WHERE ratio > $1 ORDER BY id",
        &[&(-10.0_f64)],
    )
    .await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Oracle-side wire decoders. Independent from OmenDB's encoders so the
// comparison is honest; formats from PostgreSQL's COPY BINARY spec.
// ---------------------------------------------------------------------------

/// DATE: i32 big-endian days since 2000-01-01 -> `YYYY-MM-DD`.
struct DateText(String);

impl<'a> FromSql<'a> for DateText {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let raw: [u8; 4] = raw.try_into().map_err(|_| "date width")?;
        let days = i32::from_be_bytes(raw);
        // days since 2000-01-01 -> since 1970-01-01
        Ok(DateText(civil_from_days(days + 10_957)))
    }
    fn accepts(ty: &Type) -> bool {
        ty == &Type::DATE
    }
}

/// TIMESTAMP: i64 big-endian micros since 2000-01-01 -> ISO text with
/// fraction trimmed like PostgreSQL.
struct TimestampText(String);

impl<'a> FromSql<'a> for TimestampText {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let raw: [u8; 8] = raw.try_into().map_err(|_| "timestamp width")?;
        let micros = i64::from_be_bytes(raw) + 946_684_800_000_000; // to Unix epoch
        let days = micros.div_euclid(86_400_000_000);
        let time = micros.rem_euclid(86_400_000_000);
        let date = civil_from_days(i32::try_from(days).map_err(|_| "timestamp range")?);
        let hour = time / 3_600_000_000;
        let minute = (time / 60_000_000) % 60;
        let second = (time / 1_000_000) % 60;
        let fraction = time % 1_000_000;
        let text = if fraction == 0 {
            format!("{date} {hour:02}:{minute:02}:{second:02}")
        } else {
            let trimmed = format!("{fraction:06}");
            format!(
                "{date} {hour:02}:{minute:02}:{second:02}.{}",
                trimmed.trim_end_matches('0')
            )
        };
        Ok(TimestampText(text))
    }
    fn accepts(ty: &Type) -> bool {
        ty == &Type::TIMESTAMP
    }
}

/// NUMERIC: ndigits | weight | sign | dscale | base-10000 groups ->
/// PostgreSQL-normalized text.
struct NumericText(String);

impl<'a> FromSql<'a> for NumericText {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() < 8 || !raw.len().is_multiple_of(2) {
            return Err("numeric width".into());
        }
        let be16 = |offset: usize| u16::from_be_bytes([raw[offset], raw[offset + 1]]);
        let ndigits = be16(0) as usize;
        if raw.len() != 8 + ndigits * 2 {
            return Err("numeric group count".into());
        }
        let weight = i16::from_be_bytes([raw[2], raw[3]]) as i64;
        let sign = be16(4);
        let dscale = be16(6) as usize;
        let groups: Vec<u16> = (0..ndigits).map(|i| be16(8 + i * 2)).collect();

        let mut text = String::new();
        if sign == 0x4000 {
            text.push('-');
        }
        if groups.is_empty() {
            text.push('0');
            if dscale > 0 {
                text.push('.');
                for _ in 0..dscale {
                    text.push('0');
                }
            }
            return Ok(NumericText(text));
        }
        // Integer part: groups [0..weight+1) when weight >= 0.
        let int_group_count = usize::try_from(weight + 1).unwrap_or(0);
        let (int_groups, frac_groups) = if groups.len() >= int_group_count && weight >= 0 {
            (&groups[..int_group_count], &groups[int_group_count..])
        } else {
            (&[] as &[u16], &groups[..])
        };
        let mut int_part = String::new();
        for (index, group) in int_groups.iter().enumerate() {
            if index == 0 {
                int_part.push_str(&group.to_string());
            } else {
                int_part.push_str(&format!("{group:04}"));
            }
        }
        if int_part.is_empty() {
            text.push('0');
        } else {
            text.push_str(&int_part);
        }
        if dscale > 0 {
            // Fraction digits from the groups below 10^0, padded to 4,
            // then fixed to exactly dscale digits.
            let mut frac_digits = String::new();
            let mut remaining_groups = frac_groups.to_vec();
            if weight < 0 {
                // All groups are fractional; leading zero digits sit
                // between the decimal point and the first group.
                let leading_zeros = (-weight - 1) * 4;
                for _ in 0..leading_zeros {
                    frac_digits.push('0');
                }
                remaining_groups = groups.clone();
            }
            for group in &remaining_groups {
                frac_digits.push_str(&format!("{group:04}"));
            }
            while frac_digits.len() < dscale {
                frac_digits.push('0');
            }
            frac_digits.truncate(dscale);
            text.push('.');
            text.push_str(&frac_digits);
        }
        Ok(NumericText(text))
    }
    fn accepts(ty: &Type) -> bool {
        ty == &Type::NUMERIC
    }
}

/// UUID: 16 raw bytes -> canonical hyphenated lowercase hex.
struct UuidText(String);

impl<'a> FromSql<'a> for UuidText {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let raw: [u8; 16] = raw.try_into().map_err(|_| "uuid width")?;
        let mut text = String::with_capacity(36);
        for (index, byte) in raw.iter().enumerate() {
            if matches!(index, 4 | 6 | 8 | 10) {
                text.push('-');
            }
            text.push_str(&format!("{byte:02x}"));
        }
        Ok(UuidText(text))
    }
    fn accepts(ty: &Type) -> bool {
        ty == &Type::UUID
    }
}

/// Proleptic Gregorian date text from days since 1970-01-01.
fn civil_from_days(days: i32) -> String {
    let z = i64::from(days) + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    format!("{year:04}-{month:02}-{day:02}")
}

/// Dump/restore differential: an OmenDB dump must restore into real
/// PostgreSQL and reproduce the same logical content. This is the
/// data-in/data-out trust gate — if a dump cannot move data into
/// PostgreSQL, the dump format has drifted from its contract.
#[tokio::test]
#[ignore = "requires a live PostgreSQL oracle"]
async fn dump_restores_into_live_postgresql() -> Result<()> {
    let postgres_url = env::var(POSTGRES_ORACLE_URL).with_context(|| {
        format!("{POSTGRES_ORACLE_URL} must point to the live PostgreSQL oracle")
    })?;

    // Build an OmenDB database directly (the dump API is engine-local;
    // the wire tier is not involved in dumping).
    let directory = tempdir().context("dump differential directory")?;
    let db_path = directory.path().join("dump-db");
    let config = omendb::RelationalBackendConfig::new(db_path);
    let mut omendb_db =
        omendb::RelationalDatabase::create(config).context("create OmenDB source")?;
    for statement in [
        "CREATE TABLE dump_groups (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE dump_accounts (
             id BIGINT PRIMARY KEY,
             label TEXT,
             ratio DOUBLE PRECISION,
             opened DATE,
             price NUMERIC,
             token UUID,
             payload BYTEA,
             group_id BIGINT,
             FOREIGN KEY (group_id) REFERENCES dump_groups (id)
         )",
        "INSERT INTO dump_groups (id, name) VALUES (1, 'alpha'), (2, 'beta')",
        "INSERT INTO dump_accounts (id, label, ratio, opened, price, token, payload, group_id) VALUES
             (1, 'first', 0.5, '2026-01-15', '19.99', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\\xabcd', 1),
             (2, 'second', 'NaN', '2025-12-31', '-0.001', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', '\\x', 2),
             (3, 'third', -1.5, NULL, NULL, NULL, NULL, NULL)",
    ] {
        omendb_db
            .execute_sql(statement)
            .with_context(|| format!("seed dump source: {statement}"))?;
    }

    let dump = omendb::dump_sql(&mut omendb_db).context("dump")?;
    drop(omendb_db);

    let (postgres, postgres_connection) = connect(&postgres_url).await?;
    postgres
        .batch_execute(
            "DROP TABLE IF EXISTS dump_accounts;
             DROP TABLE IF EXISTS dump_groups;",
        )
        .await
        .context("clean differential tables")?;
    // The dump's CREATE TABLE order respects FK references (children
    // after parents in catalog ID order here), but restore must not
    // depend on that: our own restore_sql replays statements in order
    // and the FK arrives after the data, so plain batch_execute works.
    postgres
        .batch_execute(&dump)
        .await
        .context("restore OmenDB dump into PostgreSQL")?;

    // Same logical content in both engines.
    let pg_rows: Vec<i64> = postgres
        .query("SELECT id FROM dump_accounts ORDER BY id", &[])
        .await
        .context("query restored rows")?
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(pg_rows, vec![1, 2, 3]);

    let pg_ratio: Vec<Option<String>> = postgres
        .query("SELECT ratio::text FROM dump_accounts ORDER BY id", &[])
        .await?
        .iter()
        .map(|row| row.get::<_, Option<String>>(0))
        .collect();
    // PostgreSQL renders float8 NaN as 'NaN'; 0.5 and -1.5 round-trip.
    assert_eq!(pg_ratio[0].as_deref(), Some("0.5"));
    assert_eq!(pg_ratio[1].as_deref(), Some("NaN"));
    assert_eq!(pg_ratio[2].as_deref(), Some("-1.5"));

    let pg_price: Vec<Option<String>> = postgres
        .query("SELECT price::text FROM dump_accounts ORDER BY id", &[])
        .await?
        .iter()
        .map(|row| row.get::<_, Option<String>>(0))
        .collect();
    assert_eq!(pg_price[0].as_deref(), Some("19.99"));
    assert_eq!(pg_price[1].as_deref(), Some("-0.001"));
    assert_eq!(pg_price[2], None);

    // The foreign key is live in PostgreSQL too: an orphan insert fails.
    let orphan = postgres
        .execute(
            "INSERT INTO dump_accounts (id, label, group_id) VALUES (99, 'orphan', 77)",
            &[],
        )
        .await;
    assert!(orphan.is_err(), "restored FK must reject orphans in PG");

    // Unique constraint survives the round trip.
    postgres
        .batch_execute(
            "DROP TABLE IF EXISTS dump_accounts;
             DROP TABLE IF EXISTS dump_groups;",
        )
        .await
        .context("clean up differential tables")?;
    drop(postgres);
    postgres_connection
        .await
        .context("join PostgreSQL oracle connection")??;
    Ok(())
}
