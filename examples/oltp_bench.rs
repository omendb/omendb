//! OLTP benchmark over the project-facing facade.
//!
//! Measures point inserts, batched inserts, primary-key reads, index
//! lookups, read-modify-write updates, and the SQL path — with and without
//! foreign keys. Run with:
//!
//! ```text
//! cargo run --release --example oltp_bench
//! ```

use std::time::Instant;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, IndexDefinition, RelationalBackendConfig,
    RelationalDatabase, Result, Row, RowIdentity, TableDefinition, TableId, Value,
};

const TENANT: u64 = 7;
const N_ROWS: usize = 20_000;
const BATCH_SIZE: usize = 100;

fn key(record: u64) -> omendb::Key {
    omendb::Key::new(TENANT, record)
}

fn row(record: u64, email: &str) -> Row {
    Row {
        primary: key(record),
        values: vec![
            Value::U64(TENANT),
            Value::U64(record),
            Value::Text(email.to_owned()),
            Value::U64(42),
        ],
    }
}

fn users_table() -> TableDefinition {
    TableDefinition {
        id: TableId(1),
        name: "bench_users".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "tenant_id".to_owned(),
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
                name: "email".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(4),
                name: "balance".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
        ],
    }
}

struct Bench<'a> {
    name: &'static str,
    database: &'a mut RelationalDatabase,
}

impl Bench<'_> {
    fn report(&self, ops: usize, started: Instant) -> f64 {
        let elapsed = started.elapsed().as_secs_f64();
        let ops_per_sec = ops as f64 / elapsed;
        println!(
            "  {:<38} {:>12.0} ops/s  ({:>8.3}s total)",
            self.name, ops_per_sec, elapsed
        );
        ops_per_sec
    }

    fn run(
        &mut self,
        name: &'static str,
        ops: impl FnOnce(&mut RelationalDatabase) -> Result<usize>,
    ) -> f64 {
        let bench = Bench {
            name,
            database: self.database,
        };
        let started = Instant::now();
        let count = ops(bench.database).expect("benchmark operation succeeds");
        bench.report(count, started)
    }
}

fn identity(table: TableId, record: u64) -> Result<RowIdentity> {
    RowIdentity::new(
        table,
        vec![ColumnId(1), ColumnId(2)],
        vec![Value::U64(TENANT), Value::U64(record)],
    )
}

fn main() {
    let directory = tempfile::tempdir().expect("tempdir");
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
            .expect("create database");

    database
        .create_table_with_schema_and_primary_key(
            users_table(),
            Some(vec![ColumnId(1), ColumnId(2)]),
            Default::default(),
        )
        .expect("create table");
    database
        .create_index(IndexDefinition {
            id: omendb::IndexId(1),
            table: TableId(1),
            columns: vec![ColumnId(3)],
            unique: true,
        })
        .expect("create index");
    database
        .create_index(IndexDefinition {
            id: omendb::IndexId(2),
            table: TableId(1),
            columns: vec![ColumnId(2)],
            unique: false,
        })
        .expect("create user_id index");

    let mut bench = Bench {
        name: "",
        database: &mut database,
    };

    println!("OLTP benchmark (release build expected)\n");

    // Warm the storage engine so first-touch allocation does not skew the
    // single-op numbers.
    for record in 0..1_000_usize {
        bench
            .database
            .insert(TableId(1), row(record as u64, &format!("w{record}@x")))
            .expect("warmup insert");
    }

    // Seed rows via one big batch so later read benches have data without
    // paying per-row commit cost.
    bench.run("batched insert (100/batch)", |db| {
        for start in (1_000..N_ROWS).step_by(BATCH_SIZE) {
            db.commit_batch((start..start + BATCH_SIZE).map(|r| {
                omendb::RelationalMutation::Insert {
                    table: TableId(1),
                    row: row(r as u64, &format!("u{r}@x")),
                }
            }))?;
        }
        Ok(N_ROWS - 1_000)
    });

    bench.run("point insert (single-row commit)", |db| {
        for record in N_ROWS..N_ROWS + 1_000 {
            db.insert(TableId(1), row(record as u64, &format!("p{record}@x")))?;
        }
        Ok(1_000)
    });

    let timing = bench.database.metrics().publication_timing;
    let total: u64 = timing.candidate_prepare_ns
        + timing.wal_write_ns
        + timing.admission_ns
        + timing.data_flush_ns
        + timing.metadata_write_ns
        + timing.blob_write_ns
        + timing.history_write_ns
        + timing.directory_sync_ns
        + timing.manifest_write_ns
        + timing.manifest_mirror_ns;
    println!("  publication phase breakdown (cumulative):");
    for (name, ns) in [
        ("candidate_prepare", timing.candidate_prepare_ns),
        ("wal_write", timing.wal_write_ns),
        ("admission", timing.admission_ns),
        ("data_flush", timing.data_flush_ns),
        ("metadata_write", timing.metadata_write_ns),
        ("blob_write", timing.blob_write_ns),
        ("history_write", timing.history_write_ns),
        ("directory_sync", timing.directory_sync_ns),
        ("manifest_write", timing.manifest_write_ns),
        ("manifest_mirror", timing.manifest_mirror_ns),
    ] {
        println!(
            "    {:<20} {:>10.1} ms ({:>5.1}%)",
            name,
            ns as f64 / 1_000_000.0,
            100.0 * ns as f64 / total.max(1) as f64
        );
    }

    bench.run("point read by composite PK", |db| {
        let mut hits = 0;
        for record in 0..N_ROWS {
            if db
                .get_by_identity(
                    TableId(1),
                    &identity(TableId(1), record as u64).expect("identity"),
                )?
                .is_some()
            {
                hits += 1;
            }
        }
        Ok(hits)
    });

    bench.run("unique-index lookup", |db| {
        let mut hits = 0;
        for record in 0..N_ROWS {
            let found = db.index_get(
                TableId(1),
                omendb::IndexId(1),
                &[Value::Text(format!("u{record}@x"))],
            )?;
            hits += usize::from(!found.is_empty());
        }
        Ok(hits)
    });

    bench.run("full scan (20k rows)", |db| {
        let rows = db.scan(TableId(1), usize::MAX)?;
        assert!(rows.len() >= N_ROWS);
        Ok(rows.len())
    });

    bench.run("read-modify-write update", |db| {
        for record in 0..5_000_usize {
            let current = db
                .get_by_identity(
                    TableId(1),
                    &identity(TableId(1), record as u64).expect("identity"),
                )?
                .expect("row exists");
            let mut updated = current;
            if let Some(Value::U64(balance)) = updated.values.last_mut() {
                *balance += 1;
            }
            db.update(TableId(1), updated)?;
        }
        Ok(5_000)
    });

    bench.run("SQL SELECT by equality (typed path)", |db| {
        let mut hits = 0;
        for record in 0..2_000_usize {
            let result = db.execute_sql(&format!(
                "SELECT balance FROM bench_users WHERE user_id = {record}"
            ))?;
            hits += result.rows.len();
        }
        Ok(hits)
    });

    bench.run("SQL UPDATE via full scan", |db| {
        for record in 0..200_usize {
            db.execute_sql(&format!(
                "UPDATE bench_users SET balance = {} WHERE user_id = {}",
                record + 1_000,
                record
            ))?;
        }
        Ok(200)
    });

    // Concurrent writers: group-commit amortizes one durability sync across
    // every transaction staged in the same wave.
    let _ = bench; // release the mutable borrow before sharing
    let shared = std::sync::Arc::new(database);
    const THREADS: usize = 8;
    const PER_THREAD: usize = 2_000;
    let started = Instant::now();
    let handles: Vec<_> = (0..THREADS)
        .map(|thread| {
            let db = std::sync::Arc::clone(&shared);
            std::thread::spawn(move || {
                for record in 0..PER_THREAD {
                    let id = 100_000 + (thread * PER_THREAD) as u64 + record as u64;
                    db.insert(TableId(1), row(id, &format!("c{id}@x")))
                        .expect("concurrent insert");
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("writer thread");
    }
    let total = THREADS * PER_THREAD;
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "  {:<38} {:>12.0} ops/s  ({:>8.3}s total, {THREADS} threads)",
        "concurrent point insert",
        total as f64 / elapsed,
        elapsed
    );

    println!("\ndone.");
    std::sync::Arc::try_unwrap(shared).ok().map(|db| db.close());
}
