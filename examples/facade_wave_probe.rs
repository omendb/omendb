//! Facade insert probe: N threads each insert single rows through
//! RelationalDatabase::insert (the typed facade path with identity
//! checks, uniqueness probes, and index maintenance), printing aggregate
//! throughput. The engine-tier equivalent is `wave_probe`; the delta
//! between the two is the facade's read-serialization cost.
//!
//! ```text
//! cargo run --release --example facade_wave_probe -- [threads] [rows-per-thread]
//! ```

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, IndexDefinition, RelationalBackendConfig,
    RelationalDatabase, Row, TableDefinition, TableId, Value,
};

fn row(record: u64, email: &str) -> Row {
    Row {
        primary: omendb::Key::new(1, record),
        values: vec![
            Value::U64(1),
            Value::U64(record),
            Value::Text(email.to_owned()),
            Value::U64(record),
        ],
    }
}

fn main() {
    let threads: usize = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(8);
    let per_thread: usize = std::env::args()
        .nth(2)
        .and_then(|value| value.parse().ok())
        .unwrap_or(2000);

    let directory = tempfile::tempdir().expect("tempdir");
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new(directory.path().join("db")))
            .expect("db");
    database
        .create_table_with_schema_and_primary_key(
            TableDefinition {
                id: TableId(1),
                name: "users".to_owned(),
                columns: vec![
                    ColumnDefinition {
                        id: ColumnId(1),
                        name: "tenant".to_owned(),
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
            },
            Some(vec![ColumnId(1), ColumnId(2)]),
            Default::default(),
        )
        .expect("table");
    database
        .create_index(IndexDefinition {
            id: omendb::IndexId(1),
            table: TableId(1),
            columns: vec![ColumnId(3)],
            unique: true,
        })
        .expect("unique index");
    database
        .create_index(IndexDefinition {
            id: omendb::IndexId(2),
            table: TableId(1),
            columns: vec![ColumnId(2)],
            unique: false,
        })
        .expect("secondary index");

    let shared = std::sync::Arc::new(database);
    let started = std::time::Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|thread| {
            let db = std::sync::Arc::clone(&shared);
            std::thread::spawn(move || {
                for step in 0..per_thread {
                    let id = (thread as u64) * (per_thread as u64) + step as u64;
                    db.insert(TableId(1), row(id, &format!("u{id}@example.test")))
                        .expect("insert");
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("thread");
    }
    let elapsed = started.elapsed();
    let total = threads * per_thread;
    println!(
        "{total} facade inserts across {threads} threads in {elapsed:?}: {:.0} ops/s",
        total as f64 / elapsed.as_secs_f64()
    );
}
