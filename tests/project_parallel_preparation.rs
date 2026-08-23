use std::path::Path;
use std::sync::{Arc, Barrier};

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DatabaseConfig, DbError, Key, OperationControl,
    RelationalBackendConfig, RelationalBackendKind, RelationalDatabaseConfig,
    RelationalDatabaseSession, RelationalSessionConfig, Row, SeerKernelConfig, TableDefinition,
    TableId, Value,
};
use tempfile::tempdir;

const TABLE: TableId = TableId(90);

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

fn table() -> TableDefinition {
    TableDefinition {
        id: TABLE,
        name: "parallel_preparation_items".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "value".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
        ],
    }
}

fn row(id: u64, value: u64) -> Row {
    Row {
        primary: Key::new(TABLE.0, id),
        values: vec![Value::U64(id), Value::U64(value)],
    }
}

fn exercise(kind: RelationalBackendKind, directory: &Path) {
    let session = Arc::new(
        RelationalDatabaseSession::create(
            RelationalDatabaseConfig::new(config(kind, directory)).with_session_config(
                RelationalSessionConfig {
                    max_in_flight: 2,
                    ..RelationalSessionConfig::default()
                },
            ),
        )
        .expect("create session"),
    );
    let control = OperationControl::default();
    session.create_table(&control, table()).expect("table");
    session.insert(&control, TABLE, row(1, 0)).expect("row 1");
    session.insert(&control, TABLE, row(2, 0)).expect("row 2");

    let barrier = Arc::new(Barrier::new(2));
    let mut workers = Vec::new();
    for id in 1..=2 {
        let session = Arc::clone(&session);
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            session.transaction_with_parallel_preparation(
                &OperationControl::default(),
                move |database, transaction| {
                    transaction.update(database, TABLE, row(id, 1))?;
                    barrier.wait();
                    Ok(())
                },
            )
        }));
    }

    let mut commits = 0;
    let mut conflicts = 0;
    for worker in workers {
        match worker.join().expect("parallel preparation worker") {
            Ok(((), _)) => commits += 1,
            Err(DbError::SerializationConflict { .. }) => conflicts += 1,
            Err(error) => panic!("unexpected parallel preparation result: {error:?}"),
        }
    }
    assert_eq!(commits, 1);
    assert_eq!(conflicts, 1);

    let control = OperationControl::default();
    let snapshot = session.commit_id(&control).expect("commit frontier");
    let rows = session
        .scan(&control, TABLE, snapshot, usize::MAX)
        .expect("final rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter()
            .filter(|row| row.values.get(1) == Some(&Value::U64(1)))
            .count(),
        1
    );
    assert_eq!(
        rows.iter()
            .filter(|row| row.values.get(1) == Some(&Value::U64(0)))
            .count(),
        1
    );
    let status = session.admission_status().expect("admission status");
    assert_eq!(status.active_operations, 0);
    assert_eq!(status.waiting_operations, 0);
    assert!(status.completed_operations >= 5);

    let session = match Arc::try_unwrap(session) {
        Ok(session) => session,
        Err(_) => panic!("session references remain"),
    };
    session.close().expect("close");
}

#[test]
fn parallel_preparation_preserves_serialized_publication_profile() {
    let temporary = tempdir().expect("temporary directory");
    exercise(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    );

    let seer = tempdir().expect("seer directory");
    exercise(RelationalBackendKind::Seer, &seer.path().join("seer"));
}
