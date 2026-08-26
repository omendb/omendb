use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::mpsc::channel;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use omendb::{
    CancellationToken, ColumnDefinition, ColumnId, ColumnType, DbError, IndexDefinition, IndexId,
    Key, OperationControl, RelationalBackendConfig, RelationalDatabase, RelationalSessionConfig,
    TableDefinition, TableId, TransactionErrorClass, Value,
};
use tempfile::tempdir;

const TABLE: TableId = TableId(1);
const VALUE_INDEX: IndexId = IndexId(1);

fn config(directory: &Path) -> RelationalBackendConfig {
    RelationalBackendConfig::new(directory.to_owned())
}

fn table() -> TableDefinition {
    TableDefinition {
        id: TABLE,
        name: "items".to_owned(),
        columns: vec![ColumnDefinition {
            id: ColumnId(1),
            name: "value".to_owned(),
            data_type: ColumnType::Text,
            nullable: false,
        }],
    }
}

fn row(primary: u64, value: &str) -> omendb::Row {
    omendb::Row {
        primary: Key::new(TABLE.0, primary),
        values: vec![Value::Text(value.to_owned())],
    }
}

fn exercise_lifecycle(directory: &Path) {
    let database_config = config(directory);
    let database = RelationalDatabase::create(database_config.clone()).expect("create");
    let session = database
        .into_session(RelationalSessionConfig {
            max_in_flight: 2,
            ..RelationalSessionConfig::default()
        })
        .expect("session");
    let control = OperationControl::default();
    let schema_commit = session.create_table(&control, table()).expect("table");
    session
        .create_index(
            &control,
            IndexDefinition {
                id: VALUE_INDEX,
                table: TABLE,
                columns: vec![ColumnId(1)],
                unique: true,
            },
        )
        .expect("value index");

    let ((), commit) = session
        .transaction(&control, |database, transaction| {
            transaction.insert(database, TABLE, row(1, "durable"))?;
            Ok(())
        })
        .expect("transaction");
    assert!(commit > schema_commit);
    assert_eq!(
        session
            .get(&control, TABLE, Key::new(TABLE.0, 1))
            .expect("read"),
        Some(row(1, "durable"))
    );
    assert_eq!(
        session.scan(&control, TABLE, 10).expect("scan"),
        vec![row(1, "durable")]
    );
    assert_eq!(
        session
            .index_get(
                &control,
                TABLE,
                VALUE_INDEX,
                &[Value::Text("durable".to_owned())],
            )
            .expect("indexed read"),
        vec![row(1, "durable")]
    );
    assert_eq!(
        session
            .index_scan(&control, TABLE, VALUE_INDEX)
            .expect("indexed scan"),
        vec![row(1, "durable")]
    );
    assert_eq!(
        session.commit_id(&control).expect("commit frontier"),
        commit
    );
    let (_, updated_commit) = session
        .transaction(&control, |database, transaction| {
            transaction.update(database, TABLE, row(1, "updated"))?;
            Ok(())
        })
        .expect("update transaction");
    assert_eq!(
        session
            .get(&control, TABLE, Key::new(TABLE.0, 1))
            .expect("current read"),
        Some(row(1, "updated"))
    );
    let cancellation = CancellationToken::new();
    let cancelled_control = OperationControl::with_cancellation(cancellation.clone());
    let cancelled = session.transaction(&cancelled_control, |database, transaction| {
        transaction.insert(database, TABLE, row(2, "aborted"))?;
        cancellation.cancel();
        Ok(())
    });
    let error = cancelled.expect_err("cancelled transaction");
    assert_eq!(error.transaction_class(), TransactionErrorClass::Cancelled);
    assert!(matches!(error, DbError::Cancelled));
    assert_eq!(
        session
            .read(&control, |database| {
                database.get(TABLE, Key::new(TABLE.0, 2))
            })
            .expect("aborted row lookup"),
        None
    );

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = session.read(&control, |_database| -> omendb::Result<()> {
            panic!("operation panic for permit-release coverage");
        });
    }));
    assert!(panicked.is_err());
    session
        .read(&control, |_database| Ok(()))
        .expect("permit released after panic");
    assert_eq!(
        session
            .admission_status()
            .expect("status")
            .active_operations,
        0
    );

    session.close().expect("close");
    let reopened = RelationalDatabase::open(database_config).expect("reopen");
    assert_eq!(reopened.commit_id(), updated_commit);
    reopened.close().expect("reopened close");
}

fn exercise_single_row_writes(directory: &Path) {
    let database_config = config(directory);
    let session = RelationalDatabase::create(database_config.clone())
        .expect("create")
        .into_session(RelationalSessionConfig::default())
        .expect("session");
    let control = OperationControl::default();
    session.create_table(&control, table()).expect("table");

    let inserted = session
        .insert(&control, TABLE, row(1, "inserted"))
        .expect("insert");
    assert_eq!(
        session.commit_id(&control).expect("insert commit"),
        inserted
    );
    assert_eq!(
        session
            .get(&control, TABLE, Key::new(TABLE.0, 1))
            .expect("inserted row"),
        Some(row(1, "inserted"))
    );

    let updated = session
        .update(&control, TABLE, row(1, "updated"))
        .expect("update");
    assert!(updated > inserted);
    assert_eq!(
        session
            .get(&control, TABLE, Key::new(TABLE.0, 1))
            .expect("updated row"),
        Some(row(1, "updated"))
    );

    let deleted = session
        .delete(&control, TABLE, Key::new(TABLE.0, 1))
        .expect("delete");
    assert!(deleted > updated);
    assert_eq!(
        session
            .get(&control, TABLE, Key::new(TABLE.0, 1))
            .expect("deleted row"),
        None
    );
    session.close().expect("close");

    let reopened = RelationalDatabase::open(database_config).expect("reopen");
    assert_eq!(reopened.commit_id(), deleted);
    assert_eq!(
        reopened
            .get(TABLE, Key::new(TABLE.0, 1))
            .expect("reopened deleted row"),
        None
    );
    reopened.close().expect("reopened close");
}

fn exercise_admission(directory: &Path) {
    let database = RelationalDatabase::create(config(directory)).expect("create");
    let session = Arc::new(
        database
            .into_session(RelationalSessionConfig {
                max_in_flight: 1,
                admission_timeout: Duration::from_millis(200),
            })
            .expect("session"),
    );
    session
        .create_table(&OperationControl::default(), table())
        .expect("table");

    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let holder_session = Arc::clone(&session);
    let holder_entered = Arc::clone(&entered);
    let holder_release = Arc::clone(&release);
    let holder = thread::spawn(move || {
        holder_session.read(&OperationControl::default(), |_database| {
            holder_entered.wait();
            holder_release.wait();
            Ok(())
        })
    });
    entered.wait();

    let busy = session.read(&OperationControl::default(), |_database| Ok(()));
    assert!(matches!(busy, Err(DbError::SessionBusy)));

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = session.read(
        &OperationControl::with_cancellation(cancellation),
        |_database| Ok(()),
    );
    assert!(matches!(cancelled, Err(DbError::Cancelled)));

    let expired = session.read(
        &OperationControl::new().with_deadline(Instant::now() + Duration::from_millis(20)),
        |_database| Ok(()),
    );
    assert!(matches!(expired, Err(DbError::DeadlineExceeded)));
    assert_eq!(
        session
            .admission_status()
            .expect("active status")
            .active_operations,
        1
    );

    let waiting_cancellation = CancellationToken::new();
    let waiting_control = OperationControl::with_cancellation(waiting_cancellation.clone());
    let waiting_session = Arc::clone(&session);
    let waiting = thread::spawn(move || waiting_session.read(&waiting_control, |_database| Ok(())));
    let waiting_deadline = Instant::now() + Duration::from_secs(1);
    while session
        .admission_status()
        .expect("waiting status")
        .waiting_operations
        == 0
    {
        assert!(
            Instant::now() < waiting_deadline,
            "waiter did not enter queue"
        );
        thread::yield_now();
    }
    waiting_cancellation.cancel();
    assert!(matches!(
        waiting.join().expect("cancelled waiter thread"),
        Err(DbError::Cancelled)
    ));
    assert_eq!(
        session
            .admission_status()
            .expect("cancelled waiter status")
            .active_operations,
        1
    );

    release.wait();
    holder.join().expect("holder thread").expect("holder read");
    session
        .read(&OperationControl::default(), |_database| Ok(()))
        .expect("admission released");
    let status = session.admission_status().expect("final status");
    assert_eq!(status.active_operations, 0);
    assert!(status.completed_operations >= 3);
    assert!(status.total_admission_wait >= status.max_admission_wait);
    assert!(status.total_operation_time >= status.max_operation_time);
    assert!(status.cancelled_operations >= 1);
    assert!(status.deadline_expired_operations >= 1);
    assert!(status.rejected_operations >= 3);
    let session = match Arc::try_unwrap(session) {
        Ok(session) => session,
        Err(_) => panic!("session references remain after admission test"),
    };
    session.close().expect("close");
}

fn exercise_reader_overlap_and_writer_exclusion(directory: &Path) {
    let database = RelationalDatabase::create(config(directory)).expect("create");
    let session = Arc::new(
        database
            .into_session(RelationalSessionConfig {
                max_in_flight: 2,
                admission_timeout: Duration::from_millis(200),
            })
            .expect("session"),
    );
    session
        .create_table(&OperationControl::default(), table())
        .expect("table");

    let (entered_tx, entered_rx) = channel();
    let mut releases = Vec::new();
    let mut readers = Vec::new();
    for _ in 0..2 {
        let (release_tx, release_rx) = channel();
        releases.push(release_tx);
        let reader_session = Arc::clone(&session);
        let reader_entered = entered_tx.clone();
        readers.push(thread::spawn(move || {
            reader_session.read(&OperationControl::default(), |_database| {
                reader_entered.send(()).expect("reader entered receiver");
                release_rx.recv().expect("reader release");
                Ok(())
            })
        }));
    }
    drop(entered_tx);

    let readers_ready = (0..2)
        .map(|_| entered_rx.recv_timeout(Duration::from_secs(2)))
        .collect::<std::result::Result<Vec<_>, _>>()
        .is_ok();
    let writer_while_readers_active = if readers_ready {
        Some(
            session.transaction(&OperationControl::default(), |_database, _transaction| {
                Ok(())
            }),
        )
    } else {
        None
    };
    for release in releases {
        release.send(()).expect("release reader");
    }
    for reader in readers {
        reader
            .join()
            .expect("reader thread")
            .expect("reader operation");
    }
    assert!(readers_ready, "both configured readers must overlap");
    assert!(matches!(
        writer_while_readers_active,
        Some(Err(DbError::SessionBusy))
    ));
    assert_eq!(
        session
            .admission_status()
            .expect("reader admission status")
            .active_operations,
        0
    );

    let (writer_entered_tx, writer_entered_rx) = channel();
    let (writer_release_tx, writer_release_rx) = channel();
    let writer_session = Arc::clone(&session);
    let writer = thread::spawn(move || {
        writer_session.transaction(&OperationControl::default(), |database, transaction| {
            transaction.insert(database, TABLE, row(1, "writer"))?;
            writer_entered_tx.send(()).expect("writer entered receiver");
            writer_release_rx.recv().expect("writer release");
            Ok(())
        })
    });
    let writer_ready = writer_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .is_ok();
    let read_while_writer_active = if writer_ready {
        Some(session.read(&OperationControl::default(), |_database| Ok(())))
    } else {
        None
    };
    writer_release_tx.send(()).expect("release writer");
    writer
        .join()
        .expect("writer thread")
        .expect("writer operation");
    assert!(writer_ready, "writer must acquire exclusive admission");
    assert!(matches!(
        read_while_writer_active,
        Some(Err(DbError::SessionBusy))
    ));
    assert_eq!(
        session
            .admission_status()
            .expect("writer admission status")
            .active_operations,
        0
    );

    let session = match Arc::try_unwrap(session) {
        Ok(session) => session,
        Err(_) => panic!("session references remain after overlap test"),
    };
    session.close().expect("close");
}

fn exercise_waitable_admission_and_writer_preference(directory: &Path) {
    let database = RelationalDatabase::create(config(directory)).expect("create");
    let session = Arc::new(
        database
            .into_session(RelationalSessionConfig {
                max_in_flight: 1,
                admission_timeout: Duration::from_secs(2),
            })
            .expect("session"),
    );
    session
        .create_table(&OperationControl::default(), table())
        .expect("table");

    let (holder_entered_tx, holder_entered_rx) = channel();
    let (holder_release_tx, holder_release_rx) = channel();
    let holder_session = Arc::clone(&session);
    let holder = thread::spawn(move || {
        holder_session.read(&OperationControl::default(), |_database| {
            holder_entered_tx.send(()).expect("holder entered receiver");
            holder_release_rx.recv().expect("holder release");
            Ok(())
        })
    });
    holder_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("holder entered");

    let (writer_entered_tx, writer_entered_rx) = channel();
    let (writer_release_tx, writer_release_rx) = channel();
    let writer_session = Arc::clone(&session);
    let writer = thread::spawn(move || {
        writer_session.transaction(&OperationControl::default(), |database, transaction| {
            transaction.insert(database, TABLE, row(1, "writer"))?;
            writer_entered_tx.send(()).expect("writer entered receiver");
            writer_release_rx.recv().expect("writer release");
            Ok(())
        })
    });

    let queue_deadline = Instant::now() + Duration::from_secs(2);
    while session
        .admission_status()
        .expect("writer queue status")
        .waiting_writers
        == 0
    {
        assert!(
            Instant::now() < queue_deadline,
            "writer did not enter queue"
        );
        thread::yield_now();
    }

    let (reader_entered_tx, reader_entered_rx) = channel();
    let reader_session = Arc::clone(&session);
    let reader = thread::spawn(move || {
        reader_session.read(&OperationControl::default(), |_database| {
            reader_entered_tx.send(()).expect("reader entered receiver");
            Ok(())
        })
    });
    let both_waiting_deadline = Instant::now() + Duration::from_secs(2);
    while session
        .admission_status()
        .expect("reader queue status")
        .waiting_operations
        < 2
    {
        assert!(
            Instant::now() < both_waiting_deadline,
            "writer and reader did not enter queue"
        );
        thread::yield_now();
    }

    holder_release_tx.send(()).expect("release holder");
    holder.join().expect("holder thread").expect("holder read");
    writer_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("writer must win admission after holder release");
    assert!(
        reader_entered_rx
            .recv_timeout(Duration::from_millis(50))
            .is_err()
    );

    writer_release_tx.send(()).expect("release writer");
    writer
        .join()
        .expect("writer thread")
        .expect("writer transaction");
    reader_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("reader enters after writer release");
    reader.join().expect("reader thread").expect("reader read");

    let status = session.admission_status().expect("final queue status");
    assert_eq!(status.active_operations, 0);
    assert_eq!(status.waiting_operations, 0);
    assert_eq!(status.waiting_writers, 0);
    let session = match Arc::try_unwrap(session) {
        Ok(session) => session,
        Err(_) => panic!("session references remain after fairness test"),
    };
    session.close().expect("close");
}

#[test]
fn public_session_preserves_lifecycle_and_transaction_ownership_on_each_backend() {
    let temporary = tempdir().expect("temporary directory");
    exercise_lifecycle(&temporary.path().join("temporary"));
}

#[test]
fn public_session_exposes_single_row_writes_on_each_backend() {
    let temporary = tempdir().expect("temporary directory");
    exercise_single_row_writes(&temporary.path().join("temporary"));
}

#[test]
fn public_session_rejects_busy_work_and_releases_permits_on_each_backend() {
    let temporary = tempdir().expect("temporary directory");
    exercise_admission(&temporary.path().join("temporary"));
}

#[test]
fn public_session_overlaps_bounded_reads_and_excludes_writers_on_each_backend() {
    let temporary = tempdir().expect("temporary directory");
    exercise_reader_overlap_and_writer_exclusion(&temporary.path().join("temporary"));
}

#[test]
fn public_session_waits_with_writer_preference_on_each_backend() {
    let temporary = tempdir().expect("temporary directory");
    exercise_waitable_admission_and_writer_preference(&temporary.path().join("temporary"));
}
