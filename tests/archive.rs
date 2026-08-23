use std::fs;
use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, ConstraintId, DatabaseConfig, DbError,
    ForeignKeyDefinition, IndexDefinition, IndexId, Key, NamedForeignKeyDefinition,
    NamedIndexDefinition, RelationalArchive, RelationalArchiveAttemptDisposition,
    RelationalArchiveAttemptPolicy, RelationalArchiveMode, RelationalBackendConfig,
    RelationalBackendKind, RelationalDatabase, RelationalSchemaDefinition,
    RelationalSnapshotCaptureOptions, Row, SeerKernelConfig, TableDefinition, TableId,
    TransactionAttemptId, TransactionAttemptOutcome, Value,
};
use tempfile::tempdir;

const ITEMS: TableId = TableId(1);

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
        id: ITEMS,
        name: "items".to_owned(),
        columns: vec![ColumnDefinition {
            id: ColumnId(1),
            name: "value".to_owned(),
            data_type: ColumnType::Text,
            nullable: false,
        }],
    }
}

fn row(id: u64, value: &str) -> Row {
    Row {
        primary: Key::new(ITEMS.0, id),
        values: vec![Value::Text(value.to_owned())],
    }
}

fn expanded_row(id: u64, value: &str, note: Value) -> Row {
    Row {
        primary: Key::new(ITEMS.0, id),
        values: vec![Value::Text(value.to_owned()), note],
    }
}

const LEDGER: TableId = TableId(10);
const LEDGER_STATE_INDEX: IndexId = IndexId(20);
const LEDGER_ENTRY_INDEX: IndexId = IndexId(21);
const PARENTS: TableId = TableId(11);
const PARENT_PRIMARY_INDEX: IndexId = IndexId(19);
const LEDGER_PARENT_FK: ConstraintId = ConstraintId(30);

fn parent_table() -> TableDefinition {
    TableDefinition {
        id: PARENTS,
        name: "ledger_parents".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "tenant_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "entry_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "label".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn parent_schema() -> RelationalSchemaDefinition {
    RelationalSchemaDefinition {
        indexes: vec![NamedIndexDefinition {
            definition: IndexDefinition {
                id: PARENT_PRIMARY_INDEX,
                table: PARENTS,
                columns: vec![ColumnId(1), ColumnId(2)],
                unique: true,
            },
            name: Some("ledger_parents_pk".to_owned()),
        }],
        foreign_keys: Vec::new(),
    }
}

fn parent_row(entry_id: u64) -> Row {
    Row {
        primary: Key::new(PARENTS.0, entry_id),
        values: vec![
            Value::U64(7),
            Value::U64(entry_id),
            Value::Text(format!("parent-{entry_id}")),
        ],
    }
}

fn composite_table() -> TableDefinition {
    TableDefinition {
        id: LEDGER,
        name: "ledger".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "tenant_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "entry_id".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(3),
                name: "state".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
        ],
    }
}

fn composite_schema() -> RelationalSchemaDefinition {
    RelationalSchemaDefinition {
        indexes: vec![NamedIndexDefinition {
            definition: IndexDefinition {
                id: LEDGER_STATE_INDEX,
                table: LEDGER,
                columns: vec![ColumnId(3)],
                unique: false,
            },
            name: Some("ledger_state".to_owned()),
        }],
        foreign_keys: vec![NamedForeignKeyDefinition {
            definition: ForeignKeyDefinition {
                id: LEDGER_PARENT_FK,
                table: LEDGER,
                columns: vec![ColumnId(1), ColumnId(2)],
                referenced_table: PARENTS,
                referenced_columns: vec![ColumnId(1), ColumnId(2)],
                on_delete: omendb::ReferentialAction::default(),
                timing: omendb::ConstraintTiming::default(),
            },
            name: Some("ledger_parent_fk".to_owned()),
        }],
    }
}

fn composite_entry_index() -> IndexDefinition {
    IndexDefinition {
        id: LEDGER_ENTRY_INDEX,
        table: LEDGER,
        columns: vec![ColumnId(2)],
        unique: false,
    }
}

fn composite_row(entry_id: u64, state: &str) -> Row {
    Row {
        primary: Key::new(LEDGER.0, entry_id),
        values: vec![
            Value::U64(7),
            Value::U64(entry_id),
            Value::Text(state.to_owned()),
        ],
    }
}

fn second_table() -> TableDefinition {
    TableDefinition {
        id: TableId(2),
        name: "events".to_owned(),
        columns: vec![ColumnDefinition {
            id: ColumnId(1),
            name: "value".to_owned(),
            data_type: ColumnType::Text,
            nullable: false,
        }],
    }
}

fn exercise(kind: RelationalBackendKind, directory: &Path) {
    let mut database = RelationalDatabase::create(config(kind, directory)).expect("create");
    database.create_table(table()).expect("table");
    let seed = database.insert(ITEMS, row(1, "before")).expect("seed");
    let lease = database.retain(seed).expect("retain seed");
    let head = database.update(ITEMS, row(1, "after")).expect("update");
    let capture = database
        .capture_selected_snapshots(&[head, seed], RelationalSnapshotCaptureOptions::new(10))
        .expect("capture");
    assert_eq!(capture.source_backend, kind);
    assert!(capture.attempts.is_empty());

    let archive =
        RelationalArchive::from_capture(capture.clone(), RelationalArchiveMode::RetainedSnapshots)
            .expect("archive");
    assert_eq!(archive.manifest.source_identity, capture.source_identity);
    assert_eq!(
        archive.manifest.attempt_disposition,
        RelationalArchiveAttemptDisposition::NoAttemptRecords
    );
    assert_eq!(archive.manifest.source_head, head);
    assert_eq!(archive.manifest.snapshots.len(), 2);
    assert_eq!(archive.snapshots, capture.snapshots);
    assert!(matches!(
        RelationalArchive::from_capture(capture.clone(), RelationalArchiveMode::CurrentState),
        Err(DbError::InvalidState(_))
    ));
    assert!(matches!(
        RelationalArchive::from_capture(capture.clone(), RelationalArchiveMode::FullHistory),
        Err(DbError::InvalidState(_))
    ));

    let label = match kind {
        RelationalBackendKind::Temporary => "temporary",
        RelationalBackendKind::Seer => "seer",
    };
    let restored_directory = directory.with_file_name(format!("{label}-restored"));
    let (mut restored, report) = archive
        .restore(config(kind, &restored_directory))
        .expect("restore archive");
    assert_eq!(report.source_identity, capture.source_identity);
    assert_eq!(report.mode, RelationalArchiveMode::RetainedSnapshots);
    assert!(report.history_preserved);
    assert_eq!(
        report.attempt_disposition,
        RelationalArchiveAttemptDisposition::NoAttemptRecords
    );
    assert_eq!(
        report
            .mappings
            .iter()
            .map(|mapping| mapping.source)
            .collect::<Vec<_>>(),
        vec![seed, head]
    );
    assert_ne!(report.source_identity, report.target_identity);
    restored.verify().expect("verify restored archive");
    restored.close().expect("close restored");

    let alternate_kind = match kind {
        RelationalBackendKind::Temporary => RelationalBackendKind::Seer,
        RelationalBackendKind::Seer => RelationalBackendKind::Temporary,
    };
    let alternate_directory = directory.with_file_name(format!("{label}-alternate-restored"));
    let (mut alternate, alternate_report) = archive
        .restore(config(alternate_kind, &alternate_directory))
        .expect("restore archive on alternate backend");
    assert_eq!(alternate.backend(), alternate_kind);
    assert_eq!(
        alternate_report
            .mappings
            .iter()
            .map(|mapping| mapping.source)
            .collect::<Vec<_>>(),
        vec![seed, head]
    );
    alternate.verify().expect("verify alternate restore");
    alternate.close().expect("close alternate restore");

    let archive_path = directory.join("logical.archive");
    archive.write(&archive_path).expect("write archive");
    assert!(matches!(
        archive.write(&archive_path),
        Err(DbError::InvalidState(_))
    ));
    let reopened = RelationalArchive::read(&archive_path).expect("read archive");
    assert_eq!(reopened, archive);

    let mut bytes = fs::read(&archive_path).expect("read archive bytes");
    bytes[88] ^= 0xff;
    fs::write(&archive_path, bytes).expect("corrupt archive");
    assert!(matches!(
        RelationalArchive::read(&archive_path),
        Err(DbError::Corruption {
            artifact: "relational archive",
            ..
        })
    ));

    database.release(lease).expect("release seed");
    database.close().expect("close");
}

#[test]
fn archive_refuses_durable_attempt_records_on_each_backend() {
    let root = tempdir().expect("root");
    for (kind, label) in [
        (RelationalBackendKind::Temporary, "temporary"),
        (RelationalBackendKind::Seer, "seer"),
    ] {
        let directory = root.path().join(label);
        let mut database = RelationalDatabase::create(config(kind, &directory)).expect("create");
        database.create_table(table()).expect("table");
        let attempt = TransactionAttemptId::new([label.as_bytes()[0]; 16]);
        let commit = match database
            .transaction_with_attempt(attempt, |database, transaction| {
                transaction.insert(database, ITEMS, row(1, "attempt"))?;
                Ok(())
            })
            .expect("attempt transaction")
        {
            TransactionAttemptOutcome::Applied { commit, .. } => commit,
            TransactionAttemptOutcome::AlreadyCommitted { .. } => {
                panic!("fresh attempt must not already be committed")
            }
        };
        let lease = database
            .retain(commit)
            .expect("retain current attempt state");
        let capture = database
            .capture_selected_snapshots(
                &[commit],
                RelationalSnapshotCaptureOptions::new(10).with_max_attempts(1),
            )
            .expect("capture attempts");
        assert_eq!(capture.attempts.len(), 1);
        assert_eq!(capture.attempts[0].attempt, attempt);
        assert_eq!(capture.attempts[0].commit, commit);
        assert!(matches!(
            RelationalArchive::from_capture(capture.clone(), RelationalArchiveMode::CurrentState),
            Err(DbError::InvalidState(message))
                if message.contains("transaction attempts")
        ));
        let archive = RelationalArchive::from_capture_with_attempt_policy(
            capture,
            RelationalArchiveMode::CurrentState,
            RelationalArchiveAttemptPolicy::ExcludeByPolicy,
        )
        .expect("explicit attempt exclusion");
        assert_eq!(
            archive.manifest.attempt_disposition,
            RelationalArchiveAttemptDisposition::ExcludedByPolicy
        );
        assert_eq!(archive.manifest.excluded_attempt_count, 1);
        let archive_path = directory.with_file_name(format!("{label}-excluded.archive"));
        archive
            .write(&archive_path)
            .expect("write excluded archive");
        assert_eq!(
            RelationalArchive::read(&archive_path).expect("read excluded archive"),
            archive
        );
        let restored_directory = directory.with_file_name(format!("{label}-excluded-restored"));
        let (restored, report) = archive
            .restore(config(kind, &restored_directory))
            .expect("restore explicitly excluded archive");
        assert_eq!(
            report.attempt_disposition,
            RelationalArchiveAttemptDisposition::ExcludedByPolicy
        );
        assert_eq!(report.excluded_attempt_count, 1);
        assert!(
            restored
                .resolve_attempt(attempt)
                .expect("resolve excluded attempt")
                .is_none()
        );
        restored.close().expect("close excluded restore");
        database.release(lease).expect("release current state");
        database.close().expect("close");
    }
}

#[test]
fn archive_transfers_attempt_records_and_remaps_target_commits() {
    let root = tempdir().expect("root");
    for (source_kind, source_label) in [
        (RelationalBackendKind::Temporary, "temporary"),
        (RelationalBackendKind::Seer, "seer"),
    ] {
        let source_directory = root.path().join(format!("{source_label}-source"));
        let mut source =
            RelationalDatabase::create(config(source_kind, &source_directory)).expect("create");
        source.create_table(table()).expect("table");
        let attempt = TransactionAttemptId::new([source_label.as_bytes()[0] + 10; 16]);
        let commit = match source
            .transaction_with_attempt(attempt, |database, transaction| {
                transaction.insert(database, ITEMS, row(1, "attempt"))?;
                Ok(())
            })
            .expect("attempt transaction")
        {
            TransactionAttemptOutcome::Applied { commit, .. } => commit,
            TransactionAttemptOutcome::AlreadyCommitted { .. } => {
                panic!("fresh attempt must not already be committed")
            }
        };
        let lease = source.retain(commit).expect("retain attempt state");
        let capture = source
            .capture_selected_snapshots(
                &[commit],
                RelationalSnapshotCaptureOptions::new(10).with_max_attempts(1),
            )
            .expect("capture attempt state");
        let archive = RelationalArchive::from_capture_with_attempt_policy(
            capture.clone(),
            RelationalArchiveMode::CurrentState,
            RelationalArchiveAttemptPolicy::Transfer,
        )
        .expect("transfer attempt archive");
        assert_eq!(
            archive.manifest.attempt_disposition,
            RelationalArchiveAttemptDisposition::Transferred
        );
        assert_eq!(archive.attempts, capture.attempts);
        let archive_path = source_directory.with_file_name(format!("{source_label}.archive"));
        archive.write(&archive_path).expect("write attempt archive");
        assert_eq!(
            RelationalArchive::read(&archive_path).expect("read attempt archive"),
            archive
        );

        let target_kind = match source_kind {
            RelationalBackendKind::Temporary => RelationalBackendKind::Seer,
            RelationalBackendKind::Seer => RelationalBackendKind::Temporary,
        };
        let target_directory = source_directory.with_file_name(format!("{source_label}-target"));
        let (restored, report) = archive
            .restore(config(target_kind, &target_directory))
            .expect("restore transferred attempt archive");
        assert_eq!(
            report.attempt_disposition,
            RelationalArchiveAttemptDisposition::Transferred
        );
        assert_eq!(report.attempt_mappings.len(), 1);
        let mapping = report.attempt_mappings[0];
        assert_eq!(mapping.source, capture.attempts[0]);
        assert_eq!(mapping.target.attempt, attempt);
        assert_eq!(mapping.target.digest, mapping.source.digest);
        assert_eq!(
            restored
                .resolve_attempt(attempt)
                .expect("resolve target attempt"),
            Some(mapping.target)
        );
        assert!(mapping.target.commit.0 > mapping.source.commit.0);
        restored.close().expect("close restored attempt archive");
        let later = source
            .insert(ITEMS, row(2, "later"))
            .expect("advance source");
        let incomplete = source
            .capture_selected_snapshots(
                &[later],
                RelationalSnapshotCaptureOptions::new(10).with_max_attempts(1),
            )
            .expect("capture incomplete attempt selection");
        assert!(matches!(
            RelationalArchive::from_capture_with_attempt_policy(
                incomplete,
                RelationalArchiveMode::CurrentState,
                RelationalArchiveAttemptPolicy::Transfer,
            ),
            Err(DbError::InvalidState(message))
                if message.contains("requires its source commit")
        ));
        source.release(lease).expect("release attempt state");
        source.close().expect("close source");
    }
}

#[test]
fn capture_bounds_durable_attempt_observation() {
    let root = tempdir().expect("root");
    let mut database = RelationalDatabase::create(config(
        RelationalBackendKind::Temporary,
        &root.path().join("temporary"),
    ))
    .expect("create");
    database.create_table(table()).expect("table");
    let attempt = TransactionAttemptId::new([7; 16]);
    let commit = match database
        .transaction_with_attempt(attempt, |database, transaction| {
            transaction.insert(database, ITEMS, row(1, "attempt"))?;
            Ok(())
        })
        .expect("attempt transaction")
    {
        TransactionAttemptOutcome::Applied { commit, .. } => commit,
        TransactionAttemptOutcome::AlreadyCommitted { .. } => {
            panic!("fresh attempt must not already be committed")
        }
    };
    let lease = database
        .retain(commit)
        .expect("retain current attempt state");
    assert!(matches!(
        database.capture_selected_snapshots(
            &[commit],
            RelationalSnapshotCaptureOptions::new(10).with_max_attempts(0),
        ),
        Err(DbError::SnapshotCaptureLimit {
            resource: "transaction attempts",
            limit: 0,
        })
    ));
    database.release(lease).expect("release current state");
    database.close().expect("close");
}

#[test]
fn logical_archives_round_trip_and_refuse_corruption_on_each_backend() {
    let root = tempdir().expect("root");
    exercise(
        RelationalBackendKind::Temporary,
        &root.path().join("temporary"),
    );
    exercise(RelationalBackendKind::Seer, &root.path().join("seer"));
}

#[test]
fn full_history_capture_and_restore_preserve_ordered_relational_boundaries() {
    let root = tempdir().expect("root");
    for (source_kind, source_label) in [
        (RelationalBackendKind::Temporary, "temporary"),
        (RelationalBackendKind::Seer, "seer"),
    ] {
        let source_directory = root.path().join(format!("{source_label}-source"));
        let target_kind = match source_kind {
            RelationalBackendKind::Temporary => RelationalBackendKind::Seer,
            RelationalBackendKind::Seer => RelationalBackendKind::Temporary,
        };
        let target_directory = root.path().join(format!("{source_label}-target"));
        let mut source =
            RelationalDatabase::create(config(source_kind, &source_directory)).expect("source");
        let mut leases = vec![source.retain(omendb::CommitId(0)).expect("retain root")];
        let table_commit = source.create_table(table()).expect("table");
        leases.push(source.retain(table_commit).expect("retain table"));
        let insert_commit = source.insert(ITEMS, row(1, "before")).expect("insert");
        leases.push(source.retain(insert_commit).expect("retain insert"));
        let update_commit = source.update(ITEMS, row(1, "after")).expect("update");
        leases.push(source.retain(update_commit).expect("retain update"));
        let unchanged_commit = source
            .update(ITEMS, row(1, "after"))
            .expect("unchanged update");
        leases.push(
            source
                .retain(unchanged_commit)
                .expect("retain unchanged update"),
        );

        let published = source.published_commit_ids().expect("published commits");
        assert_eq!(
            published,
            vec![
                omendb::CommitId(0),
                omendb::CommitId(1),
                omendb::CommitId(2),
                omendb::CommitId(3),
                omendb::CommitId(4),
            ]
        );
        let capture = source
            .capture_full_history(RelationalSnapshotCaptureOptions::new(32))
            .expect("capture full history");
        assert!(capture.complete_history);
        assert_eq!(
            capture
                .snapshots
                .iter()
                .map(|snapshot| snapshot.commit)
                .collect::<Vec<_>>(),
            published
        );
        let archive = RelationalArchive::from_capture(capture, RelationalArchiveMode::FullHistory)
            .expect("full-history archive");
        let (mut target, report) = archive
            .restore(config(target_kind, &target_directory))
            .expect("restore full history");
        assert_eq!(report.mode, RelationalArchiveMode::FullHistory);
        assert!(report.history_preserved);
        assert_eq!(
            report
                .mappings
                .iter()
                .map(|mapping| mapping.source)
                .collect::<Vec<_>>(),
            published
        );
        assert!(
            report.mappings[..4]
                .windows(2)
                .all(|pair| pair[0].target < pair[1].target)
        );
        assert_eq!(report.mappings[3].target, report.mappings[4].target);
        assert_eq!(
            target.published_commit_ids().expect("target history").len(),
            4
        );
        target.verify().expect("verify target");
        target.close().expect("close target");
        for lease in leases {
            source.release(lease).expect("release source history");
        }
        source.close().expect("close source");
        let reopened = RelationalDatabase::open(config(source_kind, &source_directory))
            .expect("reopen source");
        assert_eq!(
            reopened.published_commit_ids().expect("reopened history"),
            published
        );
        reopened.close().expect("close reopened source");
    }
}

#[test]
fn full_history_archive_handles_explicit_attempt_policies() {
    let root = tempdir().expect("root");
    for (source_kind, source_label) in [
        (RelationalBackendKind::Temporary, "temporary"),
        (RelationalBackendKind::Seer, "seer"),
    ] {
        let source_directory = root.path().join(format!("{source_label}-source"));
        let target_kind = match source_kind {
            RelationalBackendKind::Temporary => RelationalBackendKind::Seer,
            RelationalBackendKind::Seer => RelationalBackendKind::Temporary,
        };
        let target_directory = root.path().join(format!("{source_label}-target"));
        let mut source =
            RelationalDatabase::create(config(source_kind, &source_directory)).expect("source");
        let mut leases = vec![source.retain(omendb::CommitId(0)).expect("retain root")];
        let table_commit = source.create_table(table()).expect("table");
        leases.push(source.retain(table_commit).expect("retain table"));
        let insert_commit = source.insert(ITEMS, row(1, "before")).expect("insert");
        leases.push(source.retain(insert_commit).expect("retain insert"));

        let attempt = TransactionAttemptId::new([source_label.as_bytes()[0] + 20; 16]);
        let attempt_commit = match source
            .transaction_with_attempt(attempt, |database, transaction| {
                transaction.update(database, ITEMS, row(1, "attempt"))?;
                Ok(())
            })
            .expect("attempt transaction")
        {
            TransactionAttemptOutcome::Applied { commit, .. } => commit,
            TransactionAttemptOutcome::AlreadyCommitted { .. } => {
                panic!("fresh attempt must not already be committed")
            }
        };
        leases.push(source.retain(attempt_commit).expect("retain attempt"));

        let published = source.published_commit_ids().expect("published commits");
        let capture = source
            .capture_full_history(RelationalSnapshotCaptureOptions::new(32).with_max_attempts(1))
            .expect("capture full history with attempt");
        assert_eq!(capture.attempts.len(), 1);
        assert_eq!(capture.attempts[0].attempt, attempt);
        assert_eq!(capture.attempts[0].commit, attempt_commit);
        let transfer_capture = capture.clone();

        let archive = RelationalArchive::from_capture_with_attempt_policy(
            capture,
            RelationalArchiveMode::FullHistory,
            RelationalArchiveAttemptPolicy::ExcludeByPolicy,
        )
        .expect("full-history archive with explicit attempt exclusion");
        assert_eq!(
            archive.manifest.attempt_disposition,
            RelationalArchiveAttemptDisposition::ExcludedByPolicy
        );
        assert_eq!(archive.manifest.excluded_attempt_count, 1);
        assert!(archive.attempts.is_empty());

        let (mut target, report) = archive
            .restore(config(target_kind, &target_directory))
            .expect("restore full history with excluded attempt");
        assert_eq!(report.mode, RelationalArchiveMode::FullHistory);
        assert!(report.history_preserved);
        assert_eq!(
            report.attempt_disposition,
            RelationalArchiveAttemptDisposition::ExcludedByPolicy
        );
        assert_eq!(report.excluded_attempt_count, 1);
        assert!(report.attempt_mappings.is_empty());
        assert_eq!(
            report
                .mappings
                .iter()
                .map(|mapping| mapping.source)
                .collect::<Vec<_>>(),
            published
        );
        assert!(
            target
                .resolve_attempt(attempt)
                .expect("resolve excluded target attempt")
                .is_none()
        );
        target
            .verify()
            .expect("verify excluded full-history target");
        target.close().expect("close target");

        let mut reopened = RelationalDatabase::open(config(target_kind, &target_directory))
            .expect("reopen target");
        reopened.verify().expect("verify reopened target");
        assert!(
            reopened
                .resolve_attempt(attempt)
                .expect("resolve excluded attempt after reopen")
                .is_none()
        );
        reopened.close().expect("close reopened target");

        let transfer_archive = RelationalArchive::from_capture_with_attempt_policy(
            transfer_capture,
            RelationalArchiveMode::FullHistory,
            RelationalArchiveAttemptPolicy::Transfer,
        )
        .expect("full-history archive with transferred attempt");
        assert_eq!(
            transfer_archive.manifest.attempt_disposition,
            RelationalArchiveAttemptDisposition::Transferred
        );
        assert_eq!(transfer_archive.attempts.len(), 1);
        let transfer_directory = root.path().join(format!("{source_label}-transfer"));
        let (mut transferred, transfer_report) = transfer_archive
            .restore(config(target_kind, &transfer_directory))
            .expect("restore full history with transferred attempt");
        assert_eq!(transfer_report.mode, RelationalArchiveMode::FullHistory);
        assert!(transfer_report.history_preserved);
        assert_eq!(
            transfer_report.attempt_disposition,
            RelationalArchiveAttemptDisposition::Transferred
        );
        assert_eq!(transfer_report.attempt_mappings.len(), 1);
        assert_eq!(transfer_report.mappings.len(), published.len());
        let transfer_mapping = transfer_report.attempt_mappings[0];
        assert_eq!(transfer_mapping.source.attempt, attempt);
        assert_eq!(transfer_mapping.target.attempt, attempt);
        assert_eq!(
            transferred
                .resolve_attempt(attempt)
                .expect("resolve transferred attempt"),
            Some(transfer_mapping.target)
        );
        assert!(
            transferred.commit_id()
                > transfer_report
                    .mappings
                    .last()
                    .expect("history mapping")
                    .target,
            "control-plane attempt import must publish after relational history"
        );
        transferred.verify().expect("verify transferred target");
        transferred.close().expect("close transferred target");

        let mut reopened_transfer =
            RelationalDatabase::open(config(target_kind, &transfer_directory))
                .expect("reopen transferred target");
        reopened_transfer
            .verify()
            .expect("verify reopened transferred target");
        assert_eq!(
            reopened_transfer
                .resolve_attempt(attempt)
                .expect("resolve transferred attempt after reopen"),
            Some(transfer_mapping.target)
        );
        reopened_transfer
            .close()
            .expect("close reopened transferred target");

        for lease in leases {
            source.release(lease).expect("release source history");
        }
        source.close().expect("close source");
    }
}

#[test]
fn temporary_full_history_refuses_after_unprotected_reclamation() {
    let root = tempdir().expect("root");
    let mut database = RelationalDatabase::create(config(
        RelationalBackendKind::Temporary,
        &root.path().join("temporary"),
    ))
    .expect("create");
    database.create_table(table()).expect("table");
    database.insert(ITEMS, row(1, "0")).expect("seed");
    for value in 1..=70 {
        database
            .update(ITEMS, row(1, &value.to_string()))
            .expect("update");
    }
    let report = database.compact().expect("compact");
    assert!(report.row_fragments_reclaimed.unwrap_or(0) > 0);
    assert!(matches!(
        database.capture_full_history(RelationalSnapshotCaptureOptions::new(128)),
        Err(DbError::InvalidState(message))
            if message.contains("complete commit history")
    ));
    database.close().expect("close");
}

#[test]
fn seer_full_history_refuses_after_unretained_root_reuse() {
    let root = tempdir().expect("root");
    let mut database = RelationalDatabase::create(config(
        RelationalBackendKind::Seer,
        &root.path().join("seer"),
    ))
    .expect("create");
    database.create_table(table()).expect("table");
    database.insert(ITEMS, row(1, "before")).expect("insert");
    database.update(ITEMS, row(1, "after")).expect("update");
    assert!(matches!(
        database.capture_full_history(RelationalSnapshotCaptureOptions::new(128)),
        Err(DbError::StorageSnapshotUnavailable { .. })
    ));
    database.close().expect("close");
}

#[test]
fn restore_rebuilds_additive_catalog_transition() {
    let root = tempdir().expect("root");
    let source_directory = root.path().join("source");
    let target_directory = root.path().join("target");
    let mut source =
        RelationalDatabase::create(config(RelationalBackendKind::Temporary, &source_directory))
            .expect("create source");
    source.create_table(table()).expect("items table");
    let seed = source.insert(ITEMS, row(1, "before")).expect("seed");
    let lease = source.retain(seed).expect("retain seed");
    source.create_table(second_table()).expect("events table");
    let capture = source
        .capture_selected_snapshots(
            &[seed, source.commit_id()],
            RelationalSnapshotCaptureOptions::new(10),
        )
        .expect("capture");
    let archive =
        RelationalArchive::from_capture(capture, RelationalArchiveMode::RetainedSnapshots)
            .expect("archive");

    let (mut target, report) = archive
        .restore(config(RelationalBackendKind::Temporary, &target_directory))
        .expect("restore additive catalog transition");
    assert_eq!(report.mappings.len(), 2);
    assert_eq!(target.catalog(), &archive.snapshots[1].catalog);
    let historical_lease = target
        .retain(report.mappings[0].target)
        .expect("retain historical target catalog");
    assert_eq!(
        target
            .catalog_at(report.mappings[0].target)
            .expect("historical target catalog"),
        archive.snapshots[0].catalog
    );
    target
        .release(historical_lease)
        .expect("release historical target catalog");
    target.verify().expect("verify additive restore");
    target.close().expect("close target");
    assert!(target_directory.exists());
    source.release(lease).expect("release seed");
    source.close().expect("close source");
}

#[test]
fn restore_rebuilds_nullable_column_transition_across_backends() {
    let root = tempdir().expect("root");
    for (source_kind, source_label) in [
        (RelationalBackendKind::Temporary, "temporary"),
        (RelationalBackendKind::Seer, "seer"),
    ] {
        let target_kind = match source_kind {
            RelationalBackendKind::Temporary => RelationalBackendKind::Seer,
            RelationalBackendKind::Seer => RelationalBackendKind::Temporary,
        };
        let source_directory = root.path().join(format!("{source_label}-source"));
        let target_directory = root.path().join(format!("{source_label}-target"));
        let mut source =
            RelationalDatabase::create(config(source_kind, &source_directory)).expect("source");
        source.create_table(table()).expect("items table");
        let seed = source.insert(ITEMS, row(1, "before")).expect("seed");
        let lease = source.retain(seed).expect("retain seed");
        source
            .add_nullable_column(
                ITEMS,
                ColumnDefinition {
                    id: ColumnId(2),
                    name: "note".to_owned(),
                    data_type: ColumnType::Text,
                    nullable: true,
                },
            )
            .expect("add nullable column");
        let evolved = source
            .update(
                ITEMS,
                expanded_row(1, "after", Value::Text("memo".to_owned())),
            )
            .expect("write evolved row");
        let capture = source
            .capture_selected_snapshots(&[seed, evolved], RelationalSnapshotCaptureOptions::new(10))
            .expect("capture schema transition");
        let archive =
            RelationalArchive::from_capture(capture, RelationalArchiveMode::RetainedSnapshots)
                .expect("archive schema transition");
        let (mut target, report) = archive
            .restore(config(target_kind, &target_directory))
            .expect("restore schema transition");
        assert_eq!(report.mappings.len(), 2);
        assert_eq!(target.catalog(), &archive.snapshots[1].catalog);
        assert_eq!(
            target
                .get(ITEMS, report.mappings[1].target, Key::new(ITEMS.0, 1))
                .expect("current restored row"),
            Some(expanded_row(1, "after", Value::Text("memo".to_owned())))
        );
        let historical_lease = target
            .retain(report.mappings[0].target)
            .expect("retain historical restored schema");
        assert_eq!(
            target
                .catalog_at(report.mappings[0].target)
                .expect("historical restored catalog"),
            archive.snapshots[0].catalog
        );
        assert_eq!(
            target
                .get(ITEMS, report.mappings[0].target, Key::new(ITEMS.0, 1),)
                .expect("historical restored row"),
            Some(row(1, "before"))
        );
        target
            .release(historical_lease)
            .expect("release historical restored schema");
        target.verify().expect("verify restored transition");
        target.close().expect("close target");
        source.release(lease).expect("release seed");
        source.close().expect("close source");
    }
}

#[test]
fn restore_preserves_composite_identity_and_indexes_across_backends() {
    let root = tempdir().expect("root");
    for (source_kind, source_label) in [
        (RelationalBackendKind::Temporary, "temporary"),
        (RelationalBackendKind::Seer, "seer"),
    ] {
        let target_kind = match source_kind {
            RelationalBackendKind::Temporary => RelationalBackendKind::Seer,
            RelationalBackendKind::Seer => RelationalBackendKind::Temporary,
        };
        let source_directory = root.path().join(format!("{source_label}-composite-source"));
        let target_directory = root.path().join(format!("{source_label}-composite-target"));
        let mut source =
            RelationalDatabase::create(config(source_kind, &source_directory)).expect("source");
        source
            .create_table_with_schema_and_primary_key(
                parent_table(),
                Some(vec![ColumnId(1), ColumnId(2)]),
                parent_schema(),
            )
            .expect("parent schema");
        source
            .create_table_with_schema_and_primary_key(
                composite_table(),
                Some(vec![ColumnId(1), ColumnId(2)]),
                composite_schema(),
            )
            .expect("composite schema");
        source
            .insert(PARENTS, parent_row(1))
            .expect("parent composite row");
        source
            .insert(PARENTS, parent_row(2))
            .expect("second parent composite row");
        let seed = source
            .insert(LEDGER, composite_row(1, "open"))
            .expect("seed composite row");
        let seed_lease = source.retain(seed).expect("retain composite seed");
        source
            .insert(LEDGER, composite_row(2, "open"))
            .expect("insert second composite row");
        source
            .create_named_index(composite_entry_index(), "ledger_entry".to_owned())
            .expect("add second composite index");
        let head = source
            .update(LEDGER, composite_row(1, "closed"))
            .expect("update composite row");
        let head_lease = source.retain(head).expect("retain composite head");
        let capture = source
            .capture_selected_snapshots(&[head, seed], RelationalSnapshotCaptureOptions::new(10))
            .expect("capture composite snapshots");
        let archive = RelationalArchive::from_capture(
            capture.clone(),
            RelationalArchiveMode::RetainedSnapshots,
        )
        .expect("composite archive");
        assert_eq!(
            archive.snapshots[0].catalog.primary_key(LEDGER),
            Some([ColumnId(1), ColumnId(2)].as_slice())
        );
        assert_eq!(
            archive.snapshots[0].catalog.index_name(LEDGER_STATE_INDEX),
            Some("ledger_state")
        );
        assert_eq!(
            archive.snapshots[0]
                .catalog
                .foreign_key_name(LEDGER_PARENT_FK),
            Some("ledger_parent_fk")
        );
        assert_eq!(
            archive.snapshots[1].catalog.index_name(LEDGER_ENTRY_INDEX),
            Some("ledger_entry")
        );

        let (mut restored, report) = archive
            .restore(config(target_kind, &target_directory))
            .expect("restore composite archive");
        assert_eq!(restored.backend(), target_kind);
        assert_eq!(report.mappings.len(), 2);
        assert_eq!(
            restored.catalog().primary_key(LEDGER),
            Some([ColumnId(1), ColumnId(2)].as_slice())
        );
        assert_eq!(
            restored.catalog().index_name(LEDGER_STATE_INDEX),
            Some("ledger_state")
        );
        assert_eq!(
            restored.catalog().index_name(IndexId(21)),
            Some("ledger_entry")
        );
        assert_eq!(
            restored.catalog().foreign_key_name(LEDGER_PARENT_FK),
            Some("ledger_parent_fk")
        );
        assert_eq!(
            restored
                .index_get(
                    LEDGER,
                    report.mappings[1].target,
                    LEDGER_STATE_INDEX,
                    &[Value::Text("closed".to_owned())],
                )
                .expect("current composite index read"),
            archive.snapshots[1].tables[0]
                .rows
                .iter()
                .filter(|row| row.values[1] == Value::U64(1))
                .cloned()
                .collect::<Vec<_>>()
        );
        assert_eq!(
            restored
                .index_get(
                    LEDGER,
                    report.mappings[1].target,
                    IndexId(21),
                    &[Value::U64(1)],
                )
                .expect("current entry composite index read"),
            archive.snapshots[1].tables[0]
                .rows
                .iter()
                .filter(|row| row.values[1] == Value::U64(1))
                .cloned()
                .collect::<Vec<_>>()
        );
        assert_eq!(
            restored
                .index_get(
                    LEDGER,
                    report.mappings[1].target,
                    LEDGER_STATE_INDEX,
                    &[Value::Text("open".to_owned())],
                )
                .expect("current open composite index read"),
            archive.snapshots[1].tables[0]
                .rows
                .iter()
                .filter(|row| row.values[1] == Value::U64(2))
                .cloned()
                .collect::<Vec<_>>()
        );
        let historical_lease = restored
            .retain(report.mappings[0].target)
            .expect("retain restored composite seed");
        assert_eq!(
            restored
                .index_get(
                    LEDGER,
                    report.mappings[0].target,
                    LEDGER_STATE_INDEX,
                    &[Value::Text("open".to_owned())],
                )
                .expect("historical composite index read"),
            archive.snapshots[0].tables[0]
                .rows
                .iter()
                .filter(|row| row.values[1] == Value::U64(1))
                .cloned()
                .collect::<Vec<_>>()
        );
        restored
            .release(historical_lease)
            .expect("release restored composite seed");
        restored
            .verify()
            .expect("verify restored composite archive");
        restored.close().expect("close restored composite archive");
        source.release(seed_lease).expect("release source seed");
        source.release(head_lease).expect("release source head");
        source.close().expect("close source composite archive");
    }
}
