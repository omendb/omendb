use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DatabaseConfig, DbError, Key, OperationControl,
    RELATIONAL_SUPPORT_BUNDLE_VERSION, RelationalBackendConfig, RelationalBackendKind,
    RelationalDatabase, RelationalEventKind, RelationalSessionConfig, RelationalSessionEventKind,
    RelationalSessionOperationKind, SeerKernelConfig, TableDefinition, TableId, Value,
};
use tempfile::tempdir;

const TABLE: TableId = TableId(7);

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
        name: "items".to_owned(),
        columns: vec![ColumnDefinition {
            id: ColumnId(1),
            name: "value".to_owned(),
            data_type: ColumnType::Text,
            nullable: false,
        }],
    }
}

fn row(id: u64, value: &str) -> omendb::Row {
    omendb::Row {
        primary: Key::new(TABLE.0, id),
        values: vec![Value::Text(value.to_owned())],
    }
}

fn exercise(kind: RelationalBackendKind, directory: &Path) {
    let config = config(kind, directory);
    let mut database = RelationalDatabase::create(config.clone()).expect("create");
    let initial = database.support_bundle().expect("initial support bundle");
    assert_eq!(initial.version, RELATIONAL_SUPPORT_BUNDLE_VERSION);
    assert_eq!(initial.diagnostic.backend, kind);
    assert_eq!(initial.capabilities.backend, kind);
    assert!(initial.events.events.is_empty());
    assert_eq!(initial.events.dropped, 0);
    assert!(initial.session_events.events.is_empty());
    assert_eq!(initial.session_events.dropped, 0);

    database.create_table(table()).expect("table");
    let commit = database
        .insert(TABLE, row(1, "redacted-value"))
        .expect("row");
    let lease = database.retain(commit).expect("retain");
    database.checkpoint().expect("checkpoint");
    database.compact().expect("compact");
    database.verify().expect("verify");
    database.release(lease).expect("release");

    let bundle = database.support_bundle().expect("support bundle");
    assert_eq!(bundle.version, RELATIONAL_SUPPORT_BUNDLE_VERSION);
    assert_eq!(
        bundle.diagnostic.identity,
        database.storage_identity().expect("identity")
    );
    assert_eq!(bundle.diagnostic.status.commit, commit);
    assert_eq!(bundle.diagnostic.metrics.commit, commit);
    assert!(!bundle.events.is_truncated());
    let kinds: Vec<_> = bundle
        .events
        .events
        .iter()
        .map(|event| event.kind)
        .collect();
    assert!(kinds.contains(&RelationalEventKind::CommitAcknowledged));
    assert!(kinds.contains(&RelationalEventKind::SnapshotRetained));
    assert!(kinds.contains(&RelationalEventKind::SnapshotReleased));
    assert!(kinds.contains(&RelationalEventKind::CheckpointCompleted));
    assert!(kinds.contains(&RelationalEventKind::CompactionCompleted));
    assert!(kinds.contains(&RelationalEventKind::VerificationCompleted));
    assert!(
        bundle
            .events
            .events
            .windows(2)
            .all(|events| events[0].sequence < events[1].sequence)
    );

    let debug = format!("{bundle:?}");
    assert!(!debug.contains("redacted-value"));
    assert!(!debug.contains(directory.to_string_lossy().as_ref()));
    database.close().expect("close");

    let session = RelationalDatabase::open(config)
        .expect("reopen")
        .into_session(RelationalSessionConfig::default())
        .expect("session");
    let cancelled = OperationControl::default();
    cancelled.cancellation_token().cancel();
    assert!(matches!(
        session.commit_id(&cancelled),
        Err(DbError::Cancelled)
    ));
    let session_bundle = session
        .support_bundle(&OperationControl::default())
        .expect("session support bundle");
    assert_eq!(session_bundle.diagnostic.backend, kind);
    assert_eq!(session_bundle.diagnostic.status.commit, commit);
    assert!(session_bundle.events.events.is_empty());
    assert!(!session_bundle.session_events.events.is_empty());
    assert!(!session_bundle.session_events.is_truncated());
    assert!(
        session_bundle
            .session_events
            .events
            .iter()
            .any(|event| event.kind == RelationalSessionEventKind::CancellationObserved)
    );
    assert!(session_bundle.session_events.events.iter().any(|event| {
        event.kind == RelationalSessionEventKind::OperationCompleted
            && event.operation == RelationalSessionOperationKind::Read
    }));
    assert!(
        session_bundle
            .session_events
            .events
            .windows(2)
            .all(|events| events[0].sequence < events[1].sequence)
    );
    session.close().expect("session close");
}

#[test]
fn redacted_support_bundle_is_backend_neutral() {
    let temporary = tempdir().expect("temporary directory");
    exercise(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    );

    let seer = tempdir().expect("seer directory");
    exercise(RelationalBackendKind::Seer, &seer.path().join("seer"));
}
