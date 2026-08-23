use std::path::Path;

use omendb::{
    ColumnDefinition, ColumnId, ColumnType, DatabaseConfig, Key, OperationControl,
    RelationalBackendConfig, RelationalBackendKind, RelationalDatabase, RelationalDiagnosticCode,
    RelationalDiagnosticComponent, RelationalDiagnosticSeverity, SeerKernelConfig, TableDefinition,
    TableId, Value,
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
    let database_config = config(kind, directory);
    let mut database = RelationalDatabase::create(database_config.clone()).expect("create");
    let identity = database.storage_identity().expect("identity");

    let initial = database.diagnose().expect("initial diagnosis");
    assert_eq!(initial.backend, kind);
    assert_eq!(initial.identity, identity);
    assert_eq!(initial.status.commit, database.commit_id());
    assert_eq!(initial.metrics.commit, database.commit_id());
    assert_eq!(
        initial.metrics.publication.is_some(),
        kind == RelationalBackendKind::Seer
    );
    assert!(!initial.has_errors());
    assert_eq!(initial.findings.len(), 1);
    let ready = initial.findings[0];
    assert_eq!(ready.code, RelationalDiagnosticCode::Ready);
    assert_eq!(ready.component, RelationalDiagnosticComponent::Lifecycle);
    assert_eq!(ready.severity, RelationalDiagnosticSeverity::Info);
    assert_eq!(ready.value, None);
    assert_eq!(ready.message(), ready.code.message());
    assert_eq!(ready.recommended_action(), "no action");

    database.create_table(table()).expect("table");
    let commit = database.insert(TABLE, row(1, "value")).expect("row");
    let after_metrics = database.metrics().expect("metrics after write");
    assert_eq!(after_metrics.commit, commit);
    if kind == RelationalBackendKind::Seer {
        let before_publication = initial
            .metrics
            .publication
            .expect("seer publication metrics before write");
        let after_publication = after_metrics
            .publication
            .expect("seer publication metrics after write");
        assert!(after_publication.wal_bytes_written >= before_publication.wal_bytes_written);
        assert!(after_publication.data_bytes_written >= before_publication.data_bytes_written);
        assert!(
            after_publication.manifest_bytes_written >= before_publication.manifest_bytes_written
        );
        assert!(after_publication.wal_write_ns >= before_publication.wal_write_ns);
    }
    let lease = database.retain(commit).expect("retain");
    let retained = database.diagnose().expect("retained diagnosis");
    assert_eq!(retained.identity, identity);
    assert_eq!(retained.status.commit, commit);
    assert_eq!(retained.metrics.commit, commit);
    assert!(!retained.has_errors());
    assert!(retained.findings.iter().any(|finding| {
        finding.code == RelationalDiagnosticCode::RetainedSnapshots
            && finding.component == RelationalDiagnosticComponent::Retention
            && finding.severity == RelationalDiagnosticSeverity::Info
            && finding.value == Some(1)
    }));
    assert_eq!(database.commit_id(), commit);
    database.release(lease).expect("release");
    database.close().expect("close");

    let reopened = RelationalDatabase::open(database_config).expect("reopen");
    let reopened_diagnostic = reopened.diagnose().expect("reopened diagnosis");
    assert_eq!(reopened_diagnostic.backend, kind);
    assert_eq!(reopened_diagnostic.identity, identity);
    assert_eq!(reopened_diagnostic.status.commit, commit);
    assert_eq!(reopened_diagnostic.metrics.commit, commit);
    assert!(!reopened_diagnostic.has_errors());
    assert!(
        !reopened_diagnostic
            .findings
            .iter()
            .any(|finding| finding.code == RelationalDiagnosticCode::RetainedSnapshots)
    );
    reopened.close().expect("reopened close");
}

#[test]
fn project_diagnostics_are_backend_neutral_and_non_mutating() {
    let temporary = tempdir().expect("temporary directory");
    exercise(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    );

    let seer = tempdir().expect("seer directory");
    exercise(RelationalBackendKind::Seer, &seer.path().join("seer"));
}

#[test]
fn session_forwards_diagnostics_under_operation_control() {
    let temporary = tempdir().expect("temporary directory");
    let database = RelationalDatabase::create(config(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    ))
    .expect("create");
    let session = database
        .into_session(omendb::RelationalSessionConfig::default())
        .expect("session");
    let report = session
        .diagnose(&OperationControl::default())
        .expect("session diagnosis");
    assert_eq!(report.backend, RelationalBackendKind::Temporary);
    assert!(!report.has_errors());
    session.close().expect("close");
}
