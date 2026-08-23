use std::path::Path;
use std::time::Duration;

use omendb::{
    DatabaseConfig, RelationalBackendConfig, RelationalBackendKind, RelationalCapability,
    RelationalCapabilityState, RelationalDatabase, RelationalDatabaseConfig,
    RelationalDatabaseSession, RelationalSessionConfig, SeerKernelConfig,
};
use tempfile::tempdir;

fn backend_config(kind: RelationalBackendKind, directory: &Path) -> RelationalBackendConfig {
    match kind {
        RelationalBackendKind::Temporary => RelationalBackendConfig::Temporary(DatabaseConfig {
            directory: directory.to_owned(),
        }),
        RelationalBackendKind::Seer => {
            RelationalBackendConfig::Seer(SeerKernelConfig::new(directory.to_owned()))
        }
    }
}

fn exercise(kind: RelationalBackendKind, directory: &Path) {
    let direct_directory = directory.join("direct");
    let session_directory = directory.join("session");
    let direct =
        RelationalDatabase::create(backend_config(kind, &direct_directory)).expect("create");
    let direct_report = direct.capabilities();
    assert_eq!(direct_report.backend, kind);
    assert_eq!(
        direct_report.capabilities.len(),
        RelationalCapability::all().len()
    );
    assert!(direct_report.supports(RelationalCapability::TypedRelational));
    assert_eq!(
        direct_report.state(RelationalCapability::FixedSnapshotSerializedWriter),
        RelationalCapabilityState::Bounded
    );
    assert_eq!(
        direct_report.state(RelationalCapability::Sql),
        RelationalCapabilityState::Bounded
    );
    assert_eq!(
        direct_report
            .capabilities
            .iter()
            .find(|info| info.capability == RelationalCapability::Sql)
            .expect("SQL capability")
            .explanation,
        "bounded embedded SQL translates into the typed catalog and transaction facade"
    );
    assert_eq!(
        direct_report.state(RelationalCapability::CurrentStateMigration),
        match kind {
            RelationalBackendKind::Temporary => RelationalCapabilityState::Bounded,
            RelationalBackendKind::Seer => RelationalCapabilityState::Unsupported,
        }
    );
    direct.close().expect("close direct database");

    let config = RelationalDatabaseConfig::new(backend_config(kind, &session_directory))
        .with_session_config(RelationalSessionConfig {
            max_in_flight: 3,
            admission_timeout: Duration::from_secs(1),
        });
    let session = RelationalDatabaseSession::create(config).expect("create session");
    assert_eq!(
        session
            .admission_status()
            .expect("session status")
            .max_in_flight,
        3
    );
    let report = session
        .capabilities(&omendb::OperationControl::default())
        .expect("session capabilities");
    assert_eq!(report.backend, kind);
    assert!(report.supports(RelationalCapability::WaitableSessionAdmission));
    // Parallel writers are bounded: admitted only through the explicit
    // validated parallel-preparation API, not the default write path.
    assert_eq!(
        report.state(RelationalCapability::ParallelWriters),
        RelationalCapabilityState::Bounded
    );
    session.close().expect("close session");

    let reopened = RelationalDatabaseSession::open(RelationalDatabaseConfig::new(backend_config(
        kind,
        &session_directory,
    )))
    .expect("open session");
    reopened.close().expect("close reopened session");
}

#[test]
fn project_capabilities_and_common_session_config_are_backend_neutral() {
    let temporary = tempdir().expect("temporary directory");
    exercise(
        RelationalBackendKind::Temporary,
        &temporary.path().join("temporary"),
    );

    let seer = tempdir().expect("seer directory");
    exercise(RelationalBackendKind::Seer, &seer.path().join("seer"));
}
