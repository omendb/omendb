use std::path::Path;
use std::time::Duration;

use omendb::{
    RelationalBackendConfig, RelationalCapability, RelationalCapabilityState, RelationalDatabase,
    RelationalDatabaseConfig, RelationalDatabaseSession, RelationalSessionConfig,
};
use tempfile::tempdir;

fn backend_config(directory: &Path) -> RelationalBackendConfig {
    RelationalBackendConfig::new(directory.to_owned())
}

fn exercise(directory: &Path) {
    let direct_directory = directory.join("direct");
    let session_directory = directory.join("session");
    let direct = RelationalDatabase::create(backend_config(&direct_directory)).expect("create");
    let direct_report = direct.capabilities();
    assert_eq!(
        direct_report.capabilities.len(),
        RelationalCapability::all().len()
    );
    assert!(direct_report.supports(RelationalCapability::TypedRelational));
    assert_eq!(
        direct_report.state(RelationalCapability::FixedSnapshotSerializedWriter),
        RelationalCapabilityState::Supported
    );
    assert_eq!(
        direct_report.state(RelationalCapability::Sql),
        RelationalCapabilityState::Supported
    );
    direct.close().expect("close direct database");

    let config = RelationalDatabaseConfig::new(backend_config(&session_directory))
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
    assert!(report.supports(RelationalCapability::WaitableSessionAdmission));
    session.close().expect("close session");

    let reopened = RelationalDatabaseSession::open(RelationalDatabaseConfig::new(backend_config(
        &session_directory,
    )))
    .expect("open session");
    reopened.close().expect("close reopened session");
}

#[test]
fn project_capabilities_and_common_session_config_are_backend_neutral() {
    let temporary = tempdir().expect("temporary directory");
    exercise(&temporary.path().join("temporary"));
}
