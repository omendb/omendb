use std::fs;
use std::process::Command;

#[cfg(feature = "seerdb-fault-injection")]
use omendb::FaultPoint;
use omendb::{
    ColumnDefinition, ColumnId, ColumnType, CommitId, DatabaseConfig, DbError, IndexDefinition,
    IndexId, Key, KvMutation, RelationalBackendConfig, RelationalDatabase, RelationalMutation, Row,
    SeerKernel, SeerKernelConfig, SeerRelationalStore, TableDefinition, TableId, Value,
};
use tempfile::tempdir;

fn seed() -> (tempfile::TempDir, SeerKernelConfig, CommitId) {
    let directory = tempdir().expect("temporary directory");
    let config = SeerKernelConfig::new(directory.path().join("seerdb"));
    let mut kernel = SeerKernel::create(&config).expect("create SeerKernel");
    let outcome = kernel
        .commit(
            CommitId(0),
            &[KvMutation::Put {
                key: b"external/key".to_vec(),
                value: b"durable-value".to_vec(),
            }],
        )
        .expect("seed durable value");
    kernel.checkpoint().expect("verified checkpoint");
    drop(kernel);
    (directory, config, outcome.commit)
}

fn corrupt(path: &std::path::Path) {
    let mut bytes = fs::read(path).expect("read durable artifact");
    assert!(!bytes.is_empty(), "artifact must not be empty");
    bytes[0] ^= 0xA5;
    fs::write(path, bytes).expect("write corrupted durable artifact");
}

#[cfg(feature = "seerdb-fault-injection")]
fn typed_table() -> TableDefinition {
    TableDefinition {
        id: TableId(7),
        name: "users".to_owned(),
        columns: vec![
            ColumnDefinition {
                id: ColumnId(1),
                name: "email".to_owned(),
                data_type: ColumnType::Text,
                nullable: false,
            },
            ColumnDefinition {
                id: ColumnId(2),
                name: "score".to_owned(),
                data_type: ColumnType::U64,
                nullable: false,
            },
        ],
    }
}

#[cfg(feature = "seerdb-fault-injection")]
fn typed_index() -> IndexDefinition {
    IndexDefinition {
        id: IndexId(9),
        table: TableId(7),
        columns: vec![ColumnId(1)],
        unique: false,
    }
}

#[cfg(feature = "seerdb-fault-injection")]
fn typed_row(email: &str) -> Row {
    Row {
        primary: Key::new(7, 1),
        values: vec![Value::Text(email.to_owned()), Value::U64(42)],
    }
}

#[test]
fn seerdb_checkpoint_corruption_is_refused_by_public_adapter() {
    let (_directory, config, _commit) = seed();
    // The append-only meta log is the metadata authority: every publication
    // frame carries the full manifest, so corrupting any frame must refuse
    // the open closed (the former MANIFEST slots file no longer exists).
    corrupt(&config.directory.join("seerdb.meta.log"));

    let result = SeerKernel::open(&config);
    assert!(matches!(result, Err(DbError::StorageCorruption { .. })));
}

#[test]
fn seerdb_page_corruption_is_typed_at_public_read_boundary() {
    let (_directory, config, commit) = seed();
    corrupt(&config.directory.join("seerdb.data"));

    let kernel = SeerKernel::open(&config).expect("lazy open before page read");
    let result = kernel.get(commit, b"external/key");
    assert!(matches!(result, Err(DbError::StorageCorruption { .. })));
}

#[test]
fn seerdb_archive_corruption_is_refused_before_restore_destination_creation() {
    let (_source_directory, source_config, _commit) = seed();
    let archive_parent = tempdir().expect("archive parent");
    let archive = archive_parent.path().join("archive");
    let mut source = SeerKernel::open(&source_config).expect("open source");
    source.snapshot(&archive).expect("create verified archive");
    drop(source);

    corrupt(&archive.join("seerdb.data"));
    let destination_parent = tempdir().expect("destination parent");
    let destination = destination_parent.path().join("restored");
    let result = SeerKernel::restore(&SeerKernelConfig::new(destination.clone()), &archive);
    assert!(matches!(result, Err(DbError::StorageCorruption { .. })));
    assert!(
        !destination.exists(),
        "failed restore created a destination"
    );
}

#[test]
fn temporary_writer_lock_releases_after_process_termination() {
    if let Some(path) = std::env::var_os("OMENDB_TEMPORARY_LOCK_CRASH_PATH") {
        let _database =
            RelationalDatabase::create(RelationalBackendConfig::Temporary(DatabaseConfig {
                directory: path.into(),
            }))
            .expect("create temporary crash child");
        std::process::exit(137);
    }

    let root = tempdir().expect("temporary directory");
    let path = root.path().join("temporary-lock.db");
    let status = Command::new(std::env::current_exe().expect("test executable"))
        .arg("--exact")
        .arg("temporary_writer_lock_releases_after_process_termination")
        .arg("--nocapture")
        .env("OMENDB_TEMPORARY_LOCK_CRASH_PATH", &path)
        .status()
        .expect("run lock child");
    assert!(!status.success(), "lock child exited cleanly");

    RelationalDatabase::open(RelationalBackendConfig::Temporary(DatabaseConfig {
        directory: path,
    }))
    .expect("open after process termination");
}

#[cfg(feature = "seerdb-fault-injection")]
#[test]
fn seerdb_public_adapter_recovers_after_process_termination_faults() {
    if let Some(path) = std::env::var_os("SEERDB_OMENDB_CRASH_PATH") {
        let config = SeerKernelConfig::new(path.into());
        let fault = std::env::var("SEERDB_OMENDB_CRASH_FAULT").expect("fault name");
        let kernel = SeerKernel::create(&config).expect("create crash child");
        kernel
            .commit(
                CommitId(0),
                &[KvMutation::Put {
                    key: b"external/key".to_vec(),
                    value: b"before".to_vec(),
                }],
            )
            .expect("seed crash child");
        let point = match fault.as_str() {
            "before-wal" => FaultPoint::BeforeWalAppend,
            "after-wal" => FaultPoint::AfterWalAppend,
            "wal-sync" => FaultPoint::WalSync,
            "after-wal-sync" => FaultPoint::AfterWalSync,
            "data-sync" => FaultPoint::DataSync,
            "packed-page-sync" => FaultPoint::PackedPageSync,
            "manifest-mirror-sync" => FaultPoint::ManifestMirrorSync,
            "manifest-sync" => FaultPoint::ManifestSync,
            "short-write" => FaultPoint::ShortWrite,
            "torn-write" => FaultPoint::TornWrite,
            "after-manifest" => FaultPoint::AfterManifestPublish,
            _ => panic!("unknown fault {fault}"),
        };
        kernel.inject_fault(point).expect("arm crash fault");
        let _ = kernel.commit(
            CommitId(1),
            &[KvMutation::Put {
                key: b"external/key".to_vec(),
                value: b"after".to_vec(),
            }],
        );
        std::process::exit(137);
    }

    let cases = [
        ("before-wal", FaultPoint::BeforeWalAppend, false),
        ("after-wal", FaultPoint::AfterWalAppend, false),
        // The adapter's default Options defer the mutation-prefix sync. These
        // seams therefore fire while the complete commit envelope is being
        // forced; recovery may safely publish either old or complete new.
        ("wal-sync", FaultPoint::WalSync, false),
        ("after-wal-sync", FaultPoint::AfterWalSync, false),
        ("data-sync", FaultPoint::DataSync, false),
        ("packed-page-sync", FaultPoint::PackedPageSync, false),
        (
            "manifest-mirror-sync",
            FaultPoint::ManifestMirrorSync,
            false,
        ),
        ("manifest-sync", FaultPoint::ManifestSync, false),
        ("short-write", FaultPoint::ShortWrite, false),
        ("torn-write", FaultPoint::TornWrite, false),
        ("after-manifest", FaultPoint::AfterManifestPublish, true),
    ];
    let root = tempdir().expect("temporary directory");
    for (name, _point, expect_new) in cases {
        let path = root.path().join(format!("process-crash-{name}.db"));
        let status = Command::new(std::env::current_exe().expect("test executable"))
            .arg("--exact")
            .arg("seerdb_public_adapter_recovers_after_process_termination_faults")
            .arg("--nocapture")
            .env("SEERDB_OMENDB_CRASH_PATH", &path)
            .env("SEERDB_OMENDB_CRASH_FAULT", name)
            .status()
            .expect("run crash child");
        assert!(!status.success(), "crash child exited cleanly for {name}");

        let mut recovered = SeerKernel::open(&SeerKernelConfig::new(path))
            .unwrap_or_else(|error| panic!("reopen after {name}: {error}"));
        let recovered_commit = recovered.commit_id();
        assert!(
            recovered_commit == CommitId(1) || recovered_commit == CommitId(2),
            "fault {name} recovered an unexpected commit {recovered_commit:?}"
        );
        if expect_new {
            assert_eq!(recovered_commit, CommitId(2), "fault {name}");
        }
        // Some publication seams are conditional (for example, the reuse
        // boundary only fires when a generation reuses physical slots),
        // and the default adapter options defer the mutation-prefix sync.
        // The cross-repo contract is therefore old-or-complete-new, never a
        // partial batch; native feature-gated tests cover seam activation.
        let expected_value: &[u8] = if recovered_commit == CommitId(2) {
            b"after"
        } else {
            b"before"
        };
        assert_eq!(
            recovered
                .get(recovered_commit, b"external/key")
                .expect("recovered value"),
            Some(expected_value.to_vec()),
            "fault {name} exposed a partial or unexpected value"
        );
        recovered.verify().expect("recovered database verifies");
    }
}

#[cfg(feature = "seerdb-fault-injection")]
#[test]
fn seerdb_typed_adapter_recovers_after_process_termination_faults() {
    if let Some(path) = std::env::var_os("SEERDB_OMENDB_TYPED_CRASH_PATH") {
        let config = SeerKernelConfig::new(path.into());
        let fault = std::env::var("SEERDB_OMENDB_TYPED_CRASH_FAULT").expect("fault name");
        let point = match fault.as_str() {
            "before-wal" => FaultPoint::BeforeWalAppend,
            "after-wal" => FaultPoint::AfterWalAppend,
            "wal-sync" => FaultPoint::WalSync,
            "after-wal-sync" => FaultPoint::AfterWalSync,
            "data-sync" => FaultPoint::DataSync,
            "packed-page-sync" => FaultPoint::PackedPageSync,
            "manifest-sync" => FaultPoint::ManifestSync,
            "short-write" => FaultPoint::ShortWrite,
            "torn-write" => FaultPoint::TornWrite,
            "after-manifest" => FaultPoint::AfterManifestPublish,
            _ => panic!("unknown fault {fault}"),
        };
        let mut store = SeerRelationalStore::create(config).expect("create typed crash child");
        store
            .create_table(typed_table())
            .expect("create typed table");
        store
            .create_index(typed_index())
            .expect("create typed index");
        store
            .commit_batch([RelationalMutation::Insert {
                table: TableId(7),
                row: typed_row("before@example.com"),
            }])
            .expect("seed typed crash child");
        assert_eq!(store.commit_id(), CommitId(3));
        store.inject_fault(point).expect("arm typed crash fault");
        let _ = store.commit_batch([RelationalMutation::Update {
            table: TableId(7),
            row: typed_row("after@example.com"),
        }]);
        std::process::exit(137);
    }

    let cases = [
        "before-wal",
        "after-wal",
        "wal-sync",
        "after-wal-sync",
        "data-sync",
        "packed-page-sync",
        "manifest-sync",
        "short-write",
        "torn-write",
        "after-manifest",
    ];
    let root = tempdir().expect("temporary directory");
    for fault in cases {
        let path = root.path().join(format!("typed-process-crash-{fault}.db"));
        let status = Command::new(std::env::current_exe().expect("test executable"))
            .arg("--exact")
            .arg("seerdb_typed_adapter_recovers_after_process_termination_faults")
            .arg("--nocapture")
            .env("SEERDB_OMENDB_TYPED_CRASH_PATH", &path)
            .env("SEERDB_OMENDB_TYPED_CRASH_FAULT", fault)
            .status()
            .expect("run typed crash child");
        assert!(
            !status.success(),
            "typed crash child exited cleanly for {fault}"
        );

        let mut recovered = SeerRelationalStore::open(SeerKernelConfig::new(path))
            .unwrap_or_else(|error| panic!("typed reopen after {fault}: {error}"));
        let commit = recovered.commit_id();
        assert!(
            commit == CommitId(3) || commit == CommitId(4),
            "typed fault {fault} recovered unexpected commit {commit:?}"
        );
        let updated = commit == CommitId(4);
        let expected_email = if updated {
            "after@example.com"
        } else {
            "before@example.com"
        };
        assert_eq!(
            recovered
                .get(TableId(7), commit, Key::new(7, 1))
                .expect("typed recovered row"),
            Some(typed_row(expected_email)),
            "typed fault {fault} exposed a partial row"
        );
        assert!(
            !recovered
                .index_get(
                    TableId(7),
                    commit,
                    IndexId(9),
                    &[Value::Text(expected_email.to_owned())],
                )
                .expect("typed recovered index")
                .is_empty(),
            "typed fault {fault} exposed an inconsistent index"
        );
        recovered
            .verify()
            .expect("typed recovered database verifies");
        recovered.close().expect("close typed recovered database");
    }
}

#[cfg(feature = "seerdb-fault-injection")]
#[test]
fn seerdb_compaction_recovers_after_process_termination() {
    if let Some(path) = std::env::var_os("SEERDB_OMENDB_COMPACTION_CRASH_PATH") {
        let config = SeerKernelConfig::new(path.into());
        let mut kernel = SeerKernel::create(&config).expect("create compaction child");
        for commit in 1..=8 {
            kernel
                .commit(
                    CommitId(commit - 1),
                    &[KvMutation::Put {
                        key: b"compaction/key".to_vec(),
                        value: format!("value-{commit}").into_bytes(),
                    }],
                )
                .expect("seed compaction child");
        }
        kernel
            .inject_fault(FaultPoint::AfterManifestPublish)
            .expect("arm compaction crash fault");
        let _ = kernel.compact();
        std::process::exit(137);
    }

    let root = tempdir().expect("temporary directory");
    let path = root.path().join("compaction-process-crash");
    let status = Command::new(std::env::current_exe().expect("test executable"))
        .arg("--exact")
        .arg("seerdb_compaction_recovers_after_process_termination")
        .arg("--nocapture")
        .env("SEERDB_OMENDB_COMPACTION_CRASH_PATH", &path)
        .status()
        .expect("run compaction crash child");
    assert!(!status.success(), "compaction crash child exited cleanly");

    let mut recovered = SeerKernel::open(&SeerKernelConfig::new(path))
        .expect("reopen after compaction process termination");
    assert_eq!(recovered.commit_id(), CommitId(8));
    assert_eq!(
        recovered
            .get(CommitId(8), b"compaction/key")
            .expect("recovered compaction value"),
        Some(b"value-8".to_vec())
    );
    recovered.verify().expect("recovered compaction verifies");
}
