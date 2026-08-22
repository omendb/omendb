#![cfg(feature = "fault-injection")]
#![allow(clippy::disallowed_methods)]

//! Deterministic publication-order model for one durable root generation.
//!
//! This is deliberately a test model, not a second filesystem or recovery
//! implementation. It takes artifact images produced by the real publication
//! path, materializes durable prefixes in a temporary directory, and lets the
//! normal `DB::open`/`verify`/historical-read paths decide whether each state is
//! acceptable. The partial-artifact schedule models bounded write/resource
//! budgets; it does not replace real ENOSPC or block-layer fault injection. A
//! Linux power-loss harness still has to validate these assumptions against
//! ext4/XFS and real cache/barrier behavior.

use seerdb::storage::format::{CommitId, SnapshotId};
use seerdb::{BatchMutation, DB, Error, Options};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::{TempDir, tempdir};

const DATA_FILE: &str = "seerdb.data";
const BLOB_FILE: &str = "seerdb.blob";
const WAL_FILE: &str = "seerdb.wal";
const META_FILE: &str = "seerdb.meta";
const META_LOG_FILE_NAME: &str = "seerdb.meta.log";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Artifact {
    Data,
    Checkpoints,
    Blob,
    Wal,
    AuthorityFrame,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExpectedActive {
    Old,
    New,
    OldOrNew,
}

#[derive(Clone, Copy, Debug)]
struct PublicationStep {
    name: &'static str,
    artifacts: &'static [Artifact],
    expected: ExpectedActive,
}

const PUBLICATION_SCHEDULE: &[PublicationStep] = &[
    PublicationStep {
        name: "old-generation",
        artifacts: &[],
        expected: ExpectedActive::Old,
    },
    PublicationStep {
        name: "data-pages",
        artifacts: &[Artifact::Data],
        expected: ExpectedActive::Old,
    },
    // The authority log is append-only: a crash during the frame append
    // leaves either the previous frames alone or a torn tail behind them,
    // so every prefix up to the durable frame reopens the old root.
    PublicationStep {
        name: "frame-append-start",
        artifacts: &[Artifact::Data, Artifact::Checkpoints],
        expected: ExpectedActive::Old,
    },
    PublicationStep {
        name: "blob-image",
        artifacts: &[Artifact::Data, Artifact::Checkpoints, Artifact::Blob],
        expected: ExpectedActive::Old,
    },
    // The WAL commit record is durable but the frame is not: recovery may
    // discard the uncommitted suffix or replay it into a complete new
    // generation.
    PublicationStep {
        name: "commit-wal",
        artifacts: &[
            Artifact::Data,
            Artifact::Checkpoints,
            Artifact::Blob,
            Artifact::Wal,
        ],
        expected: ExpectedActive::OldOrNew,
    },
    PublicationStep {
        name: "authority-frame",
        artifacts: &[
            Artifact::Data,
            Artifact::Checkpoints,
            Artifact::Blob,
            Artifact::Wal,
            Artifact::AuthorityFrame,
        ],
        expected: ExpectedActive::New,
    },
    PublicationStep {
        name: "wal-retired",
        artifacts: &[
            Artifact::Data,
            Artifact::Checkpoints,
            Artifact::Blob,
            Artifact::AuthorityFrame,
        ],
        expected: ExpectedActive::New,
    },
];

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if source_path.is_dir() {
            copy_tree(&source_path, &destination_path);
        } else {
            fs::copy(source_path, destination_path).unwrap();
        }
    }
}

fn artifact_names(path: &Path, artifact: Artifact) -> Vec<PathBuf> {
    let mut names = Vec::new();
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let matches = match artifact {
            Artifact::Data => name == DATA_FILE,
            Artifact::Checkpoints => name == META_FILE || name.starts_with("seerdb.meta."),
            Artifact::Blob => name == BLOB_FILE,
            Artifact::Wal => name == WAL_FILE,
            Artifact::AuthorityFrame => name.starts_with("seerdb.meta."),
        };
        if matches {
            names.push(entry.path());
        }
    }
    names.sort();
    names
}

fn replace_artifact(materialized: &Path, candidate: &Path, artifact: Artifact) {
    // Capture the durable prefix length before any file is removed.
    let old_log_len = fs::metadata(materialized.join(META_LOG_FILE_NAME))
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    for path in artifact_names(materialized, artifact) {
        fs::remove_file(path).unwrap();
    }
    for source in artifact_names(candidate, artifact) {
        let name = source.file_name().unwrap();
        if artifact == Artifact::Checkpoints && name == META_LOG_FILE_NAME {
            // Model a crash before the new frame landed: only the bytes the
            // old generation already had are durable.
            let bytes = fs::read(source).unwrap();
            assert!(bytes.len() >= old_log_len as usize);
            fs::write(
                materialized.join(META_LOG_FILE_NAME),
                &bytes[..old_log_len as usize],
            )
            .unwrap();
            continue;
        }
        fs::copy(&source, materialized.join(name)).unwrap();
    }
}

struct StateOracle<'a> {
    retained: SnapshotId,
    old_commit: CommitId,
    new_commit: CommitId,
    old_inline: &'a [u8],
    new_inline: &'a [u8],
    old_blob: &'a [u8],
    new_blob: &'a [u8],
    retained_inline: &'a [u8],
    retained_blob: &'a [u8],
}

struct PublicationFixture {
    root: TempDir,
    old: PathBuf,
    candidate: PathBuf,
    retained: SnapshotId,
    old_commit: CommitId,
    new_commit: CommitId,
    old_inline: Vec<u8>,
    new_inline: Vec<u8>,
    old_blob: Vec<u8>,
    new_blob: Vec<u8>,
}

impl PublicationFixture {
    fn oracle(&self) -> StateOracle<'_> {
        StateOracle {
            retained: self.retained,
            old_commit: self.old_commit,
            new_commit: self.new_commit,
            old_inline: &self.old_inline,
            new_inline: &self.new_inline,
            old_blob: &self.old_blob,
            new_blob: &self.new_blob,
            retained_inline: &self.old_inline,
            retained_blob: &self.old_blob,
        }
    }
}

fn build_fixture() -> PublicationFixture {
    let root = tempdir().unwrap();
    let live = root.path().join("live");
    let old = root.path().join("old");
    let candidate = root.path().join("candidate");
    let recovered = root.path().join("recovered");
    let old_inline = b"inline-old".to_vec();
    let old_blob = vec![0x11; 4096];
    let new_inline = b"inline-new".to_vec();
    let new_blob = vec![0x22; 4096];

    let retained = {
        let mut db = DB::open(&live, Options::for_test()).unwrap();
        db.commit_batch(&[
            BatchMutation::Put {
                key: b"inline-key".to_vec(),
                value: old_inline.clone(),
            },
            BatchMutation::Put {
                key: b"blob-key".to_vec(),
                value: old_blob.clone(),
            },
        ])
        .unwrap();
        let commit = db.durability_status().commit_id;
        let retained = db.retain_commit(commit).unwrap();
        drop(db);
        copy_tree(&live, &old);
        retained
    };

    {
        let mut db = DB::open(&live, Options::for_test()).unwrap();
        db.put(b"inline-key", &new_inline).unwrap();
        db.put(b"blob-key", &new_blob).unwrap();
        db.inject_after_manifest_failure();
        assert!(db.flush().is_err());
        drop(db);
    }
    copy_tree(&live, &candidate);

    // Resolve the candidate once through the real recovery path so the model
    // has a complete-new image with WAL retirement already applied.
    {
        let mut db = DB::open(&live, Options::for_test()).unwrap();
        db.verify().unwrap();
        drop(db);
    }
    copy_tree(&live, &recovered);
    let old_commit = DB::open(&old, Options::for_test())
        .unwrap()
        .durability_status()
        .commit_id;
    let new_commit = DB::open(&recovered, Options::for_test())
        .unwrap()
        .durability_status()
        .commit_id;
    assert!(new_commit > old_commit);

    PublicationFixture {
        root,
        old,
        candidate,
        retained,
        old_commit,
        new_commit,
        old_inline,
        new_inline,
        old_blob,
        new_blob,
    }
}

#[derive(Clone, Debug)]
struct PartialArtifactCase {
    artifact: Artifact,
    file_name: String,
    bytes: usize,
}

fn partial_artifact_cases(candidate: &Path) -> Vec<PartialArtifactCase> {
    let artifacts = [
        Artifact::Data,
        Artifact::Checkpoints,
        Artifact::Blob,
        Artifact::Wal,
        Artifact::AuthorityFrame,
    ];
    let mut cases = Vec::new();
    for artifact in artifacts {
        for path in artifact_names(candidate, artifact).into_iter().take(2) {
            let length = fs::metadata(&path).unwrap().len() as usize;
            let mut budgets = vec![0, 1, length / 2, length.saturating_sub(1)];
            budgets.sort_unstable();
            budgets.dedup();
            for bytes in budgets.into_iter().filter(|bytes| *bytes < length) {
                cases.push(PartialArtifactCase {
                    artifact,
                    file_name: path.file_name().unwrap().to_string_lossy().into_owned(),
                    bytes,
                });
            }
        }
    }
    cases
}

fn replace_partial_artifact(materialized: &Path, candidate: &Path, case: &PartialArtifactCase) {
    for path in artifact_names(materialized, case.artifact) {
        fs::remove_file(path).unwrap();
    }
    let source = candidate.join(&case.file_name);
    let bytes = fs::read(source).unwrap();
    fs::write(materialized.join(&case.file_name), &bytes[..case.bytes]).unwrap();
}

struct PublicationSimulator<'a> {
    fixture: &'a PublicationFixture,
}

impl<'a> PublicationSimulator<'a> {
    fn new(fixture: &'a PublicationFixture) -> Self {
        Self { fixture }
    }

    fn materialize_prefix(&self, index: usize, step: PublicationStep) -> PathBuf {
        let path = self
            .fixture
            .root
            .path()
            .join(format!("publication-prefix-{index}-{}", step.name));
        copy_tree(&self.fixture.old, &path);
        for artifact in step.artifacts {
            replace_artifact(&path, &self.fixture.candidate, *artifact);
        }
        path
    }

    fn materialize_partial(&self, index: usize, case: &PartialArtifactCase) -> PathBuf {
        let path = self
            .fixture
            .root
            .path()
            .join(format!("publication-partial-{index}-{:?}", case.artifact));
        copy_tree(&self.fixture.old, &path);
        replace_partial_artifact(&path, &self.fixture.candidate, case);
        path
    }
}

fn verify_state(path: &Path, oracle: &StateOracle<'_>) -> Result<CommitId, Error> {
    let mut db = DB::open(path, Options::for_test())?;
    let commit = db.durability_status().commit_id;
    assert!(
        commit == oracle.old_commit || commit == oracle.new_commit,
        "unexpected commit {commit:?} in modeled state"
    );
    let (active_inline, active_blob) = if commit == oracle.new_commit {
        (oracle.new_inline, oracle.new_blob)
    } else {
        (oracle.old_inline, oracle.old_blob)
    };
    assert_eq!(db.get(b"inline-key")?, Some(active_inline.to_vec()));
    assert_eq!(db.get(b"blob-key")?, Some(active_blob.to_vec()));
    assert_eq!(
        db.get_at(oracle.retained, b"inline-key")?,
        Some(oracle.retained_inline.to_vec())
    );
    assert_eq!(
        db.get_at(oracle.retained, b"blob-key")?,
        Some(oracle.retained_blob.to_vec())
    );
    db.verify()?;
    drop(db);
    Ok(commit)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RefusalKind {
    Io,
    Corruption,
    Check,
    NeedsRecovery,
    Wal,
    Snapshot,
    Resource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoveryOutcome {
    Committed(CommitId),
    Refused(RefusalKind),
}

fn classify_refusal(error: &Error) -> RefusalKind {
    match error {
        Error::Io(_) => RefusalKind::Io,
        Error::Corruption(_) => RefusalKind::Corruption,
        Error::Check { .. } => RefusalKind::Check,
        Error::NeedsRecovery(_) => RefusalKind::NeedsRecovery,
        Error::Wal(_) => RefusalKind::Wal,
        Error::SnapshotUnavailable(_) => RefusalKind::Snapshot,
        Error::DiskFull | Error::CapacityPreflight | Error::Backpressure { .. } => {
            RefusalKind::Resource
        }
        other => panic!("unexpected refusal for partial publication artifact: {other}"),
    }
}

fn replay_twice(path: &Path, oracle: &StateOracle<'_>) -> RecoveryOutcome {
    let first = verify_state(path, oracle);
    let second = verify_state(path, oracle);
    match (first, second) {
        (Ok(first), Ok(second)) => {
            assert_eq!(first, second, "recovery changed commit identity on reopen");
            RecoveryOutcome::Committed(first)
        }
        (Err(first), Err(second)) => {
            let first = classify_refusal(&first);
            let second = classify_refusal(&second);
            assert_eq!(first, second, "recovery refusal changed on reopen");
            RecoveryOutcome::Refused(first)
        }
        (first, second) => {
            panic!("recovery was not deterministic: first={first:?}, second={second:?}")
        }
    }
}

fn assert_state(path: &Path, oracle: &StateOracle<'_>, expected: ExpectedActive) {
    let outcome = replay_twice(path, oracle);
    let RecoveryOutcome::Committed(commit) = outcome else {
        panic!("publication prefix refused unexpectedly: {outcome:?}");
    };
    match expected {
        ExpectedActive::Old => assert_eq!(commit, oracle.old_commit),
        ExpectedActive::New => assert_eq!(commit, oracle.new_commit),
        ExpectedActive::OldOrNew => {
            assert!(commit == oracle.old_commit || commit == oracle.new_commit)
        }
    }
}

#[test]
fn modeled_publication_prefixes_reopen_old_or_complete_new() {
    let fixture = build_fixture();
    let oracle = fixture.oracle();
    let simulator = PublicationSimulator::new(&fixture);

    for (index, step) in PUBLICATION_SCHEDULE.iter().copied().enumerate() {
        let materialized = simulator.materialize_prefix(index, step);
        assert_state(&materialized, &oracle, step.expected);
    }
}

#[test]
fn modeled_partial_artifact_schedules_reopen_whole_or_refuse() {
    let fixture = build_fixture();
    let oracle = fixture.oracle();
    let simulator = PublicationSimulator::new(&fixture);
    let cases = partial_artifact_cases(&fixture.candidate);
    assert!(
        !cases.is_empty(),
        "fixture must contain publication artifacts"
    );

    for (index, case) in cases.iter().enumerate() {
        let materialized = simulator.materialize_partial(index, case);
        match replay_twice(&materialized, &oracle) {
            RecoveryOutcome::Committed(commit) => assert!(
                commit == fixture.old_commit || commit == fixture.new_commit,
                "partial artifact {:?} at {} bytes produced unexpected commit {commit:?}",
                case,
                case.bytes,
            ),
            RecoveryOutcome::Refused(_) => {}
        }
    }
}

#[test]
fn modeled_crashed_generation_pages_fail_closed_for_retention() {
    let root = tempdir().unwrap();
    let live = root.path().join("live");
    let failed = root.path().join("failed");

    let first_commit = {
        let mut db = DB::open(&live, Options::for_test()).unwrap();
        let first = db
            .commit_batch(&[BatchMutation::Put {
                key: b"versioned".to_vec(),
                value: b"one".to_vec(),
            }])
            .unwrap();
        db.commit_batch(&[BatchMutation::Put {
            key: b"versioned".to_vec(),
            value: b"two".to_vec(),
        }])
        .unwrap();

        // The failed publication reuses the first generation's slots and
        // stamps the durable page images with generation 3 before dying at
        // the page-range sync. The crash leaves those bytes in place while
        // the manifest stays at generation 2.
        db.put(b"versioned", b"three").unwrap();
        db.inject_page_range_sync_failure();
        assert!(db.flush().is_err());
        drop(db);

        copy_tree(&live, &failed);
        first.commit_id
    };

    // Retention must refuse the historical root: the page headers prove the
    // mapped slots were rewritten by the crashed generation.
    let mut db = DB::open(&failed, Options::for_test()).unwrap();
    assert_eq!(db.get(b"versioned").unwrap(), Some(b"two".to_vec()));
    db.verify().unwrap();
    assert!(matches!(
        db.retain_commit(first_commit),
        Err(seerdb::Error::SnapshotUnavailable(message))
            if message.contains("physical pages reused")
    ));
}
