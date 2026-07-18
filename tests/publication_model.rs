#![cfg(feature = "fault-injection")]
#![allow(clippy::disallowed_methods)]

//! Deterministic publication-order model for one durable root generation.
//!
//! This is deliberately a test model, not a second filesystem or recovery
//! implementation. It takes artifact images produced by the real publication
//! path, materializes durable prefixes in a temporary directory, and lets the
//! normal `DB::open`/`verify`/historical-read paths decide whether each state is
//! acceptable. A Linux block-layer power-loss harness still has to validate
//! these assumptions against ext4/XFS and real cache/barrier behavior.

use seerdb::storage::format::{CommitId, SnapshotId};
use seerdb::{BatchMutation, DB, Options};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

const DATA_FILE: &str = "seerdb.data";
const BLOB_FILE: &str = "seerdb.blob";
const WAL_FILE: &str = "seerdb.wal";
const MANIFEST_FILE: &str = "MANIFEST";
const MANIFEST_HISTORY_FILE: &str = "seerdb.manifest-history";
const META_FILE: &str = "seerdb.meta";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Artifact {
    Data,
    Checkpoints,
    Blob,
    Wal,
    ManifestHistory,
    Manifest,
}

const PUBLICATION_PREFIXES: &[(&str, &[Artifact], bool)] = &[
    ("old-generation", &[], false),
    ("manifest-mirror", &[], false),
    ("data-pages", &[Artifact::Data], false),
    (
        "checkpoints",
        &[Artifact::Data, Artifact::Checkpoints],
        false,
    ),
    (
        "blob-image",
        &[Artifact::Data, Artifact::Checkpoints, Artifact::Blob],
        false,
    ),
    (
        "commit-wal",
        &[
            Artifact::Data,
            Artifact::Checkpoints,
            Artifact::Blob,
            Artifact::Wal,
        ],
        true,
    ),
    (
        "history-and-wal",
        &[
            Artifact::Data,
            Artifact::Checkpoints,
            Artifact::Blob,
            Artifact::Wal,
            Artifact::ManifestHistory,
        ],
        true,
    ),
    (
        "candidate-manifest",
        &[
            Artifact::Data,
            Artifact::Checkpoints,
            Artifact::Blob,
            Artifact::Wal,
            Artifact::ManifestHistory,
            Artifact::Manifest,
        ],
        false,
    ),
    (
        "wal-retired",
        &[
            Artifact::Data,
            Artifact::Checkpoints,
            Artifact::Blob,
            Artifact::ManifestHistory,
            Artifact::Manifest,
        ],
        false,
    ),
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
            Artifact::ManifestHistory => name == MANIFEST_HISTORY_FILE,
            Artifact::Manifest => name == MANIFEST_FILE,
        };
        if matches {
            names.push(entry.path());
        }
    }
    names
}

fn replace_artifact(materialized: &Path, candidate: &Path, artifact: Artifact) {
    for path in artifact_names(materialized, artifact) {
        fs::remove_file(path).unwrap();
    }
    for source in artifact_names(candidate, artifact) {
        let name = source.file_name().unwrap();
        fs::copy(&source, materialized.join(name)).unwrap();
    }
}

struct StateOracle<'a> {
    retained: SnapshotId,
    old_commit: CommitId,
    new_commit: CommitId,
    expected_commits: &'a [CommitId],
    old_inline: &'a [u8],
    new_inline: &'a [u8],
    old_blob: &'a [u8],
    new_blob: &'a [u8],
    retained_inline: &'a [u8],
    retained_blob: &'a [u8],
}

fn assert_state(path: &Path, oracle: &StateOracle<'_>) {
    for reopen in 0..2 {
        let mut db = DB::open(path, Options::for_test()).unwrap_or_else(|error| {
            panic!("modeled publication state failed to open on pass {reopen}: {error}")
        });
        let status = db.durability_status();
        assert!(
            oracle.expected_commits.contains(&status.commit_id),
            "unexpected commit {:?} in modeled state",
            status.commit_id
        );
        let (active_inline, active_blob) = if status.commit_id == oracle.new_commit {
            (oracle.new_inline, oracle.new_blob)
        } else {
            assert_eq!(status.commit_id, oracle.old_commit);
            (oracle.old_inline, oracle.old_blob)
        };
        assert_eq!(db.get(b"inline-key").unwrap(), Some(active_inline.to_vec()));
        assert_eq!(db.get(b"blob-key").unwrap(), Some(active_blob.to_vec()));
        assert_eq!(
            db.get_at(oracle.retained, b"inline-key").unwrap(),
            Some(oracle.retained_inline.to_vec())
        );
        assert_eq!(
            db.get_at(oracle.retained, b"blob-key").unwrap(),
            Some(oracle.retained_blob.to_vec())
        );
        db.verify().unwrap();
        drop(db);
    }
}

#[test]
fn modeled_publication_prefixes_reopen_old_or_complete_new() {
    let root = tempdir().unwrap();
    let live = root.path().join("live");
    let old = root.path().join("old");
    let candidate = root.path().join("candidate");
    let recovered = root.path().join("recovered");
    let retained_inline = b"inline-old".to_vec();
    let retained_blob = vec![0x11; 4096];
    let candidate_inline = b"inline-new".to_vec();
    let candidate_blob = vec![0x22; 4096];

    let retained = {
        let mut db = DB::open(&live, Options::for_test()).unwrap();
        db.commit_batch(&[
            BatchMutation::Put {
                key: b"inline-key".to_vec(),
                value: retained_inline.clone(),
            },
            BatchMutation::Put {
                key: b"blob-key".to_vec(),
                value: retained_blob.clone(),
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
        db.put(b"inline-key", &candidate_inline).unwrap();
        db.put(b"blob-key", &candidate_blob).unwrap();
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

    for (name, artifacts, allow_old_or_new) in PUBLICATION_PREFIXES {
        let materialized = root.path().join(name);
        copy_tree(&old, &materialized);
        for artifact in *artifacts {
            replace_artifact(&materialized, &candidate, *artifact);
        }

        let expected_commits = if *allow_old_or_new {
            &[old_commit, new_commit][..]
        } else if matches!(
            *name,
            "old-generation" | "manifest-mirror" | "data-pages" | "checkpoints" | "blob-image"
        ) {
            &[old_commit][..]
        } else {
            &[new_commit][..]
        };
        let oracle = StateOracle {
            retained,
            old_commit,
            new_commit,
            expected_commits,
            old_inline: &retained_inline,
            new_inline: &candidate_inline,
            old_blob: &retained_blob,
            new_blob: &candidate_blob,
            retained_inline: &retained_inline,
            retained_blob: &retained_blob,
        };
        assert_state(&materialized, &oracle);
    }
}
