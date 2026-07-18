//! Workload and verification helper for the privileged Linux power-loss runner.
//!
//! The runner keeps its oracle outside the database image. A replayed image
//! may therefore be opened repeatedly without trusting metadata written by the
//! interrupted publication itself.

use seerdb::storage::format::SnapshotId;
use seerdb::{DB, Options};
use std::env;
use std::fs;
use std::path::Path;
use std::process;

const INLINE_KEY: &[u8] = b"inline-key";
const BLOB_KEY: &[u8] = b"blob-key";
const INLINE_OLD: &[u8] = b"inline-old";
const INLINE_NEW: &[u8] = b"inline-new";
const BLOB_OLD: u8 = 0x11;
const BLOB_NEW: u8 = 0x22;
const BLOB_LEN: usize = 4096;

#[derive(Debug, Clone, Copy)]
struct Oracle {
    snapshot_id: u64,
    old_commit: u64,
    new_commit: Option<u64>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("seerdb_power_loss: {error}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();
    match args.as_slice() {
        [_, mode, db_path, oracle_path] if mode == "seed" => {
            seed(Path::new(db_path), Path::new(oracle_path))
        }
        [_, mode, db_path, oracle_path] if mode == "mutate" => {
            mutate(Path::new(db_path), Path::new(oracle_path))
        }
        [_, mode, db_path, oracle_path] if mode == "verify" => {
            verify(Path::new(db_path), Path::new(oracle_path))
        }
        _ => Err(format!(
            "usage: {} <seed|mutate|verify> <db-path> <oracle-path>",
            args.first()
                .map(String::as_str)
                .unwrap_or("seerdb_power_loss")
        )),
    }
}

fn seed(db_path: &Path, oracle_path: &Path) -> Result<(), String> {
    let mut db = DB::create(db_path, Options::default()).map_err(error)?;
    db.put(INLINE_KEY, INLINE_OLD).map_err(error)?;
    db.put(BLOB_KEY, &vec![BLOB_OLD; BLOB_LEN]).map_err(error)?;
    db.flush().map_err(error)?;
    let old_commit = db.durability_status().commit_id.get();
    let snapshot_id = db
        .retain_commit(seerdb::storage::format::CommitId::new(old_commit))
        .map_err(error)?
        .get();
    write_oracle(
        oracle_path,
        Oracle {
            snapshot_id,
            old_commit,
            new_commit: None,
        },
    )
}

fn mutate(db_path: &Path, oracle_path: &Path) -> Result<(), String> {
    let oracle = read_oracle(oracle_path)?;
    let mut db = DB::open(db_path, Options::default()).map_err(error)?;
    if db.get(INLINE_KEY).map_err(error)?.as_deref() != Some(INLINE_OLD) {
        return Err("seed generation is not active before mutation".into());
    }
    db.put(INLINE_KEY, INLINE_NEW).map_err(error)?;
    db.put(BLOB_KEY, &vec![BLOB_NEW; BLOB_LEN]).map_err(error)?;
    db.flush().map_err(error)?;
    let new_commit = db.durability_status().commit_id.get();
    if new_commit <= oracle.old_commit {
        return Err(format!(
            "new commit {new_commit} did not advance past {}",
            oracle.old_commit
        ));
    }
    write_oracle(
        oracle_path,
        Oracle {
            new_commit: Some(new_commit),
            ..oracle
        },
    )
}

fn verify(db_path: &Path, oracle_path: &Path) -> Result<(), String> {
    let oracle = read_oracle(oracle_path)?;
    let new_commit = oracle
        .new_commit
        .ok_or_else(|| "oracle has no completed mutation commit".to_string())?;
    for pass in 0..2 {
        let mut db = DB::open(db_path, Options::default()).map_err(error)?;
        let status = db.durability_status();
        let (expected_inline, expected_blob) = match status.commit_id.get() {
            commit if commit == oracle.old_commit => (INLINE_OLD, BLOB_OLD),
            commit if commit == new_commit => (INLINE_NEW, BLOB_NEW),
            commit => {
                return Err(format!(
                    "replayed image exposed unexpected commit {commit} on reopen pass {pass}"
                ));
            }
        };
        if db.get(INLINE_KEY).map_err(error)?.as_deref() != Some(expected_inline) {
            return Err(format!("inline value mismatch on reopen pass {pass}"));
        }
        if db.get(BLOB_KEY).map_err(error)?.as_deref() != Some(&vec![expected_blob; BLOB_LEN]) {
            return Err(format!("blob value mismatch on reopen pass {pass}"));
        }
        if db
            .get_at(SnapshotId::new(oracle.snapshot_id), INLINE_KEY)
            .map_err(error)?
            .as_deref()
            != Some(INLINE_OLD)
        {
            return Err(format!("retained inline value mismatch on pass {pass}"));
        }
        if db
            .get_at(SnapshotId::new(oracle.snapshot_id), BLOB_KEY)
            .map_err(error)?
            .as_deref()
            != Some(&vec![BLOB_OLD; BLOB_LEN])
        {
            return Err(format!("retained blob value mismatch on pass {pass}"));
        }
        db.verify().map_err(error)?;
    }
    Ok(())
}

fn read_oracle(path: &Path) -> Result<Oracle, String> {
    let mut snapshot_id = None;
    let mut old_commit = None;
    let mut new_commit = None;
    for line in fs::read_to_string(path).map_err(error)?.lines() {
        let Some((key, value)) = line.split_once('=') else {
            return Err(format!("malformed oracle line: {line}"));
        };
        match key {
            "snapshot_id" => snapshot_id = Some(parse_id(value, key)?),
            "old_commit" => old_commit = Some(parse_id(value, key)?),
            "new_commit" => {
                new_commit = (!value.is_empty())
                    .then(|| parse_id(value, key))
                    .transpose()?;
            }
            other => return Err(format!("unknown oracle field {other}")),
        }
    }
    Ok(Oracle {
        snapshot_id: snapshot_id.ok_or_else(|| "oracle lacks snapshot_id".to_string())?,
        old_commit: old_commit.ok_or_else(|| "oracle lacks old_commit".to_string())?,
        new_commit,
    })
}

fn write_oracle(path: &Path, oracle: Oracle) -> Result<(), String> {
    let new_commit = oracle.new_commit.map_or(String::new(), |id| id.to_string());
    fs::write(
        path,
        format!(
            "snapshot_id={}\nold_commit={}\nnew_commit={new_commit}\n",
            oracle.snapshot_id, oracle.old_commit
        ),
    )
    .map_err(error)
}

fn parse_id(value: &str, field: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|error| format!("invalid {field}: {error}"))
}

fn error(error: impl std::fmt::Display) -> String {
    error.to_string()
}
