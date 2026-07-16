//! Execute the shared DBNext R0 failure schedule against SeerDB.
//!
//! The schedule remains owned by the sibling DBNext worktree and is read at
//! runtime. SeerDB reports the native seam used for each case so an analogue
//! is not mistaken for a byte-for-byte fault-point identity.

#![allow(clippy::disallowed_methods)]

#[cfg(not(feature = "fault-injection"))]
fn main() {
    eprintln!("re-run with --features fault-injection");
}

#[cfg(feature = "fault-injection")]
mod runner {
    use seerdb::recovery::WalRecord;
    use seerdb::storage::format::MANIFEST_SLOT_SIZE;
    use seerdb::{BatchMutation, CheckFailureKind, DB, Error, Options};
    use serde_json::{Map, Value, json};
    use std::error::Error as StdError;
    use std::fs;
    use std::io::{Error as IoError, ErrorKind};
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    const DEFAULT_SCHEDULE: &str = "../monorepo-dbnext/experiments/dbnext/ai/workloads/r0-integrity-recovery/failure-schedule.jsonl";

    type AnyResult<T> = Result<T, Box<dyn StdError>>;

    fn invalid(message: impl Into<String>) -> Box<dyn StdError> {
        Box::new(IoError::new(ErrorKind::InvalidData, message.into()))
    }

    fn string_field<'a>(object: &'a Map<String, Value>, name: &str) -> AnyResult<&'a str> {
        object
            .get(name)
            .and_then(Value::as_str)
            .ok_or_else(|| invalid(format!("schedule field {name} must be a string")))
    }

    fn u64_field(object: &Map<String, Value>, name: &str) -> AnyResult<u64> {
        object
            .get(name)
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid(format!("schedule field {name} must be an integer")))
    }

    fn put_batch() -> [BatchMutation; 2] {
        [
            BatchMutation::Put {
                key: b"batch/a".to_vec(),
                value: b"a".to_vec(),
            },
            BatchMutation::Put {
                key: b"batch/b".to_vec(),
                value: b"b".to_vec(),
            },
        ]
    }

    fn inject_commit(db: &DB, inject: &str) -> AnyResult<&'static str> {
        match inject {
            "BeforeWalAppend" => {
                db.inject_wal_write_failure();
                Ok("WAL append before-write")
            }
            "AfterWalAppend" => {
                db.inject_wal_after_write_failure();
                Ok("WAL append after-write")
            }
            "WalSync" => {
                db.inject_wal_sync_failure();
                Ok("WAL sync")
            }
            "AfterWalSync" => {
                db.inject_wal_after_sync_failure();
                Ok("WAL sync after-boundary")
            }
            other => Err(invalid(format!("unsupported commit injection {other}"))),
        }
    }

    fn inject_checkpoint(db: &DB, inject: &str) -> AnyResult<&'static str> {
        match inject {
            "DataSync" => {
                db.inject_sync_failure();
                Ok("device generation sync")
            }
            "PackedPageSync" => {
                db.inject_after_write_failure();
                Ok("page write after-write analogue (packed-page sync seam absent)")
            }
            "ManifestSync" => {
                db.inject_manifest_sync_failure();
                Ok("manifest sync")
            }
            "AfterManifestPublish" => {
                db.inject_after_manifest_failure();
                Ok("post-manifest publication")
            }
            "WalTruncate" => {
                db.inject_wal_truncate_failure();
                Ok("WAL retirement")
            }
            "ShortWrite" => {
                db.inject_atomic_short_write_failure();
                Ok("atomic checkpoint short write")
            }
            "TornWrite" => {
                db.inject_atomic_torn_write_failure();
                Ok("atomic checkpoint torn write")
            }
            other => Err(invalid(format!("unsupported checkpoint injection {other}"))),
        }
    }

    fn run_commit_case(case: &str, inject: &str) -> AnyResult<Value> {
        let directory = tempdir()?;
        let path = directory.path().join("commit.db");
        let options = Options {
            sync_writes: matches!(inject, "WalSync" | "AfterWalSync"),
            ..Options::default()
        };
        let mut db = DB::open(&path, options)?;
        let seam = inject_commit(&db, inject)?;
        let result = db.commit_batch(&put_batch());
        if result.is_ok() {
            return Err(invalid(format!(
                "{case}: injected commit fault did not fail"
            )));
        }
        let fenced = db.durability_status().write_fenced;
        drop(db);

        let reopened = DB::open(&path, Options::default())?;
        let a = reopened.get(b"batch/a")?;
        let b = reopened.get(b"batch/b")?;
        let all_absent =
            a.is_none() && b.is_none() && reopened.durability_status().commit_id.get() == 0;
        let all_present = a == Some(b"a".to_vec())
            && b == Some(b"b".to_vec())
            && reopened.durability_status().commit_id.get() == 1;
        Ok(json!({
            "case": case,
            "inject": inject,
            "native_seam": seam,
            "recovered_commit_id": reopened.durability_status().commit_id.get(),
            "whole_batch": all_absent || all_present,
            "writer_fenced_before_reopen": fenced,
            "accepted": all_absent || all_present,
        }))
    }

    fn run_checkpoint_case(case: &str, inject: &str) -> AnyResult<Value> {
        let directory = tempdir()?;
        let path = directory.path().join("checkpoint.db");
        let mut db = DB::open(&path, Options::default())?;
        db.commit_batch(&[BatchMutation::Put {
            key: b"stable".to_vec(),
            value: b"one".to_vec(),
        }])?;
        db.put(b"stable", b"two")?;
        let seam = inject_checkpoint(&db, inject)?;
        let result = db.checkpoint();
        if result.is_ok() {
            return Err(invalid(format!(
                "{case}: injected checkpoint fault did not fail"
            )));
        }
        let fenced = db.durability_status().write_fenced;
        drop(db);

        let reopened = DB::open(&path, Options::default())?;
        let value = reopened.get(b"stable")?;
        let commit_id = reopened.durability_status().commit_id.get();
        let expected_prior = matches!(
            inject,
            "DataSync" | "PackedPageSync" | "ManifestSync" | "ShortWrite" | "TornWrite"
        );
        let expected_new = matches!(inject, "AfterManifestPublish" | "WalTruncate");
        let prior = value == Some(b"one".to_vec()) && commit_id == 1;
        let new = value == Some(b"two".to_vec()) && commit_id == 2;
        let accepted = (expected_prior && prior) || (expected_new && new);
        Ok(json!({
            "case": case,
            "inject": inject,
            "native_seam": seam,
            "recovered_commit_id": commit_id,
            "recovered_value": value.map(|bytes| String::from_utf8_lossy(&bytes).into_owned()),
            "writer_fenced_before_reopen": fenced,
            "accepted": accepted,
        }))
    }

    fn corrupt_manifest(path: &Path) -> AnyResult<()> {
        let mut bytes = fs::read(path.join("MANIFEST"))?;
        bytes[..MANIFEST_SLOT_SIZE].fill(0xA5);
        bytes[MANIFEST_SLOT_SIZE..MANIFEST_SLOT_SIZE * 2].fill(0x5A);
        fs::write(path.join("MANIFEST"), bytes)?;
        Ok(())
    }

    fn run_recovery_case(case: &str, inject: &str) -> AnyResult<Value> {
        let directory = tempdir()?;
        let path = directory.path().join("recovery.db");
        {
            let mut db = DB::open(&path, Options::default())?;
            db.put(b"stable", b"one")?;
            db.flush()?;
        }

        match inject {
            "corrupt-wal-byte" => {
                let mut wal = WalRecord::put(b"corrupt", b"suffix").to_bytes();
                let last = wal.len() - 1;
                wal[last] ^= 0xFF;
                fs::write(path.join("seerdb.wal"), wal)?;
            }
            "corrupt-manifest-byte" => corrupt_manifest(&path)?,
            other => {
                return Err(invalid(format!(
                    "{case}: unsupported recovery injection {other}"
                )));
            }
        }

        let open_error = DB::open(&path, Options::default()).err();
        let typed = match DB::check(&path, Options::default()) {
            Err(Error::Check { kind, .. }) => Some(format!("{kind:?}")),
            Err(error) => Some(format!("unexpected: {error:?}")),
            Ok(_) => None,
        };
        let expected_kind = if inject == "corrupt-wal-byte" {
            CheckFailureKind::Wal
        } else {
            CheckFailureKind::Manifest
        };
        Ok(json!({
            "case": case,
            "inject": inject,
            "recovered_open_refused": open_error.is_some(),
            "typed_check_kind": typed,
            "accepted": open_error.is_some() && typed.as_deref() == Some(&format!("{expected_kind:?}")),
        }))
    }

    fn run_case(object: &Map<String, Value>) -> AnyResult<Value> {
        let case = string_field(object, "case")?;
        let phase = string_field(object, "phase")?;
        let inject = string_field(object, "inject")?;
        let trace_seq = u64_field(object, "trace_seq")?;
        let result = match phase {
            "commit" => run_commit_case(case, inject)?,
            "checkpoint" => run_checkpoint_case(case, inject)?,
            "recovery" => run_recovery_case(case, inject)?,
            other => return Err(invalid(format!("{case}: unsupported phase {other}"))),
        };
        Ok(json!({
            "trace_seq": trace_seq,
            "expected": object.get("expected"),
            "result": result,
        }))
    }

    pub fn run() -> AnyResult<()> {
        let schedule = std::env::args()
            .nth(1)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_SCHEDULE));
        let bytes = fs::read(&schedule)?;
        let mut results = Vec::new();
        for (line, raw) in String::from_utf8(bytes.clone())?.lines().enumerate() {
            if raw.trim().is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(raw)
                .map_err(|error| invalid(format!("schedule line {}: {error}", line + 1)))?;
            let object = value
                .as_object()
                .ok_or_else(|| invalid(format!("schedule line {} is not an object", line + 1)))?;
            results.push(run_case(object)?);
        }
        let accepted = results
            .iter()
            .all(|result| result["result"]["accepted"].as_bool() == Some(true));
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "schedule": schedule,
                "case_count": results.len(),
                "accepted": accepted,
                "cases": results,
            }))?
        );
        if !accepted {
            return Err(invalid("one or more shared R0 fault cases failed"));
        }
        Ok(())
    }
}

#[cfg(feature = "fault-injection")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    runner::run()
}
