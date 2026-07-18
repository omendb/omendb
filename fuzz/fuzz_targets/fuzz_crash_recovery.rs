#![no_main]
#![allow(clippy::disallowed_methods)]

use libfuzzer_sys::fuzz_target;
use seerdb::{BatchMutation, DB, Options};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_CASE: AtomicU64 = AtomicU64::new(0);

fn key_for(id: u8) -> Vec<u8> {
    format!("key-{id:02}").into_bytes()
}

fn value_for(round: usize, data: &[u8]) -> Vec<u8> {
    format!("value-{round:03}-{:02x}", data.first().copied().unwrap_or(0)).into_bytes()
}

fn assert_model(db: &DB, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    for id in 0..16 {
        let key = key_for(id);
        assert_eq!(db.get(&key).unwrap(), model.get(&key).cloned());
    }

    let expected: Vec<_> = model
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(db.range(b"key-00", b"key-99").unwrap(), expected);
}

fn inject_fault(db: &DB, fault: u8) {
    match fault % 12 {
        0 => db.inject_sync_failure(),
        1 => db.inject_write_failure(),
        2 => db.inject_after_write_failure(),
        3 => db.inject_final_write_disk_full(),
        4 => db.inject_disk_full(),
        5 => db.inject_capacity_limit(0),
        6 => db.inject_atomic_rename_failure(),
        7 => db.inject_wal_write_failure(),
        8 => db.inject_wal_after_write_failure(),
        9 => db.inject_wal_sync_failure(),
        10 => db.inject_manifest_sync_failure(),
        _ => db.inject_after_manifest_failure(),
    }
}

fn case_path() -> PathBuf {
    let id = NEXT_CASE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "seerdb-fuzz-recovery-{}-{id}",
        std::process::id()
    ))
}

fn reopen(path: &Path) -> DB {
    DB::open(path, Options::for_test()).unwrap()
}

fuzz_target!(|data: &[u8]| {
    let path = case_path();
    let mut db = DB::open(&path, Options::for_test()).unwrap();
    let mut model = BTreeMap::new();

    for (round, command) in data.chunks(6).take(96).enumerate() {
        if command.len() < 3 {
            break;
        }

        let key = key_for(command[1] % 16);
        match command[0] % 8 {
            0 => {
                let value = value_for(round, command);
                db.commit_batch(&[BatchMutation::Put {
                    key: key.clone(),
                    value: value.clone(),
                }])
                .unwrap();
                model.insert(key, value);
            }
            1 => {
                db.commit_batch(&[BatchMutation::Delete { key: key.clone() }])
                    .unwrap();
                model.remove(&key);
            }
            2 => {
                assert_eq!(db.get(&key).unwrap(), model.get(&key).cloned());
            }
            3 => {
                assert_model(&db, &model);
            }
            4 => {
                drop(db);
                db = reopen(&path);
                assert_model(&db, &model);
            }
            5 | 6 => {
                let before = model.clone();
                let mut after = before.clone();
                let mutation = if command[0] % 8 == 5 {
                    let value = value_for(round, command);
                    after.insert(key.clone(), value.clone());
                    BatchMutation::Put { key, value }
                } else {
                    after.remove(&key);
                    BatchMutation::Delete { key }
                };

                inject_fault(&db, command[2]);
                let _ = db.commit_batch(std::slice::from_ref(&mutation));
                drop(db);
                db = reopen(&path);
                if db.range(b"key-00", b"key-99").unwrap()
                    == before.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<Vec<_>>()
                {
                    model = before;
                } else {
                    assert_model(&db, &after);
                    model = after;
                }
            }
            _ => {
                db.checkpoint().unwrap();
                assert_model(&db, &model);
            }
        }
    }

    drop(db);
    let reopened = reopen(&path);
    assert_model(&reopened, &model);
    drop(reopened);
    let _ = fs::remove_dir_all(path);
});
