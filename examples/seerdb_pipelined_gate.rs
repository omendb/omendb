//! Pipelined publication gate: contended admit/barrier trace with reopen
//! verification.
//!
//! Simulates the pinned group-commit worker pattern on top of the two-phase
//! API: producers submit batches over a bounded channel; one worker drains
//! to-empty-or-cap (drain-close pin), admits each batch, and runs one
//! publication_barrier per group. The key series alternates between two
//! ranges so every group rewrites pages the previous group wrote, exercising
//! reuse/churn under pipelined publication. Every run reopens and verifies
//! the complete last-write-wins state before reporting.

#![allow(clippy::disallowed_methods)]

use seerdb::{BatchMutation, DB, Options};
use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const DEFAULT_OPERATIONS: usize = 512;
const DEFAULT_MAX_GROUP: usize = 8;

fn key(range: usize, index: usize) -> Vec<u8> {
    format!("gate-key-{range}-{index:06}").into_bytes()
}

fn value(range: usize, index: usize, revision: usize) -> Vec<u8> {
    format!("gate-value-{range}-{index:06}-rev{revision:05}").into_bytes()
}

struct Config {
    operations: usize,
    max_group: usize,
    key_space: usize,
    path: Option<String>,
}

fn parse_config() -> Result<Config, Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let mut config = Config {
        operations: DEFAULT_OPERATIONS,
        max_group: DEFAULT_MAX_GROUP,
        key_space: 64,
        path: None,
    };
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--operations" => {
                config.operations = args.next().ok_or("--operations requires a value")?.parse()?;
            }
            "--max-group" => {
                config.max_group = args.next().ok_or("--max-group requires a value")?.parse()?;
            }
            "--key-space" => {
                config.key_space = args.next().ok_or("--key-space requires a value")?.parse()?;
            }
            "--path" => {
                config.path = Some(args.next().ok_or("--path requires a value")?);
            }
            "--help" | "-h" => {
                println!(
                    "usage: seerdb_pipelined_gate [--operations N] [--max-group K] [--key-space S] [--path DIR]"
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument {other}").into()),
        }
    }
    if config.operations == 0 || config.max_group == 0 || config.key_space == 0 {
        return Err("--operations/--max-group/--key-space must be positive".into());
    }
    Ok(config)
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = parse_config()?;
    let scratch;
    let db_path = match &config.path {
        Some(path) => std::path::PathBuf::from(path),
        None => {
            scratch = tempfile::tempdir()?;
            scratch.path().join("pipelined-gate.db")
        }
    };

    let (submit_tx, submit_rx) = std::sync::mpsc::sync_channel::<Option<Vec<BatchMutation>>>(64);
    let submit_rx = Arc::new(Mutex::new(submit_rx));
    let done = Arc::new(AtomicUsize::new(0));
    let groups = Arc::new(AtomicUsize::new(0));
    let group_sizes: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let worker_db_path = db_path.clone();

    let worker_rx = Arc::clone(&submit_rx);
    let worker_done = Arc::clone(&done);
    let worker_groups = Arc::clone(&groups);
    let worker_sizes = Arc::clone(&group_sizes);
    let max_group = config.max_group;
    let worker = std::thread::spawn(move || -> Result<(), Box<dyn Error + Send>> {
        let mut db = DB::open(&worker_db_path, Options::default())
            .map_err(|error| Box::new(error) as Box<dyn Error + Send>)?;
        loop {
            // Drain-close pin: take one envelope's work, then drain to empty
            // or cap. Group of 1 pays zero added latency.
            let first = {
                let guard = worker_rx.lock().unwrap();
                match guard.recv() {
                    Ok(Some(batch)) => batch,
                    Ok(None) | Err(_) => return Ok(()),
                }
            };
            let mut drained = vec![first];
            while drained.len() < max_group {
                match worker_rx.lock().unwrap().try_recv() {
                    Ok(Some(next)) => drained.push(next),
                    Ok(None) | Err(_) => break,
                }
            }
            let expected = db.durability_status().commit_id;
            for mutations in &drained {
                db.admit_batch(expected, mutations)
                    .map_err(|error| Box::new(error) as Box<dyn Error + Send>)?;
            }
            db.publication_barrier()
                .map_err(|error| Box::new(error) as Box<dyn Error + Send>)?;
            worker_groups.fetch_add(1, Ordering::Relaxed);
            worker_sizes.lock().unwrap().push(drained.len());
            worker_done.fetch_add(drained.len(), Ordering::Relaxed);
        }
    });

    // Producers alternate between two key ranges: every group rewrites pages
    // the previous group wrote, forcing out-of-place churn under pipelined
    // publication.
    let producers = 4;
    let per_producer = config.operations / producers;
    let started = Instant::now();
    let mut handles = Vec::new();
    for producer in 0..producers {
        let tx = submit_tx.clone();
        let key_space = config.key_space;
        handles.push(std::thread::spawn(move || {
            for step in 0..per_producer {
                let range = (producer + step) % 2;
                let index = producer * per_producer + step;
                let revision = step;
                let _ = tx.send(Some(vec![BatchMutation::Put {
                    key: key(range, index),
                    value: value(range, index, revision),
                }]));
            }
        }));
    }
    drop(submit_tx);
    for handle in handles {
        handle.join().unwrap();
    }
    // The worker loops forever on recv; closing the channel ends it.
    match worker.join() {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Err(format!("worker failed: {error}").into()),
        Err(join_error) => return Err(format!("worker panicked: {join_error:?}").into()),
    }

    let elapsed = started.elapsed();
    let operations = done.load(Ordering::Relaxed);
    let group_count = groups.load(Ordering::Relaxed);
    let sizes = group_sizes.lock().unwrap();
    let avg_group = if sizes.is_empty() {
        0.0
    } else {
        sizes.iter().sum::<usize>() as f64 / sizes.len() as f64
    };

    // Reopen and verify the full last-write-wins state.
    let mut expected_state: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for producer in 0..producers {
        for step in 0..per_producer {
            let range = (producer + step) % 2;
            let index = producer * per_producer + step;
            expected_state.insert(
                key(range, index),
                value(range, index, step),
            );
        }
    }
    let reopened = DB::open(&db_path, Options::default())?;
    for (expected_key, expected_value) in &expected_state {
        match reopened.get(expected_key)? {
            Some(actual) if actual == *expected_value => {}
            other => {
                return Err(format!(
                    "reopen mismatch for {expected_key:?}: expected {expected_value:?}, got {other:?}"
                )
                .into());
            }
        }
    }

    println!(
        "{}",
        serde_json::json!({
            "operations": operations,
            "groups": group_count,
            "avg_group_size": avg_group,
            "ops_per_sec": operations as f64 / elapsed.as_secs_f64(),
            "elapsed_ms": elapsed.as_millis() as u64,
            "verified_keys": expected_state.len(),
        })
    );
    Ok(())
}
