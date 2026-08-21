//! Long retention/reclamation soak evidence.
//!
//! Drives many publication generations with mixed put/delete churn,
//! periodic maintenance (prune/vacuum/GC), and periodic reopen samples,
//! recording whether disk footprint, metadata-log size, and reopen time
//! stay bounded as the generation count grows. Run against a
//! device-backed directory; on Linux /tmp is tmpfs and the footprint
//! numbers would not reflect real storage.

use seerdb::{BatchMutation, DB, Options};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

struct Args {
    path: PathBuf,
    rounds: usize,
    keys: usize,
    batch_size: usize,
    value_bytes: usize,
    maintenance_every: usize,
    reopen_sample_every: usize,
}

fn parse_args() -> Result<Args, Box<dyn std::error::Error>> {
    let mut args = Args {
        path: PathBuf::from("."),
        rounds: 200,
        keys: 4096,
        batch_size: 16,
        value_bytes: 128,
        maintenance_every: 10,
        reopen_sample_every: 25,
    };
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        let mut take = |name: &str| -> Result<String, Box<dyn std::error::Error>> {
            argv.next()
                .ok_or_else(|| format!("{name} requires a value").into())
        };
        match flag.as_str() {
            "--path" => args.path = take("--path")?.into(),
            "--rounds" => args.rounds = take("--rounds")?.parse()?,
            "--keys" => args.keys = take("--keys")?.parse()?,
            "--batch-size" => args.batch_size = take("--batch-size")?.parse()?,
            "--value-bytes" => args.value_bytes = take("--value-bytes")?.parse()?,
            "--maintenance-every" => {
                args.maintenance_every = take("--maintenance-every")?.parse()?
            }
            "--reopen-sample-every" => {
                args.reopen_sample_every = take("--reopen-sample-every")?.parse()?
            }
            "--help" | "-h" => {
                println!(
                    "usage: retention_soak [--path DIR] [--rounds N] [--keys N] [--batch-size N] [--value-bytes N] [--maintenance-every N] [--reopen-sample-every N]"
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument {other}").into()),
        }
    }
    Ok(args)
}

fn dir_usage(path: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                total += dir_usage(&entry.path())?;
            } else {
                total += metadata.len();
            }
        }
    }
    Ok(total)
}

fn key(index: usize) -> Vec<u8> {
    format!("soak-key-{index:08}").into_bytes()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args()?;
    let db_path = args.path.join("db");
    let _ = fs::remove_dir_all(&db_path);
    fs::create_dir_all(&args.path)?;

    let mut options = Options::for_test();
    options.max_wal_bytes = 8 * 1024 * 1024;
    let mut db = DB::create(&db_path, options.clone())?;

    // Golden-ratio scatter so each batch spreads across the key space.
    let scatter = |round: usize, slot: usize| {
        ((round as u64)
            .wrapping_mul(args.batch_size as u64)
            .wrapping_add(slot as u64)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            % args.keys as u64) as usize
    };
    let value = vec![0xAB; args.value_bytes];

    let mut samples: Vec<serde_json::Value> = Vec::new();
    let mut total_commits = 0u64;
    let started = Instant::now();

    for round in 0..args.rounds {
        let mutations: Vec<BatchMutation> = (0..args.batch_size)
            .map(|slot| {
                // Even rounds overwrite, odd rounds delete-then-reinsert a
                // different region so reclamation always has dead pages and
                // dead blob records to reclaim.
                let index = scatter(round, slot);
                if round % 2 == 0 {
                    BatchMutation::Put {
                        key: key(index),
                        value: value.clone(),
                    }
                } else {
                    BatchMutation::Delete { key: key(index) }
                }
            })
            .collect();
        db.commit_batch(&mutations)?;
        total_commits += 1;

        if args.maintenance_every > 0 && (round + 1) % args.maintenance_every == 0 {
            db.prune_history()?;
            db.vacuum()?;
            db.gc()?;
        }

        if args.reopen_sample_every > 0 && (round + 1) % args.reopen_sample_every == 0 {
            drop(db);
            let reopen_started = Instant::now();
            db = DB::open(&db_path, options.clone())?;
            let reopen_ns = reopen_started.elapsed().as_nanos() as u64;
            samples.push(serde_json::json!({
                "round": round + 1,
                "commits": total_commits,
                "disk_bytes": dir_usage(&db_path)?,
                "metadata_log_bytes": fs::metadata(db_path.join("seerdb.meta.log"))
                    .map(|m| m.len())
                    .unwrap_or(0),
                "reopen_ns": reopen_ns,
                "elapsed_ns": started.elapsed().as_nanos() as u64,
            }));
        }
    }

    drop(db);
    let final_reopen = Instant::now();
    let mut reopened = DB::open(&db_path, options)?;
    let reopen_ns = final_reopen.elapsed().as_nanos() as u64;

    let report = serde_json::json!({
        "schema": "seerdb-retention-soak-v1",
        "workload": {
            "rounds": args.rounds,
            "keys_space": args.keys,
            "batch_size": args.batch_size,
            "value_bytes": args.value_bytes,
            "maintenance_every": args.maintenance_every,
        },
        "total_commits": total_commits,
        "final_live_keys": args.keys, // upper bound; deletes make it lower
        "disk_bytes_final": dir_usage(&db_path)?,
        "metadata_log_bytes_final": fs::metadata(db_path.join("seerdb.meta.log"))
            .map(|m| m.len())
            .unwrap_or(0),
        "reopen_ns_final": reopen_ns,
        "verify_ok": reopened.verify().is_ok(),
        "samples": samples,
        "recorded_at": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
