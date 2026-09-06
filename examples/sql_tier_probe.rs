//! Attribute the SQL-tier statement cost that now dominates a commit.
//!
//! After the sync-class landing, the engine wave is ~223 us/txn
//! (kernel class) while the full-stack single-client differential runs
//! at ~8.2 ms/txn. This probe drives the five-statement TPC-B mix
//! through the embedded SQL tier (no wire) and splits the wall time
//! across parse, plan+execute per statement, and commit, with a
//! parse-only baseline loop so the parser's share is measured directly.
//!
//! Run with:
//!   cargo run --release -p omendb --features pgwire --example sql_tier_probe -- [txns]

#![allow(clippy::disallowed_methods)]

use omendb::{RelationalBackendConfig, RelationalDatabase};
use std::env;
use std::time::Instant;

fn main() {
    let txns: usize = env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(500);

    let directory = tempfile::tempdir().expect("tempdir");
    let mut database = RelationalDatabase::create(RelationalBackendConfig {
        path: directory.path().join("db"),
        wal_first_commits: false,
        sync_class: omendb::SyncClass::KernelBarrier,
    })
    .expect("create database");

    // TPC-B shape (smaller scale for embedded speed).
    database
        .execute_sql(
            "CREATE TABLE branches (bid BIGINT PRIMARY KEY, bbalance BIGINT NOT NULL, filler TEXT NOT NULL)",
        )
        .expect("create branches");
    database
        .execute_sql(
            "CREATE TABLE tellers (tid BIGINT PRIMARY KEY, bid BIGINT NOT NULL, tbalance BIGINT NOT NULL, filler TEXT NOT NULL)",
        )
        .expect("create tellers");
    database
        .execute_sql(
            "CREATE TABLE accounts (aid BIGINT PRIMARY KEY, bid BIGINT NOT NULL, abalance BIGINT NOT NULL, filler TEXT NOT NULL)",
        )
        .expect("create accounts");
    database
        .execute_sql("CREATE INDEX accounts_bid_idx ON accounts (bid)")
        .expect("index accounts");
    database
        .execute_sql(
            "CREATE TABLE history (tid BIGINT NOT NULL, bid BIGINT NOT NULL, aid BIGINT NOT NULL, delta BIGINT NOT NULL, mtime TIMESTAMP NOT NULL, filler TEXT NOT NULL)",
        )
        .expect("create history");
    database
        .execute_sql("INSERT INTO branches VALUES (1, 0, 'branch-filler')")
        .expect("seed branch");
    for tid in 1..=10 {
        database
            .execute_sql(&format!(
                "INSERT INTO tellers VALUES ({tid}, 1, 0, 'teller-filler')"
            ))
            .expect("seed teller");
    }
    for aid in 1..=1000 {
        database
            .execute_sql(&format!(
                "INSERT INTO accounts VALUES ({aid}, 1, 0, 'account-filler-padding-0123456789')"
            ))
            .expect("seed account");
    }

    // The five statements with literal values — exactly what the wire
    // receives from a pgbench -f client (client-side substitution).
    let statements_for = |index: usize| -> Vec<String> {
        let aid = (index * 7919) % 1000 + 1;
        let delta = (index % 2001) as i64 - 1000;
        let tid = aid % 10 + 1;
        vec![
            format!("SELECT abalance FROM accounts WHERE aid = {aid}"),
            format!("UPDATE tellers SET tbalance = tbalance + {delta} WHERE tid = {tid}"),
            format!("UPDATE accounts SET abalance = abalance + {delta} WHERE aid = {aid}"),
            "UPDATE branches SET bbalance = bbalance + 7 WHERE bid = 1".to_owned(),
            format!(
                "INSERT INTO history VALUES ({tid}, 1, {aid}, {delta}, '2026-09-06 00:00:00', '')"
            ),
        ]
    };

    // Warmup.
    for index in 0..10 {
        let mut transaction = database.begin().expect("begin");
        for sql in statements_for(index) {
            transaction
                .execute_sql(&database, &sql)
                .expect("warmup statement");
        }
        transaction.commit().expect("warmup commit");
    }

    // Phase 1: full transactions through the embedded tier, commit timed,
    // plus begin timing and the engine-phase counters around each commit.
    let metrics_before = database.metrics();
    let mut commit_ms = 0.0f64;
    let mut begin_ms = 0.0f64;
    let started = Instant::now();
    for index in 0..txns {
        let begin_at = Instant::now();
        let mut transaction = database.begin().expect("begin");
        begin_ms += begin_at.elapsed().as_secs_f64() * 1000.0;
        for sql in statements_for(index) {
            transaction.execute_sql(&database, &sql).expect("statement");
        }
        let commit_at = Instant::now();
        transaction.commit().expect("commit");
        commit_ms += commit_at.elapsed().as_secs_f64() * 1000.0;
    }
    let full_elapsed = started.elapsed();
    let metrics_after = database.metrics();
    let timing = &metrics_after.publication_timing;
    let base = &metrics_before.publication_timing;
    let delta_ms = |before: u64, after: u64| (after.saturating_sub(before)) as f64 / 1_000_000.0;
    println!(
        "engine phases per txn: candidate {:.3} wal {:.3} flush {:.3} meta {:.3} admission {:.3} (ms)",
        delta_ms(base.candidate_prepare_ns, timing.candidate_prepare_ns) / txns as f64,
        delta_ms(base.wal_write_ns, timing.wal_write_ns) / txns as f64,
        delta_ms(base.data_flush_ns, timing.data_flush_ns) / txns as f64,
        delta_ms(base.metadata_write_ns, timing.metadata_write_ns) / txns as f64,
        delta_ms(base.admission_ns, timing.admission_ns) / txns as f64,
    );

    // Phase 2: parse-only baseline. describe_parameters parses the
    // statement and resolves parameters without executing; its cost is
    // (parse + resolve) per statement.
    let parse_started = Instant::now();
    let mut parse_calls = 0usize;
    for index in 0..txns {
        for sql in statements_for(index) {
            let _ = database.sql_parameter_types(&sql);
            parse_calls += 1;
        }
    }
    let parse_elapsed = parse_started.elapsed();

    println!("== SQL-tier probe: {txns} embedded TPC-B transactions (kernel class) ==");
    println!(
        "full embedded txn:     {:>8.3} ms/txn ({} statements)",
        full_elapsed.as_secs_f64() * 1000.0 / txns as f64,
        5
    );
    println!(
        "  embedded COMMIT:     {:>8.3} ms/txn",
        commit_ms / txns as f64
    );
    println!(
        "  embedded BEGIN:      {:>8.3} ms/txn",
        begin_ms / txns as f64
    );
    println!(
        "parse-resolve only:    {:>8.3} ms/statement over {parse_calls} calls",
        parse_elapsed.as_secs_f64() * 1000.0 / parse_calls as f64
    );
    println!(
        "parse share of txn:    {:>8.1}%",
        parse_elapsed.as_secs_f64() / full_elapsed.as_secs_f64() * 100.0
    );
    println!("engine wave reference: 0.223 ms/txn (wave_cost_probe, kernel class)");
}
