//! Attribute the wire-tier cost that dominates a committed transaction.
//!
//! The 2026-09-06 attribution chain: engine wave 223 us/txn (kernel
//! class, wave_cost_probe), embedded SQL tier 1.161 ms/txn
//! (sql_tier_probe, parse only 2.2%), yet the full-stack single-client
//! differential runs at ~8.2 ms/txn. This probe closes the gap: it
//! drives the same five-statement TPC-B mix through the real wire
//! (tokio-postgres against a kernel-class omendbd) and reports the
//! per-transaction round-trip cost without pgbench in the loop.
//!
//! Run with:
//!   cargo run --release -p omendb --features pgwire --example wire_tier_probe -- [txns]

#![allow(clippy::disallowed_methods)]

use omendb::pgwire_server;
use std::env;
use std::time::Instant;
use tokio_postgres::NoTls;

fn statements_for(index: usize) -> Vec<String> {
    let aid = (index * 7919) % 1000 + 1;
    let delta = (index % 2001) as i64 - 1000;
    let tid = aid % 10 + 1;
    vec![
        format!("SELECT abalance FROM accounts WHERE aid = {aid}"),
        format!("UPDATE tellers SET tbalance = tbalance + {delta} WHERE tid = {tid}"),
        format!("UPDATE accounts SET abalance = abalance + {delta} WHERE aid = {aid}"),
        "UPDATE branches SET bbalance = bbalance + 7 WHERE bid = 1".to_owned(),
        format!("INSERT INTO history VALUES ({tid}, 1, {aid}, {delta}, '2026-09-06 00:00:00', '')"),
    ]
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let txns: usize = env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(300);

    let directory = tempfile::tempdir()?;
    let config = pgwire_server::ServerConfig::new(
        directory.path().join("db"),
        "127.0.0.1:0".parse().expect("addr"),
    )
    .with_sync_class(omendb::SyncClass::KernelBarrier);
    let server = pgwire_server::RunningServer::start(config).await?;
    let port = server.local_addr().port();

    let (client, connection) =
        tokio_postgres::connect(&format!("host=127.0.0.1 port={port} user=omendb"), NoTls).await?;
    let connection_task = tokio::spawn(connection);

    for sql in [
        "CREATE TABLE branches (bid BIGINT PRIMARY KEY, bbalance BIGINT NOT NULL, filler TEXT NOT NULL)",
        "CREATE TABLE tellers (tid BIGINT PRIMARY KEY, bid BIGINT NOT NULL, tbalance BIGINT NOT NULL, filler TEXT NOT NULL)",
        "CREATE TABLE accounts (aid BIGINT PRIMARY KEY, bid BIGINT NOT NULL, abalance BIGINT NOT NULL, filler TEXT NOT NULL)",
        "CREATE INDEX accounts_bid_idx ON accounts (bid)",
        "CREATE TABLE history (tid BIGINT NOT NULL, bid BIGINT NOT NULL, aid BIGINT NOT NULL, delta BIGINT NOT NULL, mtime TIMESTAMP NOT NULL, filler TEXT NOT NULL)",
        "INSERT INTO branches VALUES (1, 0, 'branch-filler')",
    ] {
        client.simple_query(sql).await?;
    }
    for tid in 1..=10 {
        client
            .simple_query(&format!(
                "INSERT INTO tellers VALUES ({tid}, 1, 0, 'teller-filler')"
            ))
            .await?;
    }
    for aid in 1..=1000 {
        client
            .simple_query(&format!(
                "INSERT INTO accounts VALUES ({aid}, 1, 0, 'account-filler-padding-0123456789')"
            ))
            .await?;
    }

    // Warmup.
    for index in 0..10 {
        client.simple_query("BEGIN").await?;
        for sql in statements_for(index) {
            client.simple_query(&sql).await?;
        }
        client.simple_query("COMMIT").await?;
    }

    // Phase 1: extended protocol with fresh literal SQL each execution —
    // exactly what pgbench -f sends (client-side substitution).
    let started = Instant::now();
    for index in 0..txns {
        client.simple_query("BEGIN").await?;
        for sql in statements_for(index) {
            // Extended protocol: Parse/Bind/Describe/Execute/Sync per
            // call, same as a prepared pgbench -f substitution.
            let rows = client.query(&sql, &[]).await?;
            let _ = rows;
        }
        client.simple_query("COMMIT").await?;
    }
    let extended = started.elapsed();

    // Phase 2: simple protocol, same mix (one round trip per statement),
    // with per-statement attribution.
    let mut statement_ms = [0.0f64; 5];
    let mut begin_ms = 0.0f64;
    let mut commit_ms = 0.0f64;
    let started = Instant::now();
    for index in 0..txns {
        let begin_at = Instant::now();
        client.simple_query("BEGIN").await?;
        begin_ms += begin_at.elapsed().as_secs_f64() * 1000.0;
        for (position, sql) in statements_for(index).into_iter().enumerate() {
            let each = Instant::now();
            client.simple_query(&sql).await?;
            statement_ms[position] += each.elapsed().as_secs_f64() * 1000.0;
        }
        let commit_at = Instant::now();
        client.simple_query("COMMIT").await?;
        commit_ms += commit_at.elapsed().as_secs_f64() * 1000.0;
    }
    let simple = started.elapsed();
    let names = [
        "SELECT pk-lookup",
        "UPDATE tellers",
        "UPDATE accounts",
        "UPDATE branches",
        "INSERT history",
    ];
    for (position, ms) in statement_ms.iter().enumerate() {
        println!(
            "  {:<20} {:>7.3} ms/stmt",
            names[position],
            ms / txns as f64
        );
    }
    println!("  {:<20} {:>7.3} ms/txn", "BEGIN", begin_ms / txns as f64);
    println!("  {:<20} {:>7.3} ms/txn", "COMMIT", commit_ms / txns as f64);

    // Phase 3: round-trip overhead in isolation — BEGIN/COMMIT no-op
    // pairs carry no statement work, so their cost is pure framing +
    // handler + TCP + task scheduling.
    let started = Instant::now();
    for _ in 0..txns {
        client.simple_query("BEGIN").await?;
        client.simple_query("COMMIT").await?;
    }
    let noop = started.elapsed();

    // Phase 4: one trivial SELECT round trip (PK lookup, one row).
    let started = Instant::now();
    for index in 0..txns {
        let aid = (index * 7919) % 1000 + 1;
        client
            .simple_query(&format!("SELECT abalance FROM accounts WHERE aid = {aid}"))
            .await?;
    }
    let single_select = started.elapsed();

    println!("== wire-tier probe: {txns} TPC-B transactions, kernel class ==");
    println!(
        "extended (fresh SQL): {:>8.3} ms/txn",
        extended.as_secs_f64() * 1000.0 / txns as f64
    );
    println!(
        "simple protocol:      {:>8.3} ms/txn",
        simple.as_secs_f64() * 1000.0 / txns as f64
    );
    println!(
        "BEGIN+COMMIT no-ops:  {:>8.3} ms/pair (pure round-trip overhead)",
        noop.as_secs_f64() * 1000.0 / txns as f64
    );
    println!(
        "one SELECT round trip: {:>7.3} ms",
        single_select.as_secs_f64() * 1000.0 / txns as f64
    );
    println!("embedded reference:     1.161 ms/txn (sql_tier_probe)");
    println!("engine wave reference:  0.223 ms/txn (wave_cost_probe)");

    drop(client);
    connection_task.await??;
    Ok(())
}
