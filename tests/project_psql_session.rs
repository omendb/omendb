//! Interactive psql session matrix against a live omendbd.
//!
//! Real clients do not only run SQL: psql drives catalog introspection
//! (\dt, \d), session functions, and error recovery in the same
//! session. This suite pins the alpha contract for that surface: plain
//! SQL works, unsupported meta-commands fail with an honest error and
//! leave the session usable, and capability functions (version(),
//! current_schema()) answer so clients can detect what they are talking
//! to. It requires the `psql` binary on PATH and is skipped otherwise;
//! CI pins the PostgreSQL 17 client.

#![cfg(feature = "pgwire")]

use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStdout, Command, Stdio};

use tempfile::tempdir;

/// Kill the daemon even when an assertion fails.
struct DaemonGuard(Option<Child>);

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Wait for the daemon's readiness banner and return the bound port.
fn daemon_port(stdout: &mut ChildStdout) -> u32 {
    let mut line = String::new();
    BufReader::new(stdout)
        .read_line(&mut line)
        .expect("daemon banner");
    line.strip_prefix("omendbd listening on 127.0.0.1:")
        .and_then(|rest| rest.trim().parse().ok())
        .unwrap_or_else(|| panic!("unexpected banner: {line:?}"))
}

/// Run psql with -c commands against the daemon; return (exit code,
/// combined output).
fn psql(port: u32, commands: &[&str]) -> (i32, String) {
    let url = format!("host=127.0.0.1 port={port} user=omendb");
    let output = Command::new("psql")
        .arg("-d")
        .arg(&url)
        .args(commands.iter().flat_map(|command| ["-c", command]))
        .output()
        .expect("spawn psql");
    let mut combined = String::from_utf8_lossy(&output.stdout).into_owned();
    combined.push_str(&String::from_utf8_lossy(&output.stderr));
    (output.status.code().unwrap_or(-1), combined)
}

#[test]
fn psql_session_matrix_matches_alpha_contract() {
    if !Command::new("psql")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
    {
        eprintln!("psql not found on PATH; skipping the live session matrix");
        return;
    }

    let directory = tempdir().expect("tempdir");
    let database_path = directory.path().join("db");
    let mut daemon = DaemonGuard(Some(
        Command::new(env!("CARGO_BIN_EXE_omendbd"))
            .args([
                "--path",
                database_path.to_str().expect("database path"),
                "--bind",
                "127.0.0.1:0",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn omendbd"),
    ));
    let mut stdout = daemon
        .0
        .as_mut()
        .expect("daemon")
        .stdout
        .take()
        .expect("stdout");
    let port = daemon_port(&mut stdout);

    // Plain SQL: create, write, read, and aggregate in one session.
    let (code, out) = psql(
        port,
        &[
            "CREATE TABLE accounts (id BIGINT PRIMARY KEY, balance BIGINT)",
            "INSERT INTO accounts VALUES (1, 100), (2, 40)",
            "SELECT sum(balance) FROM accounts",
        ],
    );
    assert_eq!(code, 0, "plain SQL session: {out}");
    assert!(out.contains("140"), "sum reads back: {out}");

    // Capability detection: version() names OmenDB, not PostgreSQL.
    let (code, out) = psql(port, &["SELECT version()"]);
    assert_eq!(code, 0, "version(): {out}");
    assert!(
        out.contains("OmenDB"),
        "version() must name the product honestly: {out}"
    );

    // Session defaults a client shows on connect.
    let (code, out) = psql(port, &["SELECT current_schema()"]);
    assert_eq!(code, 0, "current_schema(): {out}");
    assert!(out.contains("public"), "current_schema(): {out}");

    // SHOW answers the compatibility parameters clients probe first.
    let (code, out) = psql(port, &["SHOW server_version"]);
    assert_eq!(code, 0, "SHOW server_version: {out}");

    // Unsupported introspection: honest error, session stays usable.
    let (code, out) = psql(port, &[r"\dt"]);
    assert_ne!(
        code, 0,
        r"\dt must fail while pg_catalog is unimplemented: {out}"
    );
    assert!(
        out.contains("feature not supported"),
        r"\dt must fail as an unsupported feature, not a protocol error: {out}"
    );

    // An unsupported meta-command does not poison the session: a
    // subsequent statement in the SAME session still executes.
    let (code, out) = psql(port, &[r"\l", "SELECT count(*) FROM accounts"]);
    assert_eq!(code, 0, r"session survives \l failure: {out}");
    assert!(out.contains('2'), r"statement after \l still runs: {out}");

    // Transaction blocks across statements.
    let (code, out) = psql(
        port,
        &[
            "BEGIN",
            "INSERT INTO accounts VALUES (3, 5)",
            "SELECT count(*) FROM accounts",
            "ROLLBACK",
        ],
    );
    assert_eq!(code, 0, "explicit block: {out}");
    assert!(
        out.contains('3'),
        "the in-block write is visible before rollback: {out}"
    );
    let (code, out) = psql(port, &["SELECT count(*) FROM accounts"]);
    assert_eq!(code, 0, "post-rollback read: {out}");
    assert!(
        out.contains('2') && !out.contains('3'),
        "rollback discards the block write: {out}"
    );
}
