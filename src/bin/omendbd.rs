//! OmenDB's persistent PostgreSQL-wire daemon.
//!
//! Usage:
//!
//! ```text
//! omendbd --path DB_DIR [--bind HOST:PORT] [--max-connections N]
//!         [--statement-timeout-ms N] [--max-result-bytes N]
//! ```

#![cfg(feature = "pgwire")]

use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use omendb::pgwire_server::{RunningServer, ServerConfig};

type ParsedArgs = Option<(PathBuf, SocketAddr, usize, Option<Duration>, Option<usize>)>;

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("omendbd: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let Some((path, bind, max_connections, statement_timeout, max_result_bytes)) =
        parse_args(std::env::args().skip(1))?
    else {
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let server = RunningServer::start(
            ServerConfig::new(path, bind)
                .with_max_connections(max_connections)
                .with_statement_timeout(statement_timeout)
                .with_max_result_bytes(max_result_bytes),
        )
        .await?;
        println!("omendbd listening on {}", server.local_addr());
        std::io::stdout().flush()?;
        tokio::signal::ctrl_c().await?;
        server.shutdown().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

fn parse_args(
    mut args: impl Iterator<Item = String>,
) -> Result<ParsedArgs, Box<dyn std::error::Error>> {
    let mut path = None;
    let mut bind = "127.0.0.1:5432".parse::<SocketAddr>()?;
    let mut max_connections = 128;
    let mut statement_timeout = None;
    let mut max_result_bytes = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--path" => path = Some(PathBuf::from(required_value(&mut args, "--path")?)),
            "--bind" => bind = required_value(&mut args, "--bind")?.parse()?,
            "--max-connections" => {
                max_connections = required_value(&mut args, "--max-connections")?.parse()?;
            }
            "--statement-timeout-ms" => {
                statement_timeout = Some(Duration::from_millis(
                    required_value(&mut args, "--statement-timeout-ms")?.parse()?,
                ));
            }
            "--max-result-bytes" => {
                max_result_bytes = Some(required_value(&mut args, "--max-result-bytes")?.parse()?);
            }
            "--help" | "-h" => {
                println!(
                    "usage: omendbd --path DB_DIR [--bind HOST:PORT] [--max-connections N] [--statement-timeout-ms N] [--max-result-bytes N]"
                );
                return Ok(None);
            }
            unknown => return Err(format!("unknown argument: {unknown}").into()),
        }
    }
    let path = path.ok_or("--path is required")?;
    Ok(Some((
        path,
        bind,
        max_connections,
        statement_timeout,
        max_result_bytes,
    )))
}

fn required_value(
    args: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    args.next()
        .ok_or_else(|| format!("{option} requires a value").into())
}
