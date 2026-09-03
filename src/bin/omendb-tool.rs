//! OmenDB's logical dump/restore CLI.
//!
//! Usage:
//!
//! ```text
//! omendb-tool dump --path DB_DIR [--output FILE]     # stdout when omitted
//! omendb-tool restore --path DB_DIR [--input FILE]   # stdin when omitted
//! ```

use std::io::{Read, Write};

use omendb::{RelationalBackendConfig, RelationalDatabase};

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("omendb-tool: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let Some(command) = args.next() else {
        println!(
            "usage: omendb-tool dump --path DB_DIR [--output FILE] | restore --path DB_DIR [--input FILE]"
        );
        return Ok(());
    };
    let mut path = None;
    let mut file = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--path" => path = Some(required_value(&mut args, "--path")?),
            "--output" | "--input" => file = Some(required_value(&mut args, &argument)?),
            "--help" | "-h" => {
                println!("usage: omendb-tool {command} --path DB_DIR [--output FILE|--input FILE]");
                return Ok(());
            }
            unknown => return Err(format!("unknown argument: {unknown}").into()),
        }
    }
    let path = path.ok_or("--path is required")?;
    let config = RelationalBackendConfig::new(std::path::PathBuf::from(path));

    match command.as_str() {
        "dump" => {
            let mut database = RelationalDatabase::open(config)?;
            let dump = omendb::dump_sql(&mut database)?;
            match file {
                Some(file) => std::fs::write(file, dump)?,
                None => {
                    std::io::stdout().write_all(dump.as_bytes())?;
                    std::io::stdout().flush()?;
                }
            }
            Ok(())
        }
        "restore" => {
            // restore requires a fresh or existing-but-compatible target;
            // open refuses nothing here, so an existing database grows.
            let mut database = RelationalDatabase::open(config)?;
            let source = match file {
                Some(file) => std::fs::read_to_string(file)?,
                None => {
                    let mut buffer = String::new();
                    std::io::stdin().read_to_string(&mut buffer)?;
                    buffer
                }
            };
            omendb::restore_sql(&mut database, &source)?;
            Ok(())
        }
        unknown => Err(format!("unknown command: {unknown}").into()),
    }
}

fn required_value(
    args: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    args.next()
        .ok_or_else(|| format!("{option} requires a value").into())
}
