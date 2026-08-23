use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use omendb::{Database, DatabaseConfig, DbError, FailOnce, FaultPoint, Key, Mutation, NoFaults};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const DEFAULT_SCHEDULE: &str = "examples/fixtures/r0-failure-schedule.jsonl";

#[derive(Debug, Deserialize)]
struct FailureCase {
    case: String,
    phase: String,
    inject: String,
    trace_seq: u64,
    expected: String,
}

fn main() -> Result<()> {
    let schedule_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_SCHEDULE));
    let schedule_bytes = fs::read(&schedule_path)
        .with_context(|| format!("read failure schedule {}", schedule_path.display()))?;
    let schedule = parse_schedule(&schedule_bytes, &schedule_path)?;
    ensure!(
        !schedule.is_empty(),
        "failure schedule {} is empty",
        schedule_path.display()
    );

    let mut results = Vec::with_capacity(schedule.len());
    for case in &schedule {
        let result = match case.phase.as_str() {
            "commit" => run_commit_case(case)?,
            "checkpoint" => run_checkpoint_case(case)?,
            "recovery" => run_corruption_case(case)?,
            phase => bail!("{}: unsupported failure phase {phase}", case.case),
        };
        results.push(result);
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schedule": schedule_path,
            "schedule_sha256": hex_encode(&Sha256::digest(&schedule_bytes)),
            "cases": results,
        }))?
    );
    Ok(())
}

fn parse_schedule(bytes: &[u8], path: &Path) -> Result<Vec<FailureCase>> {
    let text = std::str::from_utf8(bytes)
        .with_context(|| format!("failure schedule {} is not UTF-8", path.display()))?;
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(line_no, line)| {
            let case: FailureCase = serde_json::from_str(line)
                .with_context(|| format!("parse {} line {}", path.display(), line_no + 1))?;
            ensure!(
                !case.case.is_empty(),
                "schedule line {} has no case",
                line_no + 1
            );
            ensure!(!case.phase.is_empty(), "{} has no phase", case.case);
            ensure!(
                !case.inject.is_empty(),
                "{} has no injection point",
                case.case
            );
            ensure!(
                case.trace_seq > 0,
                "{} has invalid trace sequence",
                case.case
            );
            ensure!(
                !case.expected.is_empty(),
                "{} has no expected outcome",
                case.case
            );
            Ok(case)
        })
        .collect()
}

fn run_commit_case(case: &FailureCase) -> Result<Value> {
    let point = fault_point(&case.inject)?;
    ensure!(
        matches!(
            point,
            FaultPoint::BeforeWalAppend
                | FaultPoint::AfterWalAppend
                | FaultPoint::WalSync
                | FaultPoint::AfterWalSync
        ),
        "{}: {} is not a commit fault",
        case.case,
        case.inject
    );
    let directory = tempfile::tempdir()?;
    let config = DatabaseConfig {
        directory: directory.path().to_path_buf(),
    };
    let mut database = Database::create(config.clone())?;
    let result = database.commit(
        vec![Mutation::Put {
            key: Key::new(1, 1),
            value: b"one".to_vec(),
        }],
        &mut FailOnce::at([point]),
    );
    ensure!(
        result.is_err(),
        "{}: injected commit fault did not fail",
        case.case
    );
    drop(database);
    let recovered = Database::open(config, &mut NoFaults)?;
    let commit_id = recovered.commit_id().0;
    let (state_digest, state_valid) = state_validation(&recovered)?;
    let commit_allowed = match point {
        FaultPoint::BeforeWalAppend => commit_id == 0,
        FaultPoint::AfterWalAppend | FaultPoint::WalSync => commit_id <= 1,
        FaultPoint::AfterWalSync => commit_id == 1,
        _ => false,
    };
    Ok(result_json(
        case,
        json!({
            "recovered_commit_id": commit_id,
            "state_digest": state_digest,
            "state_valid": state_valid,
            "accepted": commit_allowed && state_valid,
        }),
    ))
}

fn run_checkpoint_case(case: &FailureCase) -> Result<Value> {
    let point = fault_point(&case.inject)?;
    ensure!(
        matches!(
            point,
            FaultPoint::DataSync
                | FaultPoint::PackedPageSync
                | FaultPoint::ManifestSync
                | FaultPoint::AfterManifestPublish
                | FaultPoint::WalTruncate
                | FaultPoint::ShortWrite
                | FaultPoint::TornWrite
        ),
        "{}: {} is not a checkpoint fault",
        case.case,
        case.inject
    );
    let directory = tempfile::tempdir()?;
    let config = DatabaseConfig {
        directory: directory.path().to_path_buf(),
    };
    let mut database = Database::create(config.clone())?;
    database.commit(
        vec![Mutation::Put {
            key: Key::new(1, 1),
            value: b"one".to_vec(),
        }],
        &mut NoFaults,
    )?;
    let result = database.checkpoint(&mut FailOnce::at([point]));
    ensure!(
        result.is_err(),
        "{}: injected checkpoint fault did not fail",
        case.case
    );
    drop(database);
    let recovered = Database::open(config, &mut NoFaults)?;
    let (state_digest, state_valid) = state_validation(&recovered)?;
    Ok(result_json(
        case,
        json!({
            "recovered_commit_id": recovered.commit_id().0,
            "state_digest": state_digest,
            "state_valid": state_valid && recovered.commit_id().0 == 1,
            "accepted": state_valid && recovered.commit_id().0 == 1,
        }),
    ))
}

fn run_corruption_case(case: &FailureCase) -> Result<Value> {
    let manifest = match case.inject.as_str() {
        "corrupt-wal-byte" => false,
        "corrupt-manifest-byte" => true,
        inject => bail!("{}: unsupported recovery injection {inject}", case.case),
    };
    let directory = tempfile::tempdir()?;
    let config = DatabaseConfig {
        directory: directory.path().to_path_buf(),
    };
    let mut database = Database::create(config.clone())?;
    database.commit(
        vec![Mutation::Put {
            key: Key::new(1, 1),
            value: b"one".to_vec(),
        }],
        &mut NoFaults,
    )?;
    if manifest {
        database.checkpoint(&mut NoFaults)?;
        corrupt_byte(&config.directory.join("omendb.manifest"))?;
    } else {
        corrupt_byte(&config.directory.join("omendb.wal"))?;
    }
    drop(database);
    let error = Database::open(config, &mut NoFaults).expect_err("corruption must refuse recovery");
    let refused = matches!(error, DbError::Corruption { .. });
    Ok(result_json(
        case,
        json!({
            "refused": refused,
            "error": error.to_string(),
        }),
    ))
}

fn result_json(case: &FailureCase, result: Value) -> Value {
    let mut object = serde_json::Map::new();
    object.insert("case".to_owned(), json!(case.case));
    object.insert("phase".to_owned(), json!(case.phase));
    object.insert("inject".to_owned(), json!(case.inject));
    object.insert("trace_seq".to_owned(), json!(case.trace_seq));
    object.insert("expected".to_owned(), json!(case.expected));
    if let Value::Object(fields) = result {
        object.extend(fields);
    }
    Value::Object(object)
}

fn fault_point(value: &str) -> Result<FaultPoint> {
    match value {
        "BeforeWalAppend" => Ok(FaultPoint::BeforeWalAppend),
        "AfterWalAppend" => Ok(FaultPoint::AfterWalAppend),
        "WalSync" => Ok(FaultPoint::WalSync),
        "AfterWalSync" => Ok(FaultPoint::AfterWalSync),
        "DataSync" => Ok(FaultPoint::DataSync),
        "PackedPageSync" => Ok(FaultPoint::PackedPageSync),
        "ManifestSync" => Ok(FaultPoint::ManifestSync),
        "AfterManifestPublish" => Ok(FaultPoint::AfterManifestPublish),
        "WalTruncate" => Ok(FaultPoint::WalTruncate),
        "ShortWrite" => Ok(FaultPoint::ShortWrite),
        "TornWrite" => Ok(FaultPoint::TornWrite),
        value => bail!("unsupported fault point {value}"),
    }
}

fn corrupt_byte(path: &Path) -> Result<()> {
    let mut bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let offset = bytes
        .len()
        .checked_sub(1)
        .with_context(|| format!("artifact {} is empty", path.display()))?;
    bytes[offset] ^= 1;
    fs::write(path, bytes).with_context(|| format!("rewrite {}", path.display()))?;
    Ok(())
}

fn state_validation(database: &Database) -> Result<(String, bool)> {
    let key = Key::new(1, 1);
    let value = database.get(database.commit_id(), key)?;
    let canonical = value
        .as_deref()
        .map(|value| format!("1:1:{}\n", hex_encode(value)))
        .unwrap_or_default();
    let digest = hex_encode(&Sha256::digest(canonical.as_bytes()));
    let expected = match database.commit_id().0 {
        0 => hex_encode(&Sha256::digest([])),
        1 => hex_encode(&Sha256::digest(b"1:1:6f6e65\n")),
        _ => String::new(),
    };
    let valid = digest == expected;
    Ok((digest, valid))
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
