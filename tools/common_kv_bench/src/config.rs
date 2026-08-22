//! Command-line configuration and validation for the common-KV harness.

use super::BenchResult;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EngineKind {
    SeerDb,
    Fjall,
    RocksDb,
    Redb,
}

impl EngineKind {
    fn parse(value: &str) -> BenchResult<Self> {
        match value {
            "seerdb" => Ok(Self::SeerDb),
            "fjall" => Ok(Self::Fjall),
            "rocksdb" => Ok(Self::RocksDb),
            "redb" => Ok(Self::Redb),
            _ => Err(
                format!("unknown engine {value:?}; expected seerdb, fjall, rocksdb, or redb")
                    .into(),
            ),
        }
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::SeerDb => "seerdb",
            Self::Fjall => "fjall",
            Self::RocksDb => "rocksdb",
            Self::Redb => "redb",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WorkloadKind {
    BatchPut,
    Mixed,
    PointRead,
    RangeRead,
    YcsbA,
    YcsbB,
    YcsbC,
}

impl WorkloadKind {
    fn parse(value: &str) -> BenchResult<Self> {
        match value {
            "batch-put" => Ok(Self::BatchPut),
            "mixed" => Ok(Self::Mixed),
            "point-read" => Ok(Self::PointRead),
            "ycsb-a" => Ok(Self::YcsbA),
            "ycsb-b" => Ok(Self::YcsbB),
            "ycsb-c" => Ok(Self::YcsbC),
            "range-read" => Ok(Self::RangeRead),
            _ => Err(format!(
                "unknown workload {value:?}; expected batch-put, mixed, point-read, range-read, ycsb-a, ycsb-b, or ycsb-c"
            )
            .into()),
        }
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::BatchPut => "batch-put",
            Self::Mixed => "mixed",
            Self::PointRead => "point-read",
            Self::RangeRead => "range-read",
            Self::YcsbA => "ycsb-a",
            Self::YcsbB => "ycsb-b",
            Self::YcsbC => "ycsb-c",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DurabilityMode {
    Durable,
    Buffered,
}

impl DurabilityMode {
    fn parse(value: &str) -> BenchResult<Self> {
        match value {
            "durable" => Ok(Self::Durable),
            "buffered" => Ok(Self::Buffered),
            _ => Err(format!("unknown durability {value:?}; expected durable or buffered").into()),
        }
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::Buffered => "buffered",
        }
    }

    #[cfg(any(feature = "fjall", feature = "rocksdb", feature = "redb"))]
    pub(super) fn sync_writes(self) -> bool {
        matches!(self, Self::Durable)
    }
}

#[derive(Debug, Clone)]
pub(super) struct Config {
    pub(super) engine: EngineKind,
    pub(super) workload: WorkloadKind,
    pub(super) path: PathBuf,
    pub(super) output: Option<PathBuf>,
    pub(super) trace_output: Option<PathBuf>,
    pub(super) keys: usize,
    pub(super) operations: usize,
    pub(super) batch_size: usize,
    pub(super) value_bytes: usize,
    pub(super) range_width: usize,
    pub(super) seed: u64,
    pub(super) durability: DurabilityMode,
    pub(super) open_existing: bool,
    pub(super) vacuum: bool,
    pub(super) base_operations: usize,
    pub(super) progress: Option<PathBuf>,
    pub(super) progress_hold: Option<PathBuf>,
    pub(super) progress_hold_index: Option<usize>,
    pub(super) verify_prefix: Option<usize>,
}

impl Config {
    pub(super) fn parse() -> BenchResult<Self> {
        let args: Vec<String> = std::env::args().skip(1).collect();
        if args.iter().any(|arg| arg == "--help" || arg == "-h") {
            print_help();
            std::process::exit(0);
        }

        let mut engine = EngineKind::SeerDb;
        let mut workload = WorkloadKind::Mixed;
        let mut path = None;
        let mut output = None;
        let mut trace_output = None;
        let mut keys = 1_000;
        let mut operations = 4_000;
        let mut batch_size = 16;
        let mut value_bytes = 128;
        let mut range_width = 32;
        let mut seed = 7;
        let mut durability = DurabilityMode::Durable;
        let mut open_existing = false;
        let mut vacuum = false;
        let mut base_operations = 0;
        let mut progress = None;
        let mut progress_hold = None;
        let mut progress_hold_index = None;
        let mut verify_prefix = None;

        let mut index = 0;
        while index < args.len() {
            let flag = &args[index];
            if flag == "--sync" {
                durability = DurabilityMode::Durable;
                index += 1;
                continue;
            }
            if flag == "--open-existing" {
                open_existing = true;
                continue;
            }
            if flag == "--vacuum" {
                vacuum = true;
                index += 1;
                continue;
            }
            let value = args
                .get(index + 1)
                .ok_or_else(|| format!("missing value for {flag}"))?;
            match flag.as_str() {
                "--engine" => engine = EngineKind::parse(value)?,
                "--workload" => workload = WorkloadKind::parse(value)?,
                "--path" => path = Some(PathBuf::from(value)),
                "--output" => output = Some(PathBuf::from(value)),
                "--trace-output" => trace_output = Some(PathBuf::from(value)),
                "--keys" => keys = parse_usize(flag, value)?,
                "--operations" => operations = parse_usize(flag, value)?,
                "--batch-size" => batch_size = parse_usize(flag, value)?,
                "--value-bytes" => value_bytes = parse_usize(flag, value)?,
                "--range-width" => range_width = parse_usize(flag, value)?,
                "--seed" => {
                    seed = value
                        .parse()
                        .map_err(|_| format!("invalid {flag}: {value}"))?
                }
                "--durability" => durability = DurabilityMode::parse(value)?,
                "--base-operations" => base_operations = parse_usize(flag, value)?,
                "--progress" => progress = Some(PathBuf::from(value)),
                "--progress-hold" => progress_hold = Some(PathBuf::from(value)),
                "--progress-hold-index" => progress_hold_index = Some(parse_usize(flag, value)?),
                "--verify-prefix" => verify_prefix = Some(parse_usize(flag, value)?),
                _ => return Err(format!("unknown argument {flag:?}; use --help").into()),
            }
            index += 2;
        }

        if keys == 0 || operations == 0 || batch_size == 0 || range_width == 0 || value_bytes == 0 {
            return Err(
                "keys, operations, batch-size, value-bytes, and range-width must be non-zero"
                    .into(),
            );
        }

        if progress_hold.is_some() != progress_hold_index.is_some() {
            return Err(
                "--progress-hold and --progress-hold-index must be provided together".into(),
            );
        }
        if progress_hold.is_some() && progress.is_none() {
            return Err("--progress is required when a progress hold is configured".into());
        }
        if base_operations > 0 && !open_existing {
            return Err("--base-operations requires --open-existing".into());
        }
        if open_existing && workload != WorkloadKind::BatchPut {
            return Err("--open-existing currently requires --workload batch-put".into());
        }
        if verify_prefix.is_some() && (progress.is_some() || progress_hold.is_some()) {
            return Err("--verify-prefix cannot be combined with progress controls".into());
        }
        if let Some(prefix) = verify_prefix {
            if workload != WorkloadKind::BatchPut {
                return Err("--verify-prefix requires --workload batch-put".into());
            }
            if prefix > operations || prefix % batch_size != 0 {
                return Err(
                    "--verify-prefix must be a batch boundary between zero and operations".into(),
                );
            }
        }

        let path = path.unwrap_or_else(|| {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or_default();
            std::env::temp_dir().join(format!("seerdb-common-kv-{}-{nanos}", engine.name()))
        });

        Ok(Self {
            engine,
            workload,
            path,
            output,
            trace_output,
            keys,
            operations,
            batch_size,
            value_bytes,
            range_width,
            seed,
            durability,
            open_existing,
            vacuum,
            base_operations,
            progress,
            progress_hold,
            progress_hold_index,
            verify_prefix,
        })
    }
}

fn parse_usize(flag: &str, value: &str) -> BenchResult<usize> {
    value
        .parse()
        .map_err(|_| format!("invalid {flag}: {value}").into())
}

fn print_help() {
    println!(
        "Common ordered-KV comparison harness\n\n\
         --engine seerdb|fjall|rocksdb\n\
         --workload batch-put|mixed|point-read|range-read|ycsb-a|ycsb-b|ycsb-c\n\
         --path PATH             Fresh or empty database directory\n\
         --output PATH            Also write the versioned JSON result artifact\n\
         --trace-output PATH      Write the exact versioned generated operation trace\n\
         --keys N                 Initial/key-space size (default 1000)\n\
         --operations N           Measured operation count (default 4000)\n\
         --batch-size N           Write batch size (default 16)\n\
         --value-bytes N          Value size (default 128)\n\
         --range-width N          Range width in keys (default 32)\n\
         --seed N                 Deterministic trace seed (default 7)\n\
         --durability durable|buffered\n\
         --vacuum                  SeerDB only: reclaim dead pages after workload\n\
                                  Durability mode (default durable)\n\
         --sync                   Alias for --durability durable\n\n\
         --open-existing          Open an existing database for a mutation phase\n\
         --base-operations N      Existing deterministic batch prefix already durable\n\n\
         --progress PATH          Write the next batch/operation boundary here\n\
         --progress-hold PATH     Wait for this path at --progress-hold-index\n\
         --progress-hold-index N  Hold before publishing boundary N\n\
         --verify-prefix N        Open an existing batch-put DB and verify prefix N\n\n\
         Workloads use the same generated trace for every engine. Each run\n\
         verifies the oracle, closes, reopens, and verifies the final digest.\n\
         SeerDB common-KV runs support durable mode only; buffered mode is\n\
         useful for peer-only diagnostics and is not a matched SeerDB run."
    );
}
