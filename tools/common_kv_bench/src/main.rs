use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod engine;
mod trace;

use engine::Engine;
use trace::{
    Operation, apply_oracle, digest, generate_operations, initial_state, render_trace, trace_digest,
};

type BenchResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EngineKind {
    SeerDb,
    Fjall,
    RocksDb,
}

impl EngineKind {
    fn parse(value: &str) -> BenchResult<Self> {
        match value {
            "seerdb" => Ok(Self::SeerDb),
            "fjall" => Ok(Self::Fjall),
            "rocksdb" => Ok(Self::RocksDb),
            _ => {
                Err(format!("unknown engine {value:?}; expected seerdb, fjall, or rocksdb").into())
            }
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::SeerDb => "seerdb",
            Self::Fjall => "fjall",
            Self::RocksDb => "rocksdb",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkloadKind {
    BatchPut,
    Mixed,
    PointRead,
    RangeRead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurabilityMode {
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

    fn name(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::Buffered => "buffered",
        }
    }

    #[cfg(any(feature = "fjall", feature = "rocksdb"))]
    fn sync_writes(self) -> bool {
        matches!(self, Self::Durable)
    }
}

impl WorkloadKind {
    fn parse(value: &str) -> BenchResult<Self> {
        match value {
            "batch-put" => Ok(Self::BatchPut),
            "mixed" => Ok(Self::Mixed),
            "point-read" => Ok(Self::PointRead),
            "range-read" => Ok(Self::RangeRead),
            _ => Err(format!(
                "unknown workload {value:?}; expected batch-put, mixed, point-read, or range-read"
            )
            .into()),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::BatchPut => "batch-put",
            Self::Mixed => "mixed",
            Self::PointRead => "point-read",
            Self::RangeRead => "range-read",
        }
    }
}

#[derive(Debug, Clone)]
struct Config {
    engine: EngineKind,
    workload: WorkloadKind,
    path: PathBuf,
    output: Option<PathBuf>,
    trace_output: Option<PathBuf>,
    keys: usize,
    operations: usize,
    batch_size: usize,
    value_bytes: usize,
    range_width: usize,
    seed: u64,
    durability: DurabilityMode,
    open_existing: bool,
    base_operations: usize,
    progress: Option<PathBuf>,
    progress_hold: Option<PathBuf>,
    progress_hold_index: Option<usize>,
    verify_prefix: Option<usize>,
}

impl Config {
    fn parse() -> BenchResult<Self> {
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
         --workload batch-put|mixed|point-read|range-read\n\
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

#[derive(Debug, Default, Clone, Copy)]
struct SeerCounters {
    physical_page_writes: u64,
    page_bytes_written: u64,
    generation_flushes: u64,
    data_syncs: u64,
    wal_bytes_written: u64,
    metadata_bytes_written: u64,
    blob_bytes_written: u64,
    history_bytes_written: u64,
    manifest_bytes_written: u64,
    reclaimed_bytes: u64,
    candidate_prepare_ns: u64,
    wal_write_ns: u64,
    admission_ns: u64,
    data_flush_ns: u64,
    metadata_write_ns: u64,
    blob_write_ns: u64,
    history_write_ns: u64,
    directory_sync_ns: u64,
    manifest_write_ns: u64,
    manifest_mirror_ns: u64,
    cleanup_ns: u64,
}

#[derive(Debug, Default, Clone, Copy)]
struct ResourceMetrics {
    user_cpu_ns: u128,
    system_cpu_ns: u128,
    max_rss_bytes: Option<u64>,
}

#[cfg(unix)]
fn timeval_to_ns(value: libc::timeval) -> u128 {
    let seconds = u128::try_from(value.tv_sec).unwrap_or(0);
    let micros = u128::try_from(value.tv_usec).unwrap_or(0);
    seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(micros.saturating_mul(1_000))
}

#[cfg(unix)]
fn process_resource_metrics() -> ResourceMetrics {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `getrusage` writes the complete `rusage` structure for the
    // requested process and the pointer refers to valid writable storage.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return ResourceMetrics::default();
    }
    // SAFETY: A successful `getrusage` call initialized every field.
    let usage = unsafe { usage.assume_init() };
    let max_rss = u64::try_from(usage.ru_maxrss).ok().and_then(|rss| {
        if cfg!(target_os = "linux") {
            rss.checked_mul(1024)
        } else {
            Some(rss)
        }
    });
    ResourceMetrics {
        user_cpu_ns: timeval_to_ns(usage.ru_utime),
        system_cpu_ns: timeval_to_ns(usage.ru_stime),
        max_rss_bytes: max_rss,
    }
}

#[cfg(not(unix))]
fn process_resource_metrics() -> ResourceMetrics {
    ResourceMetrics::default()
}

fn resource_delta(before: ResourceMetrics, after: ResourceMetrics) -> ResourceMetrics {
    ResourceMetrics {
        user_cpu_ns: after.user_cpu_ns.saturating_sub(before.user_cpu_ns),
        system_cpu_ns: after.system_cpu_ns.saturating_sub(before.system_cpu_ns),
        max_rss_bytes: after.max_rss_bytes,
    }
}

#[derive(Debug, Default)]
struct LatencyStats {
    p50_ns: u128,
    p95_ns: u128,
    p99_ns: u128,
    max_ns: u128,
}

fn latency_stats(latencies: &mut [u128]) -> LatencyStats {
    if latencies.is_empty() {
        return LatencyStats::default();
    }
    latencies.sort_unstable();
    let percentile = |numerator: usize, denominator: usize| {
        let index = (latencies.len() * numerator)
            .div_ceil(denominator)
            .saturating_sub(1);
        latencies[index]
    };
    LatencyStats {
        p50_ns: percentile(50, 100),
        p95_ns: percentile(95, 100),
        p99_ns: percentile(99, 100),
        max_ns: *latencies.last().unwrap_or(&0),
    }
}

#[derive(Debug, Default)]
struct RunCounters {
    measured_operations: usize,
    writes: usize,
    reads: usize,
    ranges: usize,
    deletes: usize,
    write_batches: usize,
    max_write_batch_size: usize,
}

fn is_write_operation(operation: &Operation) -> bool {
    matches!(operation, Operation::Put { .. } | Operation::Delete { .. })
}

fn next_write_batch_end(operations: &[Operation], start: usize, batch_size: usize) -> usize {
    let mut end = start;
    while end < operations.len()
        && end.saturating_sub(start) < batch_size
        && is_write_operation(&operations[end])
    {
        end += 1;
    }
    end
}

#[derive(Debug)]
struct RunResult {
    config: Config,
    preload_ns: u128,
    workload_ns: u128,
    reopen_ns: u128,
    resources: ResourceMetrics,
    latency: LatencyStats,
    write_batch_latency: LatencyStats,
    counters: RunCounters,
    logical_bytes: u64,
    final_keys: usize,
    digest: u64,
    trace_digest: u64,
    disk_bytes: u64,
    seer_counters: Option<SeerCounters>,
}

fn verify_get(engine: &Engine, oracle: &BTreeMap<Vec<u8>, Vec<u8>>, key: &[u8]) -> BenchResult<()> {
    let expected = oracle.get(key).cloned();
    let actual = engine.get(key)?;
    if actual != expected {
        return Err(format!("get mismatch for key {:?}", String::from_utf8_lossy(key)).into());
    }
    Ok(())
}

fn verify_range(
    engine: &Engine,
    oracle: &BTreeMap<Vec<u8>, Vec<u8>>,
    start: &[u8],
    end: &[u8],
) -> BenchResult<()> {
    let expected = oracle
        .range(start.to_vec()..end.to_vec())
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    let actual = engine.range(start, end)?;
    if actual != expected {
        return Err(format!("range mismatch for [{:?}, {:?})", start, end).into());
    }
    Ok(())
}

fn apply_initial_state(
    engine: &mut Engine,
    oracle: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    initial: &[(Vec<u8>, Vec<u8>)],
    batch_size: usize,
) -> BenchResult<()> {
    for chunk in initial.chunks(batch_size) {
        let operations = chunk
            .iter()
            .map(|(key, value)| Operation::Put {
                key: key.clone(),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        engine.write_batch(&operations)?;
        for operation in &operations {
            apply_oracle(oracle, operation);
        }
    }
    Ok(())
}

fn logical_bytes_for_initial_state(initial: &[(Vec<u8>, Vec<u8>)]) -> u64 {
    initial
        .iter()
        .map(|(key, value)| (key.len() + value.len()) as u64)
        .sum()
}

fn publish_progress_boundary(
    progress: Option<&Path>,
    progress_hold: Option<&Path>,
    progress_hold_index: Option<usize>,
    index: usize,
) -> BenchResult<()> {
    let Some(progress) = progress else {
        return Ok(());
    };
    if let Some(parent) = progress
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(progress, format!("{index}\n"))?;
    if progress_hold_index == Some(index) {
        let hold = progress_hold.ok_or("progress hold index configured without hold path")?;
        while !hold.exists() {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    Ok(())
}

fn run_workload(
    engine: &mut Engine,
    oracle: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    operations: &[Operation],
    batch_size: usize,
    progress: Option<&Path>,
    progress_hold: Option<&Path>,
    progress_hold_index: Option<usize>,
) -> BenchResult<(u128, LatencyStats, LatencyStats, RunCounters, u64)> {
    let mut latencies = Vec::new();
    let mut write_batch_latencies = Vec::new();
    let mut counters = RunCounters::default();
    let mut logical_bytes = 0;
    let started = Instant::now();

    if operations
        .iter()
        .all(|operation| matches!(operation, Operation::Put { .. }))
    {
        for (chunk_index, chunk) in operations.chunks(batch_size).enumerate() {
            publish_progress_boundary(
                progress,
                progress_hold,
                progress_hold_index,
                chunk_index.saturating_mul(batch_size),
            )?;
            let batch_started = Instant::now();
            engine.write_batch(chunk)?;
            let batch_latency = batch_started.elapsed().as_nanos();
            latencies.push(batch_latency);
            write_batch_latencies.push(batch_latency);
            counters.write_batches += 1;
            counters.max_write_batch_size = counters.max_write_batch_size.max(chunk.len());
            for operation in chunk {
                if let Operation::Put { key, value } = operation {
                    counters.writes += 1;
                    logical_bytes += (key.len() + value.len()) as u64;
                }
                apply_oracle(oracle, operation);
            }
        }
        counters.measured_operations = operations.len();
    } else {
        let mut operation_index = 0;
        while operation_index < operations.len() {
            let operation = &operations[operation_index];
            publish_progress_boundary(
                progress,
                progress_hold,
                progress_hold_index,
                operation_index,
            )?;
            if is_write_operation(operation) {
                let batch_end = next_write_batch_end(operations, operation_index, batch_size);
                let batch = &operations[operation_index..batch_end];
                let batch_started = Instant::now();
                engine.write_batch(batch)?;
                let batch_latency = batch_started.elapsed().as_nanos();
                latencies.push(batch_latency);
                write_batch_latencies.push(batch_latency);
                counters.write_batches += 1;
                counters.max_write_batch_size = counters.max_write_batch_size.max(batch.len());
                for operation in batch {
                    match operation {
                        Operation::Put { key, value } => {
                            counters.writes += 1;
                            logical_bytes += (key.len() + value.len()) as u64;
                        }
                        Operation::Delete { key } => {
                            counters.deletes += 1;
                            logical_bytes += key.len() as u64;
                        }
                        Operation::Get { .. } | Operation::Range { .. } => {
                            unreachable!("read operation passed to a write batch")
                        }
                    }
                    apply_oracle(oracle, operation);
                }
                counters.measured_operations += batch.len();
                operation_index = batch_end;
                continue;
            }

            let operation_started = Instant::now();
            match operation {
                Operation::Get { key } => {
                    verify_get(engine, oracle, key)?;
                    counters.reads += 1;
                }
                Operation::Range { start, end } => {
                    verify_range(engine, oracle, start, end)?;
                    counters.ranges += 1;
                }
                Operation::Put { .. } | Operation::Delete { .. } => {
                    unreachable!("write operation should have taken the batch path")
                }
            }
            latencies.push(operation_started.elapsed().as_nanos());
            counters.measured_operations += 1;
            operation_index += 1;
        }
    }

    Ok((
        started.elapsed().as_nanos(),
        latency_stats(&mut latencies),
        latency_stats(&mut write_batch_latencies),
        counters,
        logical_bytes,
    ))
}

fn disk_bytes(path: &Path) -> BenchResult<u64> {
    fn visit(path: &Path) -> std::io::Result<u64> {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.is_file() {
            return Ok(metadata.len());
        }
        if !metadata.is_dir() {
            return Ok(0);
        }
        fs::read_dir(path)?.try_fold(0, |total, entry| Ok(total + visit(&entry?.path())?))
    }
    Ok(visit(path)?)
}

fn prepare_path(path: &Path) -> BenchResult<()> {
    if path.exists() {
        return Err(format!("benchmark path must not already exist: {}", path.display()).into());
    } else {
        fs::create_dir_all(path.parent().unwrap_or_else(|| Path::new(".")))?;
    }
    Ok(())
}

fn render_result(result: &RunResult) -> String {
    let config = &result.config;
    let throughput =
        result.counters.measured_operations as f64 / (result.workload_ns as f64 / 1_000_000_000.0);
    let amplification = if result.logical_bytes == 0 {
        None
    } else {
        result
            .seer_counters
            .map(|counters| counters.page_bytes_written as f64 / result.logical_bytes as f64)
    };
    format!(
        "{{\n  \"format\": \"seerdb-common-kv-v4\",\n  \"engine\": \"{}\",\n  \"workload\": \"{}\",\n  \"durability\": \"{}\",\n  \"host_os\": \"{}\",\n  \"host_arch\": \"{}\",\n  \"keys\": {},\n  \"operations\": {},\n  \"batch_size\": {},\n  \"value_bytes\": {},\n  \"range_width\": {},\n  \"seed\": {},\n  \"preload_ns\": {},\n  \"workload_ns\": {},\n  \"reopen_ns\": {},\n  \"throughput_ops_per_sec\": {:.3},\n  \"latency_unit\": \"{}\",\n  \"p50_ns\": {},\n  \"p95_ns\": {},\n  \"p99_ns\": {},\n  \"max_ns\": {},\n  \"write_batch_count\": {},\n  \"max_write_batch_size\": {},\n  \"write_batch_p50_ns\": {},\n  \"write_batch_p95_ns\": {},\n  \"write_batch_p99_ns\": {},\n  \"write_batch_max_ns\": {},\n  \"writes\": {},\n  \"deletes\": {},\n  \"point_reads\": {},\n  \"ranges\": {},\n  \"logical_bytes\": {},\n  \"final_keys\": {},\n  \"digest_fnv1a64\": \"{:016x}\",\n  \"trace_digest_fnv1a64\": \"{:016x}\",\n  \"disk_bytes\": {},\n  \"process_user_cpu_ns\": {},\n  \"process_system_cpu_ns\": {},\n  \"process_max_rss_bytes\": {},\n  \"seerdb_physical_page_writes\": {},\n  \"seerdb_page_bytes_written\": {},\n  \"seerdb_generation_flushes\": {},\n  \"seerdb_data_syncs\": {},\n  \"seerdb_wal_bytes_written\": {},\n  \"seerdb_metadata_bytes_written\": {},\n  \"seerdb_blob_bytes_written\": {},\n  \"seerdb_history_bytes_written\": {},\n  \"seerdb_manifest_bytes_written\": {},\n  \"seerdb_reclaimed_bytes\": {},\n  \"seerdb_candidate_prepare_ns\": {},\n  \"seerdb_wal_write_ns\": {},\n  \"seerdb_admission_ns\": {},\n  \"seerdb_data_flush_ns\": {},\n  \"seerdb_metadata_write_ns\": {},\n  \"seerdb_blob_write_ns\": {},\n  \"seerdb_history_write_ns\": {},\n  \"seerdb_directory_sync_ns\": {},\n  \"seerdb_manifest_write_ns\": {},\n  \"seerdb_manifest_mirror_ns\": {},\n  \"seerdb_cleanup_ns\": {},\n  \"seerdb_page_write_amplification\": {}\n}}",
        config.engine.name(),
        config.workload.name(),
        config.durability.name(),
        std::env::consts::OS,
        std::env::consts::ARCH,
        config.keys,
        config.operations,
        config.batch_size,
        config.value_bytes,
        config.range_width,
        config.seed,
        result.preload_ns,
        result.workload_ns,
        result.reopen_ns,
        throughput,
        match config.workload {
            WorkloadKind::BatchPut => "write-batch",
            WorkloadKind::Mixed => "operation-or-write-batch",
            WorkloadKind::PointRead | WorkloadKind::RangeRead => "operation",
        },
        result.latency.p50_ns,
        result.latency.p95_ns,
        result.latency.p99_ns,
        result.latency.max_ns,
        result.counters.write_batches,
        result.counters.max_write_batch_size,
        result.write_batch_latency.p50_ns,
        result.write_batch_latency.p95_ns,
        result.write_batch_latency.p99_ns,
        result.write_batch_latency.max_ns,
        result.counters.writes,
        result.counters.deletes,
        result.counters.reads,
        result.counters.ranges,
        result.logical_bytes,
        result.final_keys,
        result.digest,
        result.trace_digest,
        result.disk_bytes,
        result.resources.user_cpu_ns,
        result.resources.system_cpu_ns,
        result
            .resources
            .max_rss_bytes
            .map_or_else(|| "null".to_string(), |value| value.to_string()),
        result
            .seer_counters
            .map_or(0, |counters| counters.physical_page_writes),
        result
            .seer_counters
            .map_or(0, |counters| counters.page_bytes_written),
        result
            .seer_counters
            .map_or(0, |counters| counters.generation_flushes),
        result
            .seer_counters
            .map_or(0, |counters| counters.data_syncs),
        result
            .seer_counters
            .map_or(0, |counters| counters.wal_bytes_written),
        result
            .seer_counters
            .map_or(0, |counters| counters.metadata_bytes_written),
        result
            .seer_counters
            .map_or(0, |counters| counters.blob_bytes_written),
        result
            .seer_counters
            .map_or(0, |counters| counters.history_bytes_written),
        result
            .seer_counters
            .map_or(0, |counters| counters.manifest_bytes_written),
        result
            .seer_counters
            .map_or(0, |counters| counters.reclaimed_bytes),
        result
            .seer_counters
            .map_or(0, |counters| counters.candidate_prepare_ns),
        result
            .seer_counters
            .map_or(0, |counters| counters.wal_write_ns),
        result
            .seer_counters
            .map_or(0, |counters| counters.admission_ns),
        result
            .seer_counters
            .map_or(0, |counters| counters.data_flush_ns),
        result
            .seer_counters
            .map_or(0, |counters| counters.metadata_write_ns),
        result
            .seer_counters
            .map_or(0, |counters| counters.blob_write_ns),
        result
            .seer_counters
            .map_or(0, |counters| counters.history_write_ns),
        result
            .seer_counters
            .map_or(0, |counters| counters.directory_sync_ns),
        result
            .seer_counters
            .map_or(0, |counters| counters.manifest_write_ns),
        result
            .seer_counters
            .map_or(0, |counters| counters.manifest_mirror_ns),
        result
            .seer_counters
            .map_or(0, |counters| counters.cleanup_ns),
        amplification.map_or_else(|| "null".to_string(), |value| format!("{value:.3}")),
    )
}

fn render_prefix_verification(
    config: &Config,
    prefix: usize,
    entries: &[(Vec<u8>, Vec<u8>)],
) -> String {
    format!(
        "{{\n  \"format\": \"seerdb-common-kv-process-crash-verification-v1\",\n  \"engine\": \"{}\",\n  \"workload\": \"{}\",\n  \"durability\": \"{}\",\n  \"operations\": {},\n  \"batch_size\": {},\n  \"expected_prefix\": {},\n  \"reopen_passes\": 2,\n  \"accepted\": true,\n  \"final_keys\": {},\n  \"digest_fnv1a64\": \"{:016x}\"\n}}",
        config.engine.name(),
        config.workload.name(),
        config.durability.name(),
        config.operations,
        config.batch_size,
        prefix,
        entries.len(),
        digest(entries),
    )
}

fn expected_existing_oracle(config: &Config) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut oracle = BTreeMap::new();
    for (key, value) in initial_state(config) {
        oracle.insert(key, value);
    }
    if config.base_operations > 0 {
        let mut base_config = config.clone();
        base_config.base_operations = 0;
        base_config.operations = config.base_operations;
        for operation in generate_operations(&base_config) {
            apply_oracle(&mut oracle, &operation);
        }
    }
    oracle
}

fn verify_existing_prefix(config: &Config, prefix: usize) -> BenchResult<String> {
    let mut oracle = expected_existing_oracle(config);
    let mut prefix_config = config.clone();
    prefix_config.operations = prefix;
    for operation in generate_operations(&prefix_config) {
        apply_oracle(&mut oracle, &operation);
    }
    let expected_entries = oracle
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    let expected_digest = digest(&expected_entries);

    for pass in 0..2 {
        let reopened = Engine::open_existing(config.engine, &config.path, config.durability)?;
        let actual_entries = reopened.range(&[], &[0xff])?;
        if actual_entries != expected_entries {
            return Err(format!(
                "prefix verification reopen pass {} failed: entries differ from expected prefix",
                pass + 1
            )
            .into());
        }
        if digest(&actual_entries) != expected_digest {
            return Err(format!(
                "prefix verification reopen pass {} failed: digest differs from expected prefix",
                pass + 1
            )
            .into());
        }
        reopened.close()?;
    }

    Ok(render_prefix_verification(
        config,
        prefix,
        &expected_entries,
    ))
}

fn emit_output(output: &str, path: Option<&Path>) -> BenchResult<()> {
    println!("{output}");
    if let Some(path) = path {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, format!("{output}\n"))?;
    }
    Ok(())
}

fn emit_trace(output: &str, path: &Path) -> BenchResult<()> {
    let output = format!("{output}\n");
    if path.exists() {
        let existing = fs::read_to_string(path)?;
        if existing != output {
            return Err(format!(
                "trace artifact already exists with different content: {}",
                path.display()
            )
            .into());
        }
        return Ok(());
    }
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, output)?;
    Ok(())
}

fn main() -> BenchResult<()> {
    let config = Config::parse()?;
    if let Some(prefix) = config.verify_prefix {
        let output = verify_existing_prefix(&config, prefix)?;
        return emit_output(&output, config.output.as_deref());
    }
    if config.open_existing {
        if !config.path.exists() {
            return Err(format!(
                "--open-existing requires an existing database path: {}",
                config.path.display()
            )
            .into());
        }
    } else {
        prepare_path(&config.path)?;
    }

    let initial = initial_state(&config);
    let operations = generate_operations(&config);
    let trace_digest = trace_digest(&operations);
    if let Some(path) = config.trace_output.as_deref() {
        emit_trace(&render_trace(&config, &operations), path)?;
    }
    let resource_before = process_resource_metrics();
    let mut engine = if config.open_existing {
        Engine::open_existing(config.engine, &config.path, config.durability)?
    } else {
        Engine::create(config.engine, &config.path, config.durability)?
    };
    let mut oracle = if config.open_existing {
        expected_existing_oracle(&config)
    } else {
        BTreeMap::new()
    };

    let preload_started = Instant::now();
    if !config.open_existing {
        apply_initial_state(&mut engine, &mut oracle, &initial, config.batch_size)?;
    }
    let preload_ns = if config.open_existing {
        0
    } else {
        preload_started.elapsed().as_nanos()
    };

    let (workload_ns, latency, write_batch_latency, counters, workload_logical_bytes) =
        run_workload(
            &mut engine,
            &mut oracle,
            &operations,
            config.batch_size,
            config.progress.as_deref(),
            config.progress_hold.as_deref(),
            config.progress_hold_index,
        )?;
    let logical_bytes = if config.open_existing {
        workload_logical_bytes
    } else {
        logical_bytes_for_initial_state(&initial) + workload_logical_bytes
    };
    let expected_entries = oracle
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    let expected_digest = digest(&expected_entries);
    let seer_counters = engine.seer_counters();
    engine.close()?;

    let disk_bytes = disk_bytes(&config.path)?;
    let reopen_started = Instant::now();
    let reopened = Engine::open_existing(config.engine, &config.path, config.durability)?;
    let reopened_entries = reopened.range(&[], &[0xff])?;
    if reopened_entries != expected_entries {
        return Err("reopen verification failed: final entries differ from oracle".into());
    }
    if digest(&reopened_entries) != expected_digest {
        return Err("reopen verification failed: digest differs from oracle".into());
    }
    reopened.close()?;
    let reopen_ns = reopen_started.elapsed().as_nanos();
    let resources = resource_delta(resource_before, process_resource_metrics());

    let result = RunResult {
        config,
        preload_ns,
        workload_ns,
        reopen_ns,
        resources,
        latency,
        write_batch_latency,
        counters,
        logical_bytes,
        final_keys: expected_entries.len(),
        digest: expected_digest,
        trace_digest,
        disk_bytes,
        seer_counters,
    };
    let output = render_result(&result);
    emit_output(&output, result.config.output.as_deref())
}

#[cfg(test)]
mod tests {
    use super::{Operation, next_write_batch_end};

    fn put(index: usize) -> Operation {
        Operation::Put {
            key: vec![index as u8],
            value: vec![index as u8],
        }
    }

    #[test]
    fn write_batches_respect_batch_size() {
        let operations = vec![put(0), put(1), put(2), put(3)];

        assert_eq!(next_write_batch_end(&operations, 0, 1), 1);
        assert_eq!(next_write_batch_end(&operations, 0, 3), 3);
        assert_eq!(next_write_batch_end(&operations, 3, 3), 4);
    }

    #[test]
    fn write_batches_stop_before_reads() {
        let operations = vec![put(0), put(1), Operation::Get { key: vec![9] }, put(2)];

        assert_eq!(next_write_batch_end(&operations, 0, 4), 2);
        assert_eq!(next_write_batch_end(&operations, 3, 4), 4);
    }
}
