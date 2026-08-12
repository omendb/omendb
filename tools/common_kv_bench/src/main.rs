use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod engine;
mod report;
mod trace;

use engine::Engine;
use report::{
    LatencyStats, RunCounters, RunResult, latency_stats, process_resource_metrics,
    render_prefix_verification, render_result, resource_delta,
};
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
