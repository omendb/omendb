//! Deterministic workload execution and verification for the common-KV harness.

use super::BenchResult;
use super::config::Config;
use super::engine::Engine;
use super::report::{LatencyStats, RunCounters, latency_stats, render_prefix_verification};
use super::trace::{Operation, apply_oracle, digest, generate_operations, initial_state};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

fn is_write_operation(operation: &Operation) -> bool {
    matches!(operation, Operation::Put { .. } | Operation::Delete { .. })
}

pub(super) fn next_write_batch_end(
    operations: &[Operation],
    start: usize,
    batch_size: usize,
) -> usize {
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

pub(super) fn apply_initial_state(
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

pub(super) fn logical_bytes_for_initial_state(initial: &[(Vec<u8>, Vec<u8>)]) -> u64 {
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

pub(super) fn run_workload(
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

pub(super) fn disk_bytes(path: &Path) -> BenchResult<u64> {
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

pub(super) fn prepare_path(path: &Path) -> BenchResult<()> {
    if path.exists() {
        return Err(format!("benchmark path must not already exist: {}", path.display()).into());
    }
    fs::create_dir_all(path.parent().unwrap_or_else(|| Path::new(".")))?;
    Ok(())
}

pub(super) fn expected_existing_oracle(config: &Config) -> BTreeMap<Vec<u8>, Vec<u8>> {
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

pub(super) fn verify_existing_prefix(config: &Config, prefix: usize) -> BenchResult<String> {
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
