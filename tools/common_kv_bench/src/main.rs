use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::Instant;

mod config;
mod engine;
mod report;
mod trace;
mod workload;

use config::Config;
use engine::Engine;
use report::{RunResult, process_resource_metrics, render_result, resource_delta};
use trace::{digest, generate_operations, initial_state, render_trace, trace_digest};

type BenchResult<T> = Result<T, Box<dyn std::error::Error>>;

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
        let output = workload::verify_existing_prefix(&config, prefix)?;
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
        workload::prepare_path(&config.path)?;
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
        workload::expected_existing_oracle(&config)
    } else {
        BTreeMap::new()
    };

    let preload_started = Instant::now();
    if !config.open_existing {
        workload::apply_initial_state(&mut engine, &mut oracle, &initial, config.batch_size)?;
    }
    let preload_ns = if config.open_existing {
        0
    } else {
        preload_started.elapsed().as_nanos()
    };

    let (workload_ns, latency, write_batch_latency, counters, workload_logical_bytes) =
        workload::run_workload(
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
        workload::logical_bytes_for_initial_state(&initial) + workload_logical_bytes
    };
    let expected_entries = oracle
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    let expected_digest = digest(&expected_entries);
    let seer_counters = engine.seer_counters();
    engine.close()?;

    let disk_bytes = workload::disk_bytes(&config.path)?;
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
