//! Measurement state and versioned result-artifact rendering.
//!
//! The benchmark runner owns command-line parsing and workload execution.
//! This module owns the values produced by a run, process-resource sampling,
//! latency summaries, and the stable JSON-shaped diagnostic artifacts.

use super::config::{Config, WorkloadKind};
use super::trace::digest;

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct SeerCounters {
    pub(super) physical_page_writes: u64,
    pub(super) page_bytes_written: u64,
    pub(super) generation_flushes: u64,
    pub(super) data_syncs: u64,
    pub(super) wal_bytes_written: u64,
    pub(super) metadata_bytes_written: u64,
    pub(super) blob_bytes_written: u64,
    pub(super) history_bytes_written: u64,
    pub(super) manifest_bytes_written: u64,
    pub(super) reclaimed_bytes: u64,
    pub(super) candidate_prepare_ns: u64,
    pub(super) wal_write_ns: u64,
    pub(super) admission_ns: u64,
    pub(super) data_flush_ns: u64,
    pub(super) metadata_write_ns: u64,
    pub(super) blob_write_ns: u64,
    pub(super) history_write_ns: u64,
    pub(super) directory_sync_ns: u64,
    pub(super) manifest_write_ns: u64,
    pub(super) manifest_mirror_ns: u64,
    pub(super) cleanup_ns: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct ResourceMetrics {
    pub(super) user_cpu_ns: u128,
    pub(super) system_cpu_ns: u128,
    pub(super) max_rss_bytes: Option<u64>,
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
pub(super) fn process_resource_metrics() -> ResourceMetrics {
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
pub(super) fn process_resource_metrics() -> ResourceMetrics {
    ResourceMetrics::default()
}

pub(super) fn resource_delta(before: ResourceMetrics, after: ResourceMetrics) -> ResourceMetrics {
    ResourceMetrics {
        user_cpu_ns: after.user_cpu_ns.saturating_sub(before.user_cpu_ns),
        system_cpu_ns: after.system_cpu_ns.saturating_sub(before.system_cpu_ns),
        max_rss_bytes: after.max_rss_bytes,
    }
}

#[derive(Debug, Default)]
pub(super) struct LatencyStats {
    pub(super) p50_ns: u128,
    pub(super) p95_ns: u128,
    pub(super) p99_ns: u128,
    pub(super) max_ns: u128,
}

pub(super) fn latency_stats(latencies: &mut [u128]) -> LatencyStats {
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
pub(super) struct RunCounters {
    pub(super) measured_operations: usize,
    pub(super) writes: usize,
    pub(super) reads: usize,
    pub(super) ranges: usize,
    pub(super) deletes: usize,
    pub(super) write_batches: usize,
    pub(super) max_write_batch_size: usize,
}

#[derive(Debug)]
pub(super) struct RunResult {
    pub(super) config: Config,
    pub(super) preload_ns: u128,
    pub(super) workload_ns: u128,
    pub(super) reopen_ns: u128,
    pub(super) resources: ResourceMetrics,
    pub(super) latency: LatencyStats,
    pub(super) write_batch_latency: LatencyStats,
    pub(super) counters: RunCounters,
    pub(super) logical_bytes: u64,
    pub(super) final_keys: usize,
    pub(super) digest: u64,
    pub(super) trace_digest: u64,
    pub(super) disk_bytes: u64,
    pub(super) seer_counters: Option<SeerCounters>,
}

pub(super) fn render_result(result: &RunResult) -> String {
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

pub(super) fn render_prefix_verification(
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
