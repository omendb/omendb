use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};

#[derive(Debug, Default)]
pub struct PhaseStats {
    count: u64,
    total: Duration,
    minimum: Duration,
    maximum: Duration,
    samples: Vec<Duration>,
}

impl PhaseStats {
    pub fn record(&mut self, started: Instant) {
        let elapsed = started.elapsed();
        self.count = self.count.saturating_add(1);
        self.total = self.total.saturating_add(elapsed);
        self.minimum = if self.count == 1 {
            elapsed
        } else {
            self.minimum.min(elapsed)
        };
        self.maximum = self.maximum.max(elapsed);
        self.samples.push(elapsed);
    }
}

fn quantile_seconds(samples: &[Duration], percentile: usize) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let index = (ordered.len() - 1) * percentile / 100;
    ordered[index].as_secs_f64()
}

pub fn phase_json(phases: &BTreeMap<&'static str, PhaseStats>) -> Value {
    phases
        .iter()
        .map(|(name, stats)| {
            (
                (*name).to_owned(),
                json!({
                    "count": stats.count,
                    "total_seconds": stats.total.as_secs_f64(),
                    "min_seconds": if stats.count == 0 { 0.0 } else { stats.minimum.as_secs_f64() },
                    "max_seconds": stats.maximum.as_secs_f64(),
                    "p50_seconds": quantile_seconds(&stats.samples, 50),
                    "p95_seconds": quantile_seconds(&stats.samples, 95),
                    "p99_seconds": quantile_seconds(&stats.samples, 99),
                }),
            )
        })
        .collect::<Map<String, Value>>()
        .into()
}

pub fn storage_json(metrics: &seerdb::DBMetrics) -> Value {
    let storage = metrics.storage;
    json!({
        "logical_page_reads": storage.logical_page_reads,
        "physical_page_reads": storage.physical_page_reads,
        "physical_page_writes": storage.physical_page_writes,
        "page_bytes_read": storage.page_bytes_read,
        "page_bytes_written": storage.page_bytes_written,
        "generation_flushes": storage.generation_flushes,
        "syncs": storage.syncs,
        "reclaimed_pages": storage.reclaimed_pages,
        "reclaimed_bytes": storage.reclaimed_bytes,
        "capacity_preflight_failures": storage.capacity_preflight_failures,
        "data_bytes": metrics.data_bytes,
        "blob_bytes": metrics.blob_bytes,
        "wal_bytes": metrics.wal_bytes,
        "wal_reserved_bytes": metrics.wal_reserved_bytes,
        "reclaimable_pages": metrics.reclaimable_pages,
        "wal_admission_failures": metrics.wal_admission_failures,
    })
}
