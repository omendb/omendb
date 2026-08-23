use std::collections::BTreeMap;
use std::env;
use std::time::{Duration, Instant};

use omendb::DatabaseMetrics;
use serde_json::{Map, Value, json};

#[derive(Debug)]
struct PhaseStats {
    count: u64,
    total: Duration,
    minimum: Duration,
    maximum: Duration,
    samples: Vec<Duration>,
}

impl Default for PhaseStats {
    fn default() -> Self {
        Self {
            count: 0,
            total: Duration::ZERO,
            minimum: Duration::MAX,
            maximum: Duration::ZERO,
            samples: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
pub struct ReplayMetrics {
    phases: BTreeMap<&'static str, PhaseStats>,
    storage: DatabaseMetrics,
}

/// Enable the matched-durability comparison mode for legacy replayers.
///
/// The normal legacy runners acknowledge a WAL-synced commit and defer the
/// full checkpoint. SeerDB currently publishes a durable page generation for
/// every commit, so this opt-in mode makes the comparison explicit without
/// changing the normal conformance or product-facing path.
pub fn checkpoint_each_commit() -> bool {
    env::var_os("OMENDB_LEGACY_CHECKPOINT_EACH_COMMIT").is_some()
}

impl ReplayMetrics {
    pub fn record(&mut self, phase: &'static str, started: Instant) {
        let elapsed = started.elapsed();
        let stats = self.phases.entry(phase).or_default();
        stats.count += 1;
        stats.total += elapsed;
        stats.minimum = stats.minimum.min(elapsed);
        stats.maximum = stats.maximum.max(elapsed);
        stats.samples.push(elapsed);
    }

    pub fn add_storage(&mut self, metrics: &DatabaseMetrics) {
        self.storage.wal_bytes = self.storage.wal_bytes.saturating_add(metrics.wal_bytes);
        self.storage.fragment_bytes = self
            .storage
            .fragment_bytes
            .saturating_add(metrics.fragment_bytes);
        self.storage.packed_page_bytes = self
            .storage
            .packed_page_bytes
            .saturating_add(metrics.packed_page_bytes);
        self.storage.manifest_bytes = self
            .storage
            .manifest_bytes
            .saturating_add(metrics.manifest_bytes);
        self.storage.syncs = self.storage.syncs.saturating_add(metrics.syncs);
        self.storage.compaction_runs = self
            .storage
            .compaction_runs
            .saturating_add(metrics.compaction_runs);
        self.storage.fragments_reclaimed = self
            .storage
            .fragments_reclaimed
            .saturating_add(metrics.fragments_reclaimed);
    }

    pub fn json(&self) -> Value {
        let phases = self
            .phases
            .iter()
            .map(|(name, stats)| {
                (
                    (*name).to_owned(),
                    json!({
                        "count": stats.count,
                        "total_seconds": stats.total.as_secs_f64(),
                        "min_seconds": if stats.count == 0 {
                            0.0
                        } else {
                            stats.minimum.as_secs_f64()
                        },
                        "max_seconds": stats.maximum.as_secs_f64(),
                        "p50_seconds": quantile_seconds(&stats.samples, 50),
                        "p95_seconds": quantile_seconds(&stats.samples, 95),
                        "p99_seconds": quantile_seconds(&stats.samples, 99),
                    }),
                )
            })
            .collect::<Map<String, Value>>();
        json!({
            "storage": storage_json(&self.storage),
            "phases": phases,
        })
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

fn storage_json(metrics: &DatabaseMetrics) -> Value {
    json!({
        "wal_bytes": metrics.wal_bytes,
        "fragment_bytes": metrics.fragment_bytes,
        "packed_page_bytes": metrics.packed_page_bytes,
        "manifest_bytes": metrics.manifest_bytes,
        "syncs": metrics.syncs,
        "compaction_runs": metrics.compaction_runs,
        "fragments_reclaimed": metrics.fragments_reclaimed,
    })
}
