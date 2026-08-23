use std::collections::BTreeMap;
use std::env;

use anyhow::{Context, Result, bail};
use omendb::{GovernorConfig, OverloadPolicy, Reactor, ReactorConfig, WorkClass, WorkId, WorkerId};
use serde_json::json;

const DEFAULT_TICKS: u64 = 2_000;
const DEFAULT_BURST: u64 = 6;
const DEFAULT_WORKERS: usize = 4;
const DEFAULT_SEED: u64 = 0xDB0E_2026_0713;
const MAX_DRAIN_TICKS: u64 = 20_000;

#[derive(Clone, Debug)]
struct Submission {
    class: WorkClass,
    submitted_at: u64,
    dispatched_at: Option<u64>,
}

#[derive(Clone, Debug)]
struct Running {
    work_id: WorkId,
    finish_at: u64,
}

#[derive(Debug)]
struct ClassMetrics {
    name: &'static str,
    submitted: u64,
    completed: u64,
    rejected: u64,
    wait_ticks: Vec<u64>,
    turnaround_ticks: Vec<u64>,
}

impl ClassMetrics {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            submitted: 0,
            completed: 0,
            rejected: 0,
            wait_ticks: Vec::new(),
            turnaround_ticks: Vec::new(),
        }
    }
}

fn main() -> Result<()> {
    let (ticks, burst, workers, seed) = parse_args()?;
    if ticks == 0 && burst == 0 && workers == 0 && seed == 0 {
        return Ok(());
    }
    let mut reactor = Reactor::new(ReactorConfig {
        workers,
        governor: GovernorConfig {
            capacity: 32,
            protected_reserve: 8,
            max_queue_per_class: 32,
            max_in_flight: workers,
            overload_policy: OverloadPolicy::default(),
        },
    demotion_after: None,
    })?;
    let mut rng = seed;
    let mut running = vec![None; workers];
    let mut submissions = BTreeMap::<WorkId, Submission>::new();
    let mut metrics = [
        ClassMetrics::new("oltp"),
        ClassMetrics::new("wal"),
        ClassMetrics::new("reclaim"),
        ClassMetrics::new("schema"),
        ClassMetrics::new("scan"),
    ];
    let mut attempts = 0_u64;
    let mut accepted = 0_u64;
    let mut rejected = 0_u64;
    let mut max_queue = 0_usize;
    let mut max_in_flight = 0_usize;
    let mut max_accounted_cost = 0_usize;

    for now in 0..ticks {
        complete_ready(
            now,
            &mut reactor,
            &mut running,
            &mut submissions,
            &mut metrics,
        )?;

        for _ in 0..burst {
            attempts += 1;
            let class = choose_class(next_random(&mut rng) % 100);
            let class_index = class_index(class);
            let deadline = Some(now + deadline_ticks(class));
            match reactor.submit(class, cost(class), deadline) {
                Ok(work_id) => {
                    accepted += 1;
                    metrics[class_index].submitted += 1;
                    submissions.insert(
                        work_id,
                        Submission {
                            class,
                            submitted_at: now,
                            dispatched_at: None,
                        },
                    );
                }
                Err(_) => {
                    rejected += 1;
                    metrics[class_index].rejected += 1;
                }
            }
        }

        dispatch_ready(now, &mut reactor, &mut running, &mut submissions)?;
        record_high_watermarks(
            reactor.stats(),
            &mut max_queue,
            &mut max_in_flight,
            &mut max_accounted_cost,
        );
    }

    let mut drain_ticks = 0_u64;
    while reactor.busy_workers() != 0 || reactor.stats().accounted_cost != 0 {
        if drain_ticks == MAX_DRAIN_TICKS {
            bail!("reactor did not drain within {MAX_DRAIN_TICKS} ticks");
        }
        let now = ticks + drain_ticks;
        complete_ready(
            now,
            &mut reactor,
            &mut running,
            &mut submissions,
            &mut metrics,
        )?;
        dispatch_ready(now, &mut reactor, &mut running, &mut submissions)?;
        record_high_watermarks(
            reactor.stats(),
            &mut max_queue,
            &mut max_in_flight,
            &mut max_accounted_cost,
        );
        drain_ticks += 1;
    }

    let stats = reactor.stats();
    let completed: u64 = metrics.iter().map(|metric| metric.completed).sum();
    if stats.queued != 0 || stats.in_flight != 0 || stats.accounted_cost != 0 {
        bail!("reactor finished with non-zero accounting: {stats:?}");
    }
    if accepted != completed + stats.expired {
        bail!(
            "accepted work accounting mismatch: accepted={accepted}, completed={completed}, expired={}",
            stats.expired
        );
    }
    if rejected != stats.rejected {
        bail!(
            "rejection accounting mismatch: local={rejected}, governor={}",
            stats.rejected
        );
    }

    let classes: Vec<_> = metrics
        .iter()
        .map(|metric| {
            json!({
                "class": metric.name,
                "submitted": metric.submitted,
                "completed": metric.completed,
                "rejected": metric.rejected,
                "wait_ticks": percentiles(&metric.wait_ticks),
                "turnaround_ticks": percentiles(&metric.turnaround_ticks),
            })
        })
        .collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "experiment": "omendb-runtime-mixed-workload-v0",
            "evidence_class": "deterministic_logical_simulation",
            "hardware_benchmark": false,
            "seed": seed,
            "arrival_ticks": ticks,
            "arrival_burst": burst,
            "workers": workers,
            "drain_ticks": drain_ticks,
            "attempts": attempts,
            "accepted": accepted,
            "completed": completed,
            "rejected": rejected,
            "expired": stats.expired,
            "max_queue": max_queue,
            "max_in_flight": max_in_flight,
            "max_accounted_cost": max_accounted_cost,
            "classes": classes,
        }))?
    );
    Ok(())
}

fn parse_args() -> Result<(u64, u64, usize, u64)> {
    let mut ticks = DEFAULT_TICKS;
    let mut burst = DEFAULT_BURST;
    let mut workers = DEFAULT_WORKERS;
    let mut seed = DEFAULT_SEED;
    let mut arguments = env::args().skip(1);
    while let Some(argument) = arguments.next() {
        let mut value = || {
            arguments
                .next()
                .with_context(|| format!("{argument} requires a value"))
        };
        match argument.as_str() {
            "--ticks" => ticks = value()?.parse().context("invalid --ticks")?,
            "--burst" => burst = value()?.parse().context("invalid --burst")?,
            "--workers" => workers = value()?.parse().context("invalid --workers")?,
            "--seed" => seed = value()?.parse().context("invalid --seed")?,
            "--help" => {
                println!("usage: runtime_mix [--ticks N] [--burst N] [--workers N] [--seed N]");
                return Ok((0, 0, 0, 0));
            }
            _ => bail!("unknown argument {argument}"),
        }
    }
    if ticks == 0 || burst == 0 || workers == 0 {
        bail!("ticks, burst, and workers must be positive");
    }
    Ok((ticks, burst, workers, seed))
}

fn next_random(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

fn choose_class(bucket: u64) -> WorkClass {
    match bucket {
        0..=44 => WorkClass::OlTp,
        45..=64 => WorkClass::Wal,
        65..=79 => WorkClass::Reclaim,
        80..=89 => WorkClass::Schema,
        _ => WorkClass::Scan,
    }
}

fn class_index(class: WorkClass) -> usize {
    match class {
        WorkClass::OlTp => 0,
        WorkClass::Wal => 1,
        WorkClass::Reclaim => 2,
        WorkClass::Schema => 3,
        WorkClass::Scan => 4,
    }
}

fn cost(class: WorkClass) -> usize {
    match class {
        WorkClass::OlTp | WorkClass::Wal => 1,
        WorkClass::Reclaim => 3,
        WorkClass::Schema => 2,
        WorkClass::Scan => 4,
    }
}

fn service_ticks(class: WorkClass) -> u64 {
    match class {
        WorkClass::OlTp => 2,
        WorkClass::Wal => 1,
        WorkClass::Reclaim => 5,
        WorkClass::Schema => 6,
        WorkClass::Scan => 8,
    }
}

fn deadline_ticks(class: WorkClass) -> u64 {
    match class {
        WorkClass::OlTp => 12,
        WorkClass::Wal => 8,
        WorkClass::Reclaim => 30,
        WorkClass::Schema => 40,
        WorkClass::Scan => 20,
    }
}

fn complete_ready(
    now: u64,
    reactor: &mut Reactor,
    running: &mut [Option<Running>],
    submissions: &mut BTreeMap<WorkId, Submission>,
    metrics: &mut [ClassMetrics; 5],
) -> Result<()> {
    for (worker_index, slot) in running.iter_mut().enumerate() {
        let Some(job) = slot.as_ref() else {
            continue;
        };
        if job.finish_at > now {
            continue;
        }
        let job = slot.take().expect("running job exists");
        let work = reactor
            .complete(WorkerId(worker_index as u16))
            .with_context(|| format!("complete worker {worker_index}"))?;
        if work.id != job.work_id {
            bail!(
                "worker completion returned {:?}, expected {:?}",
                work.id,
                job.work_id
            );
        }
        let submission = submissions
            .remove(&work.id)
            .with_context(|| format!("missing submission for {:?}", work.id))?;
        let dispatched_at = submission
            .dispatched_at
            .context("completed work was never dispatched")?;
        let metric = &mut metrics[class_index(submission.class)];
        metric.completed += 1;
        metric
            .wait_ticks
            .push(dispatched_at - submission.submitted_at);
        metric.turnaround_ticks.push(now - submission.submitted_at);
    }
    Ok(())
}

fn dispatch_ready(
    now: u64,
    reactor: &mut Reactor,
    running: &mut [Option<Running>],
    submissions: &mut BTreeMap<WorkId, Submission>,
) -> Result<()> {
    for dispatch in reactor.dispatch_batch(now) {
        let worker_index = dispatch.worker.0 as usize;
        let submission = submissions
            .get_mut(&dispatch.work.id)
            .with_context(|| format!("missing submission for {:?}", dispatch.work.id))?;
        submission.dispatched_at = Some(now);
        running[worker_index] = Some(Running {
            work_id: dispatch.work.id,
            finish_at: now + service_ticks(dispatch.work.class),
        });
    }
    Ok(())
}

fn record_high_watermarks(
    stats: omendb::GovernorStats,
    max_queue: &mut usize,
    max_in_flight: &mut usize,
    max_accounted_cost: &mut usize,
) {
    *max_queue = (*max_queue).max(stats.queued);
    *max_in_flight = (*max_in_flight).max(stats.in_flight);
    *max_accounted_cost = (*max_accounted_cost).max(stats.accounted_cost);
}

fn percentiles(values: &[u64]) -> serde_json::Value {
    json!({
        "count": values.len(),
        "p50": percentile(values, 1, 2),
        "p99": percentile(values, 99, 100),
        "p999": percentile(values, 999, 1000),
        "max": values.iter().copied().max(),
    })
}

fn percentile(values: &[u64], numerator: usize, denominator: usize) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = (sorted.len() * numerator).div_ceil(denominator).max(1);
    sorted.get(rank - 1).copied()
}
