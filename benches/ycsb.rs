#![allow(clippy::disallowed_methods)]

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use seerdb::{BatchMutation, DB, Options};
use std::time::Duration;
use tempfile::{TempDir, tempdir};

const KEY_COUNT: usize = 512;
const OPERATION_COUNT: usize = 256;

#[derive(Clone, Copy, Debug)]
enum Profile {
    ReadUpdate,
    ReadHeavy,
    ReadOnly,
    ScanHeavy,
}

impl Profile {
    const ALL: [Self; 4] = [
        Self::ReadUpdate,
        Self::ReadHeavy,
        Self::ReadOnly,
        Self::ScanHeavy,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::ReadUpdate => "a_read_update",
            Self::ReadHeavy => "b_read_heavy",
            Self::ReadOnly => "c_read_only",
            Self::ScanHeavy => "e_scan_heavy",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Read,
    Update,
    Scan,
}

#[derive(Clone, Copy, Debug)]
struct WorkloadOperation {
    operation: Operation,
    key: usize,
}

struct WorkloadFixture {
    _directory: TempDir,
    db: DB,
    keys: Vec<Vec<u8>>,
    operations: Vec<WorkloadOperation>,
}

fn workload_key(index: usize) -> Vec<u8> {
    format!("ycsb-key-{index:08}").into_bytes()
}

fn workload_value(index: usize, revision: usize) -> Vec<u8> {
    format!("ycsb-value-{index:08}-revision-{revision:04}").into_bytes()
}

fn next_random(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

fn operation_for(profile: Profile, bucket: u64) -> Operation {
    match profile {
        Profile::ReadUpdate => {
            if bucket < 50 {
                Operation::Read
            } else {
                Operation::Update
            }
        }
        Profile::ReadHeavy => {
            if bucket < 95 {
                Operation::Read
            } else {
                Operation::Update
            }
        }
        Profile::ReadOnly => Operation::Read,
        Profile::ScanHeavy => {
            if bucket < 95 {
                Operation::Scan
            } else {
                Operation::Update
            }
        }
    }
}

fn fixture(profile: Profile) -> WorkloadFixture {
    let directory = tempdir().unwrap();
    let mut db = DB::open(directory.path().join("db"), Options::default()).unwrap();
    let keys: Vec<_> = (0..KEY_COUNT).map(workload_key).collect();
    let values: Vec<_> = (0..KEY_COUNT)
        .map(|index| workload_value(index, 0))
        .collect();
    let initial_batch: Vec<_> = keys
        .iter()
        .zip(&values)
        .map(|(key, value)| BatchMutation::Put {
            key: key.clone(),
            value: value.clone(),
        })
        .collect();
    db.commit_batch(&initial_batch).unwrap();
    db.flush().unwrap();

    let mut state = 0x5EED_DA7A_2026_u64 ^ profile.name().len() as u64;
    let operations = (0..OPERATION_COUNT)
        .map(|_| {
            let random = next_random(&mut state);
            WorkloadOperation {
                operation: operation_for(profile, random % 100),
                key: (random >> 16) as usize % KEY_COUNT,
            }
        })
        .collect();
    WorkloadFixture {
        _directory: directory,
        db,
        keys,
        operations,
    }
}

fn run_workload(mut fixture: WorkloadFixture) {
    let mut updates = 0;
    let mut observed = 0usize;
    for operation in fixture.operations {
        let key = &fixture.keys[operation.key];
        match operation.operation {
            Operation::Read => {
                observed = observed
                    .saturating_add(fixture.db.get(black_box(key)).unwrap().is_some() as usize);
            }
            Operation::Update => {
                updates += 1;
                let value = workload_value(operation.key, updates);
                fixture.db.put(key, &value).unwrap();
            }
            Operation::Scan => {
                let end = (operation.key + 32).min(KEY_COUNT - 1);
                observed = observed
                    .saturating_add(fixture.db.range(key, &fixture.keys[end]).unwrap().len());
            }
        }
    }
    fixture.db.flush().unwrap();
    black_box((observed, updates, fixture.db.metrics().unwrap()));
}

fn bench_ycsb_profiles(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb_style_workloads");
    group.sample_size(10);
    group.measurement_time(Duration::from_millis(250));
    group.throughput(Throughput::Elements(OPERATION_COUNT as u64));
    for profile in Profile::ALL {
        group.bench_function(BenchmarkId::from_parameter(profile.name()), |benchmark| {
            benchmark.iter_batched(|| fixture(profile), run_workload, BatchSize::SmallInput);
        });
    }
    group.finish();
}

criterion_group!(benches, bench_ycsb_profiles);
criterion_main!(benches);
