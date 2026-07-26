# seerdb

Embedded durable ordered byte-KV storage engine for Rust consumers. Written in Rust.

## What

SeerDB provides a small durable kernel for applications that need ordered byte
keys and values without a separate database server:

- **Durable generations**: pages, checkpoints, commit metadata, manifests, and
  WAL cleanup publish in a checked order. Two independently valid manifest slots
  provide a fallback root when the newest publication is damaged.
- **Out-of-place pages**: immutable page versions support generation-aware
  reclamation, compaction, vacuum, and retained historical reads.
- **WAL recovery**: committed-prefix replay, torn-tail handling, corruption
  refusal, offline checks, and destination-only repair are part of the baseline.
- **KV separation**: large values use the compatibility whole-image blob format;
  the segmented catalog layout is available as an opt-in format under separate
  qualification.
- **Reader/writer contract**: one serialized writer with concurrent reads,
  root-bound read views, retained snapshots, and expected-base conflict refusal.
- **Capacity handling**: typed ENOSPC refusal and preflight admission cover
  ordinary commits and maintenance, with same-handle retry and reopen tests.
- **SSD-aware paths**: Linux O_DIRECT, page-aligned buffers, and optional FDP/ZNS
  feature flags exist, but device-specific optimization is not required by the
  baseline contract.

SeerDB does not claim durable per-record MVCC, parallel writers, SQL, HA, or
cross-device performance parity. Those are later DBNext or post-v0.1 concerns.

## Basic use

Create a database once, then reopen the same directory on later process starts:

```rust
use seerdb::{DB, Options};

let mut db = DB::create("./my_db", Options::default())?;
db.put(b"key", b"value")?;
db.flush()?;
assert_eq!(db.get(b"key")?, Some(b"value".to_vec()));
db.close()?;

let mut db = DB::open("./my_db", Options::default())?;
assert_eq!(db.get(b"key")?, Some(b"value".to_vec()));
db.close()?;
```

Use `commit_batch` for an atomic group of byte-oriented puts and deletes. Use
`begin_read_view`, `retain_commit`, or `begin_snapshot` when a read must stay
bound to a published historical generation. Call `verify`, `check`, and
`durability_status` in operational tooling rather than treating a successful
open as a complete integrity audit.

## Direction

The accepted architecture is a record-aware, out-of-place, versioned B+tree
with WAL, immutable root generations, snapshots, blobs, and generation-safe
reclamation. The first production slice is deliberately a single-writer
durable kernel with concurrent readers. See
[`ai/design/target_architecture.md`](ai/design/target_architecture.md) for the
current design and staged optimization plan.

## Status

The current Rust lane is a single-writer durable kernel with concurrent reads,
root-generation retention, WAL recovery, crash-safe reclamation, and retryable
capacity refusal. The current release suite passes 223 unit tests, 75 DBNext R0
tests, storage properties, all-target Clippy, and warnings-as-errors docs. A
524,288-operation ARM64 Linux workload/recovery soak passed with digest/reopen
parity, and the DBNext R0 integrity gate accepts its replay and 13 fault cases.

It is not yet a v0.1 release: the local environments lack the Linux
`dm-log-writes` target for external power-loss qualification, and controlled
device-backed comparison, broader DBNext R1/R2 semantics, and final filesystem
fault races remain open. See [ai/STATUS.md](ai/STATUS.md) for the evidence and
remaining gates.

On Linux, `tools/linux_syscall_faults.sh` adds an external libc-boundary gate:
it fails each observed `fsync`, `fdatasync`, rename, and `write` call once
during a whole-image and segmented durable mutation, then requires the old or
complete-new root after two fresh reopens. This does not emulate torn writes,
block-layer cache loss, or machine power loss; the privileged
`tools/linux_power_loss.sh` gate remains separate.
The Rust positional page-write path is still covered by SeerDB's in-process
write-fault seams rather than this libc preload.

## Portable qualification

Run the deterministic mixed workload and emit a JSON qualification artifact:

```bash
cargo run --release --all-features --example seerdb_qualification -- \
  --keys 128 --operations 512 --output /tmp/seerdb-qualification.json
```

The harness checks a reference model, retained-root reads, bounded compaction,
periodic close/reopen verification, vacuum, history pruning, offline
`DB::check()`, and final close/reopen.
It reports p50/p95/p99 operation latencies, page-write and space-amplification
observations, separates user-commit work from maintenance/restart work in its
physical counters, and records the final digest. These are portable
qualification measurements, not cross-engine or device-backed performance
claims; the ext4/XFS power-loss runner remains a separate privileged gate.

For the matched ordered-KV comparison harness, use Linux and retain the raw
runs plus aggregate manifest:

```bash
tools/common_kv_qualification.sh \
  --output-dir /tmp/seerdb-common-kv-qualification \
  --engines seerdb,fjall,rocksdb \
  --workloads batch-put,mixed,point-read,range-read \
  --repetitions 2 --warmups 1
```

The comparison uses durable mode, a shared deterministic trace, an in-memory
oracle, close/reopen verification, latency quantiles, process resources, and
SeerDB publication counters. It leaves the filesystem cache intact and does
not substitute for external recovery testing or NVMe measurements.

For the portable process-termination recovery gate, run the independent
ordered-KV adapters through deterministic old-state and complete-new-state
batch boundaries:

```bash
tools/common_kv_faults.sh \
  --output-dir /tmp/seerdb-common-kv-faults
```

This writes a `seerdb-common-kv-process-crash-manifest-v1` with six Linux
cases, SIGKILL status, two reopen checks, and the accepted prefix digest for
SeerDB, Fjall, and RocksDB. It does not claim recovery from a kill during
fsync/page write or from block-layer power loss; those remain separate gates.

## Verification

```bash
cargo test --release --all-features --tests
cargo clippy --all-features --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
```

The privileged `tools/linux_power_loss.sh` runner is separate and requires a
Linux host with loop devices, `dm-log-writes`, and `replay-log`. It records an
explicit `not-run` manifest when those prerequisites are unavailable.

## References

- LeanStore (VLDB 2024, 2026) — out-of-place B-tree, SSD-aware buffer management
- "How to Write to SSDs" (VLDB 2026) — DB-SSD co-optimization, NoWA pattern
- WiscKey (FAST 2016) — key-value separation for reduced write amplification
- ZLeanStore (GitHub) — C++ implementation of out-of-place B-tree with blob storage

## License

Apache License 2.0
