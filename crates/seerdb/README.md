# seerdb

Generic durable ordered byte-KV storage engine for Rust consumers. Written in Rust and maintained as an independently versioned crate in the OmenDB workspace. SeerDB is a storage foundation, not a SQL engine or an embedded-only product.

## What

SeerDB provides a generic durable kernel for applications and database
products that need ordered byte keys and values. It can be embedded by a Rust
consumer, but its long-term role in OmenDB is the transactional storage
foundation beneath a database server:

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
- **Reader/writer contract**: the low-level `DB` uses one serialized
  publication lane with concurrent reads, root-bound read views, retained
  snapshots, and expected-base conflict refusal.
- **Transactional ordered-KV facade**: `TransactionalDB` provides first-class
  `TreeId`s, fixed snapshots, ordered scans, atomic multi-tree batches, and
  multi-writer snapshot-isolation conflict checking. Its durable conflict
  records survive reopen; publication is still serialized by the current
  physical engine.
- **Capacity handling**: typed ENOSPC refusal and preflight admission cover
  ordinary commits and maintenance, with same-handle retry and reopen tests.
- **SSD-aware paths**: Linux O_DIRECT, page-aligned buffers, and optional FDP/ZNS
  feature flags exist, but device-specific optimization is not required by the
  baseline contract.

The current development line uses a fixed 4 KiB page format, exposed as
`seerdb::PAGE_SIZE`. Page sizing is not an ignored tuning knob: changing it
requires a separately versioned format and matching buffer/device
implementation.

SeerDB does not yet claim physical per-record page MVCC, parallel physical
publication, SQL, HA, or cross-device performance parity. The transactional
facade is a qualified development surface, not a release guarantee; the
OmenDB server contract remains active architecture work.

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

Use `commit_batch` for a low-level atomic group of byte-oriented puts and
deletes. Use `TransactionalDB` when you need first-class trees, fixed
snapshots, ordered scans, and concurrent transaction conflict checking. Use
`begin_read_view`, `retain_commit`, or `begin_snapshot` when a read must stay
bound to a published historical generation. Call `verify`, `check`, and
`durability_status` in operational tooling rather than treating a successful
open as a complete integrity audit.

## Direction

The current implementation is a correctness-oriented out-of-place B+tree
kernel with WAL, immutable root generations, snapshots, blobs, and
reclamation. Its one-writer/root-generation contract is a transitional
prototype, not the target OmenDB transaction architecture.

The target SeerDB design is a generic OLTP-oriented transactional ordered-KV
engine with physical multi-writer snapshot MVCC, ordered cursors, atomic
multi-tree mutation, recoverable WAL, and a restartable committed-change
stream. The current `TransactionalDB` is the first semantic vertical slice;
its physical publication lane is intentionally not presented as the final
architecture. SeerDB remains SQL-agnostic: OmenDB owns row/index codecs and
relational meaning. See [`../../docs/architecture.md`](../../docs/architecture.md)
for the cross-project boundary and roadmap.

## Status

The current Rust development lane is a single-writer durable kernel with
concurrent reads, root-generation retention, WAL recovery, crash-safe
reclamation, and retryable capacity refusal. `TransactionalDB` now supplies
first-class trees, fixed-snapshot reads, atomic multi-tree mutations, and
reopen-persistent write-conflict records above that kernel. It is still not
the target physical multi-writer engine or a releasable `0.1.0-alpha.*` crate.
Physical per-record MVCC, concurrent page/WAL publication, broader
filesystem/block-layer fault qualification, OmenDB direct integration, and
measured device-backed performance remain open. OmenDB integration and the
alpha release gates are the authoritative cross-project qualification
surfaces.

On Linux, `tools/linux_syscall_faults.sh` adds an external libc-boundary gate:
it fails each observed `fsync`, `fdatasync`, rename, and `write` call once both
before and after completion during a whole-image and segmented durable
mutation, then requires the old or complete-new root after two fresh reopens.
This does not emulate torn writes, block-layer cache loss, or machine power
loss; the privileged
`tools/linux_power_loss.sh` gate remains separate.
The current page-write path uses `write` after seeking, so it is included in
this matrix; a future positional `pwrite` path would need a separate external
interposer.

`tools/linux_syscall_crashes.sh` uses the same interposer to hold a selected
libc call and lets the parent send `SIGKILL` either before the call or after
the real call succeeds but before it returns. The ARM64 Linux matrix covers
138 whole-image/segmented cases with two-reopen old-or-complete-new checks.
This is process-termination evidence at the libc boundary, not block-layer
power-loss, torn-write, or general filesystem-race evidence.

The same seeded mutation contract is available for the common-KV comparison:
`tools/common_kv_syscall_faults.sh` passed 210 external sync/rename cases over
SeerDB, Fjall, and RocksDB on ARM64 Linux: every observed call was tested both
before and after completion, with complete batch-prefix and two-reopen
verification. Its manifest classifies process refusal/completion, complete or
shorter-prefix recovery, stable two-reopen verification, and uncollected
resource equivalence separately. It is diagnostic recovery evidence, not
incumbent performance or block-layer power-loss equivalence.

## Portable qualification

The repository's `tools/` directory contains development-only qualification
and comparison harnesses. It is not a workspace member, runtime dependency, or
published crate content. The comparison scripts may invoke other engines as
measurement controls; SeerDB does not ship adapters for them.

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
Each accepted case also records normalized execution, recovery, reopen, and
resource outcomes so process termination, old/complete-new recovery, stable
two-reopen verification, and uncollected resource equivalence remain distinct.
On a non-Linux host, set `SEERDB_COMMON_KV_RECORD_UNSUPPORTED=1` to write an
explicit `status: unsupported`, `accepted: false` manifest instead of silently
turning the missing Linux capability into a completed result. The default
remains a fail-closed exit status 2.

## Verification

Routine CI runs the fast repository gate:

```bash
cargo check --all-features --all-targets
cargo test --all-features --all-targets
cargo fmt --all -- --check
cargo clippy --all-features --all-targets -- -D warnings
```

Extended validation is available through manual dispatch of the `Extended
validation` GitHub Actions workflow and runs automatically for `v*` release
tags. Run the same release, documentation, dependency, and dedicated
filesystem gates locally with:

```bash
cargo test --release --all-features --all-targets
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
cargo deny check all
SEERDB_ENOSPC_ROOT=/path/to/size-limited-filesystem \
  cargo test --all-features --test real_enospc -- --ignored
```

`deny.toml` is the repository dependency policy. It denies unknown registries
and Git sources, rejects wildcard requirements, allows only the reviewed
license set, and requires a crate-specific exception for the transitive
`r-efi` license expression. `cargo audit` is a complementary local RustSec
check; neither result replaces GitHub's default-branch advisory state or a
release review.

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
