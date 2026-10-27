# SeerDB qualification and historical evidence

This document keeps the existing development-harness commands and dated
legacy-engine evidence out of the API README. It has **not** been rerun for the
new persistent runtime. Run the commands below from **`crates/seerdb/`**,
where the `tools/` scripts and crate examples live, unless a command specifies
the OmenDB workspace root. The maintained [roadmap](roadmap.md) owns required
gates and [architecture](architecture.md) owns the target contract. Historical
results are specific to the engine version and operating system stated.

## Status

The current Rust development lane is a single-writer durable kernel with
concurrent reads, root-generation retention, WAL recovery, crash-safe
reclamation, and retryable capacity refusal. `TransactionDatabase` now stores
committed per-key version chains, returns explicit `{CSN, LSN}` commit
positions, and supplies first-class trees, fixed-snapshot reads, atomic
multi-tree mutations, and reopen-persistent write-conflict records above that
kernel. It is still not the target physical multi-writer
engine or a releasable `0.1.0-alpha.*` crate. Status-table freezing,
durable retained-snapshot/change-consumer leases, concurrent page/WAL publication,
broader filesystem/block-layer fault qualification, OmenDB direct integration,
and measured device-backed performance remain open. OmenDB integration and the
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

The workspace [CI configuration](../../../.github/workflows/ci.yml) owns the
current platform/MSRV, test, Clippy, package-list, live PostgreSQL and perf-smoke
matrix. Follow [crate instructions](../AGENTS.md#seerdb-kernel) from the workspace
root for routine gates; the historical standalone `Extended validation` workflow
is not present here. The [qualification skill](../../../.agents/skills/vnext-qualification/SKILL.md)
owns changed-boundary schedules, not a claim that the new runtime has passed them.

Additional local release, documentation, dependency and dedicated filesystem
checks can be run from this crate with:

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
