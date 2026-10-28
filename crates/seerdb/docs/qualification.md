# SeerDB qualification procedures

These development harnesses exercise the current generation-based engine unless
explicitly stated otherwise; their existence does not qualify the experimental
replacement. [Architecture](architecture.md) describes the boundary. Run commands
from **`crates/seerdb/`**, where `tools/` and examples live, unless they specify the
workspace root. Record revision, runtime, platform, invocation and actual outcomes.

## External failure boundaries

- `tools/linux_syscall_faults.sh` injects failures before/after observed libc
  write/sync/rename calls, then checks old-or-complete-new state through two reopens.
- `tools/linux_syscall_crashes.sh` pauses at libc calls for parent-driven SIGKILL.
- `tools/common_kv_syscall_faults.sh` applies matched recovery probes to the selected
  KV engines; recovery/resource equivalence must be reported separately.
- `tools/linux_power_loss.sh` requires privileged Linux loop devices,
  `dm-log-writes` and `replay-log`. Missing prerequisites produce `not-run`, not a pass.

Libc/process probes do not establish torn-write, block-cache-loss or machine
power-loss safety. Interposers must cover the syscalls the tested implementation
actually uses; positional I/O changes may require a different interposer.

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
