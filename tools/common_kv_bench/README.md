# Common ordered-KV comparison harness

This is an isolated diagnostic harness for the SeerDB v0.1 stability gate. It
replays the same deterministic ordered byte-KV workload against SeerDB and a
current Rust storage peer. It is deliberately not a public SeerDB trait or a
claim that the engines have identical configuration.

The harness checks the final key/value state against an in-memory oracle,
computes a canonical digest, closes and reopens the database, and checks the
digest again. It reports operation latency quantiles, throughput, logical
bytes, recursive on-disk bytes, and SeerDB's internal page/publication counters
when available. Results use the versioned `seerdb-common-kv-v4` format and
include reopen verification time, host identity, process CPU time, peak
resident memory, and cumulative SeerDB publication-phase timings when
available. Pass `--output PATH` to retain the exact JSON result beside stdout
for later comparison.

The current peer versions are `fjall 3.1.8` and optional `rocksdb 0.24.0`.
Use the same workload, seed, value size, batch size, durability mode, and
filesystem when comparing engines. These numbers are diagnostic until they
are repeated on a documented Linux/NVMe matrix with warm-up, repetitions, and
matched tuning.

`--batch-size` is an execution contract, not only a label. `batch-put` groups
the measured puts into fixed-size write batches. `mixed` groups contiguous
`put`/`delete` runs up to that size and never crosses a `get` or `range`, so
read ordering remains unchanged. Result artifacts report
`write_batch_count`, `max_write_batch_size`, and separate write-batch latency
quantiles; `latency_unit` identifies whether the general quantiles are
operations, write batches, or mixed execution units.

For a repeatable Linux baseline with raw runs and an aggregate manifest, use
`tools/common_kv_qualification.sh`. It defaults to three durable repetitions
and one discarded warm-up for `batch-put` and `mixed` across all three engines:

```bash
tools/common_kv_qualification.sh --output-dir /tmp/seerdb-common-kv-qualification
```

The script preserves each `seerdb-common-kv-v4` result and writes a
`seerdb-common-kv-qualification-v2` manifest with median latency/throughput,
reopen time, disk bytes, resource metadata, the exact trace parameters, and
the final-state digest. It leaves the filesystem cache intact and records that
policy; use a controlled Linux/NVMe host and an explicit CPU set through
`SEERDB_BENCH_CPUSET` when a qualification result needs tighter resource
control. The script refuses non-Linux hosts unless
`SEERDB_COMMON_KV_ALLOW_NONLINUX=1` is set for a diagnostic run.
It fails before running the matrix if any requested adapter fails to build or
its expected executable is missing, so a partial run cannot be mistaken for a
completed comparison manifest.

Use `--batch-sizes 1,4,16` to run one matched workload matrix across several
explicit write-batch boundaries. Each size gets distinct raw-run labels and
trace artifacts, and the manifest summarizes every engine/workload/batch-size
cell. `--batch-size N` remains the single-size spelling.

## Examples

From this directory:

```bash
# SeerDB and Fjall (Fjall is the default feature; both runs are durable)
cargo run --release -- --engine seerdb --workload batch-put \
  --durability durable --path /tmp/seerdb-bench \
  --output /tmp/seerdb-bench-result.json
cargo run --release -- --engine fjall --workload mixed \
  --durability durable --path /tmp/fjall-bench

# RocksDB uses an optional native dependency and disables the Fjall default
cargo run --release --no-default-features --features rocksdb -- \
  --engine rocksdb --workload mixed --durability durable \
  --path /tmp/rocksdb-bench
```

The path must not already exist. The default run size is intentionally small;
increase `--keys`, `--operations`, and `--value-bytes` for a real comparison.
The default is durable mode. `--durability buffered` is available for
peer-only diagnostics; SeerDB rejects it because its common-KV adapter always
publishes a durable generation per commit. `--sync` remains an alias for
`--durability durable`. Use `--help` for all workload controls.

For a seeded mutation phase, first run a durable `batch-put`, then reopen the
same path with `--open-existing --base-operations N`. The next generated
operations continue the same deterministic trace after the durable baseline;
`--verify-prefix` checks a complete batch prefix from that baseline across two
reopens. This mode is used by `tools/common_kv_syscall_faults.sh` to keep
database creation faults separate from mutation recovery.

On Linux, `tools/common_kv_syscall_faults.sh` builds all three adapters, fails
each observed `fsync`, `fdatasync`, and rename call once both before and after
completion during that seeded mutation, and writes a
`seerdb-common-kv-syscall-fault-manifest-v1` artifact. The manifest records
accepted prefixes and host/toolchain metadata. Install a native `libclang`
package when building the optional RocksDB adapter. This is external
libc-boundary evidence, not torn-write, block-layer, or power-loss
equivalence. On a non-Linux host, set
`SEERDB_COMMON_KV_RECORD_UNSUPPORTED=1` to write the same versioned manifest
with `status: unsupported` and `accepted: false`; the default remains a
fail-closed exit status 2.

`tools/common_kv_faults.sh` uses the same explicit unsupported-platform
contract for its Linux process-termination manifest. An unsupported manifest
is an outcome record, never a successful recovery or comparison result. Linux
fault manifests use normalized `execution_outcome`, `recovery_outcome`,
`reopen_outcome`, and `resource_outcome` fields: they distinguish a SIGKILL or
faulted-process refusal, old/complete-new or complete-prefix recovery, stable
two-reopen verification, and the fact that resource equivalence was not
collected.

The RocksDB adapter requires a working native RocksDB build toolchain. On
macOS that currently includes `libclang` for the published `rocksdb 0.24.0`
bindings; the SeerDB/Fjall path does not have that native requirement.
