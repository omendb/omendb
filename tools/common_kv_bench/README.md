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
include reopen verification time, host identity, process CPU time, and peak
resident memory. Pass `--output PATH` to retain the exact JSON result beside
stdout for later comparison.

The current peer versions are `fjall 3.1.8` and optional `rocksdb 0.24.0`.
Use the same workload, seed, value size, batch size, durability mode, and
filesystem when comparing engines. These numbers are diagnostic until they
are repeated on a documented Linux/NVMe matrix with warm-up, repetitions, and
matched tuning.

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

The RocksDB adapter requires a working native RocksDB build toolchain. On
macOS that currently includes `libclang` for the published `rocksdb 0.24.0`
bindings; the SeerDB/Fjall path does not have that native requirement.
