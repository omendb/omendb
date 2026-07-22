# Common ordered-KV comparison harness

This is an isolated diagnostic harness for the SeerDB v0.1 stability gate. It
replays the same deterministic ordered byte-KV workload against SeerDB and a
current Rust storage peer. It is deliberately not a public SeerDB trait or a
claim that the engines have identical configuration.

The harness checks the final key/value state against an in-memory oracle,
computes a canonical digest, closes and reopens the database, and checks the
digest again. It reports operation latency quantiles, throughput, logical
bytes, recursive on-disk bytes, and SeerDB's internal page/publication counters
when available.

The current peer versions are `fjall 3.1.8` and optional `rocksdb 0.24.0`.
Use the same workload, seed, value size, batch size, durability mode, and
filesystem when comparing engines. These numbers are diagnostic until they
are repeated on a documented Linux/NVMe matrix with warm-up, repetitions, and
matched tuning.

## Examples

From this directory:

```bash
# SeerDB and Fjall (Fjall is the default feature)
cargo run --release -- --engine seerdb --workload batch-put --path /tmp/seerdb-bench
cargo run --release -- --engine fjall --workload mixed --sync --path /tmp/fjall-bench

# RocksDB uses an optional native dependency and disables the Fjall default
cargo run --release --no-default-features --features rocksdb -- \
  --engine rocksdb --workload mixed --sync --path /tmp/rocksdb-bench
```

The path must not already exist. The default run size is intentionally small;
increase `--keys`, `--operations`, and `--value-bytes` for a real comparison.
Use `--help` for all workload controls.

The RocksDB adapter requires a working native RocksDB build toolchain. On
macOS that currently includes `libclang` for the published `rocksdb 0.24.0`
bindings; the SeerDB/Fjall path does not have that native requirement.
