# seerdb

LSM storage engine with learned data structures. Research-grade embedded storage implementing 2018-2024 papers on learned indexes, workload-aware optimization, and efficient key-value separation.

## Project Structure

| Directory | Purpose                                                                  |
| --------- | ------------------------------------------------------------------------ |
| docs/     | User/team documentation                                                  |
| ai/       | **AI session context** - workspace for tracking state across sessions    |
| src/      | Core implementation (db/, memtable/, wal/, sstable/, compaction/, vlog/) |
| tests/    | Integration tests (25+ test files)                                       |
| benches/  | Criterion benchmarks (YCSB, baseline comparisons)                        |
| fuzz/     | Fuzzing targets (db_operations, wal, sstable, vlog)                      |
| examples/ | Usage examples and baseline benchmarks                                   |

### AI Context Organization

**Purpose:** AI maintains project context between sessions using ai/

**Session files** (read every session):

- ai/STATUS.md — Current state, metrics, blockers (read FIRST)
- ai/TODO.md — Tasks (fallback, prefer beads `bd`)
- ai/ROADMAP.md — Data integrity hardening phases

**Reference files** (loaded on demand):

- ai/research/ — External research
- ai/design/ — Component specs
- ai/tmp/ — Temporary artifacts (gitignored)

**Task tracking:** Beads (`bd`) initialized. Use `bd list`, `bd create`, `bd close`.

## Technology Stack

| Component       | Technology                              |
| --------------- | --------------------------------------- |
| Language        | Rust (nightly, edition 2021)            |
| Framework       | None (embedded library)                 |
| Package Manager | Cargo                                   |
| Allocator       | jemalloc (tikv-jemallocator)            |
| Compression     | LZ4 (fast), ZSTD (high ratio)           |
| Hashing         | xxhash (bloom), foldhash (partitioning) |
| Testing         | proptest, criterion, cargo-fuzz         |

## Commands

```bash
# Build
rustup override set nightly
cargo build --release

# Test
cargo test --lib              # Unit tests
cargo test                    # All tests
cargo test --features failpoints failpoint  # Crash testing

# Bench
cargo bench

# Fuzz
cargo +nightly fuzz run db_operations

# Lint
cargo clippy --workspace --no-default-features --lib -- -D clippy::correctness -W clippy::pedantic
```

## Verification Steps

Commands to verify correctness (must pass):

- Build: `cargo build --release` (zero errors)
- Tests: `cargo test --lib` (all pass)
- Clippy: `cargo clippy --lib` (zero warnings with pedantic)
- Docs: `cargo doc --no-deps` (zero warnings)

## Architecture

- **Memtable**: Partitioned skiplist (16 partitions) with ArcSwap for lock-free reads
- **WAL**: Write-ahead log with configurable sync (SyncAll/SyncData/None)
- **SSTable**: ALEX learned index + bloom filters + LZ4/ZSTD compression
- **VLog**: WiscKey-style value separation for large values
- **Compaction**: 7-level LSM with Dostoevsky adaptive strategy

## API

```rust
use seerdb::{DB, DBOptions};

let db = DB::open(DBOptions::default())?;
db.put(b"key", b"value")?;
db.get(b"key")?;
db.delete(b"key")?;
db.flush()?;
```

## Code Standards

| Aspect      | Standard                                          |
| ----------- | ------------------------------------------------- |
| Naming      | `put`/`get`/`delete` (RocksDB convention)         |
| Visibility  | Minimal public API, internal modules `pub(crate)` |
| Errors      | `thiserror` for library errors                    |
| Async       | Sync for files, tokio for network, rayon for CPU  |
| Allocations | Prefer `&str`/`&[T]` over `String`/`Vec<T>`       |

## Verification Infrastructure

| Category       | Status | Details                                       |
| -------------- | ------ | --------------------------------------------- |
| Property-based | ✅     | proptest (8 properties)                       |
| Data integrity | ✅     | 16 tests (WAL, flush, compaction, concurrent) |
| Fuzzing        | ✅     | 4 targets: db_ops, wal, sstable, vlog         |
| Failpoints     | ✅     | 4 crash injection points                      |
| Benchmarks     | ✅     | YCSB, baseline vs RocksDB/sled/fjall          |

## Current Focus

See ai/STATUS.md for current state, ai/ROADMAP.md for data integrity hardening phases.
