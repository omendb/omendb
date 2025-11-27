# seerdb

LSM storage engine with learned data structures.

## Build

```bash
# Requires nightly
rustup override set nightly

# Build
cargo build --release

# Test
cargo test --lib

# Bench
cargo bench
```

## Architecture

- **Memtable**: Partitioned skiplist (16 partitions)
- **WAL**: Write-ahead log with configurable sync
- **SSTable**: ALEX learned index + bloom filters
- **VLog**: WiscKey value separation
- **Compaction**: 7-level LSM

## API

```rust
use seerdb::{DB, DBOptions};

let db = DB::open(DBOptions::default())?;
db.put(b"key", b"value")?;
db.get(b"key")?;
db.delete(b"key")?;
```

## Style

- `put`/`get`/`delete` (RocksDB convention)
- Minimal public API surface
- Internal modules are `pub(crate)`
