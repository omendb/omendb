# seerdb

Research-grade LSM storage engine with learned data structures.

[![Crates.io](https://img.shields.io/crates/v/seerdb.svg)](https://crates.io/crates/seerdb)
[![Docs.rs](https://docs.rs/seerdb/badge.svg)](https://docs.rs/seerdb)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Modern embedded storage engine integrating learned indexes (ALEX), key-value separation (WiscKey), and workload-aware compaction from recent systems research.

## Features

- **Learned indexes** (ALEX) for faster lookups
- **Key-value separation** (WiscKey) for lower write amplification
- **OCC transactions** with snapshot isolation
- **Point-in-time snapshots** for consistent reads
- **Range queries** with prefix scans and k-way merge iteration
- **Tiered storage** with S3/GCS/Azure cold tier support
- **Compression** (ZSTD/LZ4), SIMD optimizations, lock-free structures

## Installation

```toml
[dependencies]
seerdb = "0.0.1-beta"
```

Requires nightly Rust for SIMD features:

```bash
rustup override set nightly
```

## Quick Start

```rust
use seerdb::{DB, DBOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DB::open(DBOptions::default())?;

    // Basic operations
    db.put(b"key", b"value")?;
    let val = db.get(b"key")?;
    db.delete(b"key")?;

    // Batch writes (atomic)
    let mut batch = db.batch();
    batch.put(b"user:1", b"alice");
    batch.put(b"user:2", b"bob");
    batch.commit()?;

    // Range queries
    for result in db.range(b"user:", Some(b"user:~"))? {
        let (key, value) = result?;
        println!("{:?} = {:?}", key, value);
    }

    // Point-in-time snapshots
    let snapshot = db.snapshot();
    db.put(b"key", b"new_value")?;
    // Snapshot still sees old state
    let old_val = snapshot.get(b"key")?;

    Ok(())
}
```

## Performance

Benchmarks on Fedora (i9-13900KF, 32GB DDR5):

**Scale**
- 100M keys: 930K writes/sec sustained
- Write amplification: 1.01x

**SSTable Lookups**

| Operation | 1K entries | 10K entries | 100K entries |
|-----------|-----------|-------------|--------------|
| Existing key | 3.27 µs | 39.16 µs | 550 µs |
| Missing key (bloom) | 7.82 µs | 9.43 µs | 14.93 µs |

Bloom filters reject missing keys ~37x faster than existing key lookups at 100K entries.

Reproduce with:

```bash
cargo run --release --example write_amplification
cargo bench
```

## Architecture

| Component | Implementation |
|-----------|---------------|
| Memtable | Partitioned skiplist (16 partitions) |
| WAL | Lock-free write-ahead log |
| SSTable | ALEX learned index + bloom filters |
| Compaction | 7-level LSM with tiered/leveled hybrid |
| Value Log | WiscKey separation for large values |
| Cache | Lock-free block cache |

## References

- [ALEX: An Updatable Adaptive Learned Index](https://dl.acm.org/doi/10.1145/3318464.3389711) (Ding et al., 2020)
- [WiscKey: Separating Keys from Values](https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu) (Lu et al., 2016)
- [Dostoevsky: Better Space-Time Trade-Offs for LSM-Trees](https://dl.acm.org/doi/10.1145/3183713.3196927) (Dayan et al., 2018)

## License

[Apache License 2.0](LICENSE)
