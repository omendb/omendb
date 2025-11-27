# seerdb

Research-grade LSM storage engine with learned data structures.

[![Crates.io](https://img.shields.io/crates/v/seerdb.svg)](https://crates.io/crates/seerdb)
[![Docs.rs](https://docs.rs/seerdb/badge.svg)](https://docs.rs/seerdb)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Embedded key-value storage integrating learned indexes (ALEX), key-value separation (WiscKey), and workload-aware compaction from recent systems research.

## Installation

```toml
[dependencies]
seerdb = "0.0.1-beta"
```

Requires nightly Rust:

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

    // Range scan
    for result in db.prefix(b"user:")? {
        let (key, value) = result?;
        println!("{:?} = {:?}", key, value);
    }

    // Point-in-time snapshot
    let snapshot = db.snapshot();
    db.put(b"key", b"new_value")?;
    assert_eq!(snapshot.get(b"key")?, val); // snapshot sees old state

    Ok(())
}
```

## Features

- **Learned indexes** (ALEX) for adaptive key distribution
- **Key-value separation** (WiscKey) for reduced write amplification
- **OCC transactions** with snapshot isolation
- **Point-in-time snapshots**
- **Range queries** and prefix scans
- **Tiered storage** with S3/GCS/Azure support
- **Compression** (ZSTD/LZ4) and SIMD optimizations

## License

[Apache License 2.0](LICENSE)
