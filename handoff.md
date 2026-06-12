# seerdb Handoff

## Current State (2026-06-11)

**88 tests passing.** Transaction support added.

## What Changed

1. Added `StorageEngine` to coordinate B-tree, buffer manager, PMT, device
2. Implemented `flush`: serialize B-tree nodes to pages, write to device
3. Implemented `load_from_disk`: read pages from device, deserialize to nodes
4. Integrated `StorageEngine` into DB wrapper
5. Added persistence test: write → close → reopen → read
6. Added crash recovery via WAL replay
7. Added Put/Delete WAL record types with key-value data
8. Write WAL to disk before modifying B-tree (append mode)
9. Drop no longer calls close() to preserve WAL for crash recovery
10. Added blob file persistence (to_bytes/from_bytes)
11. Load blob files from disk on open
12. Save blob files to disk during flush
13. Added insert_blob method to BTree for proper blob pointer storage
14. Added TransactionManager to DB
15. Added begin_transaction, commit_transaction, abort_transaction methods
16. Added concurrent transaction test

## Architecture

```
DB
├── StorageEngine
│   ├── BTree (logical operations)
│   ├── BufferManager (page cache)
│   ├── PMT (page locations)
│   ├── PageAllocator (page IDs)
│   └── Device (file I/O)
├── WalManager (crash recovery)
├── BlobManager (KV separation, persistence)
└── TransactionManager (MVCC, snapshot isolation)
```

## Next Steps

1. **Benchmarks**: YCSB, micro-ops, write amplification
2. **Fuzz targets**: B-tree, WAL, blob, page parsing

## Key Documents

- `ai/PLAN.md` — Complete roadmap
- `ai/design/engine_spec.md` — Engine specification
- `ai/design/api_spec.md` — API specification
- `ai/design/shared_api.md` — Cross-implementation alignment
