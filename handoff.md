# seerdb Handoff

## Current State (2026-06-11)

**85 tests passing.** Data persists to disk. Crash recovery via WAL replay works.

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
└── BlobManager (KV separation)
```

## Next Steps

1. **KV separation**: Integrate blob manager with read/write path
2. **Concurrency**: Page guards with optimistic lock coupling
3. **Benchmarks**: YCSB, micro-ops, write amplification
4. **Fuzz targets**: B-tree, WAL, blob, page parsing

## Key Documents

- `ai/PLAN.md` — Complete roadmap
- `ai/design/engine_spec.md` — Engine specification
- `ai/design/api_spec.md` — API specification
- `ai/design/shared_api.md` — Cross-implementation alignment
