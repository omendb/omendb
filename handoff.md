# Handoff — seerdb

## Current State (2026-06-11)

**Not a working storage engine yet.** Components are implemented and tested in isolation but not integrated.

## What Exists

| Module | Tests | Purpose |
|--------|-------|---------|
| btree/node | 17 | 4KB slotted page with prefix compression |
| btree/tree | 8 | B-tree ops (in-memory Vec<Node>) |
| buffer | 5 | Buffer pool with clock eviction |
| blob | 6 | Append-only blob files |
| pmt | 7 | Page mapping table |
| wal | 6 | CRC32C checksummed WAL records |
| concurrency | 12 | Hybrid latches, transactions |
| device | 5 | File I/O with O_DIRECT |
| **Total** | **72** | |

## Integration Roadmap

Four tasks to reach a working `DB::open/put/get/delete` API:

| Order | Task | Description |
|-------|------|-------------|
| 1 | tk-veoi | DB struct + file management + page allocator |
| 2 | tk-dtfo | Write path: put → WAL → buffer → device → PMT |
| 3 | tk-utsr | Read path: get → PMT → buffer → device |
| 4 | tk-ggtk | Crash recovery: WAL replay + PMT rebuild |

After: Benchmarks (tk-u62d), Fuzz targets (tk-ircl)

## Key Files

| File | Purpose |
|------|---------|
| `ai/design/engine_spec.md` | Complete engine specification |
| `ai/PLAN.md` | Integration roadmap |
| `ai/STATUS.md` | Current state |
| `ai/DECISIONS.md` | 9 ADRs |

## Environment

- Branch: `dev` (Rust), `dev-mojo` (Mojo port at `../seerdb-mojo`)
- Rust: stable, edition 2024
- Tests: `cargo test --lib` (72 pass)
- Lint: `cargo clippy --all-features -- -D warnings` (clean)
