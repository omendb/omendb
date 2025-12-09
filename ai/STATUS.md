# STATUS - seerdb

**Last Updated**: December 9, 2025
**Version**: 0.0.9

---

## Current State

| Metric | Value |
|--------|-------|
| **Tests** | 249 (with arena-memtable), 243 (default) |
| **Clippy** | 0 warnings (pedantic) |
| **Performance** | 4.7M PUT, 5.3M GET ops/sec (crossbeam) |

---

## Active Work

### Arena Skiplist - DEFERRED

**Branch**: `feat/arena-skiplist` (not merged to main)

**Status**: Implementation complete but deferred due to performance issues

**What was built**:
- Lock-free concurrent skiplist with arena allocation (`src/arena_skiplist/`)
- ArenaMemtable wrapper (`src/memtable/arena.rs`)
- Feature flag `arena-memtable` for A/B testing
- 249 tests pass

**Performance comparison** (100K ops):
| Operation | Crossbeam | Arena | Delta |
|-----------|-----------|-------|-------|
| PUT | 213 ns | 226 ns | +6% slower |
| GET (hit) | 189 ns | 180 ns | 5% faster |
| SCAN | 14 ns/entry | 23 ns/entry | **64% slower** |

**Why deferred**:
- Length-prefix encoding required for variable-length key MVCC ordering
- This breaks lexicographic order, requiring O(n) scan + sort
- Option 3 (custom comparator) would fix this but adds bug risk
- paste crate advisory (RUSTSEC-2024-0436) is "unmaintained" not "vulnerable"

**To resume**: Implement custom comparator in ArenaSkiplist to avoid encoding overhead

---

## Recent Releases

### v0.0.9 (Dec 9)
- Clippy doc_markdown fixes

### v0.0.8 (Dec 8)
- Hot path allocation elimination (-44% write latency)
- Zero-alloc memtable lookups via InternalKeyRef
- Stack-based varint encoding

---

## Known Issues

### Fedora Concurrent Test Hang (Low Priority)
Tests hang on Fedora in release mode with 1KB memtable. Does NOT affect real-world usage (64MB+ memtable).

---

## Beads

| ID | Title | Priority | Status |
|----|-------|----------|--------|
| seerdb-vk5 | Monitor SKL 0.23 | P2 | open |

---

## Architecture

See AGENTS.md for project structure, DECISIONS.md for ADRs.
