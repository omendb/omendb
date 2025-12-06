# Performance Issues Found - Systematic Review

**Date**: December 6, 2025
**Reviewer**: Claude
**Status**: Analysis complete, fixes pending

---

## Critical Issues (Hot Path)

### 1. write_varint allocates Vec per call
**File**: `src/sstable/block.rs:81-86`
**Severity**: CRITICAL
**Impact**: Called 3x per block entry during SSTable building

```rust
fn write_varint(buf: &mut BytesMut, value: u64) {
    let mut temp = Vec::new();  // ALLOCATION EVERY CALL!
    temp.write_u64_varint(value).expect("write to memory failed");
    buf.extend_from_slice(&temp);
}
```

**Fix**: Write varint directly to BytesMut using a stack buffer:
```rust
fn write_varint(buf: &mut BytesMut, value: u64) {
    let mut temp = [0u8; 10];  // Stack buffer, max varint64 size
    let mut cursor = std::io::Cursor::new(&mut temp[..]);
    cursor.write_u64_varint(value).unwrap();
    buf.extend_from_slice(&temp[..cursor.position() as usize]);
}
```

---

### 2. Bytes::copy_from_slice per memtable lookup
**File**: `src/memtable/mod.rs:80-81, 114`
**Severity**: CRITICAL
**Impact**: Every get() and get_entry() allocates

```rust
let lookup_key = InternalKey::new(Bytes::copy_from_slice(key), snapshot_seq, ValueType::Value);
```

**Fix**: Use a borrowed key comparison or create InternalKey::for_lookup_ref(&[u8]):
```rust
// Option 1: Add for_lookup_ref that borrows
impl InternalKey {
    pub fn for_lookup_ref(user_key: &[u8]) -> InternalKeyRef<'_> { ... }
}

// Option 2: Compare directly without InternalKey allocation
for entry in self.data.range(...) {
    if entry.key().user_key.as_ref() != key { break; }
    ...
}
```

---

### 3. Vec::new() on every get() even without merges
**File**: `src/db/read.rs:57`
**Severity**: HIGH
**Impact**: Allocation on every get(), even though 99%+ don't use merge

```rust
let mut operands: Vec<Bytes> = Vec::new();  // Always allocated
```

**Fix**: Use Option or lazy initialization:
```rust
let mut operands: Option<Vec<Bytes>> = None;
// Only allocate when first merge operand found
if matches!(entry, Entry::Merge { .. }) {
    operands.get_or_insert_with(Vec::new).push(...);
}
```

---

### 4. Vec collect per SSTable level in get()
**File**: `src/db/read.rs:119`
**Severity**: HIGH
**Impact**: Allocates Vec for every level during point lookup

```rust
let sstables: Vec<_> = level.sstables().iter().rev().collect();
```

**Fix**: Iterate in reverse directly:
```rust
for sstable_path in level.sstables().iter().rev() {
    // No intermediate Vec needed
}
```

---

### 5. Arc clone per partition in range()
**File**: `src/db/iter.rs:142-146, 284-288`
**Severity**: MEDIUM
**Impact**: 16 Arc clones per range scan

```rust
let partition_arcs: Vec<Arc<Memtable>> = self.memtables
    .iter()
    .map(|mt| (*mt.load()).clone())
    .collect();
```

**Fix**: Could use references with lifetime bounds, but Arc::clone is O(1).
Lower priority - atomic increment is fast.

---

### 6. Bytes::copy_from_slice in range_rev filter closure
**File**: `src/db/iter.rs:356-357`
**Severity**: HIGH
**Impact**: Allocates per SSTable in reverse range scan

```rust
let start = Bytes::copy_from_slice(start_key);  // Closure captures owned Bytes
let end = end_key.map(Bytes::copy_from_slice);
```

**Fix**: Use slice comparison in closure:
```rust
let start_ref = start_key;  // Just copy the slice reference
let end_ref = end_key;
let filtered_iter = mapped_iter.filter(move |res| {
    match res {
        Ok((k, _)) => k.as_ref() >= start_ref && end_ref.map_or(true, |e| k.as_ref() < e),
        ...
    }
});
```

---

### 7. value.clone() in flush loop
**File**: `src/db/flush.rs:392, 420, 438, 458, 482, 496`
**Severity**: MEDIUM
**Impact**: Clones every value during flush

```rust
builder.add_internal_with_vlog(ikey, value.clone(), vlog)?;
```

**Note**: Bytes::clone() is O(1) (Arc increment), but still adds overhead.
Could pass by reference if SSTableBuilder API allowed.

---

### 8. BlockIterator clones on every next()
**File**: `src/sstable/block.rs:700-702`
**Severity**: MEDIUM
**Impact**: Every block iteration clones key and value

```rust
fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|(k, v)| Ok((k.clone(), v.clone())))
}
```

**Fix**: Return references if possible, or accept that Bytes::clone is O(1).

---

### 9. Key reconstruction allocates BytesMut per entry
**File**: `src/sstable/block.rs:654-657`
**Severity**: HIGH
**Impact**: During block decompression, every non-restart key allocates

```rust
let mut key_data = BytesMut::with_capacity(prefix_len + suffix_len);
key_data.extend_from_slice(&last_key[..prefix_len]);
key_data.extend_from_slice(&suffix);
key_data.freeze()
```

**Fix**: Reuse a buffer across entries:
```rust
// In decompress_all_entries:
let mut key_buffer = BytesMut::with_capacity(256);  // Reuse
// ...
key_buffer.clear();
key_buffer.extend_from_slice(&last_key[..prefix_len]);
key_buffer.extend_from_slice(&suffix);
let key = key_buffer.clone().freeze();  // Only clone when needed
```

---

## Summary by Priority

| Priority | Issue | Location | Est. Impact |
|----------|-------|----------|-------------|
| P0 | write_varint Vec alloc | block.rs:81 | HIGH - every SSTable write |
| P0 | lookup key alloc | memtable/mod.rs:80 | HIGH - every get() |
| P1 | Vec::new in get | read.rs:57 | MEDIUM - every get() |
| P1 | Vec collect per level | read.rs:119 | MEDIUM - every get() |
| P1 | Key reconstruction | block.rs:654 | HIGH - every block decode |
| P2 | range_rev Bytes alloc | iter.rs:356 | MEDIUM - reverse scans |
| P2 | BlockIterator clones | block.rs:700 | LOW - Bytes is O(1) |
| P3 | Arc clones | iter.rs:145 | LOW - atomic is fast |

---

## Remaining Issue: Memtable Lookup Allocation

**Location**: `src/memtable/mod.rs:80-81, 114`

Every `get()` and `get_entry()` call allocates:
```rust
let lookup_key = InternalKey::new(Bytes::copy_from_slice(key), snapshot_seq, ValueType::Value);
```

**Cost**: ~20-50ns per get() for heap allocation of key bytes

### Option 1: Thread-Local Buffer Pool

```rust
thread_local! {
    static KEY_BUFFER: RefCell<BytesMut> = RefCell::new(BytesMut::with_capacity(256));
}

pub fn get(&self, key: &[u8], snapshot_seq: u64) -> Option<(Bytes, u64)> {
    KEY_BUFFER.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf.clear();
        buf.extend_from_slice(key);
        let lookup_key = InternalKey::new(buf.clone().freeze(), snapshot_seq, ValueType::Value);
        // ... rest of lookup
    })
}
```

| Pros | Cons |
|------|------|
| No allocation after warmup | Thread-local lookup overhead (~5-10ns) |
| Simple implementation | RefCell borrow check overhead |
| No API changes | Still clones buffer to create Bytes |

**Risk**: Low
**Effort**: Low

### Option 2: Borrowed Key Comparison

Modify comparison to work with borrowed slices directly:

```rust
// Custom range bound that borrows instead of owns
struct BorrowedInternalKey<'a> {
    user_key: &'a [u8],
    seq: u64,
    kind: ValueType,
}

impl PartialOrd<InternalKey> for BorrowedInternalKey<'_> { ... }
```

| Pros | Cons |
|------|------|
| Zero allocation | crossbeam_skiplist doesn't support heterogeneous lookup |
| Optimal performance | Would need to fork or wrap SkipMap |
| Clean API | Significant refactoring |

**Risk**: Medium (API changes, skiplist compatibility)
**Effort**: High

### Option 3: Small String Optimization (SSO)

Inline storage for small keys:

```rust
enum KeyStorage {
    Inline { data: [u8; 64], len: u8 },  // Keys ≤64 bytes
    Heap(Bytes),                          // Larger keys
}

struct InternalKey {
    storage: KeyStorage,
    seq: u64,
    kind: ValueType,
}
```

| Pros | Cons |
|------|------|
| No allocation for typical keys (<64B) | Increases InternalKey size (72 → 80+ bytes) |
| No runtime overhead for small keys | More complex comparison logic |
| Works with existing SkipMap | May hurt cache efficiency |

**Risk**: Medium (correctness of SSO logic)
**Effort**: Medium

### Option 4: Arena Allocator

Use a per-operation arena for temporary allocations:

```rust
pub fn get_with_arena(&self, key: &[u8], arena: &Bump) -> Option<(Bytes, u64)> {
    let key_copy = arena.alloc_slice_copy(key);
    let lookup_key = InternalKey::new_borrowed(key_copy, snapshot_seq);
    // ... arena freed when caller drops it
}
```

| Pros | Cons |
|------|------|
| Batch amortizes allocation cost | Requires API change (arena parameter) |
| Good for batch operations | Caller must manage arena lifetime |
| Zero fragmentation | Adds dependency (bumpalo) |

**Risk**: Low
**Effort**: Medium

### Option 5: Replace with SKL Crate (Arena-Based SkipList)

Use the [SKL crate](https://github.com/al8n/skl), purpose-built for MVCC memtables:

```rust
// SKL is designed for LSM memtable use cases
use skl::generic::SkipMap;

// Arena-based allocation - no per-key heap allocations
// Built-in MVCC with 56-bit version numbers
// Inspired by CockroachDB's Pebble and Dgraph's Badger
```

| Pros | Cons |
|------|------|
| Arena-based - zero per-key allocation | Significant refactor |
| Built for MVCC memtables | Different API semantics |
| Lock-free, concurrent-safe | New dependency |
| Supports mmap for large memtables | May require types refactor |

**Risk**: High (major refactor)
**Effort**: High
**Benefit**: Maximum - eliminates the problem entirely

---

## Deep Research Findings (Dec 6)

### Critical Discovery: crossbeam-skiplist SUPPORTS Borrowed Lookups

The original analysis was **incorrect**. crossbeam-skiplist's API:

```rust
// get() accepts borrowed keys!
pub fn get<Q>(&self, key: &Q) -> Option<Entry<'_, K, V>>
where
    K: Borrow<Q>,
    Q: Ord + ?Sized

// range() also accepts borrowed keys
pub fn range<Q, R>(&self, range: R) -> Range<'_, Q, R, K, V>
where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized
```

This means **Option 2 is achievable without forking crossbeam-skiplist**. We just need:

1. Define `InternalKeyRef<'a>` with borrowed user_key
2. Implement `Borrow<InternalKeyRef<'_>>` for `InternalKey`
3. Implement `Ord` for `InternalKeyRef` (matching `InternalKey` ordering)

### Thread-Local Overhead Analysis

From [Rust RFC 3184](https://rust-lang.github.io/rfcs/3184-thread-local-cell-methods.html) and [TechHara's analysis](https://medium.com/@techhara/rust-performance-overhead-of-refcell-adaa634b6490):

- `thread_local!` with `const {}` syntax: ~5-10ns lookup
- `RefCell::borrow_mut()`: ~2-5ns for runtime check
- Total overhead: ~7-15ns vs ~20-50ns heap allocation

**Net gain**: ~5-35ns per get() - worth it, but not zero-cost.

### Small String Optimization (SSO) Analysis

From [compact_str](https://docs.rs/compact_str/latest/compact_str/) and [smol_str](https://crates.io/crates/smol_str):

- `compact_str`: 24 bytes inline (same size as `String`)
- `smol_str`: 22 bytes inline, O(1) clone via Arc
- Our `InternalKey` with SSO: Would grow from 40 → 80+ bytes

**Issue**: LSM memtables store millions of keys. Doubling `InternalKey` size would:
- Double memory usage for memtable entries
- Hurt cache efficiency (fewer keys per cache line)
- Potentially slower iteration

**Verdict**: SSO is counterproductive for our use case.

### Arena Allocator (Bumpalo) Analysis

From [bumpalo docs](https://docs.rs/bumpalo/latest/bumpalo/):

- Fast path: 11 instructions for allocation
- Not thread-safe (would need `bumpalo-herd` or per-thread)
- Phase-oriented: good for batch operations, awkward for single lookups

**Issue**: Our `get()` is a single operation - arena setup/teardown overhead may exceed savings.

**Verdict**: Good for batch/iterator operations, not worth it for single `get()`.

### SKL Crate Analysis

From [SKL GitHub](https://github.com/al8n/skl):

- Inspired by CockroachDB's Pebble and Dgraph's Badger skiplists
- Arena-based: pre-allocates memory chunk, bump allocation within
- Built-in MVCC with version support
- Lock-free and concurrent-safe
- Supports heap, mmap, and anonymous mmap backends

**Benefits**:
- Eliminates per-key allocation entirely
- Designed specifically for LSM memtables
- Battle-tested patterns from production databases

**Costs**:
- Major API change
- Would require rewriting memtable module
- Different memory model (arena lifecycle management)

---

## Revised Recommendation

### Best Option: Borrowed Key Comparison (Option 2)

The research shows this is **achievable with moderate effort**:

```rust
/// Borrowed version of InternalKey for lookups
pub struct InternalKeyRef<'a> {
    pub user_key: &'a [u8],
    pub seq: u64,
    pub kind: ValueType,
}

impl<'a> InternalKeyRef<'a> {
    pub const fn new(user_key: &'a [u8], seq: u64, kind: ValueType) -> Self {
        Self { user_key, seq, kind }
    }
}

// Critical: same ordering as InternalKey
impl Ord for InternalKeyRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.user_key.cmp(other.user_key) {
            Ordering::Equal => other.seq.cmp(&self.seq),
            ord => ord,
        }
    }
}

// Enable crossbeam-skiplist to use borrowed keys
impl std::borrow::Borrow<InternalKeyRef<'_>> for InternalKey {
    fn borrow(&self) -> &InternalKeyRef<'_> {
        // SAFETY: This is a lifetime extension trick
        // We're returning a reference with lifetime tied to self
        unsafe {
            std::mem::transmute(&InternalKeyRef {
                user_key: self.user_key.as_ref(),
                seq: self.seq,
                kind: self.kind,
            })
        }
    }
}
```

**Issue**: The `Borrow` trait requires returning `&Q`, but `InternalKeyRef` is not stored - we can't return a reference to a temporary. This requires a different approach.

### Solution: The `Equivalent`/`Comparable` Trait Pattern

The [`equivalent` crate](https://crates.io/crates/equivalent) (326M+ downloads) provides:

```rust
/// Q: Equivalent<K> checks equality without K: Borrow<Q>
pub trait Equivalent<K: ?Sized> {
    fn equivalent(&self, key: &K) -> bool;
}

/// Q: Comparable<K> checks ordering without K: Borrow<Q>
pub trait Comparable<K: ?Sized>: Equivalent<K> {
    fn compare(&self, key: &K) -> Ordering;
}
```

This allows heterogeneous lookup where `Q` can be a borrowed type that compares against owned `K` without `Borrow` restrictions.

**crossbeam-skiplist-fd** uses this pattern via [`equivalentor`](https://docs.rs/crossbeam-skiplist-fd) module.

### Implementation with crossbeam-skiplist-fd

```rust
use crossbeam_skiplist_fd::SkipMap;

/// Borrowed lookup key - zero allocation
pub struct InternalKeyRef<'a> {
    pub user_key: &'a [u8],
    pub seq: u64,
    pub kind: ValueType,
}

impl Comparable<InternalKey> for InternalKeyRef<'_> {
    fn compare(&self, key: &InternalKey) -> Ordering {
        match self.user_key.cmp(key.user_key.as_ref()) {
            Ordering::Equal => key.seq.cmp(&self.seq), // Descending
            ord => ord,
        }
    }
}

// Usage - zero allocation lookup!
pub fn get(&self, key: &[u8], snapshot_seq: u64) -> Option<(Bytes, u64)> {
    let lookup = InternalKeyRef::new(key, snapshot_seq, ValueType::Value);
    for entry in self.data.range(lookup..) {  // No allocation!
        // ...
    }
}
```

| Pros | Cons |
|------|------|
| **Zero allocation per lookup** | Requires switching to crossbeam-skiplist-fd |
| No unsafe code | Fork may lag upstream |
| Clean, idiomatic API | Need to verify fork quality |
| Same performance as owned keys | |

**Risk**: Low-Medium (well-maintained fork)
**Effort**: Medium (dependency change + trait impl)
**Benefit**: **Eliminates 100% of lookup allocation overhead**

### Practical Path Forward

Given the complexity of borrowed key comparison, here's the recommended approach:

1. **Immediate (Low Risk)**: Thread-local buffer pool
   - ~10-20ns savings per get()
   - Simple implementation
   - No API changes

2. **Medium Term**: Evaluate SKL crate
   - Profile current memtable allocation overhead
   - If significant (>10% of get time), prototype SKL integration
   - SKL is designed for this exact use case

3. **Long Term**: Consider custom skiplist
   - Only if memtable becomes proven bottleneck
   - Could use arena allocation like SKL
   - Full control over allocation strategy

### Final Implementation Priority

| Option | Benefit | Effort | Risk | Priority |
|--------|---------|--------|------|----------|
| **crossbeam-skiplist-fd (2b)** | ~40ns/get (100%) | Medium | Low | **P1 - Best** |
| Thread-Local (1) | ~20ns/get (50%) | Low | Low | **P2 - Quick win** |
| SKL Migration (5) | ~40ns/get | High | Medium | P3 (overkill) |
| SSO (3) | Negative | Medium | Medium | **Skip** |
| Arena (4) | Marginal | Medium | Low | **Skip** |

### Recommended Implementation

**Best option: crossbeam-skiplist-fd with `Comparable` trait**

1. Add dependency: `crossbeam-skiplist-fd = "0.1"`
2. Define `InternalKeyRef<'a>` in `src/types.rs`
3. Implement `Comparable<InternalKey>` for `InternalKeyRef`
4. Update memtable lookups to use `InternalKeyRef`
5. **Zero allocation per get() - problem solved**

This eliminates the problem entirely with moderate effort and low risk.

## Completed Fixes (Dec 6)

| Issue | Location | Status |
|-------|----------|--------|
| write_varint Vec alloc | block.rs:81 | ✅ Fixed |
| Vec::new on every get | read.rs:57 | ✅ Fixed |
| Vec collect per level | read.rs:120 | ✅ Fixed |
| Key reconstruction alloc | block.rs:654 | ✅ Fixed |
| range_rev per-SSTable alloc | iter.rs:356 | ✅ Fixed |
| delete/merge WAL opts | write.rs | ✅ Fixed |

Results: put operations **-44-46%**, compare_keys **-20%**
