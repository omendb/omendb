# ADR 0010: Compact row layout and optional column families

- **Status:** accepted target architecture; physical encoding not yet stable
- **Scope:** OmenDB relational row representation, primary-tree values, wide/cold
  column placement, and decode into the execution engine
- **Depends on:** [ADR 0007](0007-runtime-execution-and-contention.md) and
  [ADR 0009](0009-buffered-btree-and-materialization.md)

## Context

The current relational row format is intentionally simple:

```text
Row { primary: Key, values: Vec<Value> }

encoded row:
  magic
  version
  value count
  repeated [runtime type tag + encoded value]
```

This made the first SQL/type implementation easy to validate, but it is not a
suitable final hot-path representation. The table schema already fixes each
column's type, so storing a type tag per value is redundant. `Vec<Value>` also
forces enum dispatch and heap ownership for variable-length values on ordinary
row decode, which is directly at odds with the low-allocation OLTP and typed
batch paths in ADR 0007.

At the same time, storing every column as an independent KV entry would multiply
transaction intents/MVCC metadata and primary-key repetition. CockroachDB's
historical move from per-column KV entries to column families is useful evidence:
grouping columns substantially reduced transaction/storage overhead, while
multiple families retained the ability to avoid rewriting large/cold columns.

The target should optimize common narrow OLTP rows without making wide rows,
large values, or scan execution pathological.

## Decision

### 1. Stored row values are schema-driven and self-delimiting, not dynamically typed

A row record is interpreted against a **row layout version** derived from its
table schema. The physical record does not repeat a generic `Value` type tag for
every field.

The target family-value shape is conceptually:

```text
RowFamilyRecord
  layout_version
  flags
  null_bitmap
  optional variable-offset directory
  fixed-width values
  variable-width payload
```

The exact byte order/alignment remains benchmark-gated, but the following are
requirements:

- fixed-width fields (bool/integer/float/date/timestamp/uuid and suitable
  decimal forms) decode without allocation;
- NULL is represented by a bitmap/state bit, not a per-value enum tag;
- variable-width values are located through compact offsets/lengths and can be
  borrowed from a guarded page/value buffer when the caller does not need to
  retain them;
- corruption checks are explicit and bounded;
- the decoder can skip unrequested columns without constructing every value;
- appended nullable/default-compatible columns can be materialized logically
  without rewriting every old row immediately.

### 2. Do not duplicate row identity unless the chosen physical layout benefits

The logical primary key and physical row identity are distinct concepts. In a
clustered primary-B-tree layout, key bytes already identify the row and should
not automatically be copied into the row payload. In a heap/row-page layout,
the primary index may instead map the logical key to a compact row locator.

The row decoder receives the logical/physical identity required by its access
method and reconstructs requested primary-key columns without mandatory
duplication. Intentional duplication remains valid for covering/locality or
recovery reasons, but it is a measured layout choice rather than a format law.

This keeps ADR 0010 compatible with ADR 0013's required clustered-row versus
heap+primary-index comparison.

### 3. One primary family is the default

Normal narrow tables store their non-primary data in one primary row-family
value. This minimizes:

- MVCC/version records per row;
- write intents and transaction bookkeeping;
- B-tree lookups;
- repeated key bytes;
- checksums/framing overhead;
- cache misses.

This is the default because most OLTP rows are small enough that an extra KV
indirection per column is more expensive than decoding a compact record.

### 4. Wide/cold columns may be split into additional families

A table may have multiple physical column families sharing the same logical
primary-key identity. Families are a storage/layout optimization, not a SQL
semantic boundary.

Useful cases include:

- large JSON/text/blob-like values;
- columns updated at very different frequencies;
- cold columns rarely read by latency-critical requests;
- large values whose separate lifetime reduces write amplification;
- tables where a common hot subset fits substantially better in cache alone.

A family has a stable family identifier in the catalog and shares one logical
row identity. A clustered B-tree implementation may encode family identity next
to the primary key; a row-page implementation may keep family references in the
row header or a side structure. The physical mapping is access-method-specific.

Whichever layout wins must preserve efficient whole-row and family-specific
access without multiplying transaction/MVCC state unnecessarily.

The first implementation may use one family only. The format/catalog must avoid
making that assumption irreversible.

### 5. Large values may use stable logical handles outside the hot row page

Sufficiently large/cold variable values may move into a kernel-managed
large-value arena while the row/family stores a compact logical handle. This is
not just a size threshold: access/update frequency, locality, page pressure and
write amplification all matter.

A large-value handle must be stable across physical relocation. Physical
placement is private to the arena so compaction/GC does not rewrite every row
reference. Any adopted design needs bounded indirection depth, locality-aware
prefetch, exact liveness/retention accounting, crash-safe relocation/GC, and
failure-atomic handle publication.

This follows the trade-off seen in value-separated systems: moving bulky values
out of hot index/row pages can reduce rewrite and cache amplification, but
indirection and lost locality can make frequently-read values slower. The
threshold and arena design therefore remain benchmark-gated.

### 6. Execution uses borrowed typed views before owned `Value`s

The storage decoder exposes a row view with schema-known typed accessors. A
simple point query or predicate should be able to inspect integer/text/UUID/etc.
fields directly from guarded row bytes without first constructing
`Vec<Value>`.

Conceptually:

```text
page/value guard
      |
EncodedRowView<'guard>
      |
  +---+----------------------+
  |                          |
OLTP micro-plan          batch decoder
borrow selected fields   copy/decode selected columns
                         into TypedBatch
```

`Value` remains useful at public API/protocol boundaries and for generic code,
but it stops being the mandatory representation between every storage and
execution operator.

### 7. Batch decode is column-selective and vector-friendly

A scan plan declares the columns it needs. The storage cursor decodes only those
columns from each row/family into typed batch vectors. Selection predicates may
be evaluated before decoding expensive/cold projected columns when the family
layout allows it.

This gives OmenDB an OLTP row store without forcing analytical execution to
operate row-at-a-time.

SIMD kernels may accelerate null filtering, comparisons, numeric expressions,
UTF-8 validation, and batch decode where measurements justify it. Stored row
bytes remain architecture-neutral.

### 8. Schema evolution uses explicit layout versions

A table catalog object maps each row-layout version to column IDs/types/family
placement. New writes use the current layout. Readers can decode retained older
versions while migration/backfill proceeds.

Metadata-only changes should remain metadata-only when possible:

- rename column: no row rewrite;
- append nullable column: old layouts imply NULL;
- append compatible constant default: old layouts may imply the default when
  SQL semantics permit it;
- family placement change: online backfill rather than stop-the-world rewrite.

Changes that alter binary interpretation (incompatible type conversion, dropped
storage still needed by retained snapshots, etc.) use explicit versioned
migration and GC.

The number of simultaneously readable layout versions is bounded by catalog
retention/migration policy, not allowed to grow forever.

### 9. Secondary indexes choose covering payloads deliberately

A secondary index entry always contains enough information to reach the base row
identity. A compact generation-safe physical locator may be cached/embedded as
an optimization only if relocation/reuse semantics prove it safe; logical row
identity remains the correctness fallback. The entry may additionally include
declared/inferred covering columns in a compact schema-driven payload so common
queries avoid a base-row fetch.

Do not copy the full row into every index by default. Covering payload and index
key layout are optimizer/schema choices with write-amplification costs exposed in
EXPLAIN/diagnostics.

### 10. Column-family layout begins automatic but is inspectable/overrideable

The default should require no tuning. Initial heuristics can keep ordinary
columns in family 0 and separate values above configured/observed size and
access thresholds only when there is clear benefit.

If multiple families become user-visible, expose their effective layout and an
advanced override, but do not require application authors to become storage
engine experts for normal schemas.

Longer term, background statistics may recommend or automatically migrate family
placement under an explicit policy. Any automatic migration is online and uses
the normal transaction/change-stream machinery.

## Non-goals

- a pure columnar OLTP storage engine;
- one KV entry per SQL column;
- exposing SeerDB family keys as a stable SQL/API contract;
- architecture-specific row bytes;
- retaining `Vec<Value>` because it is convenient for the current executor.

## Migration strategy

1. Introduce a schema-derived row-layout descriptor and borrowed decoder next to
   the current codec.
2. Add a new row format version for one-family compact rows; old format remains
   read-only during qualification.
3. Move OLTP reads/predicates/index maintenance onto borrowed typed accessors.
4. Add typed selective batch decode.
5. Benchmark and only then add additional column-family placement.
6. Migrate fixtures/offline data explicitly; do not silently reinterpret old
   bytes.

## Acceptance gates

- a narrow fixed-type row has materially fewer bytes and allocations than the
  current tag-per-`Value` encoding;
- point lookup can inspect requested columns without allocating one object per
  field;
- scans decode only projected/predicate columns;
- nullable-column append remains metadata-only for old rows;
- schema/version corruption fails closed;
- row decode/encode fuzz/property tests cover every type and malformed offset;
- point/RMW, 20+ column updates, wide/cold values, and projected scans are benchmarked before choosing family heuristics;
- if large-value separation is enabled, relocation/GC/reopen tests prove stable handles cannot alias reclaimed content and measurements include both read-indirection and write-amplification effects;
- covering index payloads show their read benefit and write/space cost;
- retained snapshots remain able to decode every layout version they reference.

## Research inputs

- CockroachDB column families — reduction of per-column KV/MVCC/write overhead
  while allowing hot/cold family separation;
- Pebble value separation — large-value handles to reduce rewrite/compaction
  cost, with read-indirection trade-offs;
- SQLite/Turso record format — compact record headers and variable integer
  encodings as a simplicity reference, not a compatibility target;
- PAX/columnar block layouts — selective/vector-friendly decode ideas for
  batches and immutable secondary/cold structures.

## Consequences

- OmenDB remains a row-oriented OLTP database physically, but the row format is
  designed for typed vector execution rather than tied to generic `Value` enums.
- SeerDB stays schema-agnostic: row-layout interpretation belongs to OmenDB.
- Wide/cold data can stop polluting hot B-tree pages without forcing every table
  into vertically partitioned storage.
- Reduced allocation and cache footprint directly target the current profile's
  393 allocations / 18.8 KiB per transaction rather than treating that cost as
  inevitable.
