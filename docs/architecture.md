# OmenDB architecture

**Status:** accepted direction; implementation is incomplete and the current
alpha line is not a release of this architecture.

OmenDB is a **server-first PostgreSQL-class relational OLTP database written in
Rust**. PostgreSQL compatibility belongs at deliberate external boundaries
(wire protocol, SQL behavior, drivers, and tooling); OmenDB does not copy
PostgreSQL's internal page layout, executor, WAL, or process-per-connection
model.

The direct Rust API remains a useful embedded and testing surface. It must use
the same transaction and storage semantics as the server, not become a second
database product.

## Product shape

```text
PostgreSQL clients / future native clients
                    |
             OmenDB server
                    |
       session, auth, protocol, SQL
                    |
        planner and typed execution
                    |
       transaction and catalog layer
                    |
                 SeerDB
                    |
              local durable storage
```

The first serious deployment target is a single excellent node with WAL-based
replication and operational tooling. Distributed SQL is not a prerequisite for
the first server architecture.

OLTP and OLAP use separate physical engines. OmenDB exports a consistent
snapshot plus an ordered committed-change stream to a future `omen-olap`
consumer instead of making integrated HTAP storage a prerequisite.

## Workspace and ownership

The repository is a Cargo workspace whose root package is OmenDB:

```text
omendb/
├── Cargo.toml       # root package and workspace
├── src/             # OmenDB server and relational engine
├── crates/
│   └── seerdb/      # independent generic storage crate
├── docs/
├── benchmarks/
└── tests/
```

OmenDB remains `AGPL-3.0-only`. SeerDB remains an independently versioned and
publishable `Apache-2.0` crate. The repository is the single writable source
for both projects; SeerDB's former standalone repository is not a second
implementation source.

The current development dependency is a workspace path dependency. Registry
releases remain independent: publish and qualify a SeerDB prerelease first,
then release OmenDB against that exact SeerDB version. Neither package version
is inherited from the workspace.

## OmenDB and SeerDB boundary

SeerDB is a generic OLTP-oriented transactional ordered-KV engine. Its logical
model is deliberately small:

```text
TreeId + unsigned-lexicographically ordered key bytes + opaque value bytes
```

SeerDB owns:

- ordered B-tree access and cursors;
- transactional tree lifecycle and atomic multi-tree mutation;
- MVCC visibility and write conflicts;
- page, buffer, blob, and physical mapping management;
- WAL, checkpoint, crash recovery, durability, and storage pressure;
- generic committed changes and snapshot/restart positions.

OmenDB owns:

- SQL and PostgreSQL-facing behavior;
- catalogs, schema, row codecs, NULL/type semantics;
- primary and secondary index meaning;
- constraints, DDL, optimizer, and execution;
- relational CDC interpretation.

OmenDB encodes relational keys and rows into SeerDB's byte boundary. SeerDB
must not acquire SQL schema IDs, NULL bitmaps, column directories, or index
semantics. OmenDB must not bypass SeerDB's transaction/MVCC machinery.

A generic storage-plugin matrix is not the product architecture. OmenDB should
call SeerDB through a capability-rich Rust API. A future alternate engine can
integrate through a deliberate adapter, but OmenDB will not hard-code a list of
RocksDB, Fjall, redb, or other first-party backends. The transaction, crash,
and snapshot invariants are recorded in
[`docs/adr/0001-seerdb-transaction-contract.md`](adr/0001-seerdb-transaction-contract.md).

## Transaction and durability identities

These identities are distinct:

```text
TxnId = transaction identity
CSN   = logical committed visibility order
LSN   = durable log position
```

The storage transaction API must eventually provide:

- snapshot-isolated multi-writer transactions;
- point reads, conditional insert/put/delete, and ordered cursors;
- atomic writes across multiple trees;
- point and range dependency events for serializable certification;
- short-lived borrowed record access with explicit page-pin lifetimes;
- a durability result containing CSN and durable LSN;
- storage pressure and retention visibility.

A physical WAL is for recovery and physical replication. It is not the public
long-term CDC format. A generic committed change stream preserves transaction
boundaries and TreeId/key operations without exposing page-layout details.

Snapshot export must atomically return:

```text
snapshot CSN X + restart LSN Y
```

A consumer copies snapshot X and then consumes committed changes after Y with
no gap. This is the contract for backups, CDC, and `omen-olap` bootstrap.

## Execution and server direction

The relational engine will share one typed planner/type system across two
execution paths:

- **OLTP micro-plans** for prepared point lookups, simple DML, and short
  transactions; these avoid generic `Vec<Row>`/`Vec<Value>` hot paths.
- **Batch pipelines** for scans, joins, aggregates, sorts, and index builds;
  these use typed vectors, selection representations, bounded work, and
  cancellation.

The server target is one multi-threaded process with bounded resources and
home-worker execution for short transactions. Protocol I/O, CPU scheduling,
deadlines, cancellation, memory quotas, admission, and observability are
first-class concerns. A future io_uring or native transport path must be able
to fit behind explicit runtime boundaries rather than leak into SQL code.

## Storage direction

SeerDB's existing B-tree, out-of-place pages, checksums, blob separation, and
fault/recovery testing are useful foundations. The target storage architecture
keeps these physical concerns separate from logical transaction versions:

```text
logical key/value MVCC
        |
current record + undo/version chain
        |
ordered B-trees and optimistic page latches
        |
buffer residency and durable PageId mapping
        |
out-of-place segments, WAL, checkpoint, and GC
```

The current serialized-writer/generation API is a correctness prototype, not
the final OmenDB transaction seam. The redesign proceeds from a simple correct
multi-writer snapshot-isolation implementation, then measures serializable
certification, buffer translation, WAL commit modes, and device-specific
optimizations. No disk-format stability promise should force v1 architecture
into permanence.

## Current status and roadmap

The current tree has a working transitional Rust relational API, durable
SeerDB integration, SQL/catalog/index/constraint tests, fault and recovery
coverage, an experimental PostgreSQL wire example, and a separate direct
SeerDB qualification path (`src/seer_direct.rs`). The qualification path
exercises catalog/table/index-to-`TreeId` mapping and transaction-scoped
snapshot reads without pretending to replace the public facade yet. SeerDB's
transactional facade now persists and resolves committed per-key version
chains, while the tree still does **not** provide the server contract described
here, transaction-status indirection, append-oriented undo storage, a
production physical multi-writer storage API, production
authentication/authorization, or a PostgreSQL compatibility claim.

The roadmap is intentionally dependency-ordered, but OmenDB follows each
SeerDB milestone immediately so the storage contract is validated by a real
relational consumer:

1. **Contracts and model — substantially complete:** workspace ownership,
   generic ordered-KV boundary, TxnId/CSN/LSN rules, tree lifecycle,
   snapshot/change positioning, and crash-state invariants.
2. **SeerDB transactional foundation — in progress:** the first vertical
   slice now provides `TxnId`, `CommitSeq`, `TreeId`, fixed-snapshot
   transactions, committed per-key version chains, ordered scans, atomic
   multi-tree mutation, and durable exact write-conflict records. Transaction
   status indirection, append-oriented undo storage, ordered cursor handles,
   range dependencies, retention-aware version GC, and physical multi-writer
   WAL/page publication remain open. The transactional facade now returns an
   explicit `{CSN, LSN}` commit position and persists that position through
   manifest/WAL recovery; zero-gap committed-change export still remains open.
3. **OmenDB direct integration — qualification path started:**
   `src/seer_direct.rs` maps one catalog tree, one tree per table, and one tree
   per index without using `StorageKernel`. Next, expand its relational
   conformance and fault gates, then replace the global-generation path in one
   deliberate migration after historical retention and durability-position
   contracts exist.
4. **Server alpha foundation:** persistent multi-session lifecycle, deliberate
   PostgreSQL protocol subset, authentication, cancellation, configuration,
   diagnostics, and clean shutdown/reopen behavior.
5. **Correctness and scale:** serializable certification, snapshot plus
   zero-gap logical CDC, physical replication, typed OLTP micro-plans, batch
   execution, and benchmark-led storage/runtime optimization.

A test-only SeerDB reference model exercises snapshot visibility, disjoint and
conflicting writers, atomic multi-tree mutation, tree-ID burn on abort, and
retained snapshot visibility. It is a semantic oracle, not a production
backend. The alpha release gates define evidence requirements; the current
`0.1.0-alpha.*` line remains unreleased until the server-first storage and
server criteria are met.
