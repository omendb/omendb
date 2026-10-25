# ADR 0015: Native relational engine and transactional storage core

- **Status:** accepted server-first native-engine direction (2026-10-25), unimplemented and not performance-qualified. The eventual public isolation default remains provisional.
- **Scope:** high-concurrency server-first relational OLTP engine, optional embedded deployment, object-storage secondary tier, and extension boundaries.
- **Supersedes where they conflict:** ADR 0007's execution target, ADR 0009's ordered-tree choices, ADR 0010's row-layout alternatives, ADR 0013's first-access-method sequence and ADR 0014's implementation scheduling. Their detailed invariants remain binding unless specifically changed here. The [one implementation plan](../plans/storage-kernel-vnext.md) owns milestones and evidence.
- **Evidence:** current source at 1590011 (2026-10-13); literature and code/design review on 2026-10-25. This is design selection, not a new benchmark, runtime validation, feature release or claim of proven optimality.

## Goal and product contract

Build a fast, durable, general-purpose **native relational database**, a credible alternative to SQLite, PostgreSQL and MySQL rather than an implementation of PostgreSQL. Support the same relational fundamentals: typed tables, indexed transactions, constraints, schema evolution, concurrency, reliable recovery, query planning and operations. A high-concurrency native database server is the first performance, reliability and release target. A lightweight embedded Rust API runs the *same* storage/transaction engine, with fewer/lazier workers and no mandatory server resources. Neither depends on a cloud control plane.

PostgreSQL wire compatibility and familiar SQL syntax are useful adoption surfaces, but are **optional and bounded**. The wire layer must not decide transaction policy, type semantics, storage representation, SQL grammar, or internal execution. Do not promise full PostgreSQL SQL, system catalog, extension or wire compatibility. Explicitly document any supported dialect and compatibility subset; prefer principled native semantics to accidental divergence.

Optimize for short latency-sensitive OLTP transactions on RAM plus local NVMe, while remaining correct and useful with datasets larger than memory, long scans, multiple indexes, concurrent schema activity, constrained memory, and recoveries. Measure throughput and p99 including maintenance; never select a storage/index algorithm from resident point-read numbers alone.

Object storage (S3-compatible) is a **secondary storage tier**, initially for backups, immutable complete checkpoints, retained/archived log segments and validated cold extents. Local NVMe and its own durable decision log are the normal authoritative commit path. An object-backed primary or disaggregated profile is a distinct, later qualification target, not a mandatory network operation for every local transaction.

Text, vector, analytical and future modalities are separate **access/compute engines**, not separately committing authoritative databases inside one OmenDB transaction. They can use their own blocks, compression, layouts and caching. Standalone products and remote replicas may have explicitly independent authority and synchronization contracts. OLTP correctness and complete relational operation are the first delivery boundary; extensions are not on its critical path.

## Chosen architecture

~~~
API / network adapters (native, optional PostgreSQL wire, future protocols)
                |
sessions, parser -> one type binder and retained catalog view
                |
optimizer -> typed logical/physical plan
          /                         \
  OLTP micro-plans           typed vectorized pipelines
          \                         /
          common row/index/constraint operations
                |
       OmenDB relational semantics
                |
     SeerDB owned transaction/storage runtime
    /          |             |              \
MVCC + cert   WAL + CSN    frames/pages    lifetime/GC
    \          |             |              /
       ordered-index / clustered-row access
                 |
   durable page/extent placement, checkpoint + log
                 |
                NVMe
~~~

SeerDB owns transaction identity, decisions, MVCC/snapshots, conflicts, durable log, page/extent lifetime, buffer/cache admission, checkpoint/recovery and retention. OmenDB owns SQL/binder/catalog semantics, logical rows and constraints, the optimizer and physical execution. These are implementation/code-ownership boundaries, **not** independently recoverable databases, runtime plugins, or a mandatory byte-KV serialization between every operator and the engine. Keep the crates together in the existing OmenDB workspace; package independence is not a reason to freeze unqualified interfaces. Retain a useful separately consumable SeerDB transactional ordered-KV façade for other projects where it fits, but never force SQL typed rows through that façade or maintain another commit/recovery engine just for packaging. OmenDB's correctness, locality and performance requirements take priority.

There must be exactly **one persistent runtime owner** of all authoritative services. Construct it atomically; a bundle of optionally attached coordinators that can fence independently is not a database. Expose narrow typed capabilities instead of a hierarchy of replaceable storage-backend traits. Physical access methods are compiled components, with shared core facilities and no second WAL.

## Initial physical organization

1. **Primary/canonical rows:** start with clustered, ordered B+/B-link leaves holding compact typed row-family records. Key and record identity are logical; frame address, page/slot and on-disk offset are never durable logical identity. Separate large/rarely-read values behind generation-safe logical handles. A table without an explicit primary key still needs a stable internal row ID.
2. **Secondary/constraint indexes:** independent ordered index objects in the same transaction. Keys encode indexed columns and stable row identity, with deliberately selected covering payloads. Unique/primary/foreign-key enforcement indexes and catalog objects are authoritative. Versioned index entries and base-row lookup must resolve one transaction snapshot. Physical row-location hints are optional and validated against reuse/relocation.
3. **Nodes:** use variable-length slotted, prefix-aware leaf/internal formats with B-link fences and right links, bounded cells and logical overflow. Split feasibility is proven on *actual fully fenced* candidate pages, including internal/root propagation, before accepting a durable mutation. Never make the page-fit limit silently become a logical value limit. Page size, 4/8-byte key heads, abbreviated comparisons, SIMD search and split policies are measured format choices.
4. **Buffer and persistence:** mutable frame-resident pages with fine-grained guarded access, dirty coalescing, and asynchronously written out-of-place physical images. A versioned page map resolves logical page IDs to storage; no transaction-wide tree/root cloning. Coalesce hot updates before SSD writeback. Keep large immutable extents outside the small mutable-page cache policy when appropriate. Reclaim overwritten images with retained-checkpoint/history/reader correctness and explicit space budgets.
5. **Read synchronization:** start from validated, safe frame latches and existing exact-incarnation single-flight Loading ownership. Benchmark optimistic reads/lock coupling and BBTree-style batched lookups; introduce them only with a Rust memory-model-safe lifetime and byte-access protocol. A seqlock counter alone does not make racing non-atomic bytes safe. Buffer translation, swizzling, learned routing and predictive mapping are measured fast-path candidates, not format contracts.
6. **Historical versions:** compact current metadata and bounded recent history; older versions live in a managed history/overflow facility. MVCC visibility and retention are kernel services, but physical version encoding can differ by access method. Reclamation respects active reader snapshots, serializable conflict evidence, CDC, backup, checkpoints and later replicas separately.

**Rejected as the baseline:** universal opaque-KV representation, COW generation publication per transaction, an LSM-plus-memtable as the mandatory primary row layout, separately maintained heap and clustered row engines, one physical page size for column/posting/vector payload, one separately recovering mini-database per index, and a speculative backend/plugin matrix. They remain experiment candidates only when complete end-to-end workloads show a gain.

An LSM can legitimately beat this B-tree for sustained ingestion and may be an appropriate later specialized immutable index. We are selecting the relational primary path, not claiming B-trees always beat LSMs. Compare a row-page-plus-primary-index layout with the clustered baseline using a small isolated experiment if measured update, scan, fragmentation or index economics warrant it; do not maintain both indefinitely without a clear workload advantage.

## Transactions, visibility and isolation

- Each transaction owns its snapshot, retained catalog/schema view, private writes, logical read/absence/range/name dependencies, resource reservation and final outcome responsibility. Each statement produces a **private atomic delta** spanning rows, indexes, cascades and catalog effects. A failed statement cannot leave a committable orphan row or index entry. Explicit blocks become rollback-only after declared errors, without silently introducing implicit savepoints.
- Distinguish transaction ID, commit sequence (CSN), WAL sequence (LSN), object incarnation, version identity and physical page/frame incarnation. They cannot be substituted for one another. The accepted write order is the serial order in the initial serializable protocol.
- Offer correct snapshot reads and **serializable** transactions from a common MVCC engine. Initial serializable writers use decision-ordered optimistic certification against point, absent-key/gap, range, catalog/name and constraint reads; they must see *previously accepted but not yet installed/visible writes*. Required unique/FK/catalog enforcement dependencies are checked even in weaker isolation modes. Conflict summaries are budgeted and cannot be silently discarded under retention pressure.
- **Isolation policy:** implement SERIALIZABLE first as the reference correctness contract and provisionally favor it for the native server's default, conditional on demonstrated cost and retry ergonomics. Provide READ COMMITTED with statement snapshots, safe wait-and-recheck of updated predicates and explicit locks for workloads requiring fewer retries; an optional REPEATABLE READ mode must state precisely whether it means snapshot isolation and must not claim write-skew protection. PostgreSQL defaults to READ COMMITTED, MySQL/InnoDB to REPEATABLE READ, and CockroachDB to SERIALIZABLE; there is no universal industry default. SQLite's SERIALIZABLE via a single writer is not OmenDB's concurrency strategy. Design mixed-isolation interactions explicitly: a serializable transaction cannot rely on all writers participating in its certification when weaker writers do not. Benchmark long readers, write skew, hot-row updates, mixed isolation, abort/retry rate and application-visible p99 before freezing the default.
- For single-statement/autocommit operations, safe transparent retries may be supported before any result or side effect escapes the engine. For native transaction-closure APIs, opt-in automatic retries must document non-database side-effect/idempotency restrictions. An arbitrary multi-statement network transaction cannot generally be replayed by the server; return an unambiguous retryable outcome and require a client-level retry policy. Do not auto-retry uncertain durable outcomes.
- Low-contention operations should not hold a database/global write mutex or use per-read heavy lock-manager objects. For contended keys, support an explicit queued intent/wait strategy so hot-row workloads need not endlessly spin/retry. A statement-refreshing READ COMMITTED mode is a useful general-purpose interface; if implemented, it must recheck predicates and constraints after waits. Never silently refresh a fixed SERIALIZABLE snapshot. **SQL-facing default isolation and retry policy remain a measured product decision** rather than an implementation assumption.
- Record schema/catalog *object and namespace-absence dependencies* as well as data dependencies; binder, optimizer, row decoder, auth and DDL see compatible catalog lifetimes. Grant checks are an explicit authorization boundary, not inherited from plan-cache contents. One schema transaction owns DDL publication and its validation.
- Treat long transactions, large write sets, large read/range dependencies and retention as explicitly admission-controlled resources. Pressure may refuse/expire abortable transactions under a declared policy; it may never pressure-abort an irrevocably decided commit.

## Commit, log and failure protocol

**One recoverable transaction decision is the source of durable authority.** The ordinary commit path is:

1. Admit transaction resources before it can block other work; stage changes privately. Normalize row/index/catalog effects and preserve a complete original logical mutation stream.
2. At commit, acquire required intents and reserve worst-case WAL bytes, predecessor history, overflow, structural page work, pins, allocator/free space, certification evidence and installer capacity. Reject deterministic infeasibility **before** entering device append.
3. Prepare record bodies and recoverable predecessor history outside the decision lane. Certify against the prior accepted (including not-yet-installed) decision prefix, assign CSN and append an authenticated complete decision. Keep the protected order/append point short; prepare/encode outside it.
4. Synchronize the recoverable WAL decision with bounded, adaptive group commit. Do not add a mandatory batching sleep for one writer. Large external payload dependencies may require further durability barriers.
5. Install all authoritative effects and complete leaf-parent-root/overflow structures. Publish transaction readiness; advance the **contiguous visible CSN prefix**, and acknowledge only when the caller's CSN is covered.
6. Materialize page/history images and publish coherent checkpoints asynchronously. They are never required as separate ordinary per-transaction acknowledgement barriers once complete-log recoverability is proved.

No pre-commit private delta is globally visible. **Durable decision is irrevocable**, even if a client cancels/disconnects and does not receive its response. An owner capability is armed before possible append; uncertain append, installation panic, or missing durability proof fences normal traffic until recovery/resolution. Completion resources, intents and checkpoint membership cannot be released earlier. Provide durable request/outcome identity for safe reconciliation of ambiguous acknowledgements; don't auto-retry potentially committed writes.

Keep the initial post-durability installer protocol: it is easier to prove than in-place speculative shared mutation. If profiles show installation on the ACK path dominates, compare safe preconstruction, bounded parallel installation and possibly guarded pre-durable preparation. No speculative installed page may be persisted, read, or reclaimed as an authoritative committed state without a complete failure/visibility proof. Do not trade correctness for a shorter-looking commit sequence.

The **complete-log** target must recover current rows, all authoritative indexes/catalog effects, transaction outcomes and every retained history view from an identified checkpoint plus a validated committed log suffix. The current separately synchronized undo format remains necessary until that property passes crash-and-history tests. A max page LSN or a working spill page is not proof that all prior logical effects are replayable.

## Checkpoints, I/O and bounded recovery

- Publish a store-bound, checksummed, versioned checkpoint manifest only after its *complete reference closure* and durability: roots, objects/schema generations, right links/fences, extents/overflow, page map, allocation state, owner outcomes and retained history. Preserve the previous complete authority until candidate publication succeeds. Recovery refuses unknown/foreign/corrupt references.
- Start with a quiesced complete checkpoint as a correctness baseline but design for a **bounded-cut incremental/epoch checkpoint** that captures immutable page images or fenced versions while concurrent writers continue. Long readers must not turn checkpoint capture into an unlimited pause; every copy, pin and staged image consumes a budget.
- Recovery validates the newest complete checkpoint, replays the log suffix in decision order through the same logical effect reducer/installer, reconstructs retained MVCC identity, and only then admits clients. A crash *during recovery*, repeated crash/reopen, and a subsequent ordinary transaction must be safe.
- Bound WAL distance, replay memory, history/index state, cleanup debt, space amplification, orphaned preparation/recovery allocations and repeated failed recovery attempts. Reclaim only when every checkpoint, snapshot/consumer and physical-generation floor allows it.
- Keep a portable filesystem baseline and a separately optimized Linux NVMe I/O path. Direct I/O, io_uring, ZNS/FDP and page packing are experiments with explicit alignment, sync, error and operational implications, not prerequisites for correctness or the first server. Distinguish OS/kernel-sync from device/power-loss guarantees in measurements.

## Relational execution and API

- One parser -> **non-executing typed binder** -> optimizer -> executable plan path. Binding resolves types, result shapes, privileges, name and catalog dependencies *without evaluating protected queries or probing data*. SQL dialect correctness is owned by OmenDB; a PostgreSQL client adapter must never re-infer semantics from SQL text. Retain/sqlparser or other mature parser libraries where practical.
- A prepared logical IR is architecture-neutral. Short point/range/indexed transactions use specialized, allocation-light interpreted or compiled **micro-plans**. Scans, joins, aggregations, sorts and analytical work use selection-vector/typed **batches** with projection/predicate pushdown and explicit work/byte budgets. Both share type, NULL, collation, expression and statement semantics.
- Row formats are schema-versioned: null bitmap, compact fixed fields, variable offsets and inline payloads, optional large-value references. Borrow needed fields from a short-lived guarded record, or copy into a bounded batch before a yield. Avoid mandatory generic Value/Row allocation on the hot path. Support evolution through retained layout descriptors, not synchronous rewriting of every row.
- A cursor carries a logical exclusive restart key, snapshot, catalog/layout generation, private-overlay position and physical-work budget. It must neither duplicate boundary rows nor rescan the entire prefix per morsel. Limit invisible-version visits, overflow work, I/O, allocated output, not just visible row count.
- One cloneable concurrent Database handle replaces an outer relational RwLock. Server sessions do not own dedicated blocking worker threads. Resource admission protects commit, WAL and necessary reclaim progress separately from scans and background builds. Embedded mode may use a small/lazy worker configuration. JIT, specialized SIMD kernels, NUMA pinning and high-level optimizer heuristics follow measured need, not initial architecture complexity.
- Correctness before breadth: fix current Describe/privilege leakage, failed-statement partial mutations, FK publication/timing, catalog-view mismatches, secondary cursor restarts, batch input validation and dump/restore semantics. Preserve their independent oracles, not incorrect legacy output.

## Specialized engines and future distribution

The relational core publishes logical row/object identity, transaction-atomic snapshot handles, versioned schema descriptors, bounded typed selection/batch access, consistent change streams with retained bootstrap, and object lifecycle/GC leases. This is the **extension contract**, not a universal storage-backend trait.

- Authoritative constraint and secondary index maintenance commits with the source transaction.
- Derived text/posting, vector/ANN, graph and columnar engines have their own physical representations, but must prove coverage of the *requested snapshot*, a complete transactional delta, and an exact/explicitly approximate fallback. An applied high-water mark alone is insufficient; deletion, authentication and filter eligibility remain exact.
- Standalone OLAP/vector deployments can own separate durability when ingesting independent data, or explicitly consume OmenDB transaction-complete CDC. They do not silently inherit distributed SQL atomicity.
- Regional replication/HA can later replicate **one logical decision stream** and bootstrap from retained complete checkpoints. Logical placement ranges are not physical B-tree page boundaries and not necessarily a universal byte-KV address space. General distributed SQL, 2PC and global consensus are deferred until single-node correctness and a real deployment requirement justify them.

**Neki lessons (PlanetScale, September–October 2026):** design future declarative placement around *co-located queries/transactions*, with shard groups, routing-key expressions, stable topology epochs and metadata identities; allow group reshaping without redesigning the local page layout. Reuse snapshot + retained transactional change suffix + fenced cutover for online moves, resharding and schema migration. A router should lazily decode only fields required for routing or execution, not allocate generic rows for pass-through results. Physical connection pooling, operator/controller services and etcd are Neki's solutions to managing unmodified PostgreSQL instances; do not copy that stack into one native OmenDB server. The local fast path must not use a distributed router or coordinator. Neki currently documents no atomic shared-snapshot cross-shard transaction; OmenDB must *reject unsupported cross-range writes*, not silently expose partially committed SQL. Other choices (globally replicated reference tables, global secondary indexes, volatile expressions and cross-range constraints) need separately defined durability/snapshot semantics when distribution is actually implemented.

## Optional object-storage profile

Use content-addressed or immutable versioned objects for archived log segments, complete checkpoint page/extent snapshots, manifests and large cold segments. Upload staged data, validate checksums/lengths and retention dependencies, and only then publish a verifiable manifest whose atomic authority is backed by a proven local/quorum metadata/log decision. Object stores' strong read-after-write consistency and single-key atomic writes do **not** provide an atomic multi-object database transaction. Explicitly model upload interruption, missing/foreign/stale objects, eventual failed restore, request/egress costs, range-read latency, cache pollution and garbage collection across snapshots. Do not let remote deletion invalidate an admitted snapshot or a recoverable acknowledged commit. For the first server, implement backup/restore and archived immutable-object readers; qualify a remote-primary/disaggregated profile separately before advertising it.

## Alternatives considered and thresholds for reversal

| Question | Selected starting point | Revisit when |
| --- | --- | --- |
| Clustered B+tree vs LSM | Buffered clustered tree | Sustained write-heavy/mixed workload including reads, compaction, p99 and total bytes proves an LSM superior for primary rows |
| Clustered vs heap + primary index | Clustered rows | Wide/mutable rows, multi-index churn, scan locality or relocation costs demonstrate lower *total* cost with heap/PAX |
| Durable-decision-first vs shared speculative installation | Private staging, complete WAL, then install/publish | Measured post-durable install latency warrants a safe, independently proven pipelined alternative |
| Serializable OCC vs SSI/2PL/timestamp algorithms | Ordered certification plus optional hot-key waiting | Measured abort/certification retention at realistic contention justifies a different isolation algorithm and its proof burden |
| Mutable buffered pages vs transaction COW | Mutable buffer + asynchronous out-of-place pages | A different layout wins matched device-backed, crash-safe mixed workload with lower total write/recovery costs |
| Custom SQL executor vs imported engine | Native binder/IR with specialized micro-plan and batch paths | A reusable optimizer/operator component preserves one semantic/cancel/resource authority and measurably reduces total cost |
| Default isolation | Provisional SERIALIZABLE, plus explicit READ COMMITTED | Real SQL/server workload and reliable retry UX favor a weaker default without hidden correctness costs |
| Local NVMe vs object-store-primary | Local NVMe hot log/pages; S3-compatible backups/archive/immutable extents | A separately verified remote-primary design wins end-to-end latency, durability, recoverability and cost |
| SeerDB library reuse | Shared kernel with optional standalone ordered KV façade | A reuse abstraction imposes material SQL hot-path, recovery, or ownership complexity |
| Broad SQL/PG compatibility vs native semantics | Native documented SQL first | Real adopter needs justify compatibility work without distorting internal engine |

Neither the selected design nor the new research establishes a globally optimal engine. Record workload, hardware, sync/isolation, values/key distribution, long readers, memory pressure, checkpoint/GC debt, measurements and correctness evidence **before** promoting experiments into contracts.

## References (research inputs, not SeerDB benchmark results)

- [B-Trees Are Back (SIGMOD 2025)](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrees-are-back.pdf): pageable cache-friendly B-tree layouts.
- [How to Write to SSDs / ZLeanStore (PVLDB 2026)](https://arxiv.org/abs/2603.09927): in-memory B-tree updates with out-of-place materialization and SSD write amplification.
- [BBTree, 2026-10-23](https://blog.xiangpeng.systems/posts/bbtree/): node search, concurrency and batched in-memory probes, not a drop-in durable transactional engine.
- [SurrealKV 1.0, 2026-10-13](https://surrealdb.com/blog/we-are-replacing-rocksdb-with-surrealkv-1-0): LSM/radix memtable and grouped commit as serious alternatives, with different isolation/compaction economics.
- [Predictive Translation (SIGMOD 2026)](https://db.in.tum.de/~zinsmeister/papers/predictive-translation.pdf): benchmark candidate for page-to-frame translation.
- [TigerBeetle architecture](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/ARCHITECTURE.md): explicit failure authority, bounded resource accounting and deterministic fault testing; its specialized serial transaction model is not adopted wholesale.
- [FoundationDB layers](https://apple.github.io/foundationdb/layer-concept.html): ordered transactional KV is a valid alternative; the lower native SeerDB boundary is chosen for typed layout and batching control, not because KV lacks relational semantics.
- [PostgreSQL 18 isolation](https://www.postgresql.org/docs/18/transaction-iso.html), [MySQL 8.4 isolation](https://dev.mysql.com/doc/refman/8.4/en/set-transaction.html), [SQLite](https://www.sqlite.org/isolation.html), [CockroachDB](https://www.cockroachlabs.com/docs/stable/developer-basics.html): conventional defaults differ; SERIALIZABLE correctness vs retries is a product contract.
- [Neki architecture (2026-09-18)](https://planetscale.com/blog/the-architecture-of-neki), [shard groups (2026-10-23)](https://planetscale.com/blog/shard-groups-the-unit-of-co-location), [lazy routing (2026-10-01)](https://planetscale.com/blog/designing-neki-for-performance): distribution/operations and reduced decoding, not a replacement for local transactional storage.
- [Amazon S3 consistency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/): strong single-object read-after-write and single-key atomicity do not establish multi-object transactional commit.
- [OmenDB October 13 research evidence](https://github.com/nijaru/knowledge): earlier observations are dated, not vNext qualification.
