# Database design alternatives and evidence

**Status:** supporting rationale, not a roadmap or performance qualification.
The September 2026 survey informed deployment/runtime/distribution ADRs;
local-kernel sources were refreshed on 2026-10-11. Accepted choices belong to
[ADR 0009](../adr/0009-buffered-btree-and-materialization.md),
[ADR 0007](../adr/0007-runtime-execution-and-contention.md),
[ADR 0013](../adr/0013-storage-kernel-and-access-methods.md) and
[ADR 0014](../adr/0014-vnext-installation-and-recovery.md).

## What the evidence supports

Retain a local RAM+NVMe OLTP kernel with out-of-place materialization and concrete
physical access methods. Redesign unsafe ownership/admission boundaries rather
than replace the B-tree by reputation. No inspected paper or vendor result
establishes OmenDB throughput, latency, recovery or availability.

| Decision | Independent evidence | Interpretation and limits |
| --- | --- | --- |
| Bounded ordered cells and logical overflow | [SQLite file format §§1.6–1.7](https://www.sqlite.org/fileformat2.html), [PostgreSQL B-tree](https://www.postgresql.org/docs/current/btree.html), checked 2026-10-11 | SQLite overflows large index keys to preserve fanout; PostgreSQL restricts index entries to approximately one-third page after applicable compression. Neither defines a SeerDB logical row-size limit. Shortened separators alone cannot bound arbitrarily long shared prefixes. |
| Decision-ordered optimistic certification | [FoundationDB developer guide](https://apple.github.io/foundationdb/developer-guide.html#transactions-in-foundationdb), checked 2026-10-11 | Fixed snapshots plus validation can support serializable writes. SeerDB must couple certification/conflict publication with its own decision lane and retain evidence; it does not adopt FDB record/duration limits or universal KV physical layout. |
| SSI is an alternative, not a synonym for snapshot isolation | [PostgreSQL 18 isolation](https://www.postgresql.org/docs/18/transaction-iso.html), [Ports/Grittner SSI](https://drkp.net/papers/ssi-vldb12.pdf), checked 2026-10-11 | SSI requires predicate/dependency bookkeeping and safe abort/retention rules; use only if conservative OCC aborts justify the complexity. |
| Constraint timing needs an observable boundary | [PostgreSQL SET CONSTRAINTS](https://www.postgresql.org/docs/18/sql-set-constraints.html), checked 2026-10-11 | Immediate FK checks differ from deferred commit checks; nondeferrable uniqueness has stricter checks. Legacy publication-time FK validation is not immediate-statement compatibility. |
| Modern pageable B-trees remain credible | [B-Trees Are Back, SIGMOD 2025](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrees-are-back.pdf) | Variable-record/pageable layout techniques are candidates, not a prescribed SeerDB node encoding. |
| Out-of-place SSD placement | [How to Write to SSDs, extended v3](https://arxiv.org/pdf/2603.09927), [ZLeanStore artifact](https://github.com/LeeBohyun/ZLeanStore), checked 2026-10-11 | Joint host/device write-amplification evidence supports placement experiments after reference lifetime/GC qualification. Direct-device/ZNS/FDP results do not establish portable filesystem behavior. |
| Translation is a measured seam | [Predictive Translation, SIGMOD 2026](https://db.in.tum.de/~zinsmeister/papers/predictive-translation.pdf), [PrediCache artifact](https://github.com/umbra-db/predictivetranslation-sigmod2026), checked 2026-10-11 | Linux-oriented fast translation candidate; fix load ownership first and compare cached/spill behavior before adoption. |
| Durability scheduling can evolve without relaxing ACK | [Autonomous commit, SIGMOD 2025](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/latency.pdf) | Compare group/adaptive/autonomous scheduling on actual hardware, separately from WAL/undo and contiguous-visibility obligations. |
| Common versus large/history-heavy transactions may merit different version placement | [Swan, PVLDB 19(9)](https://www.vldb.org/pvldb/vol19/p1977-guo.pdf), [artifact](https://github.com/LAccordeur/swan), checked 2026-10-11; [CedarDB 2026-10-08 releases](https://cedardb.com/docs/releases/#v2026-10-08) | Swan's LSM-specific mechanisms are not a B-tree mandate. CedarDB's history/recovery improvements are vendor observations, not reproduced OmenDB evidence. |

## Runtime comparisons

[pgrust v0.3](https://github.com/malisper/pgrust/tree/79ad992ede22bcf6ae0c4fdead6fb01eeac5a990)
was inspected at `79ad992ede22bcf6ae0c4fdead6fb01eeac5a990` on 2026-10-11:

- [admission](https://github.com/malisper/pgrust/blob/79ad992ede22bcf6ae0c4fdead6fb01eeac5a990/crates/backend/tcop/postgres/src/admission.rs)
  retains ownership across a transaction, avoiding holders queued behind waiters.
  This supports transaction-grain admission, not copying session exemptions.
- [flush completion](https://github.com/malisper/pgrust/blob/79ad992ede22bcf6ae0c4fdead6fb01eeac5a990/crates/backend/access/transam/transam_xlog/src/flushpipe.rs)
  keeps visibility/lock release/ACK after durability despite broader README
  early-release language. Directed waits are not a different commit protocol.
- [vectorized/fused execution](https://pgrust.com/blog/how-we-made-postgres-hundreds-of-times-faster-the-query-engine/)
  and [cheap JIT](https://pgrust.com/blog/jit-compiling-code-in-5-us/)
  motivate typed batches/IR experiments; author performance/conformance counts
  and architecture-specific compilation times were not reproduced here.

September inputs also included [Turso's common-core frontends](https://turso.tech/blog/a-new-modern-version-of-postgres-in-rust),
[Seastar locality](https://www.scylladb.com/product/technology/shard-per-core-architecture/),
[bounded deterministic execution](https://docs.tigerbeetle.com/concepts/performance/)
and [FoundationDB layers](https://apple.github.io/foundationdb/layer-concept.html).
These support explicit sessions, bounded workers/resources and typed semantic
boundaries—not a mandatory VM, shard-per-core engine or plugin framework.

## Deployment alternatives remain conditional

These September sources were not refreshed as current product claims on 2026-10-11:

| Profile | Sources | Relevance |
| --- | --- | --- |
| Cloud storage economics | [Haubenschild & Leis, VLDB Journal 2025](https://doi.org/10.1007/s00778-025-00913-z) | Local NVMe, replicated log and immutable object storage have different costs/latencies; no universal physical profile wins. |
| Separate log/cache/materializer | [Neon storage](https://neon.com/storage), [pageserver design](https://github.com/neondatabase/neon/blob/main/docs/pageserver-storage.md), [BtrLog, VLDB 2026 program](https://vldb.org/2026/program.html) | Mechanism references for later HA/disaggregation, not operational availability evidence or permission to put object requests in local commits. |
| Object-native LSM | [OceanBase Bacchus](https://arxiv.org/abs/2602.23571) | A legitimate cost-first shared-storage alternative, not a reason to make every local access method an LSM. |
| Distributed routing over PostgreSQL | [Neki router](https://planetscale.com/blog/what-is-a-neki-router), [topology](https://planetscale.com/blog/what-is-a-data-topology), [query lifecycle](https://planetscale.com/blog/the-lifecycle-of-a-sharded-postgres-query), [Multigres](https://github.com/multigres/multigres) | Stable placement identity, co-location and bounded coordinators are useful; sidecars, backend-session scrubbing and dual SQL planners solve stock PostgreSQL constraints, not OmenDB requirements. |

Refresh these sources when an implementation decision depends on current behavior.
Compare actual OmenDB paths under stated durability, isolation, workload, platform
and resource budgets. Mechanism quality, storage durability and operational HA
are separate claims.
