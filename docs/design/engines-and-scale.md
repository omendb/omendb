# Specialized engines, storage tiers and horizontal scale

**Maintained direction, 2026-10-25.** Not part of the first single-node
relational implementation gate. [Architecture](../architecture.md);
[SeerDB kernel](storage-kernel.md); [roadmap](../roadmap.md).
Do not mistake a repository boundary for a durable transaction boundary.

## Multi-engine architecture

OmenDB should eventually support dedicated physical **engines** for relational
records/ordered indexes, text/inverted indexes and BM25, vector/ANN, columnar
analytics, and later graph/time-series/geospatial access when warranted.

Each engine may choose its page/extent layout, index algorithm, encoding,
compression, prefetch and cache units. It consumes OmenDB typed semantics
where needed and SeerDB transaction/object/snapshot/retention services when
participating in the same authoritative data. The shared layer is **commit
and visibility authority**, not a mandatory B-tree, byte KV facade, fixed
page size or polymorphic backend-plugin ABI.

Required primary/constraint indexes are synchronous participants.
Rebuildable search/column indexes are derived only with:
source incarnation and schema dependencies; reader-stable generation;
exact snapshots actually retained; complete mutation/delete/schema
deltas and lease closure; or an explicit fallback/staleness/approximation.
An index at frontier F cannot automatically serve *every* snapshot ≤ F.
ANN may approximate ranking, not identity, eligibility or authorization.
Synchronous search maintenance, bounded exact delta overlays and
asynchronous derived indexes are choices to benchmark under churn and
selective filtering, not architectural dogma.

Existing [OmenDB Vector](https://github.com/omendb/omendb-vector) and
the private independently ingested OLAP engine remain separate products
with their own qualified WAL/catalog when necessary. Shared algorithms
should have one source owner when real consumers need them. Do not embed
an independently committing Store inside a SQL transaction, force OLAP
through SeerDB's row B-tree or publish/merge private code merely to
make the product diagram tidy. A projection across engines/processes
advertises its own durability, lag and exactness.

## CDC and projection contract

Atomically retain source incarnation, compatible schema, snapshot CSN,
restart log position and snapshot/change leases *before copying*.
Deliver committed transactions with complete effects, deletes, schema
and object lifecycle; apply consumer effects **and progress** atomically
and idempotently. Consumer advancement releases source history only
when safe. A remote projection without coverage cannot silently serve
the exact current SQL snapshot; wait, fall back, rebootstrap or return
declared staleness. Reuse this mechanism for online index rebuild and
schema rewrite where it matches actual semantics.

## Distributed scale target

The aspiration is **horizontal scale comparable in kind to Neki/Vitess**:
predictable partition-local throughput growth, placement control,
isolated failures, replication, online shard movement/rebalancing and
schema changes. Not a promise to reproduce Neki's 118.5M-QPS
512-shard read-only primary-only benchmark on one machine or with
cross-shard serializable writes and HA under that same number.

- **Placement groups** express intended co-location of related tables,
  constraints and indexes. Route by catalog-declared, versioned hash/range
  expressions selected for common **joins and transactions**, not
  necessarily each table's physical primary key or a universal SeerDB KV
  range. A group can split, move and rebalance without changing local
  page format.
- Every partition/range has identity, ownership epoch, members,
  replicas/leader and declared durability policy. A local unsharded
  store pays no network/router/quorum tax. A partition-local operation
  uses its native transaction path; cached routing plans resolve
  topology at execution and stale epochs are rejected.
- Regional HA with fenced single-leader/quorum durability comes before
  general distributed SQL. Replica/bootstrap from retained checkpoints
  plus complete log suffix, failure handling, leader transfer,
  promotion, read frontiers and safe retry identity are separate
  correctness protocols. An orchestration controller is not the data
  authority; healthy shards should not depend on its constant presence.
- **Cross-partition SQL atomicity** is a future explicit protocol
  (prepared durable participants, irrevocable coordinator decision,
  recovery, global snapshot and deadlock/retention rules). Until proved,
  fail unsupported multi-shard writes **atomically** and document the
  limitation. Don't imitate a preview system's partial commit behavior.
- Reshard, online DDL and table movement reuse retained snapshot +
  transaction-complete change suffix + catch-up + fenced epoch cutover;
  preserve external indexes/constraints and avoid dual-write races.
  Native typed physical fragments and batched remote execution can
  outperform proxying SQL text to independently planned servers,
  but need optimizer cost/streaming/resource evidence.

Neki is a relevant **operations and placement reference**, built above
stock PostgreSQL. Its PostgresManager/sidecar/process pool, type-OID
rewriting and etcd/Kubernetes operators are compatibility choices,
not mandatory OmenDB components. For a native engine, favor fewer
process layers, bounded lazy routing/decoding and a single plan IR,
provided correctness and failover semantics remain explicit.
See [Neki architecture](https://planetscale.com/blog/the-architecture-of-neki)
and [data topology](https://planetscale.com/blog/what-is-a-data-topology).

## Object storage

The local transactional authority is RAM/NVMe. S3-compatible objects
first store immutable checkpoints, archive logs, backups and cold extents.
Upload then validate referenced objects before manifest publication;
single-object atomicity does not imply multi-object database commits.
Reader leases, restoration, credentials, request/egress cost, short reads,
cache amplification, corruption and GC are part of the contract.
Disaggregated/remote-primary storage is **not** inferred from adding
object placement to a library. It is a separately proven deployment,
requiring an explicit remote authority/quorum and good fault economics.

## Decision gates

1. Trustworthy server-first single-node relational core and SeerDB
   transaction/recovery contract, including larger-than-memory behavior.
2. Replication/HA with full durability and failover evidence.
3. Logical placement/shard-local correctness and online moves, *without*
   single-node tax, then cross-shard query plans and atomic transactions.
4. Integrated or remote text/vector/analytical engines only where exactness,
   freshness, total cost and unused-feature overhead justify them.

Qualify scaling by per-shard QPS, p99, router overhead, network bytes,
replication policy, reshards/cutover downtime, fault recovery and skew/
hot-shard distribution as well as total throughput. Use same semantics
and number of shards for competitor comparisons.

