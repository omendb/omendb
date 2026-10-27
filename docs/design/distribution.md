# Distribution, replication and online movement

**Long-term maintained target, 2026-10-25.** A native OmenDB server can
eventually scale out like Neki/Vitess in placement and operations without
copying their stock-PostgreSQL compatibility stack. This document does not
claim distributed correctness or a selected implementation.
[Architecture](../architecture.md); [SeerDB](../../crates/seerdb/docs/architecture.md);
[transactional projections](extensions.md); [OmenDB roadmap](../roadmap.md).


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
and [data topology](https://planetscale.com/blog/what-is-a-data-topology).## Placement and transaction authority must remain distinct

Neki's [October 23, 2026 shard-group analysis](https://planetscale.com/blog/shard-groups-the-unit-of-co-location)
shows how common tenant/customer-key placement can keep joins and
transactions local even when those columns differ from each table's PK.
This supports a logical OmenDB placement/catalog tier **above SeerDB**,
not a required physical B-tree range prefix or shared multishard WAL.
Reference/lookup tables and global secondary index routes are useful
candidates, but independently updated reference copies and GSIs can
create cross-shard anomalies without a declared atomic/freshness
protocol; do not adopt Neki's currently non-atomic cross-shard write
semantics as a guarantee of one OmenDB SQL transaction. The first
distributed profile can reject unsupported cross-shard changes before
any partial effect; prepare/recover/coordinator ownership for
fully atomic distributed transactions remains a later explicit gate.

## Distribution acceptance gates

1. An unsharded server uses its original local SeerDB commit and query path
   without router, quorum, membership or per-request topology overhead.
2. Regional HA first establishes fenced durable log ownership, replica
   catch-up and failure recovery. Test partitions, stale epochs, ambiguous
   commits, follower read frontiers and deterministic retry behavior.
3. For multiple ranges, prove predictable shard-local throughput, planner
   routing, skew/hot shard behavior, online split/move, and zero-gap retained
   snapshot/change catch-up across repeated leader failures.
4. Do **not** advertise cross-shard atomic writes or global unique/FK
   constraints until prepared participants, coordinator outcome recovery,
   global snapshot/read dependencies and retention are proven. If
   unsupported, reject cross-range mutations before any partial effects.
5. Compare against Neki/Vitess-style systems at matched number of shards,
   replication, client locality, workload and durability. Report per-shard
   and aggregate throughput plus p99, router/network cost, failover behavior,
   resharding downtime and resource/maintenance debt. The published Neki
   read-only multi-shard QPS headline is not an equivalent workload.

Object storage is initially a **SeerDB secondary immutable tier** for backup,
snapshots, archived WAL and cold extents, not automatic distributed commit
authority. See the [storage specification](../../crates/seerdb/docs/architecture.md#storage-tiers-and-supported-io).
A remote-primary, quorum-backed or disaggregated deployment needs its own
authority and durability protocol.

