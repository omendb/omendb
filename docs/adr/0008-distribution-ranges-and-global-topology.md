# ADR 0008: Distribution, ranges, and global topology

- **Status:** historical design record; maintained target now in [living specification](../design/engines-and-scale.md). This document retains rationale, not an independent current contract.
- **Scope:** horizontal scale, replication placement, resharding, distributed SQL, and global deployments
- **Current product authority:** [ADR 0015](0015-native-relational-engine.md); distributed design must remain compatible with the native relational model, not a universal ordered-KV substrate.
- **Depends on:** [ADR 0001](0001-seerdb-transaction-contract.md),
  [ADR 0006](0006-deployment-storage-and-durability.md), and
  [ADR 0007](0007-runtime-execution-and-contention.md)

## Context

OmenDB's first production target remains one excellent node. Horizontal and
multi-region scale must not impose a permanent coordination, indirection, or
metadata cost on local transactions before the workload needs it.

At the same time, distribution is easier to add cleanly when the local engine
already has explicit transaction identities, logical commit order, a restartable
change stream, immutable/versioned catalog state, and a durability layer that is
not synonymous with one local page file.

Current systems provide complementary lessons:

- Spanner/CockroachDB/Yugabyte show range/tablet ownership and consensus as a
  scalable distribution unit;
- FoundationDB shows the value of a small ordered transactional KV substrate;
- Neki and Multigres show explicit table co-location and topology-aware routing
  while preserving PostgreSQL-facing interfaces;
- Aurora DSQL shows query processing, optimistic transaction adjudication,
  durable journaling, and storage materialization can be disaggregated;
- Neon/Aurora/Socrates-style systems show that compute replacement and storage
  movement are simpler when durable log/checkpoint state is external to one
  process.

OmenDB should take the useful primitives without inheriting a proxy-over-stock-
PostgreSQL architecture or requiring distributed consensus for a local DB.

## Decision

### 1. Distribution is logical placement, not SeerDB's universal ordered keyspace

The movement and replication unit is a **logical placement range** over a
catalog-declared routing key (hash, ordered value, or another validated mapping):

```text
RangeId
PlacementGroupId / routing-key expression + codec version
[start_route, end_route)
ownership epoch
replica set / leader / declared durability policy
```

Routing keys are selected from the **relational workload and co-location needs**,
not automatically the primary B-tree key, physical PageId or a mandatory encoded
KV universe. Primary/secondary indexes, heap candidates, posting and analytical
representations may use different physical formats. A range is logical
ownership, never a physical page slice. Resharding does not require page-stable
boundaries. The exact future range/log/quorum implementation remains unselected
until local correctness, actual workload and a distributed transaction protocol
earn it.

Local single-node mode may represent the whole database as one local placement
without running a distributed topology service. Do not impose router hops,
consensus calls or per-request shard bookkeeping on the server-first local path.

### 2. Placement groups express intentional co-location

OmenDB's relational catalog gains a distribution/placement layer only when the
feature is enabled. Tables and indexes may belong to a **placement group** that
states which relational objects share range boundaries and therefore can join,
constrain, and transact locally for the same distribution key.

A placement group records:

- distribution key expression(s);
- hash, range, or future routing function;
- member tables/indexes;
- range boundaries;
- locality/replication policy;
- optional reference/replicated-small-table semantics.

This is conceptually similar to Neki shard groups / Multigres table groups, but
it is OmenDB catalog state feeding OmenDB's own planner and storage engine.
Neki's 2026-10-23 study shows the critical design criterion: choose the key by
common **joins and transactions**, not independently by each table's primary
key. A customer row and its order/line tables should normally be local together.
Treat reference-table replication and global secondary indexes as distinct
cross-range synchronization/uniqueness problems, not free routing metadata.
Neki currently documents non-atomic/no-shared-snapshot cross-shard writes; this
is a boundary OmenDB must not emulate implicitly.

No table must be sharded. The default database remains unpartitioned until
measured scale or locality requires otherwise.

### 3. One native planner produces local and distributed fragments

A distributed OmenDB query has one semantic plan. The optimizer inserts
exchange/routing operators and lowers the plan into typed fragments:

```text
client
  |
logical/bound plan
  |
distribution-aware optimizer
  |
  +-- fragment -> range/node A
  +-- fragment -> range/node B
  +-- coordinator/local fragment
```

Remote nodes execute OmenDB physical fragments, not SQL text that a second
independent PostgreSQL planner must reinterpret. The PostgreSQL wire protocol is
an external interface, not the inter-node execution protocol.

Prepared/cached plans retain **symbolic routing expressions** plus catalog and
topology-version dependencies. Binding/execution resolves the current
`RangeId`/epoch/leader from parameter values and cached topology; a plan does
not bake one physical endpoint into a long-lived prepared statement.

Distributed costing adds dimensions that local costing does not have: range
fan-out, remote request count, network bytes, exchange/repartition work,
coordinator memory, spill, and tail-latency exposure. The optimizer prefers
co-located execution, predicate routing, decomposable partial aggregation, and
local joins before repartitioning data. Scatter/gather is a last resort made
explicit in plans/EXPLAIN.

Fragments stream typed batches rather than generic rows or SQL text. Each remote
execution carries an explicit immutable context containing the authenticated
principal/capabilities, transaction or snapshot identity, catalog/topology
versions, relevant session semantics, deadline/cancellation token, and resource
budget. Process-local session state is never an implicit distributed contract.

### 4. The local transaction remains the fast path

If all touched keys resolve to one range/replication group, execution uses the
normal local SeerDB transaction path plus that range's durability policy. No
2PC coordinator is allocated.

Cross-range transactions use an explicit distributed transaction protocol. The
target shape is:

1. establish one logical read timestamp/snapshot contract;
2. execute and validate participants independently;
3. prepare participants durably;
4. make one distributed commit/abort decision durable;
5. publish participant outcomes and clean up asynchronously.

The exact concurrency protocol (OCC validation, timestamp ordering, pessimistic
intents, or a hybrid) remains benchmark/research-gated. The SQL isolation
contract is stable above it.

### 5. Regional HA is the default distributed durability primitive

Each writable range normally has a single active write leader in one region and
a quorum-replicated log as described by ADR 0006. Followers/materializers can
serve reads when the requested consistency level proves their snapshot safe.

Each range has an explicit durability policy as versioned topology/catalog
state. Membership or leader changes must preserve or deliberately transition
that policy; durability is not an incidental process configuration.

During failover, a router/coordinator may buffer only operations whose retry
safety is known, under explicit byte/count/time bounds, and must release queued
work gradually so a replacement leader is not hit by a thundering herd.
Ambiguous writes are never replayed merely because the client has not yet seen a
response; safe replay requires request identity/idempotence or a proven
pre-decision failure.

Closed timestamps / safe read frontiers are valid candidates for follower reads.
The normal local/regional write should not contact unrelated ranges or a global
coordinator.

### 6. Global placement is locality-first; active-active everywhere is not a
free default

A global deployment places range leaders near their primary workload and may
replicate followers to other regions. Strong writes pay the WAN latency required
by the selected replication policy; OmenDB must not hide that physics.

Future multi-region active-active writes can be added for workloads that justify
the complexity, but the architecture does not require every range to have
multi-writer WAN consensus. A witness/quorum region, timestamp service, or
clock-bound API may be introduced only when its concrete transaction protocol
is selected and simulation-tested.

The optimizer/router should expose locality and consistency choices rather than
silently weakening semantics.

### 7. Resharding is snapshot + zero-gap change catch-up

Range movement reuses SeerDB's existing snapshot/change-position contract:

```text
source range at snapshot CSN X
            + restart LSN Y
                 |
        copy snapshot X
                 |
consume committed changes > Y
                 |
catch up to cutover frontier
                 |
fence source writes / finalize ownership epoch
                 |
publish new topology
                 |
GC old ownership after safety window
```

No dual-write protocol is required during the bulk copy. If a future storage
profile can clone immutable checkpoints more cheaply than logical key copy, the
same logical cutover protocol can use that physical acceleration.

This copy + zero-gap change-catch-up + epoch cutover primitive should be reused
where it genuinely matches the semantics: table/range moves, online index builds,
physical row/layout rewrites, or selected online schema rewrites. Native typed
committed changes are preferred over replaying SQL text.

Topology epochs prevent a stale router or participant from committing against
ownership that has moved.

### 8. Topology is versioned control-plane state, not query-local folklore

Routing state has explicit versions/epochs and is cached at compute nodes.
Operations such as split, merge, move, replica change, and leader move are state
machines with resumable durable intent.

The control plane owns desired placement and orchestration. The data plane owns
transaction correctness. Controllers reconcile **desired topology** with
observed replica/leader state through idempotent, resumable transitions rather
than assuming orchestration succeeded. Losing the control plane must not make
already healthy ranges unable to serve local traffic.

Routers/coordinators cache topology but are not durable data authorities. Cache
refresh and epoch rejection repair stale routing; durable range ownership,
transaction decisions, replica membership and catalog state live in replicated
authoritative state below/beside the coordinator.

A small authoritative metadata range/service may own global database metadata,
but ordinary query routing should use cached topology rather than synchronously
consulting it on every statement.

### 9. Object storage accelerates bootstrap and recovery

Immutable range checkpoints and archived logs can live in object storage.
Adding/rebuilding a replica should normally bootstrap from the newest compatible
checkpoint then replay the replicated/archive log, rather than stream the full
history from a live leader.

This keeps object storage off the fine-grained commit path while making global
movement, disaster recovery, and elastic compute substantially cheaper.

### 10. Global identifiers and sequences are explicit distributed objects

Features whose semantics require a single global order (for example strict SQL
sequences) cannot be disguised as ordinary local counters. The implementation
may use leased blocks/ranges for performance while preserving the documented
sequence semantics.

Where applications only require unique IDs rather than gapless/global order,
OmenDB should expose or support decentralizable identifiers so distribution
does not create an unnecessary central bottleneck.

### 11. Distribution must be deterministically simulatable

Before a global mode is trusted, deterministic tests must explore:

- range split/move concurrent with transactions;
- stale topology epochs;
- leader failure during prepare/commit;
- partitions and delayed/reordered messages;
- duplicate requests and retries;
- checkpoint bootstrap plus log catch-up;
- coordinator failure after participant prepare;
- GC with lagging replicas/readers;
- distributed deadlocks or cyclic dependencies;
- clock/timestamp uncertainty if the chosen protocol uses time.

## Non-goals for the first server release

- automatic sharding;
- multi-region active-active writes;
- cross-range distributed joins as a requirement for local SQL completeness;
- a mandatory external topology service for single-node use;
- exposing storage pages or replica internals through the SQL contract.

## Acceptance gates before distribution ships

- single-range mode remains measurably equivalent to the local fast path;
- range routing is predicate/parameter-aware, cached plans re-resolve symbolic routes after topology changes, and plans expose scatter/exchange costs;
- split/move preserves transactions through snapshot + change catch-up with no
  gap or duplicate committed effect;
- range epochs reject stale owners/routers;
- quorum loss, coordinator loss, participant loss, bounded safe-retry buffering, and control-plane unavailability have deterministic recovery/service outcomes;
- distributed operators respect explicit coordinator memory/spill/network budgets and apply backpressure rather than allowing remote producers to exhaust a node;
- object-store bootstrap and local replica catch-up are benchmarked for time,
  bytes, and cost;
- a TPC-C-style partitionable workload demonstrates that local transactions
  dominate when the schema is co-located intentionally;
- global/WAN benchmarks publish p50/p95/p99 latency by locality rather than one
  misleading aggregate number.

## Research provenance

- [Neki's architecture (2026-09-18)](https://planetscale.com/blog/the-architecture-of-neki): shard groups, topology/router, pooled session scopes and online migration workflows; the PostgresManager/Sidecar/Operator stack is a stock-Postgres constraint, not a mandatory native-engine design.
- [Query-first shard groups (2026-10-23)](https://planetscale.com/blog/shard-groups-the-unit-of-co-location): co-location, reference tables, routing expressions/global indexes and the current cross-shard atomicity limitation.
- [Lazy Neki routing (2026-10-01)](https://planetscale.com/blog/designing-neki-for-performance): don't decode untouched results/parameters, and don't require the local OmenDB server to introduce a remote routing tier.
- [Neki 118.5M QPS experiment (2026-09-11)](https://planetscale.com/blog/118-million-queries-per-second-on-neki): 512 primary-only shards, single-shard point reads, no writes/failover; not one-node durable OLTP evidence.

## Consequences

- OmenDB can remain simple and fast locally while preserving a coherent path to
  horizontal/global scale.
- Distribution metadata becomes part of the relational optimizer/catalog, not
  a transparent proxy trick.
- Co-location is a first-class schema/placement concept.
- The atomically leased snapshot/bootstrap handoff (incarnation, catalog, snapshot CSN, restart LSN, retained history/change coverage) also governs resharding; a bare `{CSN, LSN}` pair is insufficient.
- Object storage is useful for elasticity and recovery without becoming the
  random-write engine.
- The design remains free to choose the best distributed transaction protocol
  after local storage/runtime architecture is stronger and benchmarks justify
  the additional complexity.
