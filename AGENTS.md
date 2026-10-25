# OmenDB workspace

## Maintained ownership

- [Product architecture](docs/architecture.md) describes OmenDB goals
  and boundaries. [Relational design](docs/design/relational-engine.md)
  specifies SQL/catalog/execution; [extensions](docs/design/extensions.md)
  and [distribution](docs/design/distribution.md) specify later interfaces.
- The [OmenDB roadmap](docs/roadmap.md) owns product integration and
  release work. **SeerDB has its own** [architecture](crates/seerdb/docs/architecture.md),
  [roadmap](crates/seerdb/docs/roadmap.md), and nested
  [AGENTS.md](crates/seerdb/AGENTS.md) for kernel work. Neither roadmap
  duplicates the other's implementation sequence.
- [ADRs](docs/adr/README.md) retain dated decisions only; previous vNext
  handoffs and `docs/plans/` are not active targets. README/source expose
  current behavior, while [gap register](docs/gap-register.md),
  [release gates](docs/alpha-release-gates.md) and
  [optional Pgwire matrix](docs/pgwire-compatibility.md) retain evidence.

## Non-obvious invariants

OmenDB is a native server-first relational engine, not PostgreSQL's storage/
catalog implementation. Keep one SeerDB transaction/log/recovery authority
for canonical records, required indexes and OmenDB catalog. OmenDB owns
the *meaning* of catalog, types, constraints and query plans. Standalone
Vector/OLAP products do not inherit SQL atomicity by sharing code.
Durable decision ≠ publication/ACK: complete authoritative effects must be
visible in the contiguous committed prefix before successful return.
Checkpoint plus committed-log history, not a spill or max page LSN,
determines recovery. Unsupported exact snapshots may not silently use
stale derived projections.

SERIALIZABLE is the reference correctness protocol, but SQL default isolation
is not final; evaluate READ COMMITTED, retry UX and p99 under real workloads.
Replace obsolete development interfaces after qualified cutover rather
than retaining a backend matrix.

## Verification

Follow the **affected subtree's** instructions and
[CI](.github/workflows/ci.yml); for kernel persistence changes load
[qualification skill](.agents/skills/vnext-qualification/SKILL.md).
From the workspace root:

~~~sh
cargo fmt --all -- --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo test --workspace --all-features --all-targets
~~~

Security/SQL live-oracle checks, platform/durability and performance
qualification require their separate environments. Do not call a
component test or preview benchmark an integrated server guarantee.

