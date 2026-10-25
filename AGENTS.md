# OmenDB workspace

## Source of truth

- [Architecture](docs/architecture.md) is the product and module map.
  Living implementation contracts: [SeerDB](docs/design/storage-kernel.md),
  [relational](docs/design/relational-engine.md) and
  [specialized engines/distribution](docs/design/engines-and-scale.md).
- [Roadmap](docs/roadmap.md) **alone** owns cross-engine implementation order
  and gates. [ADRs](docs/adr/README.md) are historical decisions, not parallel
  accepted specs; old `vnext` handoffs/roadmaps must not override living design.
- README/source describe *implemented* behavior. [Gap register](docs/gap-register.md),
  [alpha gates](docs/alpha-release-gates.md) and [Pgwire matrix](docs/pgwire-compatibility.md)
  describe current defects, release proof and optional adapter coverage.
  Do not claim new SQL/storage features or performance before qualification.

## Architecture guardrails

- Server-first **native relational** OmenDB; PostgreSQL compatibility is optional.
  SeerDB is a reusable kernel where beneficial, not a universal byte KV,
  small-page or OLTP-only mandate. One transaction/log/catalog authority
  covers canonical rows, required indexes and catalog.
- Durable decisions are irrevocable; synchronous acknowledgement also requires
  complete authoritative installation and contiguous visibility. No uncertain
  commit is silently aborted after cancel/unwind. A page LSN or working image
  alone is not restart authority; snapshot/export requires leased retained
  checkpoint *and* change coverage.
- Prefer complete redesign/removal of an obsolete experimental interface over
  maintenance of multiple permanent backends or storage-format shims. Keep
  existing engine only as a fallible semantic/fault oracle until proven cutover.
- Separate specialized physical engines and independent Vector/OLAP products
  from shared transactional authority. No independent mini-WAL inside a
  canonical OmenDB transaction; a rebuildable index must prove requested
  snapshot coverage, not merely a latest applied timestamp.
- Isolation default remains a product **decision pending** server workloads,
  retry UX and p99 evidence; SERIALIZABLE is the reference correctness model,
  READ COMMITTED a separate correctly implemented contract.

## Qualification

Before modifying `crates/seerdb/src/vnext` commit, MVCC, buffer, checkpoint,
recovery, or page formats, load
[.agents/skills/vnext-qualification/SKILL.md](.agents/skills/vnext-qualification/SKILL.md).
Use real persistent/runtime + direct/server entry points for integration claims.
No isolated component test or vendor microbenchmark closes a roadmap gate.

From repository root:

~~~sh
cargo fmt --all -- --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo test --workspace --all-features --all-targets
~~~

For focused kernel work: `cargo test -p seerdb --lib vnext:: --locked`.
[CI](.github/workflows/ci.yml) covers additional MSRV/platform/live-PG and
perf-smoke surfaces. A live PostgreSQL differential tests the advertised
compatible subset, not full PostgreSQL as the product contract.

