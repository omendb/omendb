# OmenDB workspace

## Navigation

[Architecture](docs/architecture.md) describes product boundaries;
[known gaps](docs/gap-register.md) describe serving-path limitations.
Implementation plans, detailed target contracts and research live in project
knowledge, not the public docs. Consult them when planning or changing a contract;
inspect source for current behavior. Kernel work follows
[`crates/seerdb/AGENTS.md`](crates/seerdb/AGENTS.md).

## Non-obvious constraints

- Dependency: OmenDB → SeerDB → durable-fs. SQL semantics, relational codecs,
  catalog, constraints and query scheduling stay in OmenDB; reusable storage,
  transactions and recovery stay in SeerDB.
- Canonical rows, required indexes and catalog share one SeerDB decision.
  Durable decision is not ACK: complete effects must be readable through the
  contiguous committed prefix. Restart authority is a complete checkpoint plus
  required committed log/history, not a spill or maximum page LSN.
- SERIALIZABLE is the reference correctness protocol, not a selected SQL default.
  Exact queries cannot silently use stale projections. Shared algorithms do not
  give standalone Vector/OLAP products SQL atomicity.

## Verification

Follow the affected subtree and [CI](.github/workflows/ci.yml). Kernel persistence
changes use [qualification](.agents/skills/vnext-qualification/SKILL.md).
From the workspace root:

~~~sh
cargo fmt --all -- --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo test --workspace --all-features --all-targets
~~~

Live SQL/security oracles, device durability and performance qualification need
separate environments; component or preview results do not qualify the server.
