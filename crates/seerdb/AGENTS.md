# SeerDB kernel

This subtree owns the **reusable storage kernel**, not OmenDB's relational
catalog/binder/executor. Read [architecture](docs/architecture.md) for
transaction/physical/recovery invariants and [roadmap](docs/roadmap.md) for
kernel sequence. [Qualification](docs/qualification.md) has real fault and
workload harnesses. OmenDB's [consumer design](../../docs/design/relational-engine.md)
and [roadmap](../../docs/roadmap.md) own SQL/server integration.

## Code placement

**SeerDB:** reusable transaction, MVCC, WAL, recovery, page/extent, buffer,
index algorithms, storage I/O, retention and ordered-KV primitives.

**OmenDB:** schema/SQL/row and relational index codecs, catalog/constraints,
binder, optimizer, executor, query scheduling, server and wire adapters.
OmenDB-specific code must never enter SeerDB for convenience; evaluate whether
an independent non-SQL storage consumer can use the proposed type/behavior.
The [kernel ownership table](docs/architecture.md#source-ownership-and-dependency-direction)
is authoritative for ambiguous cases. `durable-fs` retains generic
filesystem helpers rather than SQL or SeerDB transaction semantics.

Keep one store incarnation, log/decision authority, visibility and failure
lifecycle across compiled ordered/typed access paths. A standalone KV facade
is desirable, but not a universal page/opaque-KV contract imposed on OmenDB
or specialized engines. Do not maintain independent KV and SQL WALs or a
permanent generation-COW versus vNext backend switch.

For WAL/MVCC/certification, page/buffer, checkpoint/recovery or GC changes,
load [the qualification skill](../../.agents/skills/vnext-qualification/SKILL.md).
The skill is currently named after the actual experimental `src/vnext/`
implementation; it does not establish the long-term module layout.

Run from the **workspace root**, not from this crate's directory:

~~~sh
cargo test -p seerdb --lib vnext:: --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo test --workspace --all-features --all-targets
~~~

Hardware/perf and external crash tests remain separate. Report code revision,
actual invocation, failed/unrun checks, and durability class. Do not equate
legacy fixture success with integrated runtime or OmenDB server qualification.
