# SeerDB kernel

This subtree owns reusable storage, transactions, MVCC, WAL, buffers/pages/extents,
retention and recovery. SQL/catalog/row codecs, constraints and query scheduling
stay in OmenDB. [Architecture](docs/architecture.md) owns the public boundary;
detailed target contracts and the kernel implementation plan live in project
knowledge. Keep one runtime/log/failure authority across ordered and typed access.

For commit/certification, MVCC, record admission, buffer materialization,
checkpoint/recovery or GC changes, load
[qualification](../../.agents/skills/vnext-qualification/SKILL.md). The experimental
`src/vnext/` name does not establish the permanent module layout.

Run the affected kernel tests from the **workspace root**:

~~~sh
cargo test -p seerdb --lib vnext:: --locked
~~~

Then follow the root checks and [CI](../../.github/workflows/ci.yml).
[Qualification](docs/qualification.md) documents additional fault/device harnesses;
legacy-engine results do not qualify the replacement runtime or SQL server.
