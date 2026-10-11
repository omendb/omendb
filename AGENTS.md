# OmenDB

## Design owners and entry points

- For kernel boundary changes, read [ADR 0013](docs/adr/0013-storage-kernel-and-access-methods.md). SeerDB is the shared transaction/storage kernel; ordered KV is one access method, not the universal physical model.
- For vNext commit, MVCC or persistence changes, read [ADR 0014](docs/adr/0014-vnext-installation-and-recovery.md) and the [vNext plan](docs/plans/storage-kernel-vnext.md). The ADR owns the protocol; the plan owns milestone order and open gates. Older handoffs are historical context.
- For relational or wire changes, start with [architecture](docs/architecture.md), the [compatibility matrix](docs/pgwire-compatibility.md), and the [SQL gap register](docs/gap-register.md). PostgreSQL-wire support is not full PostgreSQL compatibility.
- Keep the existing engine as a semantic/fault reference until qualified cutover, not an infallible specification. Resolve known defects against accepted contracts and independent oracles. Do not maintain a permanent engine matrix.

## Persistence boundaries

- A durable decision is irrevocable commit authority; synchronous success also requires contiguous visibility. Ready publication alone is not acknowledgment.
- Working spill pages are not restart authority without a complete checkpoint and retained WAL suffix. A page's maximum LSN is not logical replay completeness.
- One transaction/log/catalog authority spans authoritative access methods. Derived structures need retained snapshot coverage or an exact delta/fallback path; an applied frontier alone does not prove an older snapshot is readable.

## Qualification

For vNext commit, MVCC, buffer, checkpoint or recovery changes, load [.agents/skills/vnext-qualification/SKILL.md](.agents/skills/vnext-qualification/SKILL.md).

Focused kernel check (repository root):

```sh
cargo test -p seerdb --lib vnext:: --locked
```

Primary implementation gates:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo test --workspace --all-features --all-targets
```

[CI](.github/workflows/ci.yml) also owns MSRV, platform, package-list, live PostgreSQL differential and perf-smoke checks. Read its current configuration rather than copying toolchain versions into notes. Product perf smoke is not evidence of isolated vNext performance.
