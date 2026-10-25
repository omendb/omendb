# OmenDB

## Design owners and entry points

- For the accepted server-first native relational design and superseded choices, read [ADR 0015](docs/adr/0015-native-relational-engine.md). Isolation defaults remain provisional pending measured server retry/latency evidence. For kernel boundary details, read [ADR 0013](docs/adr/0013-storage-kernel-and-access-methods.md). SeerDB is the shared transaction/storage kernel; ordered KV is one access method, not the universal physical model.
- For ordered-page layout, read [ADR 0009](docs/adr/0009-buffered-btree-and-materialization.md); for vNext decisions, certification, lifecycle or persistence, read [ADR 0014](docs/adr/0014-vnext-installation-and-recovery.md). The [vNext roadmap](docs/plans/storage-kernel-vnext.md) alone owns sequence and open gates.
- For relational/runtime semantics, read [ADR 0007](docs/adr/0007-runtime-execution-and-contention.md) and [ADR 0010](docs/adr/0010-row-layout-and-column-families.md). Check the [compatibility matrix](docs/pgwire-compatibility.md) and [gap register](docs/gap-register.md) for current behavior; target contracts are not implementation claims.
- Keep the existing engine as a semantic/fault reference until qualified cutover, not an infallible specification. Resolve known defects against accepted contracts and independent oracles. Do not maintain a permanent engine matrix.

## Persistence boundaries

- A durable decision is irrevocable commit authority; synchronous success also requires contiguous visibility. Ready publication alone is not acknowledgment.
- Working spill pages are not restart authority without a complete checkpoint and retained WAL suffix. A page's maximum LSN is not logical replay completeness.
- One transaction/log/catalog authority spans authoritative access methods. Derived structures need retained snapshot coverage or an exact delta/fallback path; an applied frontier alone does not prove an older snapshot is readable.

## Qualification

For vNext decisions, certification, MVCC, ordered layout, buffer, checkpoint or recovery changes, load [.agents/skills/vnext-qualification/SKILL.md](.agents/skills/vnext-qualification/SKILL.md).

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
