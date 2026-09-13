# OmenDB

## Architecture and authority

- The storage-kernel rewrite is governed by [ADR 0013](docs/adr/0013-storage-kernel-and-access-methods.md), [ADR 0014](docs/adr/0014-vnext-installation-and-recovery.md), and the [vNext plan](docs/plans/storage-kernel-vnext.md). Read these before changing vNext protocol or persistence. The plan owns milestone order and open gates; older handoffs are context, not competing roadmaps.
- Preserve the existing engine as the semantic/fault oracle until replacement qualification passes. Do not preserve obsolete physical ownership or maintain a permanent engine matrix.
- One transaction/log/catalog authority spans authoritative access methods. Derived structures need explicit snapshot coverage or a correct delta/fallback path.
- A durable decision is irrevocable commit authority; synchronous success additionally requires contiguous visibility coverage. Ready publication alone is not acknowledgment.
- Working spill pages are not restart authority without a complete, validated checkpoint and retained WAL suffix. A page's maximum LSN is not a logical replay-completeness marker.
- Correctness and representative vNext performance gates precede product cutover. Research suggests experiments; it does not establish performance or authorize speculative protocol changes.

## Qualification

For vNext commit, MVCC, buffer, checkpoint or recovery changes, load [.agents/skills/vnext-qualification/SKILL.md](.agents/skills/vnext-qualification/SKILL.md).

Primary local gates (repository root):

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo test --workspace --all-features --all-targets
```

[CI](.github/workflows/ci.yml) also owns MSRV, platform, package-list, live PostgreSQL differential and perf-smoke checks. Read its current configuration rather than copying toolchain versions into notes. Product perf smoke is not evidence of isolated vNext performance.
