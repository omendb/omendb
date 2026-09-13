---
name: vnext-qualification
description: Use when changing or reviewing vNext commit completion, MVCC, buffer materialization, checkpoint publication or recovery, or qualifying a storage-kernel milestone. Not for unrelated SQL, UI or documentation-only edits.
---

# Qualify a vNext storage change

Run from the repository root. This procedure does not grant implementation or publication authority.

1. Inspect the current branch, working tree and source. Read [the plan](../../../docs/plans/storage-kernel-vnext.md) for open gates and [ADR 0014](../../../docs/adr/0014-vnext-installation-and-recovery.md) for the protocol. Distinguish implemented behavior from intended behavior; do not inherit a handoff's green status or closed finding without checking.
2. Identify which boundary changes: pre-decision refusal, WAL/undo durability, guarded page installation, ready publication, contiguous visibility, checkpoint authority or reclamation. State the failure outcome and ownership at that boundary before changing it.
3. Write a test schedule that crosses the boundary through the real coordinator/runtime. For visibility, hold an earlier installer while a later independent commit becomes ready, then exercise both successful gap closure and earlier failure. For checkpoint cuts, race already-admitted writers and structural changes with admission closure/drain. A frontier or codec unit test alone does not cover these integration risks.
4. For persistent changes, use the repository's fault machinery at candidate publication and recovery boundaries. Verify prior authority remains reopenable, foreign/missing/corrupt references fail closed, and committed suffix replay remains correct after a crash during recovery. Require two consecutive reopens, including a normal operation against recovered state when the test's risk involves continued use.
5. Run targeted tests plus the local gates in [AGENTS.md](../../../AGENTS.md). Consult [CI](../../../.github/workflows/ci.yml) for the complete matrix. Report exact revision and failed/unrun checks; do not describe legacy SQL/perf gates as exercising isolated vNext.
6. If the change addresses performance, exercise the actual vNext path under the plan's workloads and agreed budgets. Separate barrier, frontier-wait, history-read and checkpoint costs. Record hardware, durability/isolation, dataset, concurrency and comparison conditions. A paper or microbenchmark is not end-to-end qualification.
7. Update the plan only to close or revise a genuine gate with evidence. Update the ADR only for a contract change. Keep transient execution notes in the session, not a duplicate roadmap in this skill.
