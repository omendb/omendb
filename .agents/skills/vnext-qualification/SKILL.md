---
name: vnext-qualification
description: Use when changing or reviewing vNext decisions/certification, commit responsibility, record layout, MVCC, buffer materialization, checkpoint publication or recovery, or qualifying a storage-kernel milestone. Not for unrelated SQL, UI or documentation-only edits.
---

# Qualify a vNext storage change

Run from the repository root. This procedure does not grant implementation or publication authority.

1. Inspect the current branch, working tree and source. Read [the plan](../../../docs/plans/storage-kernel-vnext.md) for open gates and [ADR 0014](../../../docs/adr/0014-vnext-installation-and-recovery.md) for the protocol. Distinguish implemented behavior from intended behavior; do not inherit a handoff's green status or closed finding without checking.
2. Identify which boundary changes: pre-decision refusal, WAL/undo durability, guarded page installation, ready publication, contiguous visibility, checkpoint authority or reclamation. State the failure outcome and ownership at that boundary before changing it.
3. Exercise the changed boundary through the real coordinator/runtime. Choose the schedule for its risk:
   - **Certification:** pause an earlier accepted decision before install; later point/absence/range/catalog/constraint observations must certify against its effects, not only visible pages. Pressure cannot discard evidence and permit success.
   - **Responsibility/resources:** unwind or cancel around possible append, sync and structural finish; fence before unresolved ownership drops. Deterministic refusal precedes device entry; healthy admitted decisions retain protected installer capacity.
   - **Visibility:** hold an earlier installer while a later commit becomes ready; test gap closure and earlier failure.
   - **Checkpoint:** race admission closure/drain with admitted writers and structural changes, including unresolved unwinding in a continuing process.
   - **Buffer loading:** delay a read across competing mutation/writeback/eviction; check a new post-ACK snapshot.
   - **Record admission:** prove actual fenced leaf/internal/root candidates, overflow-chain closure, future undo conversion and replay of accepted effects—not only empty-leaf fit. Preserve legitimate record sizes and old-value atomicity on genuine refusal.
   A frontier or codec unit test alone does not cover these integration risks.
4. For persistent changes, use the repository's fault machinery at candidate publication and recovery boundaries. Verify prior authority remains reopenable, foreign/missing/corrupt references fail closed, and committed suffix replay remains correct after a crash during recovery. Require two consecutive reopens, including a normal operation against recovered state when the test's risk involves continued use. Crash during recovery and measure abandoned-allocation/retry-space behavior where that lifecycle changes.
5. Run targeted tests plus the local gates in [AGENTS.md](../../../AGENTS.md). Consult [CI](../../../.github/workflows/ci.yml) for the complete matrix. Report exact revision and failed/unrun checks; do not describe legacy SQL/perf gates as exercising isolated vNext.
6. If the change addresses performance, exercise the actual vNext path under the plan's workloads and agreed budgets. Separate barrier, frontier-wait, history-read and checkpoint costs. Record hardware, durability/isolation, dataset, concurrency and comparison conditions. A paper or microbenchmark is not end-to-end qualification.
7. Update the plan only to close or revise a genuine gate with evidence. Update the ADR only for a contract change. Keep transient execution notes in the session, not a duplicate roadmap in this skill.
