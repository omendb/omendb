# Architectural decision history

The numbered ADRs explain *why* prior designs were selected, challenged or
revised. They are **not independently maintained specifications or plans**.
Some older documents use "accepted target" or future tense; where they
conflict with current design, these are historical records rather than
requirements to preserve.

**Living authority:** [product architecture](../architecture.md),
[SeerDB contract](../design/storage-kernel.md),
[relational engine](../design/relational-engine.md),
[specialized engines and distribution](../design/engines-and-scale.md),
and the **one** [roadmap](../roadmap.md).

- 0001–0006: historical SeerDB transaction/recovery/group-commit and early
  integration choices.
- 0007, 0010: prior relational execution/runtime/row-layout reasoning.
- 0008: proposed distributed placement and transaction strategies; several
  specifics are deferred/unselected.
- 0009, 0013, 0014: buffered B-tree, SeerDB/access-method boundary, and
  durable-decision-first/recovery reasoning. Safety invariants are carried
  into the maintained storage contract; implementation sequence is not.
- 0011, 0012: analytical/multi-engine boundaries and repository decisions.
- [0015](0015-native-relational-engine.md): October 25 native server-first
  product pivot, currently summarized as a decision, not a duplicate spec.

The full older contents are recoverable from Git history; never promote a
historical performance result into a current implementation claim.

