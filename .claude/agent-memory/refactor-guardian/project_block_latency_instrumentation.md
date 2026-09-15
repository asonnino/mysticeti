---
name: block-latency-instrumentation
description: Invariants behind the per-committed-block latency histogram in CommitHandler::handle_commit — anchor uniqueness, genesis guard, cached label children, transaction_time lock scope
metadata:
  type: project
---

`CommitHandler::handle_commit` observes `block_latency_s{kind}` once per committed block,
inside the `transaction_time` mutex guard that the existing per-transaction loop already
holds.

**Why:** #267 task 2 wanted proposal-to-commit latency split leader / non-leader without
paying label-hashing or extra locking on the commit path.

**How to apply:**
- **One leader per sub-DAG, always.** `Linearizer::collect_sub_dag` pushes the leader into
  `to_commit` first and does `assert!(self.committed.insert(leader_block_ref))`, and the
  shared `committed` set makes every block appear in exactly one sub-DAG. So
  `*block.reference() == commit.anchor` selects exactly one block per sub-DAG, and the
  anchor can never be absent from `blocks`. Any change to the linearizer that relaxes that
  assert breaks the `kind` split silently.
- **Genesis must stay excluded.** Genesis blocks are inserted into the store at startup
  (`Core::new`) and are pulled into the *first* sub-DAG through round-1 `includes`. They
  carry `timestamp_ns == 0`, so without the `round() != GENESIS_ROUND` guard every one of
  them lands in `+Inf` and poisons `block_latency_squared_s`.
- **Label children are pre-resolved in `CoarseMetrics::new`.** `register_*_vec_with_registry!`
  registers a `.clone()` of the vec and prometheus `MetricVec` is `Arc`-backed, so the
  registry keeps the core alive after the local binding drops, and `with_label_values`
  children created at registration are the same objects `registry.gather()` reports. Keep
  the cached-child pattern for anything on the commit path — `with_label_values` per
  observation costs a hash plus an `RwLock` read.
- **Do not add work under that guard casually.** The block-level observation is roughly one
  histogram bucket scan plus one f64 CAS per block; it is only acceptable because the loop
  already holds the lock and the per-transaction work inside it dominates. Anything O(blocks)
  and heavier belongs after the guard drops.
- **Recovery replay inflates the histogram** for one rate window: replayed old blocks are
  timestamped by their proposer, and cross-replica skew is clamped by `saturating_sub`, so
  negative skew shows as a pile-up in the lowest bucket rather than as an error.
  `latency_s` already behaves this way — consistent, not a regression.

Related: [[267-commit-path-labels]], [[committer-cursor-semantics]].
