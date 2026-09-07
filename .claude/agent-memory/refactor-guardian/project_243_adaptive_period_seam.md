---
name: 243-adaptive-period-seam
description: "#243 adaptive Steelhead period (28de48f): adaptive-off inertness proof, drain-truncation invariants, replay determinism, and the two latent couplings worth re-checking"
metadata:
  type: project
---

Commit 28de48f adds adaptive Steelhead periods via counterfactual replay
(`consensus::replay`). Audited 2026-09-04; adaptive-off paths verified inert.

**Adaptive-off inertness (how it holds):**
- `SteelheadMode.period_schedule` is `vec![(0, schedule.period)]` for static configs;
  `period_at` reverse-scans it (O(1): newest entry checked first). This replaced the
  inline Copy read of `SteelheadSchedule::is_async_round` — one extra heap load per
  call in `wave_for`/`elect_leader`/`get_leaders`, no allocation.
- `update_threshold()` = `schedule.adaptive.map(..)` → None when static, so the
  `.scan` stage in `try_commit`'s drain is a pure pass-through (block.clone() only
  inside the commit-past-threshold branch; Data<Block> clone is cheap/Arc-like).
- `apply_pending_period_update` early-returns on `steelhead: None` then
  `pending_anchor: None`; the `steelhead_period` gauge and period cell are only
  touched past those guards, so unconditional `with_metrics` in replica.rs is inert.
- `RoundTimeouts` lost Copy (gained `QuorumTimeoutRounds::Dynamic(Arc<AtomicU64>)`);
  only value-copy site was `round_timeout_task` (now one clone at task start).

**Drain truncation invariants (adaptive):**
- Scan yields skips past the threshold without truncating; the first COMMIT strictly
  above threshold sets `pending_anchor`, is itself yielded, then everything after is
  suppressed. State mutates only when the anchor is actually *pulled* by the consumer
  (lazy scan), so early iterator drop cannot corrupt.
- `apply_pending_period_update` runs first in the next `try_commit`, sets
  `last_update_round = anchor_round` unconditionally (even when chosen == current),
  so the threshold advances by `interval` and an anchor cannot fire twice.
- Documented reliance: a consumer that pulls the anchor MUST advance last_decided
  past it (Core::try_commit does; the cadence test uses `.take(chunk)` + records
  everything pulled). Violating it can re-decide an IndirectCommit anchor under the
  new schedule.

**Replay determinism:** window rounds sorted by (author, digest); support map uses
keyed HashMap lookups only (no iteration); StakeAggregator dedups authorities via
AuthoritySet so equivocators count once and quorum-reached is order-independent;
equivocating leader representative = min digest; u128 scores bounded by
~n² × round × 100 ≪ u128::MAX.

**Latent couplings to re-check on future changes:**
- `collect_window`'s `.expect("causal history must be complete")` is safe only
  because adaptive `interval <= RETAIN_BELOW_COMMIT_ROUNDS` (protocol.rs validation
  imports the dag constant — a shared constant since the steelhead branch; the value
  was raised 100 -> 512 in c6c7f28, see [[237-probe-replay-seam]]). The tightness
  argument is value-independent.
- `#[serde(default)]` on Steelhead `period` made a previously-required config field
  optional (omission now parses as period ∞ instead of erroring).
- Replay runs synchronously inside `try_commit`; cost ~O(candidates × async slots ×
  n × wave-window blocks × includes) — fine for small committees, could stall the
  commit path once per interval at n≈100.

Related: [[240-steelhead-seam]], [[241-leader-timeout-wiring]].
