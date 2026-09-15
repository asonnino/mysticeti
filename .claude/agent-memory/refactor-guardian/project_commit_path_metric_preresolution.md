---
name: commit-path-metric-preresolution
description: Invariants for the pre-resolved committed_leaders_total counters and the batched benchmark_duration bump on the replica commit path
metadata:
  type: project
---

Hot-path metric writes in the commit path are pre-resolved at registration rather than
looked up per event.

**Why:** `with_label_values` per decided leader cost a `String` alloc + two FNV label
hashes + an `RwLock` read + an `Arc` clone/drop; the `benchmark_duration` bump cost a
monotonic clock read and two relaxed atomics per *committed transaction*.

**How to apply:**
- `CoarseMetrics::new(registry, committee_size)` pre-creates six `IntCounter` children per
  authority (`DecidedLeaderCounters`), and `Metrics::inc_decided_leaders` indexes them by
  `status.authority().index()`. **The `committee_size` handed to `Metrics::new` /
  `new_for_test` must equal the committee the `Core` runs on** — an out-of-range authority
  now *panics* (`Vec` index) where the old path silently minted a new series.
  `PreciseMetrics::observe_connection_latency` is the graceful precedent (`get` + `debug_assert`);
  the leader counters deliberately are not. `Metrics::new_for_test(0)` exists in
  `crates/replica/tests/sync.rs` but only feeds `CommitHandler`/`NetworkSyncer`, never a `Core`.
- Pre-resolution is wire-visible: prometheus `Registry::gather` prunes empty metric
  families, so `committed_leaders_total` used to be **absent** from `/metrics` until the
  first decided leader and now exists with value 0 from startup. Any consumer that treats
  "family present" as "something committed" becomes vacuous — `crates/replica/tests/smoke.rs`
  (`string.contains("committed_leaders_total")`) is exactly that. Sum/rate-based consumers
  (`MetricsSnapshot`, `SnapshotAggregate`, the Grafana `sum by (commit_type)` panel) are
  unaffected; per-series *averaging* (orchestrator `LiveStats`) is diluted by the zero series.
- `CommitHandler::handle_commit` sets a `committed_transactions` flag inside the
  per-transaction loop and calls `update_benchmark_duration()` once after the loops. The flag
  must be set **before** `update_metrics`, because `update_metrics` early-returns on a
  transaction with no extractable timestamp and such transactions still advanced the clock
  in the old code. The counter is a monotone `floor(elapsed since handler start)` resynced at
  every batch, so batching changes nothing at its one-second granularity; under
  `SimulatorContext` the virtual clock cannot advance mid-call, so it is bit-identical.

Related: [[267-commit-path-labels]], [[block-latency-instrumentation]].
