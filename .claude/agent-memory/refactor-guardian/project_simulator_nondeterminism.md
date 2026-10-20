---
name: simulator-nondeterminism
description: Simulator runs diverge across processes because tokio's unbiased select! picks branches from process entropy; adding `biased;` at the 7 select sites makes same-seed runs agree, which makes output diffing a usable validation technique
metadata:
  type: project
---

The discrete-event simulator has a seeded `rng_seed`, but two runs of the same binary still
diverge (observed on `examples/single.yaml`: leader commits 434 vs 432, ~1500 differing metric
lines).

**Dominant cause: tokio's unbiased `select!`.** Its branch-order RNG is seeded from process
entropy, not from the simulator seed. When several branches are simultaneously ready the pick
is process-random — e.g. in the leader-timeout task the sleep and the notify can both be ready,
and the choice decides whether `force_new_block` fires.

Confirmed experimentally (throwaway worktree of `main` @ 0669999): adding `biased;` to the 7
`select!` sites and nothing else makes two same-seed runs agree on **every consensus metric** —
commit counts and all latency histograms/buckets. The sites are
`crates/dag/src/context.rs:147`, `crates/dag/src/sync/network.rs:312`,
`crates/dag/src/sync/net_sync.rs:267/286/306`, `crates/dag/src/sync/synchronizer.rs:254`,
`crates/simulator/src/executor.rs:129`.

Residual differences under `biased;`, all benign: wall-clock metrics (`utilization_timer`,
`*_cleanup_util`, the `meta.yaml` timestamp), and — seen only on `twin.yaml` — last-digit
differences in float `*_sum` metrics with identical counts and buckets, i.e. observation order
only. The `HashSet` iteration at `crates/dag/src/core/block_manager.rs:91` is an **unconfirmed
candidate for that residual only**; it produced no behavioural divergence once select was
biased. `thread_rng` in `crates/dag/src/sync/synchronizer.rs:322` (`sample_peer`) is a further
unseeded source, not exercised in a healthy full mesh.

**Why:** knowing the cause turns output diffing from useless into the strongest available check.

**How to apply:** output diffing **can** validate a simulator refactor, provided both sides are
built with the biased-select patch and you ignore the wall-clock metrics and float-sum last
digits. Done for #277 commit 1 over `single/suite/twin.yaml`: identical. Without that patch,
diffing proves nothing and equivalence must be argued from the code — see
[[277-latency-model-seam]].
