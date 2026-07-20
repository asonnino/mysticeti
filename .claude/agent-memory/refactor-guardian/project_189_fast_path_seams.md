---
name: 189-fast-path-seams
description: "Issue #189 dual-path (optimistic fast-path) extension of the committer; PR-1 adds inert seams, equal-by-construction quorum invariant"
type: project
---

Issue #189 is a multi-PR effort adding an optimistic fast path (dual-path) to the shared consensus committer. PR-1 (branch `fast-path-seams`) was strictly behavior-preserving: it wired seams that stayed completely inert for every protocol shipped at the time. DagHydrangea (`Protocol::dag_hydrangea`) has since shipped as the first real dual-path protocol and exercises these seams.

**Equal-by-construction invariant (single-path protocols only):** In `Protocol`, `certificate_quorum == direct_commit_quorum == quorum_threshold` and `fast_path == None` for the 7 single-path constructors (cordial_miners_partially_synchronous, cordial_miners_asynchronous, mysticeti, blue_bottle, orcaella, mahi_mahi, nemo_nemo). Orcaella's value is `total_stake - f - c`. DagHydrangea is the deliberate exception — `fast_path: Some(..)` with distinct quorum thresholds by design — and must NOT be lumped into the invariant set during audits. If a future PR makes these diverge for a single-path protocol, `DagConsensus::quorum_threshold` (feeds dag threshold clock + block validity) and `is_certificate`'s inner threshold change → behavior change.

**Seam guard pattern (the thing to verify inert):** New fast-path logic in `base.rs` is guarded by `if let Some(fast_path) = &self.fast_path` / `let Some(..) else { return false }`. The guard MUST short-circuit before any `get_blocks_by_round` / storage access so the hot path takes no extra DAG traversal when `fast_path` is None. Seams: `enough_fast_path_support` (new, OR'd before `enough_leader_support` in `try_direct_decide`), and the rung-2 "weak indirect quorum" block in `decide_leader_from_anchor` (after the rung-1 certified check + uniqueness panic + trace log).

**cfg-gating gotcha:** `Committer::has_fast_path` field and the whole test-helper `impl Committer` block (containing `earliest_decision_round_for`, `voting_round_for`, `decision_round_for`, `new_for_test`) share ONE block-level `#[cfg(any(test, feature = "test-utils"))]`. So a non-gated method in that block referencing the gated field is fine — they compile/vanish together. Don't flag it as a production-build break.

**Feature-unification masking:** `cargo test --workspace` unifies `consensus/test-utils` (pulled by consensus's own dev-dep self-ref) onto the consensus lib shared with replica/cli/simulator, so a test run can NOT prove a plain `cargo build` compiles. To verify production build health of consensus, use `cargo check -p consensus` (default features, no test cfg).
