import Hydrozoan.Optimal.IndirectLiveness.Proof
import HydrozoanTest.Optimal.SlotAgreement

/-!
# Witness: Optimal indirect liveness, applied

`Optimal.IndirectLiveness.holds` on the safety witnesses — no new tables:

* `AnchoredTotality` on `OD` from the anchor 22 at slot 6: slot 2 gets a
  verdict (the firing rung is the evidence rung — no certificate exists;
  this is the one application whose anchor premises are load-bearing, the
  slot having no direct route), slot 1 gets one (rung 3, both rungs
  refuted for its candidate 29; slot 1 is also directly skipped), and
  slot 3 gets one (rung 1; slot 3 is also directly committed). Also from
  the anchor 13 at slot 3, committed through the **slow** path, for
  slot 0; and from a strict sub-view withholding the abstaining vote 28.
* `DecidedBelowRun` on `OE`, whose slots 1–4 all fast-commit: the runs
  `1, 2, 3` (`c = 3`) and `1, …, 4` (`c = 4`) decide slot 0. Disclosed:
  slot 0 of `OE` is also *directly* skipped in the full view (four blames
  — the equivocating leader's other copy blames — and three no-evidence
  blocks), so the descent's only below-run slot is not ladder-only; the
  ladder's own verdict for it, a skip anchored on 14, is derived and
  shown to agree with the direct one through slot agreement. A
  ladder-only below-run slot needs a table of its own (deferred).
* The runway bound is tight: `c = 2` does not span.
-/

namespace HydrozoanTest

open Hydrozoan Hydrozoan.Optimal

set_option maxRecDepth 16384

-- Totality on OD from the anchor 22 (slot 6): slots 2, 1 and 3 each get
-- a verdict; the eligible in-betweens 5 (for slot 2) and 4, 5 (for
-- slot 1) are supplied as their skips, and none exist for slot 3.
example : ∃ v, DecidedOpt OD VD 2 v :=
  (Optimal.IndirectLiveness.holds (Fin 4) (Fin 30) OD).1 VD 2 6 22 (by decide)
    (DecidedOpt.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 3 ∨ i = 4 ∨ i = 5 := by omega
      rcases hi with rfl | rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide)
      · exact skipD5)
example : ∃ v, DecidedOpt OD VD 1 v :=
  (Optimal.IndirectLiveness.holds (Fin 4) (Fin 30) OD).1 VD 1 6 22 (by decide)
    (DecidedOpt.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 2 ∨ i = 3 ∨ i = 4 ∨ i = 5 := by omega
      rcases hi with rfl | rfl | rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide)
      · exact skipD4
      · exact skipD5)
example : ∃ v, DecidedOpt OD VD 3 v :=
  (Optimal.IndirectLiveness.holds (Fin 4) (Fin 30) OD).1 VD 3 6 22 (by decide)
    (DecidedOpt.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 4 ∨ i = 5 := by omega
      rcases hi with rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide))

-- Totality for slot 0 from an anchor committed through the SLOW path:
-- slot 3's candidate 13 has three certifiers, and no slot between 0 and
-- 3 is eligible.
example : ∃ v, DecidedOpt OD VD 0 v :=
  (Optimal.IndirectLiveness.holds (Fin 4) (Fin 30) OD).1 VD 0 3 13 (by decide)
    (DecidedOpt.directSlow (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 1 ∨ i = 2 := by omega
      rcases hi with rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide))

/-- A strict sub-view of `UD` withholding the abstaining round-7 block 28
(a sink); the anchor's three votes remain. -/
def VDsub : View OD.toBlockUniverse where
  ids := Finset.univ.erase 28
  subset_ids := by decide
  complete := by decide

-- Totality in the sub-view: slot 2 from the anchor 22, the eligible
-- in-between 5 skipped in that view.
example : ∃ v, DecidedOpt OD VDsub 2 v :=
  (Optimal.IndirectLiveness.holds (Fin 4) (Fin 30) OD).1 VDsub 2 6 22 (by decide)
    (DecidedOpt.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 3 ∨ i = 4 ∨ i = 5 := by omega
      rcases hi with rfl | rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide)
      · exact DecidedOpt.directSkip (by decide))

-- The runway bound is tight: c = 2 does not span — a run starting at
-- b = 1 ends at slot 2, which slot 0 cannot anchor on.
example : ¬ Hydrozoan.IndirectLiveness.SpansEligible (Fin 4) 2 :=
  fun h => absurd (h 1 0 Nat.one_pos) (by decide)

/-- The pipelined `Fin 4` schedule spans eligibility at `c = 3`: a run's
last slot sits at round `b + 2`, three rounds past every slot below. -/
theorem spansEligible_fourOpt : Hydrozoan.IndirectLiveness.SpansEligible (Fin 4) 3 := by
  intro b i h
  change i + 2 < b + 3 - 1
  omega

/-- ... and at `c = 4`. -/
theorem spansEligible_fourOpt4 : Hydrozoan.IndirectLiveness.SpansEligible (Fin 4) 4 := by
  intro b i h
  change i + 2 < b + 4 - 1
  omega

-- OE: slots 1, 2, 3 (and 4) fast-commit — copies 4, 10, 14 at exactly
-- qFastOpt — while slot 0's candidate 3 has a single vote.
example :
    supporters UE 4 2 = {0, 1, 3} ∧ supporters UE 10 3 = {1, 2, 3} ∧
      supporters UE 14 4 = {1, 2, 3} ∧ supporters UE 3 1 = {0} := by
  decide

-- DecidedBelowRun on OE at b = 1 with c = 3 and with c = 4: the run
-- discharged by the fast commits; slot 0 is decided. (Residual, as in
-- Hydrozoan's witness: only pipelined runs, so `0 < c` mutated to
-- `3 ≤ c` would survive.)
example : ∀ i, i < 1 → ∃ v, DecidedOpt OE VE i v :=
  (Optimal.IndirectLiveness.holds (Fin 4) (Fin 22) OE).2 VE 1 3 (by omega)
    spansEligible_fourOpt
    (fun j h1 h2 => by
      have hj : j = 1 ∨ j = 2 ∨ j = 3 := by omega
      rcases hj with rfl | rfl | rfl
      · exact ⟨4, DecidedOpt.directFast (by decide) (by decide)⟩
      · exact ⟨10, DecidedOpt.directFast (by decide) (by decide)⟩
      · exact ⟨14, DecidedOpt.directFast (by decide) (by decide)⟩)
example : ∀ i, i < 1 → ∃ v, DecidedOpt OE VE i v :=
  (Optimal.IndirectLiveness.holds (Fin 4) (Fin 22) OE).2 VE 1 4 (by omega)
    spansEligible_fourOpt4
    (fun j h1 h2 => by
      have hj : j = 1 ∨ j = 2 ∨ j = 3 ∨ j = 4 := by omega
      rcases hj with rfl | rfl | rfl | rfl
      · exact ⟨4, DecidedOpt.directFast (by decide) (by decide)⟩
      · exact ⟨10, DecidedOpt.directFast (by decide) (by decide)⟩
      · exact ⟨14, DecidedOpt.directFast (by decide) (by decide)⟩
      · exact ⟨18, DecidedOpt.directFast (by decide) (by decide)⟩)

-- What the ladder finds for slot 0 at anchor 14: no certificate for 3
-- (one vote), and no evidence quorum either (only block 11 references
-- the vote 5) — so the ladder's verdict is a skip ...
example : ¬ CertifiedIn UE 14 3 0 := fun h =>
  absurd ((certifiedIn_iff_history (by decide)).mp h) (by decide)
example : ¬ EvidenceLinked UE 14 3 0 := fun h =>
  absurd ((evidenceLinked_iff_history (by decide)).mp h) (by decide)

/-- ... derived: slot 0 skipped through rung 3, anchored on slot 3. -/
theorem oe_slot0_ladder : DecidedOpt OE VE 0 none := by
  have hall : ∀ M : Fin 22, IsLeaderBlock UE 0 M → M = 3 := by decide
  refine DecidedOpt.indirectSkip (j := 3) (A := 14) (by omega) (by decide)
    (DecidedOpt.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 1 ∨ i = 2 := by omega
      rcases hi with rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide))
    (fun L hL hcert => by
      have := hall L hL
      subst this
      exact absurd ((certifiedIn_iff_history (by decide)).mp hcert) (by decide))
    (fun L hL hev => by
      have := hall L hL
      subst this
      exact absurd ((evidenceLinked_iff_history (by decide)).mp hev) (by decide))

-- Disclosed: slot 0 is also directly skipped in the full view (blames
-- {0, 1, 2, 3} — the equivocating leader's other copy 4 blames — and
-- no-evidence blocks 9, 10, 12), and the two verdicts agree.
example : blames UE 0 = {0, 1, 2, 3} ∧ DecidedOpt OE VE 0 none :=
  ⟨by decide, DecidedOpt.directSkip (by decide)⟩
example : ∀ v, DecidedOpt OE VE 0 v → v = none := fun v h =>
  (Optimal.SlotAgreement.holds (Fin 4) (Fin 22) OE VE VE 0 _ v oe_slot0_ladder h).symm

end HydrozoanTest
