import Hydrozoan.Optimal.SlotAgreement.Proof
import HydrozoanTest.Optimal.Decided

/-!
# Witness: Optimal slot agreement, applied

`Optimal.SlotAgreement.holds` applied to the O4 universes, so that a
silently strengthened hypothesis fails the build, and so that its
consequences are seen on data:

* on `OD`, the evidence-rung commit of slot 2 (the route with no
  certificate anywhere) is the slot's **only** verdict: no skip, no other
  block — across every view; slot 0's fast commit rules out a skip in any
  view; and the anchor's fast commit in the full view fixes the verdict of
  the one-vote-short view, which cannot fast-commit it;
* on `OX`, the fast commit of copy 4 at the equivocating slot 1 rules out
  its rival copy 5 as a verdict in any view (the universe holds no anchor,
  so the data alone already forbids 5 — the application checks the
  theorem's shape), and the direct skip of slot 0 in the full view is the
  only verdict the sub-view `VXs` could ever agree with — `VXs` itself is
  undecided on slot 0;
* on `OE`, **the arc's headline**: the same equivocating leader, but now
  every decision-round block of its slot *witnesses* the equivocation —
  two votes for copy 4, one for copy 5 — so each is fast evidence for 4
  through the `tEquiv` case, none is a certificate, and each omits the
  leader's own block as `leader_excluded` demands. An anchor two rounds
  above reaches the three of them, slot 1 commits copy 4 through the
  evidence rung with rung 1 refuted for *both* copies, and the theorem
  forbids copy 5 as a verdict in every view. (The fast commit of 4 is
  derivable too, at exactly `qFastOpt`; the evidence-rung derivation is
  the one that runs through the witnessing blocks.)

Deferred, as in the earlier witnesses: an `f = 0` universe for the
non-equivocation branch of fast/fast agreement, and a configuration
separating the four quorums that coincide at `n = 4`.
-/

namespace HydrozoanTest

open Hydrozoan Hydrozoan.Optimal

set_option maxRecDepth 16384

/-- Slot 2 of `OD` commits 8 through the evidence rung (the derivation of
`HydrozoanTest/Optimal/Decided.lean`, named so it can be fed to the
theorem). -/
theorem od_slot2_evidence : DecidedOpt OD VD 2 (some 8) := by
  have hall : ∀ M : Fin 30, IsLeaderBlock UD 2 M → M = 8 := by decide
  refine DecidedOpt.indirectEvidence (j := 6) (A := 22) (by omega) (by decide)
    (DecidedOpt.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 3 ∨ i = 4 ∨ i = 5 := by omega
      rcases hi with rfl | rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide)
      · exact skipD5)
    (fun L' hL' hcert => by
      have := hall L' hL'
      subst this
      exact absurd ((certifiedIn_iff_history (by decide)).mp hcert) (by decide))
    (by decide)
    ((evidenceLinked_iff_history (by decide)).mpr (by decide))

-- The evidence-rung verdict is the only one, in every view.
example : ∀ (V : View OD.toBlockUniverse) v, DecidedOpt OD V 2 v → v = some 8 :=
  fun V v h => (Optimal.SlotAgreement.holds (Fin 4) (Fin 30) OD VD V 2 _ v od_slot2_evidence h).symm
example : ¬ DecidedOpt OD VD 2 none := fun h =>
  Option.some_ne_none 8 (Optimal.SlotAgreement.holds (Fin 4) (Fin 30) OD VD VD 2 _ none
    od_slot2_evidence h)
example : ∀ L, DecidedOpt OD VD 2 (some L) → L = 8 := fun L h =>
  Option.some.inj (Optimal.SlotAgreement.holds (Fin 4) (Fin 30) OD VD VD 2 _ (some L)
    od_slot2_evidence h).symm

-- Slot 0 fast-commits 3, so no view ever skips it (fast vs. skip, both
-- direct and indirect).
example : ∀ V : View OD.toBlockUniverse, ¬ DecidedOpt OD V 0 none := fun V h =>
  Option.some_ne_none 3 (Optimal.SlotAgreement.holds (Fin 4) (Fin 30) OD VD V 0 _ none
    (DecidedOpt.directFast (by decide) (by decide)) h)

/-- The one-vote-short view of `UD`, typed at the projection. -/
def VDs' : View OD.toBlockUniverse := VDm

-- The anchor 22 fast-commits in the full view; the one-vote-short view
-- cannot fast-commit it (two of three votes), yet whatever verdict it
-- reaches on slot 6 — by whatever route — is that commit.
example : ¬ FastCommitOptInView OD.toBlockUniverse VDs' 22 (Slots.slotRound (Fin 4) 6) := by
  decide
example : ∀ v, DecidedOpt OD VDs' 6 v → v = some 22 := fun v h =>
  (Optimal.SlotAgreement.holds (Fin 4) (Fin 30) OD VD VDs' 6 _ v
    (DecidedOpt.directFast (by decide) (by decide)) h).symm

-- Slot 1 of OX: copy 4 fast-commits, so its rival copy 5 is never a
-- verdict, in any view. (OX holds no anchor, so the data alone already
-- forbids 5; the headline version, with an anchor, is OE below.)
theorem ox_slot1_fast : DecidedOpt OX VX 1 (some 4) :=
  DecidedOpt.directFast (by decide) (by decide)
example : ∀ V : View OX.toBlockUniverse, ¬ DecidedOpt OX V 1 (some 5) := fun V h =>
  absurd (Option.some.inj (Optimal.SlotAgreement.holds (Fin 4) (Fin 16) OX VX V 1 _ _
    ox_slot1_fast h)) (by decide)

-- Slot 0 of OX is skipped in the full view; the sub-view VXs cannot skip
-- it directly (and, holding no anchor, reaches no verdict on it at all),
-- so the only verdict it could ever agree with is that skip.
example : ¬ SkippedLeaderOptInView OX.toBlockUniverse VXs 0 := by decide
example : ∀ v, DecidedOpt OX VXs 0 v → v = none := fun v h =>
  (Optimal.SlotAgreement.holds (Fin 4) (Fin 16) OX VX VXs 0 _ v
    (DecidedOpt.directSkip (by decide)) h).symm

-- The headline universe: O3's equivocation, with every decision-round
-- block of slot 1 witnessing it, and an anchor above.

/-- Twenty-two blocks over six rounds. Ids 0–3: genesis. Round 1: copies 4
and 5 by the Byzantine leader `0`, ids 6–8 by `1`, `2`, `3`. Round 2 (slot
1's voting round): 9 by `0` and 10 by `1` reference copy 4 (with 6, 7); 11
by `2` references copy 5 (with 6, 7); 12 by `3` references copy 4 (with 6,
8). Round 3 (slot 1's decision round): 13, 14, 15 by `1`, `2`, `3` each
reference `{10, 11, 12}` — two votes for 4, one for 5 — and omit the
leader's block 9. Round 4: 16, 17, 18 by `1`, `2`, `3` reference
`{13, 14, 15}`; 18 is slot 4's candidate (leader `3`). Round 5: 19, 20, 21
by `1`, `2`, `3` reference `{16, 17, 18}` — three votes for the anchor. -/
def lkE : Fin 22 → Block (Fin 4) (Fin 22) := fun i =>
  if h : (i : ℕ) < 4 then
    { round := 0, author := ⟨i, by omega⟩, parents := ∅ }
  else if (i : ℕ) = 4 then
    { round := 1, author := 0, parents := {0, 1, 2} }
  else if (i : ℕ) = 5 then
    { round := 1, author := 0, parents := {0, 1, 3} }
  else if h : (i : ℕ) < 9 then
    { round := 1, author := ⟨(i : ℕ) - 5, by omega⟩, parents := {0, 1, 2} }
  else if (i : ℕ) = 9 then
    { round := 2, author := 0, parents := {4, 6, 7} }
  else if (i : ℕ) = 10 then
    { round := 2, author := 1, parents := {4, 6, 7} }
  else if (i : ℕ) = 11 then
    { round := 2, author := 2, parents := {5, 6, 7} }
  else if (i : ℕ) = 12 then
    { round := 2, author := 3, parents := {4, 6, 8} }
  else if h : (i : ℕ) < 16 then
    { round := 3, author := ⟨(i : ℕ) - 12, by omega⟩, parents := {10, 11, 12} }
  else if h : (i : ℕ) < 19 then
    { round := 4, author := ⟨(i : ℕ) - 15, by omega⟩, parents := {13, 14, 15} }
  else
    { round := 5, author := ⟨(i : ℕ) - 18, by omega⟩, parents := {16, 17, 18} }

/-- The base universe: every id. -/
def UE : BlockUniverse (Fin 4) (Fin 22) where
  ids := Finset.univ
  block := lkE
  complete := by decide
  valid := by decide
  no_equivocation := by decide

/-- ... as an `OptUniverse`: the witnessing blocks 13, 14, 15 omit the
leader's block 9. Discharged through the bounded bridge (rounds stop at
5). -/
def OE : OptUniverse (Fin 4) (Fin 22) :=
  { UE with
    leader_excluded :=
      leaderExcluded_of_bounded UE 5 5 (fun k hk => by change k + 2 ≤ 5 at hk; omega)
        (by decide) (by decide) }

/-- The full view, typed at the projection. -/
def VE : View OE.toBlockUniverse := View.full UE

-- The seam on data: each decision-round block of slot 1 witnesses the
-- equivocation, is evidence for copy 4 by the tEquiv case (two votes,
-- rival at one), is not evidence for copy 5, is not a certificate, and
-- omits the leader's block; no certificate for either copy exists.
example :
    WitnessesEquivocation UE 1 13 ∧ IsFastEvidence UE 1 13 4 ∧ ¬ IsFastEvidence UE 1 13 5 ∧
      votesFor UE 13 4 = {1, 3} ∧ votesFor UE 13 5 = {2} ∧ ¬ IsCertificate UE 13 4 ∧
      (∀ j ∈ (UE.block 13).parents, (UE.block j).author ≠ 0) ∧
      certificates UE 4 1 = ∅ ∧ certificates UE 5 1 = ∅ := by
  decide

-- Slot 4's candidate 18 is the anchor, fast-committed by three votes; it
-- reaches the three witnessing evidence blocks.
example : IsLeaderBlock UE 4 18 ∧ supporters UE 18 5 = {1, 2, 3} := by decide
example : EvidenceLinked UE 18 4 1 :=
  (evidenceLinked_iff_history (by decide)).mpr (by decide)

/-- Slot 1 commits copy 4 through the evidence rung, anchored on slot 4,
with rung 1 refuted for both copies. -/
theorem oe_slot1_evidence : DecidedOpt OE VE 1 (some 4) := by
  have hall : ∀ M : Fin 22, IsLeaderBlock UE 1 M → M = 4 ∨ M = 5 := by decide
  refine DecidedOpt.indirectEvidence (j := 4) (A := 18) (by omega) (by decide)
    (DecidedOpt.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 2 ∨ i = 3 := by omega
      rcases hi with rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide))
    (fun L' hL' hcert => by
      rcases hall L' hL' with rfl | rfl
      · exact absurd ((certifiedIn_iff_history (by decide)).mp hcert) (by decide)
      · exact absurd ((certifiedIn_iff_history (by decide)).mp hcert) (by decide))
    (by decide)
    ((evidenceLinked_iff_history (by decide)).mpr (by decide))

-- Hence copy 5 is never a verdict, and copy 4 the only one, in every view.
example : ∀ V : View OE.toBlockUniverse, ¬ DecidedOpt OE V 1 (some 5) := fun V h =>
  absurd (Option.some.inj (Optimal.SlotAgreement.holds (Fin 4) (Fin 22) OE VE V 1 _ _
    oe_slot1_evidence h)) (by decide)
example : ∀ (V : View OE.toBlockUniverse) v, DecidedOpt OE V 1 v → v = some 4 :=
  fun V v h =>
    (Optimal.SlotAgreement.holds (Fin 4) (Fin 22) OE VE V 1 _ v oe_slot1_evidence h).symm

end HydrozoanTest
