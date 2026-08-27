import Hydrozoan.Optimal.DirectSafety.Proof
import Hydrozoan.Optimal.Helpers.Decided
import HydrozoanTest.Optimal.Decided

/-!
# Witness: Optimal direct-rule safety, applied

`Optimal.DirectSafety.holds` applied to the O4 universes, so that a
silently strengthened hypothesis in the statement fails the build, and
so that its consequences are seen on data:

* on `OX`, fast/fast agreement does real work: slot 1 has **two**
  candidates, copies 4 and 5 of the equivocating Byzantine leader, and
  copy 4 fast-commits at exactly `qFastOpt`; the theorem then says every
  fast-committing candidate of the slot *is* 4 — a conclusion the
  candidate premise alone does not force;
* on `OD`, fast/fast pins the anchor, fast/slow pins slot 3, and the two
  inherited rows — certificate uniqueness and slow/slow, the latter
  across two distinct views — pin slot 0;
* on `OX`, commit/skip exclusion turns the direct skip of slot 0 into a
  refutation of both direct commits of its candidate 3, in any view — and,
  with the universe too short to hold any anchor, into
  `¬ DecidedOpt OX VX 0 (some 3)` outright: a skipped slot commits by no
  route.

Deferred (needs an equivocation in `UD`, or `n ≥ 5`): a Byzantine replica
voting and blaming at once (three votes, two blames), a skip with exactly
`qCert < qFast` blames, and an `f = 0` universe for fast/fast's
non-equivocation branch.
-/

namespace HydrozoanTest

open Hydrozoan Hydrozoan.Optimal

set_option maxRecDepth 16384

-- Fast/fast with a rival: slot 1 of OX has candidates 4 and 5 (the
-- Byzantine leader's two copies); 4 gathers exactly qFastOpt votes.
example :
    IsLeaderBlock UX 1 4 ∧ IsLeaderBlock UX 1 5 ∧ supporters UX 4 2 = {0, 1, 3} ∧
      supporters UX 5 2 = {2} := by
  decide
example : FastCommitOptInView OX.toBlockUniverse VX 4 (Slots.slotRound (Fin 4) 1) := by
  decide

-- So every fast-committing candidate of slot 1 is 4 — not 5, which the
-- premise `IsLeaderBlock` alone would allow.
example :
    ∀ L, IsLeaderBlock OX.toBlockUniverse 1 L →
      FastCommitOptInView OX.toBlockUniverse VX L (Slots.slotRound (Fin 4) 1) → L = 4 :=
  fun L hL h =>
    (Optimal.DirectSafety.holds (Fin 4) (Fin 16) OX).1 VX VX 1 L 4 hL (by decide) h (by decide)

-- Fast/fast agreement at slot 6 of OD: whatever fast-commits there is 22.
example :
    ∀ L, IsLeaderBlock OD.toBlockUniverse 6 L →
      FastCommitOptInView OD.toBlockUniverse VD L (Slots.slotRound (Fin 4) 6) → L = 22 :=
  fun L hL h =>
    (Optimal.DirectSafety.holds (Fin 4) (Fin 30) OD).1 VD VD 6 L 22 hL (by decide) h (by decide)

/-- The one-vote-short view of `UD`, typed at the projection. -/
def VDs : View OD.toBlockUniverse := VDm

-- The inherited rows on slot 0 of OD: certificate uniqueness (universe
-- level) and slow/slow agreement across two distinct views.
example :
    ∀ L, IsLeaderBlock OD.toBlockUniverse 0 L →
      (certificates OD.toBlockUniverse L (Slots.slotRound (Fin 4) 0)).Nonempty → L = 3 :=
  fun L hL h =>
    (Optimal.DirectSafety.holds (Fin 4) (Fin 30) OD).2.1 0 L 3 hL (by decide) h (by decide)
example :
    ∀ L, IsLeaderBlock OD.toBlockUniverse 0 L →
      SlowCommitInView OD.toBlockUniverse VD L (Slots.slotRound (Fin 4) 0) → L = 3 :=
  fun L hL h =>
    (Optimal.DirectSafety.holds (Fin 4) (Fin 30) OD).2.2.1 VD VDs 0 L 3 hL (by decide) h
      (by decide)

-- Fast/slow agreement at slot 3 of OD, where both routes fire on 13.
example :
    ∀ L, IsLeaderBlock OD.toBlockUniverse 3 L →
      SlowCommitInView OD.toBlockUniverse VD L (Slots.slotRound (Fin 4) 3) → L = 13 :=
  fun L hL h =>
    ((Optimal.DirectSafety.holds (Fin 4) (Fin 30) OD).2.2.2.1 VD VD 3 13 L (by decide) hL
      (by decide) h).symm

-- Commit/skip exclusion at slot 0 of OX: the direct skip holds in the
-- full view, so neither direct commit of candidate 3 can — in ANY view.
example :
    ∀ V : View OX.toBlockUniverse,
      ¬ FastCommitOptInView OX.toBlockUniverse V 3 (Slots.slotRound (Fin 4) 0) :=
  fun V h =>
    (Optimal.DirectSafety.holds (Fin 4) (Fin 16) OX).2.2.2.2 V VX 0 3 (by decide) (Or.inl h)
      (by decide)
example :
    ∀ V : View OX.toBlockUniverse,
      ¬ SlowCommitInView OX.toBlockUniverse V 3 (Slots.slotRound (Fin 4) 0) :=
  fun V h =>
    (Optimal.DirectSafety.holds (Fin 4) (Fin 16) OX).2.2.2.2 V VX 0 3 (by decide) (Or.inr h)
      (by decide)

-- No slot of OX at or above round 3 has a candidate — slot 3's leader is
-- replica 2, whose round-3 block is exactly the excluded id 15 — so
-- nothing can anchor an indirect decision of slot 0 ...
theorem ox_no_anchor {j : ℕ} {A : Fin 16} (hj : 2 < j)
    (hA : IsLeaderBlock OX.toBlockUniverse j A) : False := by
  have hr : ∀ A ∈ UX.ids, (UX.block A).round ≤ 3 := by decide
  have h1 := hr A hA.1
  have h2 : (UX.block A).round = j := hA.2.1
  have hj3 : j = 3 := by omega
  subst hj3
  exact absurd hA (by revert A; decide)

-- ... hence the skipped slot commits its candidate by no route at all.
example : ¬ DecidedOpt OX VX 0 (some 3) := fun h => by
  cases h with
  | directFast hL hf =>
    exact (Optimal.DirectSafety.holds (Fin 4) (Fin 16) OX).2.2.2.2 VX VX 0 3 hL (Or.inl hf)
      (by decide)
  | directSlow hL hs =>
    exact (Optimal.DirectSafety.holds (Fin 4) (Fin 16) OX).2.2.2.2 VX VX 0 3 hL (Or.inr hs)
      (by decide)
  | indirectCert hkj helig hanchor _ _ _ =>
    exact ox_no_anchor helig (isLeaderBlock_of_decidedOpt hanchor)
  | indirectEvidence hkj helig hanchor _ _ _ _ =>
    exact ox_no_anchor helig (isLeaderBlock_of_decidedOpt hanchor)

end HydrozoanTest
