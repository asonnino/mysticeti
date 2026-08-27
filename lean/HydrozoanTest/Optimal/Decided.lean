import Hydrozoan.Optimal.Model.Decided
import Hydrozoan.Optimal.Helpers.DirectRules
import Hydrozoan.Optimal.Helpers.IndirectRules
import Hydrozoan.Model.Liveness
import HydrozoanTest.Optimal.Universe

/-!
# Witness: the Optimal-Hydrozoan decision rules

Two universes on `fourReplicasOpt` (four replicas, `0` Byzantine) under the
pipelined schedule `fourSlotsOpt` (slot `k` at round `k`, leader
`(k + 3) % 4`). Thresholds: `q = qFastOpt = qCert = qSlow = 3`,
`tPlain = 1`, `tEquiv = 2`.

**Part 1** reads O3's `UX` (`HydrozoanTest/Optimal/Universe.lean`), the
universe with an equivocation, for the evidence predicate itself:

* block 13 witnesses slot 1 and references two votes for copy 4 and one
  for copy 5 — evidence for 4 (`2 ≥ tEquiv`, rival `1 < tEquiv`), not for 5
  (`1 = tEquiv − 1`): exclusivity and the boundary in one block;
* block 14 does not witness — evidence for 4 by the `tPlain` case, not for
  5 (no vote), and, as the paper's procedure, also "evidence" for the
  non-candidate 6 (consumers guard with `IsLeaderBlock`);
* slot 0 (leader `3`, candidate the genesis block 3) is **directly
  skipped with a candidate present**: every voting-round author blames it
  (block 4 does; the equivocator's other copy 5 votes for it), and three
  of the four decision-round blocks reference no vote for it — exactly
  `qCert` no-evidence authors — while block 11 is evidence and does not
  count. A sub-view withholding block 12 keeps the blames but drops the
  no-evidence count to two, so the in-view skip fails. Slot 1 is not
  skipped, for both reasons at once: no voting block blames it, and
  blocks 13 and 14 are evidence for copy 4. Its candidate 3 has no
  evidence quorum in reach of block 14 either (`¬ EvidenceLinked`).

**Part 2** is a fresh eight-round universe `UD` (ids `Fin 30`, no
equivocation) exercising every constructor of `DecidedOpt`, anchored on
slot 6 (id 22), fast-committed at exactly `qFastOpt` votes — one fewer
than Hydrozoan's `qFast`, pinned side by side:

* slot 0 (candidate 3): `directFast` and `directSlow`;
* slot 1 (candidate 29, which no block ever references): `directSkip`,
  and `indirectSkip` anchored on slot 6 with both rungs **refuted for the
  candidate** — the eligible in-between slots 4 and 5 disposed of by
  their skips;
* slots 4, 5: no candidate, `directSkip` (the vacuous no-evidence case);
* slot 2 (candidate 8, FinWhale's Figure 4): a single correct vote (id 14)
  that every round-4 block references — no certificate exists anywhere,
  the direct skip's blame half holds but its no-evidence half fails, and
  `indirectEvidence` is the **only** route;
* slot 3 (candidate 13): `indirectCert` through certificate 18, a parent
  of the anchor — at `n = 4`, where `qCert = qFastOpt`, `directFast` and
  `directSlow` are derivable too (the relation is order-free);
* slot 6: `directFast`; a view withholding one vote has `2 < 3`.

Every premise is stated against the base universe `UD`; the `OptUniverse`
`OD` is `UD` with the (vacuous, equivocation-free) exclusion clause, and
definitional unfolding carries the facts across.

Not exercised here, deliberately: a no-evidence quorum with fewer than
`qCert` blames, a rival at exactly `tEquiv`, and a witnessing block that
is evidence for nothing all need `q ≥ 4` parents (at `n = 4`, `q = n − 1`),
and separating `q`, `qFastOpt`, `qCert`, `qSlow` (all `3` here) needs a
larger committee — witness material for the safety phases.
-/

namespace HydrozoanTest

open Hydrozoan Hydrozoan.Optimal

set_option maxRecDepth 16384

-- Part 1: the evidence predicate on the equivocation universe UX.

-- Block 13 witnesses slot 1's equivocation: two votes for 4, one for 5.
example : votesFor UX 13 4 = {1, 3} ∧ votesFor UX 13 5 = {2} := by decide
example :
    IsFastEvidence UX 1 13 4 ∧ ¬ IsFastEvidence UX 1 13 5 ∧ ¬ IsNoFastEvidence UX 1 13 := by
  decide

-- Block 14 does not witness: three votes for 4, none for 5 — and it is
-- "evidence" for the non-candidate 6 too (its parents all reference 6).
example : votesFor UX 14 4 = {0, 1, 3} ∧ votesFor UX 14 5 = ∅ := by decide
example : IsFastEvidence UX 1 14 4 ∧ ¬ IsFastEvidence UX 1 14 5 := by decide
example : IsFastEvidence UX 1 14 6 ∧ ¬ IsLeaderBlock UX 1 6 := by decide

-- Slot 0 of UX: candidate 3 (genesis, leader 3), blamed by every
-- voting-round author, with exactly qCert no-evidence decision blocks.
example : IsLeaderBlock UX 0 3 ∧ blames UX 0 = {0, 1, 2, 3} := by decide
example :
    IsNoFastEvidence UX 0 9 ∧ IsNoFastEvidence UX 0 10 ∧ IsNoFastEvidence UX 0 12 ∧
      ¬ IsNoFastEvidence UX 0 11 := by
  decide
example :
    authorsOf UX.block ((blocksAt UX 2).filter fun b => IsNoFastEvidence UX 0 b) = {0, 1, 3} := by
  decide
example : SkippedLeaderOpt UX 0 ∧ ¬ SkippedLeaderOpt UX 1 := by decide
-- ... for two reasons: no blames, and no no-evidence quorum either.
example : blames UX 1 = ∅ ∧ ¬ NoEvidenceQuorum UX 1 := by decide

/-- The full view of `UX`, typed at the `OptUniverse` projection so the
rule instances match. -/
def VX : View OX.toBlockUniverse := View.full UX

example : DecidedOpt OX VX 0 none := DecidedOpt.directSkip (by decide)

-- Candidate 3's evidence in reach of block 14: author 2 only (block 11
-- is not in 14's history), far below qCert.
example : ¬ EvidenceLinked UX 14 3 0 := fun h =>
  absurd ((evidenceLinked_iff_history (by decide)).mp h) (by decide)

/-- A sub-view withholding decision block 12 and its referrers 13, 14
(ref-closure). -/
def VXs : View OX.toBlockUniverse where
  ids := (((Finset.univ.erase 15).erase 12).erase 13).erase 14
  subset_ids := by decide
  complete := by decide

-- In it the blames are intact but the no-evidence authors are only
-- {0, 1}: the in-view skip fails on its second half alone.
example :
    qCert (Fin 4) ≤ (blamesInView OX.toBlockUniverse VXs 0).card ∧
      ¬ NoEvidenceQuorumInView OX.toBlockUniverse VXs 0 ∧
      ¬ SkippedLeaderOptInView OX.toBlockUniverse VXs 0 := by
  decide

-- Part 2: the six-route universe.

/-- Thirty blocks over eight rounds; see the module docstring.
Ids 0–3: genesis. Round 1, ids 4–6 by `1`, `2`, `3`: all reference the
genesis block 3; id 29 by `0` references 0, 1, 2 instead — slot 1's
candidate, which nothing references. Round 2, ids 7–10 by `0`–`3`: all
reference 4, 5, 6 — certificates for 3; id 8 is slot 2's candidate. Round 3: ids 11–13 by
`0`, `1`, `2` reference 7, 9, 10 (no vote for 8); id 14 by `3` references
8, 9, 10 — the one vote. Round 4 (replica `3` absent): id 15 by `0`
references 12, 13, 14; ids 16, 17 by `1`, `2` reference 11, 13, 14 — all
vote for 13 and reference the vote 14. Round 5 (replica `0` absent): ids
18–20 by `1`, `2`, `3` reference 15, 16, 17 — certificates for 13. Round 6:
ids 21–24 by `0`–`3` reference 18, 19, 20; id 22 is the anchor. Round 7:
ids 25–27 by `1`, `2`, `3` reference 22, 23, 24; id 28 by `0` references
21, 23, 24 and abstains. -/
def lkD : Fin 30 → Block (Fin 4) (Fin 30) := fun i =>
  if h : (i : ℕ) < 4 then
    { round := 0, author := ⟨i, by omega⟩, parents := ∅ }
  else if h : (i : ℕ) < 7 then
    { round := 1, author := ⟨(i : ℕ) - 3, by omega⟩, parents := {1, 2, 3} }
  else if h : (i : ℕ) < 11 then
    { round := 2, author := ⟨(i : ℕ) - 7, by omega⟩, parents := {4, 5, 6} }
  else if h : (i : ℕ) < 14 then
    { round := 3, author := ⟨(i : ℕ) - 11, by omega⟩, parents := {7, 9, 10} }
  else if (i : ℕ) = 14 then
    { round := 3, author := 3, parents := {8, 9, 10} }
  else if (i : ℕ) = 15 then
    { round := 4, author := 0, parents := {12, 13, 14} }
  else if h : (i : ℕ) < 18 then
    { round := 4, author := ⟨(i : ℕ) - 15, by omega⟩, parents := {11, 13, 14} }
  else if h : (i : ℕ) < 21 then
    { round := 5, author := ⟨(i : ℕ) - 17, by omega⟩, parents := {15, 16, 17} }
  else if h : (i : ℕ) < 25 then
    { round := 6, author := ⟨(i : ℕ) - 21, by omega⟩, parents := {18, 19, 20} }
  else if h : (i : ℕ) < 28 then
    { round := 7, author := ⟨(i : ℕ) - 24, by omega⟩, parents := {22, 23, 24} }
  else if (i : ℕ) = 28 then
    { round := 7, author := 0, parents := {21, 23, 24} }
  else
    { round := 1, author := 0, parents := {0, 1, 2} }

/-- The base universe: every id. -/
def UD : BlockUniverse (Fin 4) (Fin 30) where
  ids := Finset.univ
  block := lkD
  complete := by decide
  valid := by decide
  no_equivocation := by decide

/-- ... as an `OptUniverse`: no author has two blocks in one round, so
nothing witnesses an equivocation and the exclusion clause is vacuous. -/
def OD : OptUniverse (Fin 4) (Fin 30) :=
  { UD with leader_excluded := leaderExcluded_of_noEquivocation UD (by decide) }

/-- The full view, typed at the `OptUniverse` projection. -/
def VD : View OD.toBlockUniverse := View.full UD

-- Slot 0: candidate 3, three votes (exactly qFastOpt), four certifiers.
example :
    IsLeaderBlock UD 0 3 ∧ supporters UD 3 1 = {1, 2, 3} ∧
      certifiers UD 3 0 = {0, 1, 2, 3} := by
  decide
example : DecidedOpt OD VD 0 (some 3) :=
  DecidedOpt.directFast (by decide) (by decide)
example : DecidedOpt OD VD 0 (some 3) :=
  DecidedOpt.directSlow (by decide) (by decide)

-- The arc's headline, on data: three votes fast-commit here, and would
-- not in Hydrozoan (qFast = 4).
example : FastCommitOpt UD 3 0 ∧ ¬ FastCommit UD 3 0 := by decide

-- Slot 6, the anchor: candidate 22, exactly three votes, one abstention.
example : IsLeaderBlock UD 6 22 ∧ supporters UD 22 7 = {1, 2, 3} := by decide
example : DecidedOpt OD VD 6 (some 22) :=
  DecidedOpt.directFast (by decide) (by decide)

/-- A view withholding the round-7 vote 27 (a sink, so ref-closure is
immediate). -/
def VDm : View UD where
  ids := Finset.univ.erase 27
  subset_ids := by decide
  complete := by decide

-- Two votes are one short.
example : supportersInView UD VDm 22 7 = {1, 2} ∧ ¬ FastCommitOptInView UD VDm 22 6 := by
  decide

-- Slot 1's candidate 29 is referenced by nothing: no vote, no
-- certificate, and every decision-round block is no-evidence for it —
-- non-vacuously. Slots 4 and 5 have no candidate: their decision-round
-- blocks are no-evidence vacuously.
example :
    IsLeaderBlock UD 1 29 ∧ supporters UD 29 2 = ∅ ∧ certificates UD 29 1 = ∅ ∧
      (∀ L, ¬ IsLeaderBlock UD 4 L) ∧ (∀ L, ¬ IsLeaderBlock UD 5 L) := by
  decide
example :
    blames UD 1 = {0, 1, 2, 3} ∧ blames UD 4 = {1, 2, 3} ∧
      blames UD 5 = {0, 1, 2, 3} := by
  decide
example : IsNoFastEvidence UD 1 11 ∧ IsNoFastEvidence UD 4 21 ∧ IsNoFastEvidence UD 5 25 := by
  decide

theorem skipD1 : DecidedOpt OD VD 1 none := DecidedOpt.directSkip (by decide)
theorem skipD4 : DecidedOpt OD VD 4 none := DecidedOpt.directSkip (by decide)
theorem skipD5 : DecidedOpt OD VD 5 none := DecidedOpt.directSkip (by decide)

-- Anchor eligibility: slot 6 anchors slots 1, 2, 3; slots 4 and 5 sit
-- inside slot 3's decision window and cannot.
example :
    EligibleAsAnchor (Fin 4) 1 6 ∧ EligibleAsAnchor (Fin 4) 2 6 ∧
      EligibleAsAnchor (Fin 4) 3 6 ∧ ¬ EligibleAsAnchor (Fin 4) 3 5 ∧
      EligibleAsAnchor (Fin 4) 1 4 := by
  decide

-- Slot 2: candidate 8 with one vote (from 3) and three blames; no
-- certificate for 8 exists anywhere; every round-4 block is evidence.
example :
    IsLeaderBlock UD 2 8 ∧ supporters UD 8 3 = {3} ∧ blames UD 2 = {0, 1, 2} ∧
      certificates UD 8 2 = ∅ := by
  decide
example : IsFastEvidence UD 2 15 8 ∧ IsFastEvidence UD 2 16 8 ∧ IsFastEvidence UD 2 17 8 := by
  decide

-- The direct skip's blame half holds, its no-evidence half fails: no
-- direct route decides slot 2 in the full view.
example : qCert (Fin 4) ≤ (blames UD 2).card ∧ ¬ NoEvidenceQuorum UD 2 := by decide
example :
    ¬ FastCommitOptInView OD.toBlockUniverse VD 8 2 ∧
      ¬ SlowCommitInView OD.toBlockUniverse VD 8 2 ∧
      ¬ SkippedLeaderOptInView OD.toBlockUniverse VD 2 := by
  decide

-- The rungs at the anchor: no certificate in reach, but a quorum of
-- evidence blocks — 15, 16, 17, two references below 22.
example : ¬ CertifiedIn UD 22 8 2 := fun h =>
  absurd ((certifiedIn_iff_history (by decide)).mp h) (by decide)
example : EvidenceLinked UD 22 8 2 :=
  (evidenceLinked_iff_history (by decide)).mpr (by decide)

-- indirectEvidence end to end: slot 2 commits 8 anchored on slot 6; the
-- eligible in-between slot 5 is disposed of by its skip.
example : DecidedOpt OD VD 2 (some 8) := by
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

-- Slot 3: candidate 13 with three votes and three certifiers; certificate
-- 18 is a parent of the anchor.
example :
    IsLeaderBlock UD 3 13 ∧ supporters UD 13 4 = {0, 1, 2} ∧
      certifiers UD 13 3 = {1, 2, 3} := by
  decide
example : CertifiedIn UD 22 13 3 := ⟨18, by decide, Reaches.single (by decide)⟩

-- indirectCert end to end; no slot between 3 and 6 is eligible.
example : DecidedOpt OD VD 3 (some 13) :=
  DecidedOpt.indirectCert (j := 6) (A := 22) (by omega) (by decide)
    (DecidedOpt.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 4 ∨ i = 5 := by omega
      rcases hi with rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide))
    (by decide)
    ⟨18, by decide, Reaches.single (by decide)⟩

-- At n = 4 the direct routes reach the same verdict (qCert = qFastOpt).
example : DecidedOpt OD VD 3 (some 13) :=
  DecidedOpt.directFast (by decide) (by decide)
example : DecidedOpt OD VD 3 (some 13) :=
  DecidedOpt.directSlow (by decide) (by decide)

-- Both rungs are empty for candidate 29 at the anchor.
example : ¬ CertifiedIn UD 22 29 1 := fun h =>
  absurd ((certifiedIn_iff_history (by decide)).mp h) (by decide)
example : ¬ EvidenceLinked UD 22 29 1 := fun h =>
  absurd ((evidenceLinked_iff_history (by decide)).mp h) (by decide)

-- indirectSkip end to end: slot 1 anchored on slot 6 with the eligible
-- slots 4 and 5 skipped, and both rungs refuted for its candidate.
example : DecidedOpt OD VD 1 none := by
  have hall : ∀ M : Fin 30, IsLeaderBlock UD 1 M → M = 29 := by decide
  refine DecidedOpt.indirectSkip (j := 6) (A := 22) (by omega) (by decide)
    (DecidedOpt.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 2 ∨ i = 3 ∨ i = 4 ∨ i = 5 := by omega
      rcases hi with rfl | rfl | rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide)
      · exact skipD4
      · exact skipD5)
    (fun L hL hcert => by
      have := hall L hL
      subst this
      exact absurd ((certifiedIn_iff_history (by decide)).mp hcert) (by decide))
    (fun L hL hev => by
      have := hall L hL
      subst this
      exact absurd ((evidenceLinked_iff_history (by decide)).mp hev) (by decide))

end HydrozoanTest
