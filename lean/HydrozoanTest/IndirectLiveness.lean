import Hydrozoan.IndirectLiveness.Proof
import Hydrozoan.SlotAgreement.Proof
import HydrozoanTest.SlotAgreement
import HydrozoanTest.DirectLiveness

/-!
# Witness: indirect liveness fires

Two end-to-end applications, one per claim:

* **Totality, on the frozen `U5`** (seven replicas, six rounds): slot 0
  anchored on the fast-committed slot 4 with the in-between slots
  handled exactly as the graded rule demands — 1 and 2 ineligible, 3
  direct-skipped. The theorem returns a verdict for slot 0; the frozen
  `SlotAgreement` witness pins which one (`some 2`, via rung 1).
* **Descent, on a fresh `U8`** — the first table with three consecutive
  committed slots. The frozen low-fault `Fin 4` configuration and its
  pipelined schedule (leader `k % 4`, replica 1 crashed): slots 2, 3, 4
  are led by correct replicas and fast-commit, and the run decides both
  slots below it. Slot 1 is the load-bearing one: its leader is the
  crashed replica, **no candidate block exists at all** — no commit rule
  can apply — and the run still forces a verdict, anchoring on its end
  (slots 2 and 3, though committed, are too close to be eligible).
  Disclosure: in this fully-referenced table the direct skip also
  reaches slot 1 (with no candidate, every voting-round block blames
  vacuously), so its `none` verdict has a direct derivation too; `U9`
  below is where a verdict is provably out of the direct rules' reach.

Plus a **positive `indirectWeak` witness** on a fresh `U9` (seven
replicas, seven rounds), the first where the weak rung is the only route
in *any* view (the frozen `DirectSafety` witness also derives
`indirectWeak`, but only in a withheld view — its full view
fast-commits): slot 0's candidate gathers exactly `q_weak = 3` votes —
too few for a fast commit (`q_fast = 6`), too few for any certificate
to exist (`q_cert = 5`), and leaving only 3 blames (< `q_fast`), so
**no direct rule can ever decide the slot, in any view**. Anchored on
the fast-committed slot 3 (the nearest possible anchor — no eligible
slots in between), the weak rung fires at the sole candidate.

Hardening sections at the end (from the phase's cold audits): the run
at slots 3–5 of the extended `U9` — slot 5 led by the well-behaved
Byzantine replica — decides slot 0 *below a run*, so the descent is
exercised where only the indirect ladder works; a nonzero sub-quorum
`WeakLinked` negative (two of three votes anchor-reachable); and the
`Fin 5` tie-break table `U11`, where two equivocating leader copies
both clear the weak rung and the deterministic tie-break — for the
first time doing real work — commits the least and provably not the
greatest.
-/

namespace HydrozoanTest

open Hydrozoan

set_option maxRecDepth 16384

-- ## Totality on U5 (slot 0 anchored on slot 4)

-- The anchor data, pinned: slot 4's candidate fast-commits in the full
-- view; slots 1 and 2 cannot anchor slot 0, slot 3 can and is skipped.
example : IsLeaderBlock U5 4 31 ∧ FastCommitInView U5 Vfull5 31 4 := by decide
example : ¬ EligibleAsAnchor (Fin 7) 0 2 ∧ EligibleAsAnchor (Fin 7) 0 3 := by
  decide

-- End-to-end: AnchoredTotality applied to U5 with every hypothesis
-- discharged concretely (k = 0, j = 4, A = 31) — the mechanical guard
-- against a silently strengthened Statement hypothesis.
example : ∃ v, Decided U5 Vfull5 0 v :=
  (IndirectLiveness.holds (Fin 7) (Fin 39) U5).1 Vfull5 0 4 31
    (by decide)
    (Decided.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 1 ∨ i = 2 ∨ i = 3 := by omega
      rcases hi with rfl | rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide)
      · exact Decided.directSkip (by decide))

-- ## Descent at low faults (Fin 4: f = 0, c = 1, k = 1; thresholds
-- pinned in `HydrozoanTest/DirectLiveness.lean`)

/-- Eighteen blocks: rounds 0–5 × the three correct replicas {0, 2, 3}
(round `r` holds ids `3r, 3r + 1, 3r + 2`), each non-genesis block
referencing all three blocks of the round below. -/
def lk8 : Fin 18 → Block (Fin 4) (Fin 18) := fun i =>
  { round := (i : ℕ) / 3,
    author := ⟨if (i : ℕ) % 3 = 0 then 0 else (i : ℕ) % 3 + 1,
      by split <;> omega⟩,
    parents :=
      if h : (i : ℕ) < 3 then ∅
      else {⟨(i : ℕ) / 3 * 3 - 3, by omega⟩, ⟨(i : ℕ) / 3 * 3 - 2, by omega⟩,
        ⟨(i : ℕ) / 3 * 3 - 1, by omega⟩} }

/-- The committed-run universe. -/
def U8 : BlockUniverse (Fin 4) (Fin 18) where
  ids := Finset.univ
  block := lk8
  complete := by decide
  valid := by decide
  no_equivocation := by decide

-- The run: slots 2, 3, 4 are led by the correct replicas 2, 3, 0, whose
-- blocks (ids 7, 11, 12) all fast-commit in the full view.
example : Slots.leader (Replica := Fin 4) 2 = 2 ∧
    Slots.leader (Replica := Fin 4) 3 = 3 ∧
    Slots.leader (Replica := Fin 4) 4 = 0 := by decide
example : IsLeaderBlock U8 2 7 ∧ IsLeaderBlock U8 3 11 ∧
    IsLeaderBlock U8 4 12 := by decide
example : FastCommitInView U8 (View.full U8) 7 2 ∧
    FastCommitInView U8 (View.full U8) 11 3 ∧
    FastCommitInView U8 (View.full U8) 12 4 := by decide

-- Below the run, slot 1 has no candidate block at all: its leader is
-- the crashed replica 1, so no commit rule can apply. (With no
-- candidate every round-2 block blames vacuously, so the direct skip
-- happens to fire here as well — U9 below is where the direct rules
-- are provably out of reach.)
example : Slots.leader (Replica := Fin 4) 1 = 1 := by decide
example : ∀ L : Fin 18, ¬ IsLeaderBlock U8 1 L := by decide

-- Slot 1's nearest eligible anchor is the run's END: slots 2 and 3,
-- though committed, sit inside its decision window.
example : ¬ EligibleAsAnchor (Fin 4) 1 2 ∧ ¬ EligibleAsAnchor (Fin 4) 1 3 ∧
    EligibleAsAnchor (Fin 4) 1 4 := by decide

-- The runway bound is tight: c = 2 does not span — a run starting at
-- b = 1 ends at slot 2, which slot 0 cannot anchor on.
example : ¬ IndirectLiveness.SpansEligible (Fin 4) 2 :=
  fun h => absurd (h 1 0 Nat.one_pos) (by decide)

/-- The pipelined `Fin 4` schedule spans eligibility at `c = 3`: a run's
last slot sits at round `b + 2`, three rounds past every slot below the
run. -/
theorem spansEligible_four : IndirectLiveness.SpansEligible (Fin 4) 3 := by
  intro b i h
  change 1 * (i / 1) + 2 < 1 * ((b + 3 - 1) / 1)
  omega

-- End-to-end: DecidedBelowRun applied to U8 at b = 2, c = 3, the run
-- discharged by the three fast commits — the mechanical guard against a
-- silently strengthened Statement hypothesis. (Known residual gap: the
-- instantiation sets c = 3, so a mutation of `0 < c` to `3 ≤ c` would
-- survive it; killing that needs a faster-than-pipelined schedule
-- witness with c < 3.)
example : ∀ i, i < 2 → ∃ v, Decided U8 (View.full U8) i v :=
  (IndirectLiveness.holds (Fin 4) (Fin 18) U8).2 (View.full U8) 2 3
    (by omega) spansEligible_four
    (fun j h1 h2 => by
      have hj : j = 2 ∨ j = 3 ∨ j = 4 := by omega
      rcases hj with rfl | rfl | rfl
      · exact ⟨7, Decided.directFast (by decide) (by decide)⟩
      · exact ⟨11, Decided.directFast (by decide) (by decide)⟩
      · exact ⟨12, Decided.directFast (by decide) (by decide)⟩)

-- The verdicts behind the descent's bare existence, pinned: slot 0
-- fast-commits, slot 1 skips (here even directly — all three round-2
-- blocks blame, meeting q_fast exactly).
example : Decided U8 (View.full U8) 0 (some 0) :=
  Decided.directFast (by decide) (by decide)
example : Decided U8 (View.full U8) 1 none :=
  Decided.directSkip (by decide)

-- ## The weak rung fires (U9, seven replicas)

/-- Forty-three blocks over rounds 0–6. Round 0: all seven genesis
blocks. From round 1 on, six blocks per round (crashed replica 1
silent). At round 1 exactly three blocks (authors 2, 3, 4 — ids 7, 8, 9)
vote for slot 0's candidate (genesis id 2); the other three (authors
0, 5, 6 — ids 10, 11, 12) reference five round-0 blocks avoiding it.
Rounds 2–6 each reference five blocks of the round below, carrying the
three votes into every later block's history. Rounds 4–6 fast-commit
the run at slots 3, 4, 5: every round's blocks reference the previous
slot's candidate — id 23 (leader 5), id 30 (leader 6), id 31 (leader 0,
the Byzantine replica behaving well for this slot). -/
def lk9 : Fin 43 → Block (Fin 7) (Fin 43) := fun i =>
  if h : (i : ℕ) < 7 then
    { round := 0, author := ⟨i, by omega⟩, parents := ∅ }
  else if h : (i : ℕ) < 10 then
    { round := 1, author := ⟨(i : ℕ) - 5, by omega⟩,
      parents := {0, 2, 3, 4, 5} }
  else if h : (i : ℕ) < 13 then
    { round := 1,
      author := ⟨if (i : ℕ) = 10 then 0 else (i : ℕ) - 6, by split <;> omega⟩,
      parents := {0, 1, 3, 4, 5} }
  else if h : (i : ℕ) < 19 then
    { round := 2,
      author := ⟨if (i : ℕ) = 13 then 0 else (i : ℕ) - 12, by split <;> omega⟩,
      parents := {7, 8, 9, 10, 11} }
  else if h : (i : ℕ) < 25 then
    { round := 3,
      author := ⟨if (i : ℕ) = 19 then 0 else (i : ℕ) - 18, by split <;> omega⟩,
      parents := {13, 14, 15, 16, 17} }
  else if h : (i : ℕ) < 31 then
    { round := 4,
      author := ⟨if (i : ℕ) = 25 then 0 else (i : ℕ) - 24, by split <;> omega⟩,
      parents := {20, 21, 22, 23, 24} }
  else if h : (i : ℕ) < 37 then
    { round := 5,
      author := ⟨if (i : ℕ) = 31 then 0 else (i : ℕ) - 30, by split <;> omega⟩,
      parents := {26, 27, 28, 29, 30} }
  else
    { round := 6,
      author := ⟨if (i : ℕ) = 37 then 0 else (i : ℕ) - 36, by split <;> omega⟩,
      parents := {31, 32, 33, 34, 35} }

/-- The weak-rung universe. -/
def U9 : BlockUniverse (Fin 7) (Fin 43) where
  ids := Finset.univ
  block := lk9
  complete := by decide
  valid := by decide
  no_equivocation := by decide

-- Slot 0's candidate draws exactly q_weak = 3 supporters — and with
-- only 3 votes in the whole universe, no certificate can ever form
-- (q_cert = 5), while the 3 non-voters fall short of a skip quorum
-- (q_fast = 6): no direct rule decides slot 0, in any view.
example : IsLeaderBlock U9 0 2 := by decide
example : supporters U9 2 1 = {2, 3, 4} := by decide
example : certificates U9 2 0 = ∅ := by decide
example : blames U9 0 = {0, 5, 6} := by decide
example : ¬ FastCommitInView U9 (View.full U9) 2 0 ∧
    ¬ SlowCommitInView U9 (View.full U9) 2 0 ∧
    ¬ SkippedLeaderInView U9 (View.full U9) 0 := by decide

-- The anchor: slot 3 is the nearest eligible slot (slots 1 and 2 are
-- too close), and its candidate id 23 fast-commits at round 4.
example : ¬ EligibleAsAnchor (Fin 7) 0 2 ∧ EligibleAsAnchor (Fin 7) 0 3 := by
  decide
example : IsLeaderBlock U9 3 23 ∧ FastCommitInView U9 (View.full U9) 23 3 := by
  decide

-- The three votes sit in the anchor's causal history: the weak rung's
-- footprint, at exactly q_weak.
example : qWeak (Fin 7) ≤ (authorsOf U9.block ((blocksAt U9 1).filter
    fun b => IsVote U9 b 2 ∧ b ∈ history U9 23)).card := by decide

-- The positive `indirectWeak` derivation — the first on a universe
-- where the direct rules fail in EVERY view (the frozen DirectSafety
-- witness's failure is view-relative): every premise discharged
-- concretely, with the in-between premise vacuous (no eligible slot
-- between 0 and 3) and the tie-break settled by candidate uniqueness.
example : Decided U9 (View.full U9) 0 (some 2) := by
  have hall : ∀ L' : Fin 43, IsLeaderBlock U9 0 L' → L' = 2 := by decide
  refine Decided.indirectWeak (j := 3) (A := 23) (by omega) (by decide)
    (Decided.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 1 ∨ i = 2 := by omega
      rcases hi with rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide))
    (fun L' hL' hcert => by
      have := hall L' hL'
      subst this
      exact absurd ((certifiedIn_iff_history (by decide)).mp hcert) (by decide))
    (by decide)
    ((weakLinked_iff_history (by decide)).mpr (by decide))
    (fun L' hL' _ => by
      have := hall L' hL'
      subst this
      exact lt_irrefl _)

-- ## Descent over a run whose below-slot is indirect-only (U9 extended)

-- The run at slots 3, 4, 5: candidates 23 (leader 5), 30 (leader 6),
-- and 31 — slot 5 is led by the BYZANTINE replica 0, whose candidate
-- commits anyway: commitment needs votes, not a correct leader.
example : IsLeaderBlock U9 4 30 ∧ IsLeaderBlock U9 5 31 := by decide
example : FastCommitInView U9 (View.full U9) 30 4 ∧
    FastCommitInView U9 (View.full U9) 31 5 := by decide

/-- The pipelined `Fin 7` schedule spans eligibility at `c = 3`. -/
theorem spansEligible_seven : IndirectLiveness.SpansEligible (Fin 7) 3 := by
  intro b i h
  change 1 * (i / 1) + 2 < 1 * ((b + 3 - 1) / 1)
  omega

-- End-to-end: DecidedBelowRun at b = 3, c = 3 — the below-run slots now
-- include slot 0, which NO direct rule can decide in any view (pinned
-- above): the run genuinely needs the indirect ladder to clear it.
example : ∀ i, i < 3 → ∃ v, Decided U9 (View.full U9) i v :=
  (IndirectLiveness.holds (Fin 7) (Fin 43) U9).2 (View.full U9) 3 3
    (by omega) spansEligible_seven
    (fun j h1 h2 => by
      have hj : j = 3 ∨ j = 4 ∨ j = 5 := by omega
      rcases hj with rfl | rfl | rfl
      · exact ⟨23, Decided.directFast (by decide) (by decide)⟩
      · exact ⟨30, Decided.directFast (by decide) (by decide)⟩
      · exact ⟨31, Decided.directFast (by decide) (by decide)⟩)

-- Totality applied at the eligibility BOUNDARY: slot 3 = k + 3 is the
-- nearest anchor the hypothesis admits (slot 0's decision round 2 sits
-- one below its propose round 3) — killing an off-by-one strengthening
-- of the eligibility premise.
example : ∃ v, Decided U9 (View.full U9) 0 v :=
  (IndirectLiveness.holds (Fin 7) (Fin 43) U9).1 (View.full U9) 0 3 23
    (by decide)
    (Decided.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 1 ∨ i = 2 := by omega
      rcases hi with rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide))

-- ## A nonzero sub-quorum weak-rung negative (U9)

-- Id 1 (the crashed replica's genesis) draws three votes, but only two
-- are anchor-reachable (ids 10, 11 — id 12 is outside the anchor's
-- history): a NONZERO footprint that still misses q_weak = 3. Guards
-- the weak threshold itself — a weakening to "any vote suffices" would
-- flip this example.
example : (authorsOf U9.block ((blocksAt U9 1).filter
    fun b => IsVote U9 b 1 ∧ b ∈ history U9 23)).card = 2 := by decide
example : ¬ WeakLinked U9 23 1 0 := fun h =>
  absurd ((weakLinked_iff_history (by decide)).mp h) (by decide)

-- ## The tie-break direction bites (U11, five replicas)

/-- The tie-break configuration: one Byzantine replica, no crashes,
`k = 1` — the tight count `n = 5`, and the first configuration with
`p = 0` (no fast allowance at all). -/
instance fiveReplicas : Faults (Fin 5) where
  f := 1
  c := 0
  k := 1
  byzantine := {0}
  crashed := ∅
  byzantine_disjoint_crashed := by decide
  card_replicas := by decide
  card_byzantine := by decide
  card_crashed := by decide

/-- Pipelined single-leader schedule on five replicas: slot 0 is led by
the Byzantine replica 0. -/
instance : Slots (Fin 5) :=
  Slots.uniformSingle 1 (by omega) fun k => ⟨k % 5, by omega⟩

-- The threshold table at n = 5, f = 1, c = 0, k = 1.
example : p (Fin 5) = 0 ∧ q (Fin 5) = 4 ∧ qFast (Fin 5) = 5 ∧
    qCert (Fin 5) = 4 ∧ qSlow (Fin 5) = 3 ∧ qWeak (Fin 5) = 2 := by decide

/-- Twenty-six blocks over rounds 0–4. Round 0 has SIX blocks: ids 0
and 1 are the Byzantine leader's equivocating slot-0 copies, ids 2–5
the other genesis blocks (authors 1–4). Round 1's five voters split
between the copies — ids 6, 7, 8 (authors 0, 1, 2) vote copy 0, ids
9, 10 (authors 3, 4) vote copy 1 — so both copies clear `q_weak = 2`
and neither can ever reach `q_cert = 4`. Rounds 2–3 carry all five
voters into the history of slot 3's candidate (id 19, correct leader
3); round 4 fast-commits it. -/
def lk11 : Fin 26 → Block (Fin 5) (Fin 26) := fun i =>
  if h : (i : ℕ) < 2 then
    { round := 0, author := 0, parents := ∅ }
  else if h : (i : ℕ) < 6 then
    { round := 0, author := ⟨(i : ℕ) - 1, by omega⟩, parents := ∅ }
  else if h : (i : ℕ) < 9 then
    { round := 1, author := ⟨(i : ℕ) - 6, by omega⟩,
      parents := {0, 2, 3, 4} }
  else if h : (i : ℕ) < 11 then
    { round := 1, author := ⟨(i : ℕ) - 6, by omega⟩,
      parents := {1, 2, 3, 4} }
  else if h : (i : ℕ) < 14 then
    { round := 2, author := ⟨(i : ℕ) - 11, by omega⟩,
      parents := {6, 7, 8, 9} }
  else if h : (i : ℕ) < 16 then
    { round := 2, author := ⟨(i : ℕ) - 11, by omega⟩,
      parents := {7, 8, 9, 10} }
  else if h : (i : ℕ) < 21 then
    { round := 3, author := ⟨(i : ℕ) - 16, by omega⟩,
      parents := {12, 13, 14, 15} }
  else
    { round := 4, author := ⟨(i : ℕ) - 21, by omega⟩,
      parents := {16, 17, 18, 19, 20} }

/-- The tie-break universe. -/
def U11 : BlockUniverse (Fin 5) (Fin 26) where
  ids := Finset.univ
  block := lk11
  complete := by decide
  valid := by decide
  no_equivocation := by decide

-- Slot 0's candidates are exactly the two equivocating copies; the
-- voters split 3/2, so no direct rule can decide the slot in any view
-- (q_fast = 5, q_cert = 4, and nobody blames — every voter voted).
example : ∀ L : Fin 26, IsLeaderBlock U11 0 L → L = 0 ∨ L = 1 := by decide
example : supporters U11 0 1 = {0, 1, 2} ∧ supporters U11 1 1 = {3, 4} := by
  decide
example : certificates U11 0 0 = ∅ ∧ certificates U11 1 0 = ∅ := by decide
example : blames U11 0 = ∅ := by decide
example : ¬ FastCommitInView U11 (View.full U11) 0 0 ∧
    ¬ FastCommitInView U11 (View.full U11) 1 0 ∧
    ¬ SkippedLeaderInView U11 (View.full U11) 0 := by decide

-- The anchor: slot 3's candidate id 19, fast-committed, with BOTH
-- copies' votes in its history — both clear the weak rung.
example : IsLeaderBlock U11 3 19 ∧
    FastCommitInView U11 (View.full U11) 19 3 := by decide
example : WeakLinked U11 19 0 0 :=
  (weakLinked_iff_history (by decide)).mpr (by decide)
example : WeakLinked U11 19 1 0 :=
  (weakLinked_iff_history (by decide)).mpr (by decide)

-- The derivation commits the LEAST copy — and here the tie-break
-- premise does real work for the first time: copy 1 also clears the
-- rung, and the premise must refute `1 < 0`. Reversing the tie-break
-- direction (committing the greatest copy) makes this underivable.
theorem u11_decided : Decided U11 (View.full U11) 0 (some 0) := by
  have hall : ∀ L' : Fin 26, IsLeaderBlock U11 0 L' → L' = 0 ∨ L' = 1 := by
    decide
  refine Decided.indirectWeak (j := 3) (A := 19) (by omega) (by decide)
    (Decided.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 1 ∨ i = 2 := by omega
      rcases hi with rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide))
    (fun L' hL' hcert => by
      rcases hall L' hL' with rfl | rfl
      · exact absurd ((certifiedIn_iff_history (by decide)).mp hcert)
          (by decide)
      · exact absurd ((certifiedIn_iff_history (by decide)).mp hcert)
          (by decide))
    (by decide)
    ((weakLinked_iff_history (by decide)).mpr (by decide))
    (fun L' hL' _ => by
      rcases hall L' hL' with rfl | rfl
      · exact lt_irrefl _
      · exact fun hlt => absurd hlt (by decide))

-- The greatest copy is NOT committed: slot agreement applied end-to-end
-- against the derivation above — the second kill for the argmin
-- mutation.
example : ¬ Decided U11 (View.full U11) 0 (some 1) := fun h =>
  absurd
    (SlotAgreement.holds (Fin 5) (Fin 26) U11 (View.full U11) (View.full U11)
      0 (some 0) (some 1) u11_decided h)
    (by decide)

end HydrozoanTest
