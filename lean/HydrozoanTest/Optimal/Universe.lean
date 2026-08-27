import Hydrozoan.Optimal.Helpers.Universe
import HydrozoanTest.Optimal.Thresholds

/-!
# Witness: the leader-exclusion rule

A four-round universe on `fourReplicasOpt` (four replicas, replica `0`
Byzantine) under a pipelined schedule — slot `k` at round `k`, leader
`(k + 3) % 4`, so slot 1's leader is the Byzantine replica `0`. The
scenario is FinWhale's Figure 1 at `f = p = 1`:

* round 1: replica `0` equivocates — copy A (id 4) and copy B (id 5) —
  while `1`, `2`, `3` propose normally (ids 6–8);
* round 2 (slot 1's voting round): `0`, `1`, `3` vote for A (ids 9, 10,
  12), `2` votes for B (id 11);
* round 3 (slot 1's decision round): id 13 by `1` references the votes
  of `1`, `2`, `3` — it sees A and B, **witnesses** the equivocation, and
  omits replica `0`'s vote; id 14 by `3` references the votes of `0`, `1`,
  `3` — all for A — so it does not witness, and may reference `0`'s vote.

Ids 0–14 form `U`, an `OptUniverse`: the base conditions and the
leader-exclusion clause hold. Id 15, by replica `2`, references the votes
of `0`, `1`, `2` — it witnesses the equivocation *and* references
replica `0`'s block. Ids 0–15 form `Ubad`, a perfectly good Hydrozoan
`BlockUniverse` that **no** `OptUniverse` extends: the clause bites, at
exactly `(b, k, j) = (15, 1, 9)`.

The `leader_excluded` field is discharged through the generated bridge
`leaderExcluded_of_bounded` (rounds stop at 3, so only slots up to a
bound matter), which turns the `∀ k` into a decidable `∀ k ≤ B`.

The same table is read a second time under a **two-slots-per-round**
schedule (`fourSlotsTwoPerRound`): round 1 then carries slot 2, led by
the Byzantine `0`, and slot 3, led by `1`. Block 13 witnesses slot 2's
equivocation and excludes `0`, while it does not witness anything in
slot 3 and legitimately keeps replica `1`'s vote (id 10) — the rule
applies to each slot of a round separately, as the core docstring
claims.
-/

namespace HydrozoanTest

open Hydrozoan Hydrozoan.Optimal

set_option maxRecDepth 8192

/-- The pipelined schedule on four replicas: slot `k` at round `k`, led
by `(k + 3) % 4` — slot 1 by the Byzantine replica `0`. -/
instance fourSlotsOpt : Slots (Fin 4) where
  slotRound k := k
  leader k := ⟨(k + 3) % 4, Nat.mod_lt _ (by decide)⟩
  mono := fun _ _ h => h
  unbounded := fun n => ⟨n, le_rfl⟩
  keyed := fun _ _ h => congrArg Prod.fst h

/-- Sixteen blocks over four rounds; see the module docstring. Ids 0–3:
genesis. Ids 4 and 5: replica `0`'s two round-1 copies. Ids 6–8: round 1
by `1`, `2`, `3`. Ids 9–12: round 2 by `0`, `1`, `2`, `3`, voting A, A, B,
A. Ids 13, 14: round 3 by `1` (witnessing, excludes `0`) and `3`
(leader-consistent, includes `0`). Id 15: round 3 by `2`, witnessing
*and* including `0` — the invalid block. -/
def lkX : Fin 16 → Block (Fin 4) (Fin 16) := fun i =>
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
  else if (i : ℕ) = 13 then
    { round := 3, author := 1, parents := {10, 11, 12} }
  else if (i : ℕ) = 14 then
    { round := 3, author := 3, parents := {9, 10, 12} }
  else
    { round := 3, author := 2, parents := {9, 10, 11} }

/-- The Hydrozoan universe of ids 0–14 (id 15 left out). -/
def UX : BlockUniverse (Fin 4) (Fin 16) where
  ids := Finset.univ.erase 15
  block := lkX
  complete := by decide
  valid := by decide
  no_equivocation := by decide

/-- ... which is an `OptUniverse`: the leader-exclusion clause holds. -/
def OX : OptUniverse (Fin 4) (Fin 16) :=
  { UX with
    leader_excluded :=
      leaderExcluded_of_bounded UX 3 3 (fun k hk => by change k + 2 ≤ 3 at hk; omega)
        (by decide) (by decide) }

-- Slot 1 has two candidates, 4 and 5, both by the Byzantine leader 0.
example : IsLeaderBlock UX 1 4 ∧ IsLeaderBlock UX 1 5 ∧ ¬ IsLeaderBlock UX 1 6 := by
  decide

-- Id 13 witnesses slot 1's equivocation (its parents 10 and 11 vote for
-- 4 and 5); id 14 does not (its parents all vote for 4); and the
-- voting-round block 9, which references copy 4 directly, does not
-- either — witnessing is about the parents' votes, one round up.
example :
    WitnessesEquivocation UX 1 13 ∧ ¬ WitnessesEquivocation UX 1 14 ∧
      ¬ WitnessesEquivocation UX 1 9 := by
  decide

-- The clause, instantiated: the witnessing block 13 has no parent by
-- replica 0, while the non-witnessing block 14 does (id 9) and is
-- allowed to.
example :
    (∀ j ∈ (UX.block 13).parents, (UX.block j).author ≠ 0) ∧
      (9 ∈ (UX.block 14).parents ∧ (UX.block 9).author = 0) := by
  decide

-- Exclusion sits exactly on the DAG quorum: after dropping replica 0's
-- vote, block 13 keeps q = 3 parents — the correct replicas alone, as in
-- the paper's lem:opt-admissible at the tight committee ...
example : (authors UX.block (UX.block 13)).card = q (Fin 4) := by decide

/-- ... and exclusion cannot go below it: a witnessing block that also
dropped replica 3's vote would have two parents. -/
def twoParentWitness : Block (Fin 4) (Fin 16) :=
  { round := 3, author := 2, parents := {10, 11} }

example : ¬ ValidWrt lkX twoParentWitness := by decide

-- The single-candidate branch is vacuous by name: slot 0 (leader 3) has
-- the one candidate 3, so block 12 — at slot 0's decision round — does
-- not witness and keeps replica 3's block 8 freely.
example :
    ¬ WitnessesEquivocation UX 0 12 ∧ 8 ∈ (UX.block 12).parents ∧
      (UX.block 8).author = Slots.leader 0 := by
  decide

/-- Two slots per round: slot `k` at round `k / 2`, led by `(k + 2) % 4`.
Round 1 carries slot 2 (leader `0`, Byzantine) and slot 3 (leader `1`).
A local schedule, not an instance — `Fin 4` carries `fourSlotsOpt`. -/
@[instance_reducible]
def fourSlotsTwoPerRound : Slots (Fin 4) where
  slotRound k := k / 2
  leader k := ⟨(k + 2) % 4, Nat.mod_lt _ (by decide)⟩
  mono := fun _ _ h => Nat.div_le_div_right h
  unbounded := fun n => ⟨2 * n, by omega⟩
  keyed := fun a b h => by
    have h1 : a / 2 = b / 2 := congrArg Prod.fst h
    have h2 : (a + 2) % 4 = (b + 2) % 4 := congrArg (fun p : ℕ × Fin 4 => (p.2 : ℕ)) h
    omega

/-- The same table under the two-slots-per-round schedule is again an
`OptUniverse`: slots with a decision round ≤ 3 have index ≤ 3. -/
def OX2 : @OptUniverse (Fin 4) (Fin 16) _ _ _ _ fourSlotsTwoPerRound :=
  letI := fourSlotsTwoPerRound
  { UX with
    leader_excluded :=
      leaderExcluded_of_bounded UX 3 3
        (fun k hk => by change k / 2 + 2 ≤ 3 at hk; omega) (by decide) (by decide) }

-- Under that schedule, round 1 has two slots: 2 (candidates 4 and 5, by
-- replica 0) and 3 (the single candidate 6, by replica 1). Block 13
-- witnesses slot 2 and not slot 3, excludes replica 0's vote, and keeps
-- replica 1's vote 10 — one round, two slots, two verdicts.
example :
    @IsLeaderBlock (Fin 4) (Fin 16) _ _ _ fourSlotsTwoPerRound UX 2 4 ∧
      @IsLeaderBlock (Fin 4) (Fin 16) _ _ _ fourSlotsTwoPerRound UX 2 5 ∧
      @IsLeaderBlock (Fin 4) (Fin 16) _ _ _ fourSlotsTwoPerRound UX 3 6 ∧
      @WitnessesEquivocation (Fin 4) (Fin 16) _ _ _ fourSlotsTwoPerRound UX 2 13 ∧
      ¬ @WitnessesEquivocation (Fin 4) (Fin 16) _ _ _ fourSlotsTwoPerRound UX 3 13 ∧
      (∀ j ∈ (UX.block 13).parents, (UX.block j).author ≠ 0) ∧
      10 ∈ (UX.block 13).parents ∧ (UX.block 10).author = 1 := by
  decide

/-- The same table with id 15 admitted: a valid Hydrozoan universe. -/
def UbadX : BlockUniverse (Fin 4) (Fin 16) where
  ids := Finset.univ
  block := lkX
  complete := by decide
  valid := by decide
  no_equivocation := by decide

-- Id 15 witnesses the equivocation and references replica 0's vote 9 ...
example :
    WitnessesEquivocation UbadX 1 15 ∧ 9 ∈ (UbadX.block 15).parents ∧
      (UbadX.block 9).author = Slots.leader 1 := by
  decide

-- ... so no `OptUniverse` extends `UbadX`: the clause bites, at
-- exactly (b, k, j) = (15, 1, 9).
example : ¬ ∃ O : OptUniverse (Fin 4) (Fin 16), O.toBlockUniverse = UbadX := by
  rintro ⟨O, h⟩
  have hx := O.leader_excluded 15 (by rw [h]; decide) 1 (by rw [h]; decide)
    (by rw [h]; decide) 9 (by rw [h]; decide)
  rw [h] at hx
  exact hx (by decide)

end HydrozoanTest
