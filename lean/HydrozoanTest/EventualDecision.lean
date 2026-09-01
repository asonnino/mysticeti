import Hydrozoan.EventualDecision.Proof
import HydrozoanTest.IndirectLiveness

/-!
# Witness: eventual decision fires

The low-fault `Fin 4` configuration (replica 1 crashed, pipelined
schedule leader `k % 4`) on an eight-round table:

* **`RunDecidesBelow` end-to-end, fully concrete**: the run at slots
  2, 3, 4 (leaders 2, 3, 0 — all correct) with every hypothesis
  discharged — synchrony from round 0, population over the run's span
  (rounds 2–6), leaders pinned — decides slots 0 and 1. Rounds 0–6 are
  load-bearing: the composition derives the run commits via direct
  liveness, whose slot-4 wave needs its decision round 6 populated (the
  indirect-liveness descent, by contrast, consumed the commits
  ready-made); round 7 is unconsumed headroom.
* **The partial view `v10`** (rounds 0–6; round 7 outside):
  `RunDecidesBelow` applied at it pins the statement's `CoversUpto`
  horizon — every other application discharges the premise with
  `coversUpto_full`, which proves *any* horizon, so this is the one
  guard against a silently raised horizon constant.
* **`fairRun_four`**: the pipelined schedule is provably fair to the
  correct set at `c = 3` — runs start at every `k' = 4k + 2` — proved
  from the definition, not by finite enumeration.
* **`RunsRecur` and `ledgerProgress` end-to-end**: applied with every
  hypothesis discharged concretely. Their conclusions produce an opaque
  bound `b` (existential), so no finite table can also instantiate what
  follows it — the concrete guard for that part is the
  `RunDecidesBelow` application above, which is why the workhorse sits
  in the Statement rather than in a helper.
-/

namespace HydrozoanTest

open Hydrozoan

set_option maxRecDepth 16384

/-- Twenty-four blocks: rounds 0–7 × the three correct replicas
{0, 2, 3} (round `r` holds ids `3r, 3r + 1, 3r + 2`), each non-genesis
block referencing all three blocks of the round below. -/
def lk10 : Fin 24 → Block (Fin 4) (Fin 24) := fun i =>
  { round := (i : ℕ) / 3,
    author := ⟨if (i : ℕ) % 3 = 0 then 0 else (i : ℕ) % 3 + 1,
      by split <;> omega⟩,
    parents :=
      if h : (i : ℕ) < 3 then ∅
      else {⟨(i : ℕ) / 3 * 3 - 3, by omega⟩, ⟨(i : ℕ) / 3 * 3 - 2, by omega⟩,
        ⟨(i : ℕ) / 3 * 3 - 1, by omega⟩} }

/-- The eight-round universe. -/
def U10 : BlockUniverse (Fin 4) (Fin 24) where
  ids := Finset.univ
  block := lk10
  complete := by decide
  valid := by decide
  no_equivocation := by decide

-- The run at slots 2, 3, 4: correct leaders, candidates ids 7, 11, 12.
example : IsLeaderBlock U10 2 7 ∧ IsLeaderBlock U10 3 11 ∧
    IsLeaderBlock U10 4 12 := by decide

-- Synchronised from round 0 (the round-bounding pattern).
theorem u10_synchronised : Synchronised U10 0 := by
  intro n hn b hb hbr hbc a ha har hac
  have hmax : ∀ c : Fin 24, (U10.block c).round ≤ 7 := by decide
  have hb2 := hmax b
  have hn2 : n = 0 ∨ n = 1 ∨ n = 2 ∨ n = 3 ∨ n = 4 ∨ n = 5 ∨ n = 6 := by
    omega
  clear hb2 hmax hn
  rcases hn2 with rfl | rfl | rfl | rfl | rfl | rfl | rfl <;>
    (revert b a; decide)

-- The correct replicas fill every round of the run's span (rounds 2–6).
theorem u10_populated : ∀ r, Slots.slotRound (Replica := Fin 4) 2 ≤ r →
    r ≤ Slots.slotRound (Replica := Fin 4) (2 + 3 - 1) + 2 →
    PopulatedOn U10 (Correct : Finset (Fin 4)) r := by
  intro r h1 h2
  change 1 * (2 / 1) ≤ r at h1
  change r ≤ 1 * ((2 + 3 - 1) / 1) + 2 at h2
  have : r = 2 ∨ r = 3 ∨ r = 4 ∨ r = 5 ∨ r = 6 := by omega
  rcases this with rfl | rfl | rfl | rfl | rfl <;> decide

-- End-to-end: RunDecidesBelow applied to U10 at b = 2, c = 3 with every
-- hypothesis discharged concretely — the mechanical guard against a
-- silently strengthened Statement hypothesis. (Known residual gap,
-- shared with the indirect-liveness descent witness: c = 3 also
-- satisfies a `3 ≤ c` strengthening; killing it needs a
-- faster-than-pipelined schedule.)
example : ∀ i, i < 2 → ∃ v, Decided U10 (View.full U10) i v :=
  (EventualDecision.holds (Fin 4) (Fin 24)).1 U10
    (Correct : Finset (Fin 4)) 0 2 3
    (by decide) (by decide) u10_synchronised (by omega) spansEligible_four
    (by decide)
    (by intro i hi
        have : i = 0 ∨ i = 1 ∨ i = 2 := by omega
        rcases this with rfl | rfl | rfl <;> decide)
    u10_populated (View.full U10) (View.coversUpto_full U10 _)

/-- The partial view of `U10` holding exactly rounds 0–6 — the run's
last decision round, with round 7 outside. -/
def v10 : View U10 where
  ids := Finset.univ.filter fun i => (U10.block i).round ≤ 6
  subset_ids := Finset.filter_subset _ _
  complete := by decide

/-- `v10` is caught up to the run's last decision round. -/
theorem v10_coversUpto : v10.CoversUpto 6 := by
  change ∀ b ∈ U10.ids, (U10.block b).round ≤ 6 → b ∈ v10.ids
  decide

-- Genuinely partial: the round-7 block 21 is outside.
example : ¬ v10.CoversUpto 7 := fun h =>
  absurd (h 21 (by decide) (by decide)) (by decide)

-- The same run harvested at the partial view — the guard that pins the
-- statement's horizon at the run's last decision round: a raised
-- horizon constant leaves `v10` short and fails this application.
example : ∀ i, i < 2 → ∃ v, Decided U10 v10 i v :=
  (EventualDecision.holds (Fin 4) (Fin 24)).1 U10
    (Correct : Finset (Fin 4)) 0 2 3
    (by decide) (by decide) u10_synchronised (by omega) spansEligible_four
    (by decide)
    (by intro i hi
        have : i = 0 ∨ i = 1 ∨ i = 2 := by omega
        rcases this with rfl | rfl | rfl <;> decide)
    u10_populated v10 v10_coversUpto

-- The same run applied at R = 2 = slotRound b — the exact boundary of
-- `R ≤ slotRound b`, killing a strengthening to strict inequality
-- (synchrony-from-2 restricts from the round-0 theorem).
example : ∀ i, i < 2 → ∃ v, Decided U10 (View.full U10) i v :=
  (EventualDecision.holds (Fin 4) (Fin 24)).1 U10
    (Correct : Finset (Fin 4)) 2 2 3
    (by decide) (by decide)
    (fun n _ => u10_synchronised n (Nat.zero_le n))
    (by omega) spansEligible_four (by decide)
    (by intro i hi
        have : i = 0 ∨ i = 1 ∨ i = 2 := by omega
        rcases this with rfl | rfl | rfl <;> decide)
    u10_populated (View.full U10) (View.coversUpto_full U10 _)

-- A fairness negative: consecutive slots never share a leader, so a
-- singleton T is starved at c = 2 — FairRunOn is not trivially true.
example : ¬ EventualDecision.FairRunOn (Fin 4) {0} 2 := fun h => by
  obtain ⟨k', -, hl⟩ := h 0
  have h0 : ((k' + 0) % 4 : ℕ) = 0 :=
    congrArg Fin.val (Finset.mem_singleton.mp (hl 0 (by omega)))
  have h1 : ((k' + 1) % 4 : ℕ) = 0 :=
    congrArg Fin.val (Finset.mem_singleton.mp (hl 1 (by omega)))
  omega

-- The schedule is provably fair to the correct set at c = 3: a run
-- starts at every 4k + 2 (leaders 2, 3, 0). Proved from the definition
-- — finite enumeration cannot reach a ∀-over-ℕ claim.
theorem fairRun_four :
    EventualDecision.FairRunOn (Fin 4) (Correct : Finset (Fin 4)) 3 := by
  intro k
  refine ⟨4 * k + 2, by omega, fun i hi => ?_⟩
  have hmem : ∀ m : Fin 4, m ≠ 1 → m ∈ (Correct : Finset (Fin 4)) := by
    decide
  exact hmem _ (Fin.ne_of_val_ne (by
    change (4 * k + 2 + i) % 4 ≠ (1 : Fin 4).val
    omega))

-- End-to-end: RunsRecur applied concretely — fairness places a
-- correct-led run past slot 5 at or after round 3. The bound is opaque
-- (existential); the concrete-run guard is the application above.
example : ∃ b, 5 ≤ b ∧ 3 ≤ Slots.slotRound (Replica := Fin 4) b ∧
    ∀ i, i < 3 → Slots.leader (Replica := Fin 4) (b + i) ∈
      (Correct : Finset (Fin 4)) :=
  (EventualDecision.holds (Fin 4) (Fin 24)).2
    (Correct : Finset (Fin 4)) 3 5 3 fairRun_four

-- End-to-end: the composed headline, all hypotheses discharged.
example : ∃ b, 5 ≤ b ∧ 3 ≤ Slots.slotRound (Replica := Fin 4) b ∧
    ∀ (U : BlockUniverse (Fin 4) (Fin 24)),
      SynchronisedOn U (Correct : Finset (Fin 4)) 3 →
      (∀ r, Slots.slotRound (Replica := Fin 4) b ≤ r →
        r ≤ Slots.slotRound (Replica := Fin 4) (b + 3 - 1) + 2 →
        PopulatedOn U (Correct : Finset (Fin 4)) r) →
      ∀ i, i < b → ∃ v, Decided U (View.full U) i v := by
  obtain ⟨b, hk, hR, hrest⟩ :=
    EventualDecision.ledgerProgress (Fin 4) (Fin 24)
      (Correct : Finset (Fin 4)) 3 5 3
      (by decide) (by decide) (by omega) spansEligible_four fairRun_four
  exact ⟨b, hk, hR, fun U hsync hpop =>
    hrest U hsync hpop (View.full U) (View.coversUpto_full U _)⟩

end HydrozoanTest
