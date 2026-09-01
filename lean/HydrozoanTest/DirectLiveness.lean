import Hydrozoan.DirectLiveness.Proof
import HydrozoanTest.Liveness
import HydrozoanTest.DirectRules

/-!
# Witness: liveness fires

Two configurations, one per claim:

* **Slow path, on the frozen `U6`** (pipelined schedule; slot 0's
  leader is replica 2 — correct — with candidate id 0): all five
  round-2 blocks certify, the slot slow-commits and is Decided at the
  eventual view — while the fast path **cannot** fire there (five
  supporters < q_fast = 6; actual faults 2 > p = 1). The slow path
  fires exactly where the fast path is out of reach: the design story
  in one pair of examples.
* **Fast path, on a fresh low-fault configuration** `Fin 4` with
  `f = 0, c = 1, k = 1` (also exercising the `f = 0` corner, where the
  fast quorum equals the correct pool): one actual fault = p, so
  `|Correct| = 3 = q_fast` and the fast path fires at exact quorum with
  the crashed replica silent.
-/

namespace HydrozoanTest

open Hydrozoan

set_option maxRecDepth 8192

-- ## Slow path on U6

-- Slot 0's candidate is the round-0 block of correct leader 2.
example : IsLeaderBlock U6 0 0 := by decide

-- Every round-2 block certifies it; the slot slow-commits.
example : certificates U6 0 0 = {10, 11, 12, 13, 14} := by decide
example : SlowCommit U6 0 0 := by decide

-- The fast path is out of reach: only the five correct replicas voted.
example : supporters U6 0 1 = {2, 3, 4, 5, 6} := by decide
example : ¬ FastCommit U6 0 0 := by decide

-- The harvest form: Decided at the eventual view, via the slow route.
example : Decided U6 (View.full U6) 0 (some 0) :=
  Decided.directSlow (by decide) (by decide)

-- End-to-end: the headline theorem applied to U6 with every hypothesis
-- discharged concretely (T = Correct, R = 0, k = 0) — the mechanical
-- guard against a silently strengthened Statement hypothesis. (Known
-- residual gap: with R = 0 and slotRound 0 = 0, a mutation of
-- `R ≤ slotRound k` to equality would survive this instantiation.)
example : ∃ L, IsLeaderBlock U6 0 L ∧ SlowCommit U6 L 0 ∧
    Decided U6 (View.full U6) 0 (some L) :=
  DirectLiveness.holds (Fin 7) (Fin 15) U6 (Correct : Finset (Fin 7)) 0 0
    (by decide) (by decide) u6_synchronised (by decide) (by decide)
    (by decide) (by decide) (by decide)
    (View.full U6) (View.coversUpto_full U6 _)

-- ## Fast path at low faults (Fin 4, f = 0, c = 1, k = 1)

/-- The low-fault configuration: no Byzantine replica, one crashed —
one actual fault, equal to the fast allowance p = 1. -/
instance fourReplicas : Faults (Fin 4) where
  f := 0
  c := 1
  k := 1
  byzantine := ∅
  crashed := {1}
  byzantine_disjoint_crashed := by decide
  card_replicas := by decide
  card_byzantine := by decide
  card_crashed := by decide

/-- Pipelined single-leader schedule on four replicas; slot 0 led by
replica 0. -/
instance : Slots (Fin 4) :=
  Slots.uniformSingle 1 (by omega) fun k => ⟨k % 4, by omega⟩

-- The threshold table at n = 4, f = 0, c = 1, k = 1: the fast quorum
-- equals the correct pool exactly.
example : p (Fin 4) = 1 ∧ q (Fin 4) = 3 ∧ qFast (Fin 4) = 3 ∧
    qCert (Fin 4) = 3 ∧ qSlow (Fin 4) = 2 ∧ qWeak (Fin 4) = 2 := by decide

-- The actual faults fit the fast allowance.
example : ((fourReplicas.byzantine ∪ fourReplicas.crashed).card ≤
    p (Fin 4)) := by decide

/-- Nine blocks: rounds 0–2 × the three correct replicas {0, 2, 3},
each non-genesis block referencing all three blocks of the round
below. -/
def lk7 : Fin 9 → Block (Fin 4) (Fin 9) := fun i =>
  if h : (i : ℕ) < 3 then
    { round := 0,
      author := ⟨if (i : ℕ) = 0 then 0 else (i : ℕ) + 1, by split <;> omega⟩,
      parents := ∅ }
  else if h : (i : ℕ) < 6 then
    { round := 1,
      author := ⟨if (i : ℕ) = 3 then 0 else (i : ℕ) - 2, by split <;> omega⟩,
      parents := {0, 1, 2} }
  else
    { round := 2,
      author := ⟨if (i : ℕ) = 6 then 0 else (i : ℕ) - 5, by split <;> omega⟩,
      parents := {3, 4, 5} }

/-- The low-fault universe. -/
def U7 : BlockUniverse (Fin 4) (Fin 9) where
  ids := Finset.univ
  block := lk7
  complete := by decide
  valid := by decide
  no_equivocation := by decide

-- Both rounds are fully populated by the correct replicas, and the
-- universe is synchronised from round 0.
example : Populated U7 0 ∧ Populated U7 1 ∧ Populated U7 2 := by decide
theorem u7_synchronised : Synchronised U7 0 := by
  intro n hn b hb hbr hbc a ha har hac
  have hmax : ∀ c : Fin 9, (U7.block c).round ≤ 2 := by decide
  have hb2 := hmax b
  have hn2 : n = 0 ∨ n = 1 := by omega
  clear hb2 hmax hn
  rcases hn2 with rfl | rfl
  · revert b a; decide
  · revert b a; decide

-- The fast path fires at exact quorum: all three correct replicas vote
-- for the correct leader's block.
example : IsLeaderBlock U7 0 0 := by decide
example : supporters U7 0 1 = {0, 2, 3} := by decide
example : FastCommit U7 0 0 := by decide

-- End-to-end: fastLatency applied with every hypothesis discharged
-- concretely — the mechanical guard against a silently strengthened
-- Statement hypothesis. (Disclosure: at this configuration
-- f + c = 1 = p, so the fault hypothesis is satisfied by every
-- admissible fault assignment — it does no work here; U6 above p is
-- where it bites. Also R = 0 = slotRound 0 sits at the R-boundary,
-- so an `R = slotRound k` strengthening survives; a strict-R kill
-- would need a sparse-schedule fast commit like U14's.)
example : ∃ L, IsLeaderBlock U7 0 L ∧ FastCommit U7 L 0 :=
  DirectLiveness.fastLatency (Fin 4) (Fin 9) U7 0 0 (by decide)
    u7_synchronised (by decide) (by decide) (by decide) (by decide)

-- The performance pair harvested as Decided verdicts at low faults.
example : Decided U7 (View.full U7) 0 (some 0) :=
  Decided.directFast (by decide) (by decide)
example : Decided U7 (View.full U7) 1 none :=
  Decided.directSkip (by decide)

-- ## Direct skip at low faults: the other half of the opportunistic
-- pair. Slot 1's leader is the crashed replica 1 — no candidate exists,
-- every round-2 block blames vacuously, and the three correct blamers
-- meet q_fast exactly.
example : Slots.leader (Replica := Fin 4) 1 = 1 := by decide
example : ∀ L : Fin 9, ¬ IsLeaderBlock U7 1 L := by decide
example : blames U7 1 = {0, 2, 3} := by decide
example : SkippedLeader U7 1 := by decide

-- End-to-end: skipLatency applied with every hypothesis discharged.
example : SkippedLeader U7 1 :=
  DirectLiveness.skipLatency (Fin 4) (Fin 9) U7 1 (by decide) (by decide)
    (by decide)

end HydrozoanTest
