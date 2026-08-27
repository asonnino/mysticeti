import Hydrozoan.Helpers.History
import HydrozoanTest.Model

/-!
# Witness: views and causal reachability

Extends the 3a scenario with a third round so reachability has real
multi-hop content, then carves a view with withheld blocks out of it.
`lk2`/`U2` repeat the fourteen-block table of
`HydrozoanTest.BlockUniverse` at `Fin 15` (parent sets are `Fin`-typed,
so the committed table cannot be reused) and add one round-2 block whose
parent set deliberately excludes ids 8 and 11 — the absences behind the
history pin and the view-boundary examples (the genesis and
round-monotonicity negatives stand on their own).
-/

namespace HydrozoanTest

open Hydrozoan

/-- Fifteen blocks: ids 0–6 genesis (author = id), ids 7/8 the round-1
equivocating pair by Byzantine replica 0, ids 9–13 round-1 blocks by
replicas 2–6 (crashed replica 1 halts after genesis), and id 14 a
round-2 block by replica 2 referencing `{7, 9, 10, 12, 13}` (authors
0, 2, 3, 5, 6) — excluding id 8 (the second equivocation) and id 11. -/
def lk2 : Fin 15 → Block (Fin 7) (Fin 15) := fun i =>
  if h : (i : ℕ) < 7 then
    { round := 0, author := ⟨i, by omega⟩, parents := ∅ }
  else if (i : ℕ) = 7 then
    { round := 1, author := 0, parents := {0, 1, 2, 3, 4} }
  else if (i : ℕ) = 8 then
    { round := 1, author := 0, parents := {0, 1, 2, 3, 5} }
  else if h : (i : ℕ) < 14 then
    { round := 1, author := ⟨(i : ℕ) - 7, by omega⟩, parents := {0, 1, 2, 3, 4} }
  else
    { round := 2, author := 2, parents := {7, 9, 10, 12, 13} }

/-- The three-round witness universe. -/
def U2 : BlockUniverse (Fin 7) (Fin 15) where
  ids := Finset.univ
  block := lk2
  complete := by decide
  valid := by decide
  no_equivocation := by decide

/-- A replica's local view: everything except id 8 (withheld — Byzantine
replica 0 sent this replica only its first round-1 block) and id 11 (an
honest block not yet delivered). Ref-closed because id 14's parents
avoid both. -/
def V2 : View U2 where
  ids := {0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 12, 13, 14}
  subset_ids := by decide
  complete := by decide

-- Direct reference: the round-2 block reaches its parents.
example : Reaches U2 14 7 := Reaches.single (by decide)

-- Multi-hop: down to genesis through id 7.
example : Reaches U2 14 0 :=
  Reaches.of_mem_parents (i := 14) (j := 7) (by decide) (Reaches.single (by decide))

-- Reflexivity: every block is in its own causal history.
example : Reaches U2 3 3 := Reaches.refl

-- Genesis reaches only itself — reachability tracks the reference
-- structure, not just rounds (ids 0 and 1 both sit at round 0).
example : ¬ Reaches U2 0 1 := fun h =>
  absurd (eq_of_reaches_of_parents_empty (by decide) h) (by decide)

-- Round monotonicity: nothing reaches a strictly higher round.
example : ¬ Reaches U2 0 14 := not_reaches_of_round_lt (by decide) (by decide)

-- The computable surrogate, pinned: id 14's history misses ids 5, 6, 8,
-- and 11 exactly.
example : history U2 14 = {14, 7, 9, 10, 12, 13, 0, 1, 2, 3, 4} := by decide

-- Existence ≠ reachability: id 5 is in the universe, but id 14 does not
-- reach it (only the excluded id 8 references genesis 5).
example : ¬ Reaches U2 14 5 := fun h => by
  have := (mem_history_iff (U := U2) (b := 14) (i := 5) (by decide)).mpr h
  revert this; decide

-- Withholding is real: the view has the first equivocation, not the
-- second.
example : (7 : Fin 15) ∈ V2.ids ∧ (8 : Fin 15) ∉ V2.ids := by decide

-- Causal history never escapes the view — id 14's whole history is
-- held, pinned in full ...
example : ∀ b ∈ history U2 14, b ∈ V2.ids := by decide

-- ... and the same fact shown structurally for one element, through the
-- view-closure lemma.
example : (0 : Fin 15) ∈ V2.ids :=
  View.mem_of_reaches (V := V2) (c := 14) (by decide)
    (Reaches.of_mem_parents (i := 14) (j := 7) (by decide) (Reaches.single (by decide)))

-- The view's boundary is a causal boundary: no block the view holds
-- reaches the withheld id 8.
example : ¬ Reaches U2 14 8 := fun h =>
  absurd (View.mem_of_reaches (V := V2) (c := 14) (by decide) h) (by decide)

end HydrozoanTest
