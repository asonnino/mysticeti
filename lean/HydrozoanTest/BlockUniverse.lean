import Hydrozoan.Helpers.Block
import HydrozoanTest.Model

/-!
# Witness: a block universe with faults

A concrete two-round `BlockUniverse` over the seven-replica fault model
of `HydrozoanTest.Model`: Byzantine replica 0 equivocates at round 1
(two blocks, ids 7 and 8), and crashed replica 1 authors its genesis
block but nothing afterwards — halting made visible. All universe
conditions are checked by `decide`.

The negative examples keep the definitions biting: the *unguarded*
non-equivocation is false in this universe (so the `NonByzantine` guard
does real work), and each `ValidWrt` field individually rejects a
malformed block.
-/

namespace HydrozoanTest

open Hydrozoan

/-- Fourteen blocks over the seven replicas: ids 0–6 genesis (author =
id), ids 7 and 8 round-1 blocks both by Byzantine replica 0
(equivocation, with different parent quorums), ids 9–13 round-1 blocks
by replicas 2–6. Crashed replica 1 authors only its genesis block. -/
def lk : Fin 14 → Block (Fin 7) (Fin 14) := fun i =>
  if h : (i : ℕ) < 7 then
    { round := 0, author := ⟨i, by omega⟩, parents := ∅ }
  else if (i : ℕ) = 7 then
    { round := 1, author := 0, parents := {0, 1, 2, 3, 4} }
  else if (i : ℕ) = 8 then
    { round := 1, author := 0, parents := {0, 1, 2, 3, 5} }
  else
    { round := 1, author := ⟨(i : ℕ) - 7, by omega⟩, parents := {0, 1, 2, 3, 4} }

/-- The witness universe: all fourteen blocks satisfy completeness,
validity, and (`NonByzantine`-guarded) non-equivocation. -/
def U : BlockUniverse (Fin 7) (Fin 14) where
  ids := Finset.univ
  block := lk
  complete := by decide
  valid := by decide
  no_equivocation := by decide

-- The guard does real work: WITHOUT the `NonByzantine` restriction,
-- non-equivocation is FALSE in `U` — ids 7 and 8 are a genuine Byzantine
-- equivocations.
example :
    ¬ (∀ i ∈ U.ids, ∀ j ∈ U.ids,
      (U.block i).author = (U.block j).author →
      (U.block i).round = (U.block j).round → i = j) := by
  decide

-- Crashed replica 1 halts after genesis: no round-1 block by it exists.
example : ∀ i : Fin 14, (lk i).round = 1 → (lk i).author ≠ 1 := by decide

-- `predecessor` bites: a round-2 block referencing genesis blocks.
example :
    ¬ ValidWrt lk { round := 2, author := 2, parents := {0, 1, 2, 3, 4} } := by
  decide

-- `distinct_authors` bites: parents include both of author 0's
-- equivocating round-1 blocks (five distinct authors, so `quorum` alone
-- would pass).
example :
    ¬ ValidWrt lk { round := 2, author := 2, parents := {7, 8, 9, 10, 11, 12} } := by
  decide

-- `quorum` bites: only four distinct authors, one short of q = 5.
example :
    ¬ ValidWrt lk { round := 1, author := 2, parents := {0, 1, 2, 3} } := by
  decide

end HydrozoanTest
