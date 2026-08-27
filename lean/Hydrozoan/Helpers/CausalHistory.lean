import Hydrozoan.Model.View
import Hydrozoan.Model.CausalHistory

/-!
# Causal-reachability lemmas

Generated infrastructure over `Model/CausalHistory.lean` and
`Model/View.lean`. Nothing here is part of the audit surface.
-/

namespace Hydrozoan

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [F : Faults Replica] {U : BlockUniverse Replica BlockId}

namespace Reaches

/-- Every block is in its own causal history. -/
@[refl]
theorem refl {c : BlockId} : Reaches U c c :=
  Relation.ReflTransGen.refl

/-- A direct reference is one step of causal history. -/
theorem single {i j : BlockId} (h : j ∈ (U.block i).parents) : Reaches U i j :=
  Relation.ReflTransGen.single h

/-- Causal history is transitive. -/
theorem trans {a b c : BlockId} (h₁ : Reaches U a b) (h₂ : Reaches U b c) :
    Reaches U a c :=
  Relation.ReflTransGen.trans h₁ h₂

/-- Prepend a direct reference to a reachability chain. -/
theorem of_mem_parents {i j b : BlockId} (hij : j ∈ (U.block i).parents)
    (hjb : Reaches U j b) : Reaches U i b :=
  trans (single hij) hjb

end Reaches

/-- Parents of a universe member sit exactly one round below it. -/
theorem round_of_mem_parents {i j : BlockId} (hi : i ∈ U.ids)
    (hj : j ∈ (U.block i).parents) :
    (U.block j).round + 1 = (U.block i).round :=
  (U.valid i hi).predecessor j hj

/-- Causal history stays inside the universe. -/
theorem mem_ids_of_reaches {c b : BlockId} (hc : c ∈ U.ids)
    (h : Reaches U c b) : b ∈ U.ids := by
  induction h with
  | refl => exact hc
  | tail _ hstep ih => exact U.complete _ ih _ hstep

/-- A block with no parents reaches only itself. -/
theorem eq_of_reaches_of_parents_empty {c b : BlockId}
    (hc : (U.block c).parents = ∅) (h : Reaches U c b) : b = c := by
  rcases h.cases_head with rfl | ⟨j, hstep, -⟩
  · rfl
  · simp [RefStep, hc] at hstep

/-- Causal history only ever runs downward: following references never
raises the round. -/
theorem round_le_of_reaches {c b : BlockId} (hc : c ∈ U.ids)
    (h : Reaches U c b) : (U.block b).round ≤ (U.block c).round := by
  induction h with
  | refl => exact le_refl _
  | tail hr hstep ih =>
      have hmem := mem_ids_of_reaches hc hr
      have := round_of_mem_parents hmem hstep
      omega

/-- A block never reaches a strictly higher round. -/
theorem not_reaches_of_round_lt {c b : BlockId} (hc : c ∈ U.ids)
    (h : (U.block c).round < (U.block b).round) : ¬ Reaches U c b := fun hr =>
  absurd (round_le_of_reaches hc hr) (by omega)

/-- Causal history never escapes a view. -/
theorem View.mem_of_reaches {V : View U} {c b : BlockId}
    (hc : c ∈ V.ids) (h : Reaches U c b) : b ∈ V.ids := by
  induction h with
  | refl => exact hc
  | tail _ hstep ih => exact V.complete _ ih _ hstep

end Hydrozoan
