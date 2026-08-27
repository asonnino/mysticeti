import Hydrozoan.Helpers.CausalHistory
import Hydrozoan.Helpers.Block
import Mathlib.Data.Finset.Union

/-!
# Computable causal history

Generated: a fuel-indexed `Finset` surrogate for `Reaches`, so witness
models can decide reachability (`Reaches` itself is a bare `Prop` and
stays one). `mem_history_iff` pins the surrogate's meaning against the
audited relation; nothing here is part of the audit surface.
-/

namespace Hydrozoan

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [F : Faults Replica]

/-- The causal history of `b`, computed with `n` rounds of fuel. -/
def historyUpto (U : BlockUniverse Replica BlockId) :
    ℕ → BlockId → Finset BlockId
  | 0, b => {b}
  | n + 1, b => insert b ((U.block b).parents.biUnion (historyUpto U n))

/-- The causal history of `b`, as a `Finset`: fuel `round + 1` always
suffices (references descend one round per step). -/
def history (U : BlockUniverse Replica BlockId) (b : BlockId) :
    Finset BlockId :=
  historyUpto U ((U.block b).round + 1) b

variable {U : BlockUniverse Replica BlockId}

/-- Soundness: everything in the computed history is reachable. -/
theorem reaches_of_mem_historyUpto {n : ℕ} {b i : BlockId}
    (h : i ∈ historyUpto U n b) : Reaches U b i := by
  induction n generalizing b with
  | zero =>
      rw [historyUpto, Finset.mem_singleton] at h
      subst h
      exact Reaches.refl
  | succ n ih =>
      simp only [historyUpto, Finset.mem_insert, Finset.mem_biUnion] at h
      rcases h with rfl | ⟨j, hj, hi⟩
      · exact Reaches.refl
      · exact Reaches.of_mem_parents hj (ih hi)

/-- Completeness: with fuel at least the block's round, the computed
history contains everything reachable. -/
theorem mem_historyUpto_of_reaches {n : ℕ} {b i : BlockId} (hb : b ∈ U.ids)
    (hn : (U.block b).round ≤ n) (h : Reaches U b i) :
    i ∈ historyUpto U n b := by
  induction n generalizing b with
  | zero =>
      have hpar : (U.block b).parents = ∅ :=
        (U.valid b hb).parents_empty_of_round_zero (by omega)
      have := eq_of_reaches_of_parents_empty hpar h
      simp [historyUpto, this]
  | succ n ih =>
      rcases h.cases_head with rfl | ⟨j, hstep, hreach⟩
      · simp [historyUpto]
      · have hj : j ∈ U.ids := U.complete b hb j hstep
        have hround := round_of_mem_parents hb hstep
        simp only [historyUpto, Finset.mem_insert, Finset.mem_biUnion]
        exact Or.inr ⟨j, hstep, ih hj (by omega) hreach⟩

/-- The surrogate is faithful: for a universe member, membership of
`history` and reachability coincide. -/
theorem mem_history_iff {b i : BlockId} (hb : b ∈ U.ids) :
    i ∈ history U b ↔ Reaches U b i := by
  unfold history
  exact ⟨reaches_of_mem_historyUpto, mem_historyUpto_of_reaches hb (by omega)⟩

end Hydrozoan
