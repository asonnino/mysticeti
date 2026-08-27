import Hydrozoan.EventualDecision.Statement
import Hydrozoan.DirectLiveness.Proof
import Hydrozoan.Helpers.IndirectLiveness

/-!
# Helpers: eventual decision

Generated: the two conjuncts of the eventual-decision statement. The
run-commits lemma works at `Type` (it invokes the direct-liveness
headline, which quantifies over `Type`); the schedule lemma is
universe-polymorphic.
-/

namespace Hydrozoan
namespace EventualDecision

/-- Fairness places a run past any slot and round: pick a slot `k₀`
whose round reaches `R` (`Slots.unbounded`), ask fairness for a run past
`max k k₀`, and monotonicity carries both bounds. -/
theorem runsRecur (Replica : Type*) [S : Slots Replica] :
    RunsRecur Replica := by
  intro T c k R hfair
  obtain ⟨k₀, hk₀⟩ := S.unbounded R
  obtain ⟨b, hb, hlead⟩ := hfair (max k k₀)
  exact ⟨b, le_trans (le_max_left _ _) hb,
    le_trans hk₀ (S.mono (le_trans (le_max_right _ _) hb)), hlead⟩

variable {Replica BlockId : Type} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [LinearOrder BlockId] [F : Faults Replica]
  [S : Slots Replica]

/-- The composition: direct liveness commits each run slot (its round,
population, and leader hypotheses all restrict from the run-wide ones by
schedule monotonicity), and the indirect descent settles every slot
below the run. -/
theorem runDecidesBelow (U : BlockUniverse Replica BlockId) :
    RunDecidesBelow U := by
  intro T R b c hT hcard hsync hc hspan hRb hlead hpop i hi
  have hrun : ∀ j, b ≤ j → j ≤ b + c - 1 →
      ∃ B, Decided U (View.full U) j (some B) := by
    intro j h1 h2
    -- The slot's leader is in T: it is the (j - b)-th slot of the run.
    have hleadj : S.leader j ∈ T := by
      have := hlead (j - b) (by omega)
      rwa [Nat.add_sub_cancel' h1] at this
    -- Round bounds for the slot's three-round wave, from monotonicity.
    have hRj : R ≤ S.slotRound j := le_trans hRb (S.mono h1)
    have hbj : S.slotRound b ≤ S.slotRound j := S.mono h1
    have hjn : S.slotRound j ≤ S.slotRound (b + c - 1) := S.mono h2
    obtain ⟨L, -, -, hdec⟩ :=
      DirectLiveness.holds Replica BlockId U T R j hT hcard hsync hRj
        (hpop _ hbj (by omega)) (hpop _ (by omega) (by omega))
        (hpop _ (by omega) (by omega)) hleadj
    exact ⟨L, hdec⟩
  exact decided_below_of_committed_run (by omega)
    (fun i' hi' => hspan b i' hi') hrun i hi

end EventualDecision
end Hydrozoan
