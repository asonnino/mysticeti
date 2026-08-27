import Hydrozoan.Optimal.EventualDecision.Statement
import Hydrozoan.Optimal.DirectLiveness.Proof
import Hydrozoan.Optimal.Helpers.IndirectLiveness
import Hydrozoan.EventualDecision.Proof

/-!
# Optimal-Hydrozoan: eventual decision — proof

Generated proof layer; not part of the audit surface. The composition of
`Optimal.DirectLiveness.holds` (each run slot commits) with the descent
`decidedOpt_below_of_committed_run`; `RunsRecur` is Hydrozoan's theorem,
reused. `ledgerProgress` is the composed headline, as in Hydrozoan.
-/

namespace Hydrozoan

namespace Optimal

namespace EventualDecision

open Hydrozoan.IndirectLiveness (SpansEligible)
open Hydrozoan.EventualDecision (FairRunOn RunsRecur)

variable {Replica BlockId : Type} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica] [S : Slots Replica]

/-- The composition: direct liveness commits each run slot, and the
indirect descent settles every slot below the run. -/
theorem runDecidesBelow (U : OptUniverse Replica BlockId) : RunDecidesBelow U := by
  intro T R b c hT hcard hsync hc hspan hRb hlead hpop i hi
  have hrun : ∀ j, b ≤ j → j ≤ b + c - 1 →
      ∃ B, DecidedOpt U (View.full U.toBlockUniverse) j (some B) := by
    intro j h1 h2
    have hleadj : S.leader j ∈ T := by
      have := hlead (j - b) (by omega)
      rwa [Nat.add_sub_cancel' h1] at this
    have hRj : R ≤ S.slotRound j := le_trans hRb (S.mono h1)
    have hbj : S.slotRound b ≤ S.slotRound j := S.mono h1
    have hjn : S.slotRound j ≤ S.slotRound (b + c - 1) := S.mono h2
    obtain ⟨L, -, -, hdec⟩ :=
      (Optimal.DirectLiveness.holds Replica BlockId U).1 T R j hT hcard hsync hRj
        (hpop _ hbj (by omega)) (hpop _ (by omega) (by omega))
        (hpop _ (by omega) (by omega)) hleadj
    exact ⟨L, hdec⟩
  exact decidedOpt_below_of_committed_run (by omega)
    (fun i' hi' => hspan b i' hi') hrun i hi

theorem holds : Statement := by
  intro Replica BlockId _ _ _ _ _
  exact ⟨fun U => runDecidesBelow U, Hydrozoan.EventualDecision.runsRecur Replica⟩

/-- **The ledger does not stall** (the composed corollary): under a fair
schedule, past every slot `k` and round `R` there is a bound `b` such
that any Optimal universe in which `T` is synchronised and fills the
run's span has every slot below `b` decided at the eventual view. -/
theorem ledgerProgress :
    ∀ (Replica BlockId : Type) [Fintype Replica] [DecidableEq Replica]
      [DecidableEq BlockId] [OptimalFaults Replica] [S : Slots Replica],
    ∀ (T : Finset Replica) (R k c : ℕ),
      T ⊆ (Correct : Finset Replica) → q Replica ≤ T.card →
      0 < c → SpansEligible Replica c →
      FairRunOn Replica T c →
      ∃ b, k ≤ b ∧ R ≤ S.slotRound b ∧
        ∀ (U : OptUniverse Replica BlockId),
          SynchronisedOn U.toBlockUniverse T R →
          (∀ r, S.slotRound b ≤ r → r ≤ S.slotRound (b + c - 1) + 2 →
            PopulatedOn U.toBlockUniverse T r) →
          ∀ i, i < b → ∃ v, DecidedOpt U (View.full U.toBlockUniverse) i v := by
  intro Replica BlockId _ _ _ _ S T R k c hT hcard hc hspan hfair
  obtain ⟨b, hkb, hRb, hlead⟩ := Hydrozoan.EventualDecision.runsRecur Replica T c k R hfair
  exact ⟨b, hkb, hRb, fun U hsync hpop =>
    runDecidesBelow U T R b c hT hcard hsync hc hspan hRb hlead hpop⟩

end EventualDecision

end Optimal

end Hydrozoan
