import Hydrozoan.Helpers.EventualDecision

/-!
# Proof: eventual decision

Generated. The two conjuncts come from the helpers; `ledgerProgress` is
the composed headline — "the ledger does not stall" — obtained by
feeding the run location `RunsRecur` produces into `RunDecidesBelow`.
-/

namespace Hydrozoan
namespace EventualDecision

theorem holds : Statement := by
  intro Replica BlockId _ _ _ _ _ _
  exact ⟨fun U => runDecidesBelow U, runsRecur Replica⟩

/-- **The ledger does not stall** (the composed corollary): under a fair
schedule, past every slot `k` and round `R` there is a bound `b` such
that any universe in which `T` is synchronised and fills the run's span
has every slot below `b` decided at the eventual view. -/
theorem ledgerProgress :
    ∀ (Replica BlockId : Type) [Fintype Replica] [DecidableEq Replica]
      [DecidableEq BlockId] [LinearOrder BlockId] [Faults Replica]
      [S : Slots Replica],
    ∀ (T : Finset Replica) (R k c : ℕ),
      T ⊆ (Correct : Finset Replica) → q Replica ≤ T.card →
      0 < c → IndirectLiveness.SpansEligible Replica c →
      FairRunOn Replica T c →
      ∃ b, k ≤ b ∧ R ≤ S.slotRound b ∧
        ∀ (U : BlockUniverse Replica BlockId),
          SynchronisedOn U T R →
          (∀ r, S.slotRound b ≤ r → r ≤ S.slotRound (b + c - 1) + 2 →
            PopulatedOn U T r) →
          ∀ i, i < b → ∃ v, Decided U (View.full U) i v := by
  intro Replica BlockId _ _ _ _ _ S T R k c hT hcard hc hspan hfair
  obtain ⟨b, hkb, hRb, hlead⟩ := runsRecur Replica T c k R hfair
  exact ⟨b, hkb, hRb, fun U hsync hpop =>
    runDecidesBelow U T R b c hT hcard hsync hc hspan hRb hlead hpop⟩

end EventualDecision
end Hydrozoan
