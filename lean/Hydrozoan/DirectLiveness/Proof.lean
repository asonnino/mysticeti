import Hydrozoan.DirectLiveness.Statement
import Hydrozoan.Helpers.DirectLiveness

/-!
# Direct-commit liveness — proof

Generated proof layer; not part of the audit surface. Both conjuncts
are the wave chain of `Helpers/DirectLiveness.lean`; the harvest form
adds the eventual-view lift and `Decided.directSlow`. `fastLatency` is
the demoted performance characterization, proven with the same care.
-/

namespace Hydrozoan

namespace DirectLiveness

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [LinearOrder BlockId] [F : Faults Replica]
  [S : Slots Replica] {U : BlockUniverse Replica BlockId}

theorem holds : Statement := by
  intro Replica BlockId _ _ _ _ _ _ U T R k hT hcard hs hRk hpop0 hpop1 hpop2 hlead
  obtain ⟨L, hL⟩ := exists_isLeaderBlock_of_populated hpop0 hlead
  have hslow := slowCommit_of_synchronised hcard hs hRk hpop1 hpop2 hL
    (by rw [hL.2.2]; exact hlead)
  exact ⟨L, hL, hslow,
    Decided.directSlow hL (slowCommitInView_full_of_slowCommit hslow)⟩

theorem fastLatency :
    ∀ (Replica BlockId : Type) [Fintype Replica] [DecidableEq Replica]
      [DecidableEq BlockId] [LinearOrder BlockId] [Faults Replica]
      [Slots Replica] (U : BlockUniverse Replica BlockId),
      FastLatency U := by
  intro Replica BlockId _ _ _ _ _ _ U R k hfaults hs hRk hpop0 hpop1 hlead
  obtain ⟨L, hL⟩ := exists_isLeaderBlock_of_populated hpop0 hlead
  refine ⟨L, hL, ?_⟩
  have hsub := subset_supporters_of_synchronised hs hRk hpop1 hL
    (by rw [hL.2.2]; exact hlead)
  have h1 := Finset.card_le_card hsub
  have h2 := qFast_le_card_correct hfaults
  simp only [FastCommit]
  omega

theorem skipLatency :
    ∀ (Replica BlockId : Type) [Fintype Replica] [DecidableEq Replica]
      [DecidableEq BlockId] [LinearOrder BlockId] [Faults Replica]
      [Slots Replica] (U : BlockUniverse Replica BlockId),
      SkipLatency U := by
  intro Replica BlockId _ _ _ _ _ _ U k hfaults hpop hnolead
  have hsub : (Correct : Finset Replica) ⊆ blames U k := by
    intro v hv
    obtain ⟨b, hb, hbr, hba⟩ := hpop v hv
    exact mem_blames.mpr ⟨b, hb, hbr, fun j _ => hnolead j, hba⟩
  have h1 := Finset.card_le_card hsub
  have h2 := qFast_le_card_correct hfaults
  simp only [SkippedLeader]
  omega

end DirectLiveness

end Hydrozoan
