import Hydrozoan.Optimal.DirectLiveness.Statement
import Hydrozoan.Optimal.Helpers.DirectLiveness

/-!
# Optimal-Hydrozoan: direct liveness — proof

Generated proof layer; not part of the audit surface. `CommitLiveness`
is Hydrozoan's wave chain harvested as `DecidedOpt.directSlow`;
`SkipLiveness` is the guaranteed skip of `Optimal/Helpers/DirectLiveness.lean`
harvested as `DecidedOpt.directSkip`; `fastLatency` is Hydrozoan's
argument with `qFastOpt`.
-/

namespace Hydrozoan

namespace Optimal

namespace DirectLiveness

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica] [S : Slots Replica]

theorem holds : Statement := by
  intro Replica BlockId _ _ _ _ _ U
  refine ⟨?_, ?_⟩
  · intro T R k hT hcard hs hRk hpop0 hpop1 hpop2 hlead
    obtain ⟨L, hL⟩ := exists_isLeaderBlock_of_populated hpop0 hlead
    have hslow := slowCommit_of_synchronised hcard hs hRk hpop1 hpop2 hL
      (by rw [hL.2.2]; exact hlead)
    exact ⟨L, hL, hslow,
      DecidedOpt.directSlow hL (slowCommitInView_full_of_slowCommit hslow)⟩
  · intro T k hT hcard hpop1 hpop2 hnolead
    have hskip := skippedLeaderOptInView_full_of_populated hcard hpop1 hpop2 hnolead
    exact ⟨skippedLeaderOpt_of_skippedLeaderOptInView hskip, DecidedOpt.directSkip hskip⟩

theorem fastLatency :
    ∀ (Replica BlockId : Type) [Fintype Replica] [DecidableEq Replica]
      [DecidableEq BlockId] [OptimalFaults Replica] [Slots Replica]
      (U : OptUniverse Replica BlockId),
      FastLatency U := by
  intro Replica BlockId _ _ _ _ _ U R k hfaults hs hRk hpop0 hpop1 hlead
  obtain ⟨L, hL⟩ := exists_isLeaderBlock_of_populated hpop0 hlead
  refine ⟨L, hL, ?_⟩
  have hsub := subset_supporters_of_synchronised hs hRk hpop1 hL
    (by rw [hL.2.2]; exact hlead)
  have h1 := Finset.card_le_card hsub
  have h2 := qFastOpt_le_card_correct hfaults
  simp only [FastCommitOpt]
  omega

end DirectLiveness

end Optimal

end Hydrozoan
