import Hydrozoan.Optimal.DirectSafety.Statement
import Hydrozoan.Optimal.Helpers.Counting
import Hydrozoan.DirectSafety.Proof

/-!
# Optimal-Hydrozoan: direct-rule safety — proof

Generated proof layer; not part of the audit surface. The inherited rows
are Hydrozoan's `DirectSafety.holds` on the underlying universe. The
Optimal rows follow Hydrozoan's proofs with `qFastOpt` in place of
`qFast`: overlap two author quorums in a non-Byzantine replica
(`Helpers/Counting.lean`), collapse its voting blocks through
`no_equivocation`, and collapse the two candidates through
`distinct_authors`. Fast/fast at `f = 0` is non-equivocation of the
leader directly.
-/

namespace Hydrozoan

namespace Optimal

namespace DirectSafety

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica] [S : Slots Replica]
  {U : BlockUniverse Replica BlockId}

omit S in
/-- Universe-level fast/fast core, given `f ≥ 1`. -/
theorem eq_of_fastCommitOpt {L₁ L₂ : BlockId} {r : ℕ} (hf : 1 ≤ O.f)
    (hauthor : (U.block L₁).author = (U.block L₂).author)
    (h₁ : FastCommitOpt U L₁ r) (h₂ : FastCommitOpt U L₂ r) : L₁ = L₂ := by
  by_contra hne
  have hsub : supporters U L₁ (r + 1) ∩ supporters U L₂ (r + 1) ⊆
      O.byzantine := by
    intro v hv
    obtain ⟨hv₁, hv₂⟩ := Finset.mem_inter.mp hv
    exact byzantine_of_votes_two hne hauthor hv₁ hv₂
  have h1 := Finset.card_union_add_card_inter
    (supporters U L₁ (r + 1)) (supporters U L₂ (r + 1))
  have h2 : (supporters U L₁ (r + 1) ∪ supporters U L₂ (r + 1)).card ≤
      Fintype.card Replica := by
    rw [← Finset.card_univ]; exact Finset.card_le_univ _
  have h3 := Finset.card_le_card hsub
  have h4 := O.card_byzantine
  have h5 := nf_lt_two_qFastOpt (Replica := Replica) hf
  simp only [FastCommitOpt] at h₁ h₂
  omega

omit S in
/-- Universe-level fast/slow core. -/
theorem eq_of_fastCommitOpt_of_slowCommit {L₁ L₂ : BlockId} {r : ℕ}
    (hauthor : (U.block L₁).author = (U.block L₂).author)
    (h₁ : FastCommitOpt U L₁ r) (h₂ : SlowCommit U L₂ r) : L₁ = L₂ := by
  by_contra hne
  obtain ⟨C, hC⟩ := certificates_nonempty_of_slowCommit h₂
  obtain ⟨hCi, hCr, hcert⟩ := mem_certificates.mp hC
  have hcard2 : qCert Replica ≤ (supporters U L₂ (r + 1)).card := by
    have hle := Finset.card_le_card
      (authors_voteBlocks_subset_supporters (L := L₂) hCi hCr)
    simp only [IsCertificate] at hcert
    omega
  have hsub : supporters U L₁ (r + 1) ∩ supporters U L₂ (r + 1) ⊆
      O.byzantine := by
    intro v hv
    obtain ⟨hv₁, hv₂⟩ := Finset.mem_inter.mp hv
    exact byzantine_of_votes_two hne hauthor hv₁ hv₂
  have h1 := Finset.card_union_add_card_inter
    (supporters U L₁ (r + 1)) (supporters U L₂ (r + 1))
  have h2 : (supporters U L₁ (r + 1) ∪ supporters U L₂ (r + 1)).card ≤
      Fintype.card Replica := by
    rw [← Finset.card_univ]; exact Finset.card_le_univ _
  have h3 := Finset.card_le_card hsub
  have h4 := O.card_byzantine
  have h5 := nf_lt_qFastOpt_add_qCert (Replica := Replica)
  simp only [FastCommitOpt] at h₁
  omega

/-- Universe-level: a fast commit leaves fewer than `qCert` blames. -/
theorem blames_lt_of_fastCommitOpt {k : ℕ} {L : BlockId}
    (hL : IsLeaderBlock U k L) (h : FastCommitOpt U L (S.slotRound k)) :
    (blames U k).card < qCert Replica := by
  have h' : qFastOpt Replica ≤ (supporters U L (votingRound Replica k)).card := h
  have hsub : supporters U L (votingRound Replica k) ∩ blames U k ⊆
      O.byzantine := by
    intro v hv
    obtain ⟨hv₁, hv₂⟩ := Finset.mem_inter.mp hv
    exact byzantine_of_votes_and_blames hL hv₁ hv₂
  have h1 := Finset.card_union_add_card_inter
    (supporters U L (votingRound Replica k)) (blames U k)
  have h2 : (supporters U L (votingRound Replica k) ∪ blames U k).card ≤
      Fintype.card Replica := by
    rw [← Finset.card_univ]; exact Finset.card_le_univ _
  have h3 := Finset.card_le_card hsub
  have h4 := O.card_byzantine
  have h5 := nf_lt_qFastOpt_add_qCert (Replica := Replica)
  omega

/-- Universe-level: a slow commit leaves fewer than `qCert` blames. -/
theorem blames_lt_of_slowCommit {k : ℕ} {L : BlockId}
    (hL : IsLeaderBlock U k L) (h : SlowCommit U L (S.slotRound k)) :
    (blames U k).card < qCert Replica := by
  obtain ⟨C, hC⟩ := certificates_nonempty_of_slowCommit h
  obtain ⟨hCi, hCr, hcert⟩ := mem_certificates.mp hC
  have hcard2 : qCert Replica ≤
      (supporters U L (votingRound Replica k)).card := by
    have hle := Finset.card_le_card
      (authors_voteBlocks_subset_supporters (L := L) hCi hCr)
    simp only [IsCertificate] at hcert
    have : votingRound Replica k = S.slotRound k + 1 := rfl
    rw [this]
    omega
  have hsub : supporters U L (votingRound Replica k) ∩ blames U k ⊆
      O.byzantine := by
    intro v hv
    obtain ⟨hv₁, hv₂⟩ := Finset.mem_inter.mp hv
    exact byzantine_of_votes_and_blames hL hv₁ hv₂
  have h1 := Finset.card_union_add_card_inter
    (supporters U L (votingRound Replica k)) (blames U k)
  have h2 : (supporters U L (votingRound Replica k) ∪ blames U k).card ≤
      Fintype.card Replica := by
    rw [← Finset.card_univ]; exact Finset.card_le_univ _
  have h3 := Finset.card_le_card hsub
  have h4 := O.card_byzantine
  have h5 := nf_lt_two_qCert (Replica := Replica)
  omega

theorem holds : Statement := by
  intro Replica BlockId _ _ _ O _ U
  have hbase := Hydrozoan.DirectSafety.holds Replica BlockId U.toBlockUniverse
  refine ⟨?_, hbase.2.1, hbase.2.2.1, ?_, ?_⟩
  · intro V₁ V₂ k L₁ L₂ hL₁ hL₂ h₁ h₂
    by_cases hf : 1 ≤ O.f
    · exact eq_of_fastCommitOpt hf (by rw [hL₁.2.2, hL₂.2.2])
        (fastCommitOpt_of_fastCommitOptInView h₁)
        (fastCommitOpt_of_fastCommitOptInView h₂)
    · have hf0 : O.f = 0 := by omega
      have hempty := byzantine_eq_empty_of_f_eq_zero (Replica := Replica) hf0
      have hnb : (U.block L₁).author ∈ (NonByzantine : Finset Replica) := by
        rw [mem_nonByzantine, hempty]
        exact Finset.notMem_empty _
      exact U.no_equivocation L₁ hL₁.1 L₂ hL₂.1 hnb (by rw [hL₁.2.2, hL₂.2.2])
        (by rw [hL₁.2.1, hL₂.2.1])
  · intro V₁ V₂ k L₁ L₂ hL₁ hL₂ h₁ h₂
    exact eq_of_fastCommitOpt_of_slowCommit (by rw [hL₁.2.2, hL₂.2.2])
      (fastCommitOpt_of_fastCommitOptInView h₁) (slowCommit_of_slowCommitInView h₂)
  · intro V₁ V₂ k L hL hcommit hskip
    have hb := qCert_le_blames_of_skippedLeaderOptInView hskip
    rcases hcommit with h | h
    · exact absurd hb (Nat.not_le.mpr
        (blames_lt_of_fastCommitOpt hL (fastCommitOpt_of_fastCommitOptInView h)))
    · exact absurd hb (Nat.not_le.mpr
        (blames_lt_of_slowCommit hL (slowCommit_of_slowCommitInView h)))

end DirectSafety

end Optimal

end Hydrozoan
