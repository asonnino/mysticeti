import Hydrozoan.DirectSafety.Statement
import Hydrozoan.Helpers.Counting
import Hydrozoan.Helpers.DirectRules

/-!
# Direct-rule safety — proof

Generated proof layer; not part of the audit surface. Each conjunct
lifts the view rules to the universe (Phase 4a bridges), overlaps two
author quorums in a non-Byzantine replica (`Helpers/Counting.lean`),
collapses its voting blocks through `no_equivocation`, and collapses
the two candidates through `distinct_authors`.
-/

namespace Hydrozoan

namespace DirectSafety

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [F : Faults Replica] [S : Slots Replica]
  {U : BlockUniverse Replica BlockId}

omit S in
/-- Universe-level fast/fast core. -/
theorem eq_of_fastCommit {L₁ L₂ : BlockId} {r : ℕ}
    (hauthor : (U.block L₁).author = (U.block L₂).author)
    (h₁ : FastCommit U L₁ r) (h₂ : FastCommit U L₂ r) : L₁ = L₂ := by
  by_contra hne
  have hsub : supporters U L₁ (r + 1) ∩ supporters U L₂ (r + 1) ⊆
      F.byzantine := by
    intro v hv
    obtain ⟨hv₁, hv₂⟩ := Finset.mem_inter.mp hv
    exact byzantine_of_votes_two hne hauthor hv₁ hv₂
  have h1 := Finset.card_union_add_card_inter
    (supporters U L₁ (r + 1)) (supporters U L₂ (r + 1))
  have h2 : (supporters U L₁ (r + 1) ∪ supporters U L₂ (r + 1)).card ≤
      Fintype.card Replica := by
    rw [← Finset.card_univ]; exact Finset.card_le_univ _
  have h3 := Finset.card_le_card hsub
  have h4 := F.card_byzantine
  have h5 := nf_lt_two_qFast (Replica := Replica)
  simp only [FastCommit] at h₁ h₂
  omega

omit S in
/-- Universe-level certificate-uniqueness core. -/
theorem eq_of_certificates_nonempty {L₁ L₂ : BlockId} {r : ℕ}
    (hauthor : (U.block L₁).author = (U.block L₂).author)
    (h₁ : (certificates U L₁ r).Nonempty)
    (h₂ : (certificates U L₂ r).Nonempty) : L₁ = L₂ := by
  obtain ⟨C₁, hC₁⟩ := h₁
  obtain ⟨C₂, hC₂⟩ := h₂
  obtain ⟨hC₁i, hC₁r, hcert₁⟩ := mem_certificates.mp hC₁
  obtain ⟨hC₂i, hC₂r, hcert₂⟩ := mem_certificates.mp hC₂
  obtain ⟨b, hb₁, hb₂⟩ :=
    exists_common_mem_of_author_quorums (s := voteBlocks U C₁ L₁)
      (t := voteBlocks U C₂ L₂) (r := r + 1)
      (fun b hb => ⟨(mem_voteBlocks_spec hC₁i hC₁r hb).1,
        (mem_voteBlocks_spec hC₁i hC₁r hb).2.1⟩)
      (fun b hb => ⟨(mem_voteBlocks_spec hC₂i hC₂r hb).1,
        (mem_voteBlocks_spec hC₂i hC₂r hb).2.1⟩)
      (by
        have h5 := nf_lt_two_qCert (Replica := Replica)
        simp only [IsCertificate] at hcert₁ hcert₂
        omega)
  have hbids : b ∈ U.ids := (mem_voteBlocks_spec hC₁i hC₁r hb₁).1
  exact (U.valid b hbids).distinct_authors
    L₁ (mem_voteBlocks_spec hC₁i hC₁r hb₁).2.2
    L₂ (mem_voteBlocks_spec hC₂i hC₂r hb₂).2.2 hauthor

omit S in
/-- Universe-level fast/slow core. -/
theorem eq_of_fastCommit_of_slowCommit {L₁ L₂ : BlockId} {r : ℕ}
    (hauthor : (U.block L₁).author = (U.block L₂).author)
    (h₁ : FastCommit U L₁ r) (h₂ : SlowCommit U L₂ r) : L₁ = L₂ := by
  by_contra hne
  obtain ⟨C, hC⟩ := certificates_nonempty_of_slowCommit h₂
  obtain ⟨hCi, hCr, hcert⟩ := mem_certificates.mp hC
  have hcard2 : qCert Replica ≤ (supporters U L₂ (r + 1)).card := by
    have hle := Finset.card_le_card
      (authors_voteBlocks_subset_supporters (L := L₂) hCi hCr)
    simp only [IsCertificate] at hcert
    omega
  have hsub : supporters U L₁ (r + 1) ∩ supporters U L₂ (r + 1) ⊆
      F.byzantine := by
    intro v hv
    obtain ⟨hv₁, hv₂⟩ := Finset.mem_inter.mp hv
    exact byzantine_of_votes_two hne hauthor hv₁ hv₂
  have h1 := Finset.card_union_add_card_inter
    (supporters U L₁ (r + 1)) (supporters U L₂ (r + 1))
  have h2 : (supporters U L₁ (r + 1) ∪ supporters U L₂ (r + 1)).card ≤
      Fintype.card Replica := by
    rw [← Finset.card_univ]; exact Finset.card_le_univ _
  have h3 := Finset.card_le_card hsub
  have h4 := F.card_byzantine
  have h5 := nf_lt_qFast_add_qCert (Replica := Replica)
  simp only [FastCommit] at h₁
  omega

/-- Universe-level fast-commit/skip exclusion core. -/
theorem not_skippedLeader_of_fastCommit {k : ℕ} {L : BlockId}
    (hL : IsLeaderBlock U k L) (h : FastCommit U L (S.slotRound k)) :
    ¬ SkippedLeader U k := by
  intro hskip
  have h' : qFast Replica ≤ (supporters U L (votingRound Replica k)).card := h
  have hsub : supporters U L (votingRound Replica k) ∩ blames U k ⊆
      F.byzantine := by
    intro v hv
    obtain ⟨hv₁, hv₂⟩ := Finset.mem_inter.mp hv
    exact byzantine_of_votes_and_blames hL hv₁ hv₂
  have h1 := Finset.card_union_add_card_inter
    (supporters U L (votingRound Replica k)) (blames U k)
  have h2 : (supporters U L (votingRound Replica k) ∪ blames U k).card ≤
      Fintype.card Replica := by
    rw [← Finset.card_univ]; exact Finset.card_le_univ _
  have h3 := Finset.card_le_card hsub
  have h4 := F.card_byzantine
  have h5 := nf_lt_two_qFast (Replica := Replica)
  simp only [SkippedLeader] at hskip
  omega

/-- Universe-level slow-commit/skip exclusion core. -/
theorem not_skippedLeader_of_slowCommit {k : ℕ} {L : BlockId}
    (hL : IsLeaderBlock U k L) (h : SlowCommit U L (S.slotRound k)) :
    ¬ SkippedLeader U k := by
  intro hskip
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
      F.byzantine := by
    intro v hv
    obtain ⟨hv₁, hv₂⟩ := Finset.mem_inter.mp hv
    exact byzantine_of_votes_and_blames hL hv₁ hv₂
  have h1 := Finset.card_union_add_card_inter
    (supporters U L (votingRound Replica k)) (blames U k)
  have h2 : (supporters U L (votingRound Replica k) ∪ blames U k).card ≤
      Fintype.card Replica := by
    rw [← Finset.card_univ]; exact Finset.card_le_univ _
  have h3 := Finset.card_le_card hsub
  have h4 := F.card_byzantine
  have h5 := nf_lt_qFast_add_qCert (Replica := Replica)
  simp only [SkippedLeader] at hskip
  omega

theorem holds : Statement := by
  intro Replica BlockId _ _ _ _ _ U
  refine ⟨?_, ?_, ?_, ?_, ?_⟩
  · intro V₁ V₂ k L₁ L₂ hL₁ hL₂ h₁ h₂
    exact eq_of_fastCommit (by rw [hL₁.2.2, hL₂.2.2])
      (fastCommit_of_fastCommitInView h₁) (fastCommit_of_fastCommitInView h₂)
  · intro k L₁ L₂ hL₁ hL₂ h₁ h₂
    exact eq_of_certificates_nonempty (by rw [hL₁.2.2, hL₂.2.2]) h₁ h₂
  · intro V₁ V₂ k L₁ L₂ hL₁ hL₂ h₁ h₂
    exact eq_of_certificates_nonempty (by rw [hL₁.2.2, hL₂.2.2])
      (certificates_nonempty_of_slowCommit (slowCommit_of_slowCommitInView h₁))
      (certificates_nonempty_of_slowCommit (slowCommit_of_slowCommitInView h₂))
  · intro V₁ V₂ k L₁ L₂ hL₁ hL₂ h₁ h₂
    exact eq_of_fastCommit_of_slowCommit (by rw [hL₁.2.2, hL₂.2.2])
      (fastCommit_of_fastCommitInView h₁) (slowCommit_of_slowCommitInView h₂)
  · intro V₁ V₂ k L hL hcommit hskip
    have hsk := skippedLeader_of_skippedLeaderInView hskip
    rcases hcommit with h | h
    · exact not_skippedLeader_of_fastCommit hL
        (fastCommit_of_fastCommitInView h) hsk
    · exact not_skippedLeader_of_slowCommit hL
        (slowCommit_of_slowCommitInView h) hsk

end DirectSafety

end Hydrozoan
