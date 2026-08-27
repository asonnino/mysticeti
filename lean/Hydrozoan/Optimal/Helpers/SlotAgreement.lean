import Hydrozoan.Optimal.Model.Decided
import Hydrozoan.Optimal.Helpers.Counting
import Hydrozoan.Optimal.Helpers.Decided
import Hydrozoan.Optimal.DirectSafety.Proof
import Hydrozoan.Helpers.SlotAgreement

/-!
# Optimal-Hydrozoan: the seam lemmas

Generated proof infrastructure for slot agreement; not part of the audit
surface. Hydrozoan's seam (`Helpers/SlotAgreement.lean`) with the weak
rung replaced by the evidence rung:

* the crown lemma — a fast commit makes every decision-round block fast
  evidence for the committed block (the paper's `lem:opt-fast-evidence`;
  the witnessing case is where `OptUniverse.leader_excluded` enters);
* exclusivity — a decision-round block is evidence for one candidate at
  most (`lem:opt-exclusive`);
* rung 2 fires — every block from round `r + 3` on reaches an evidence
  quorum for a fast-committed block (`lem:opt-fast-propagation`);
* starvation and skip — no certificate and no evidence quorum for a
  rival of a fast-committed block, nor for any candidate of a directly
  skipped slot (`lem:opt-commit-excludes-direct-skip`);
* uniqueness at an anchor — two evidence quorums name one candidate
  (`lem:opt-evidence-unique`), which is what lets the evidence rung go
  without a tie-break;
* the at-anchor wrappers for `DecidedOpt`.
-/

namespace Hydrozoan

namespace Optimal

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica]

omit [DecidableEq BlockId] in
/-- `1 ≤ t_plain`, from the committee bound and `f + c ≥ 1`. -/
theorem tPlain_pos : 1 ≤ tPlain Replica := by
  have hn := O.card_replicas
  have hnt := O.nontrivial
  simp only [tPlain, pOpt, p]
  omega

omit [DecidableEq BlockId] in
/-- `q_fast + q = n + f + t_plain` (the `EvidencePlain` row). -/
theorem qFastOpt_add_q_eq :
    qFastOpt Replica + q Replica = Fintype.card Replica + O.f + tPlain Replica := by
  have hn := O.card_replicas
  simp only [tPlain, qFastOpt, q, pOpt, p]
  omega

omit [DecidableEq BlockId] in
/-- `n + f + t_equiv ≤ q_fast + q + 1` (the `EvidenceEquiv` row). -/
theorem nf_add_tEquiv_le :
    Fintype.card Replica + O.f + tEquiv Replica ≤ qFastOpt Replica + q Replica + 1 := by
  have hn := O.card_replicas
  simp only [tEquiv, qFastOpt, q, pOpt, p]
  omega

omit [DecidableEq BlockId] in
/-- `q_cert ≤ q` (the `SlowCollectible` row). -/
theorem qCert_le_q_opt : qCert Replica ≤ q Replica := by
  have hn := O.card_replicas
  simp only [qCert, q]
  omega

omit [DecidableEq BlockId] in
/-- `1 ≤ q_cert`. -/
theorem qCert_pos_opt : 1 ≤ qCert Replica := by
  simp only [qCert]; omega

section Universe

variable {B : BlockUniverse Replica BlockId}

/-- Membership in `votesFor`, unfolded. -/
theorem mem_votesFor {C L : BlockId} {v : Replica} :
    v ∈ votesFor B C L ↔
      ∃ p ∈ (B.block C).parents, IsVote B p L ∧ (B.block p).author = v := by
  simp only [votesFor, voteBlocks, mem_authorsOf, Finset.mem_filter]
  tauto

/-- A view's fast commit lifts to the universe, then to the fast/fast
core with the `f = 0` branch by non-equivocation. -/
theorem eq_of_fastCommitOpt_leader [S : Slots Replica] {k : ℕ} {L₁ L₂ : BlockId}
    (hL₁ : IsLeaderBlock B k L₁) (hL₂ : IsLeaderBlock B k L₂)
    (h₁ : FastCommitOpt B L₁ (S.slotRound k)) (h₂ : FastCommitOpt B L₂ (S.slotRound k)) :
    L₁ = L₂ := by
  by_cases hf : 1 ≤ O.f
  · exact DirectSafety.eq_of_fastCommitOpt hf (by rw [hL₁.2.2, hL₂.2.2]) h₁ h₂
  · have hf0 : O.f = 0 := by omega
    have hempty := byzantine_eq_empty_of_f_eq_zero (Replica := Replica) hf0
    have hnb : (B.block L₁).author ∈ (NonByzantine : Finset Replica) := by
      rw [mem_nonByzantine, hempty]
      exact Finset.notMem_empty _
    exact B.no_equivocation L₁ hL₁.1 L₂ hL₂.1 hnb (by rw [hL₁.2.2, hL₂.2.2])
      (by rw [hL₁.2.1, hL₂.2.1])

variable [S : Slots Replica]

/-- A full skip seen in a view holds in the universe. -/
theorem skippedLeaderOpt_of_skippedLeaderOptInView {V : View B} {k : ℕ}
    (h : SkippedLeaderOptInView B V k) : SkippedLeaderOpt B k := by
  refine ⟨qCert_le_blames_of_skippedLeaderOptInView h, ?_⟩
  obtain ⟨s, hs, hcard⟩ := h.2
  exact ⟨s, fun b hb => ⟨(hs b hb).1, (hs b hb).2.2⟩, hcard⟩

omit [DecidableEq BlockId] in
/-- An equivocating leader is Byzantine: two distinct candidates of one
slot share author and round, which non-equivocation forbids for a
non-Byzantine author. -/
theorem leader_byzantine_of_witnesses {k : ℕ} {C : BlockId}
    (hw : WitnessesEquivocation B k C) : S.leader k ∈ O.byzantine := by
  obtain ⟨L₁, L₂, hL₁, hL₂, hne, -, -⟩ := hw
  by_contra hb
  have hnb : (B.block L₁).author ∈ (NonByzantine : Finset Replica) := by
    rw [mem_nonByzantine, hL₁.2.2]; exact hb
  exact hne (B.no_equivocation L₁ hL₁.1 L₂ hL₂.1 hnb (by rw [hL₁.2.2, hL₂.2.2])
    (by rw [hL₁.2.1, hL₂.2.1]))

/-- A non-Byzantine parent-author of `C` that supports `L` at the voting
round contributes its parent block as a vote for `L` in `C`. -/
theorem mem_votesFor_of_nonByzantine {k : ℕ} {C L : BlockId} {v : Replica}
    (hC : C ∈ B.ids) (hCr : (B.block C).round = decisionRound Replica k)
    (hvA : v ∈ authorsOf B.block (B.block C).parents)
    (hvS : v ∈ supporters B L (S.slotRound k + 1)) (hvnb : v ∉ O.byzantine) :
    v ∈ votesFor B C L := by
  obtain ⟨p', hp', hpc⟩ := mem_authorsOf.mp hvA
  obtain ⟨b, hbi, hbr, hbv, hbc⟩ := mem_supporters.mp hvS
  have hpi : p' ∈ B.ids := B.complete C hC p' hp'
  have hpr := round_of_mem_parents hC hp'
  have hnb : (B.block p').author ∈ (NonByzantine : Finset Replica) := by
    rw [mem_nonByzantine, hpc]; exact hvnb
  have hpb : p' = b :=
    B.no_equivocation p' hpi b hbi hnb (by rw [hpc, hbc])
      (by simp only [decisionRound] at hCr; omega)
  subst hpb
  exact mem_votesFor.mpr ⟨p', hp', hbv, hpc⟩

end Universe

section Crown

variable [S : Slots Replica] {U : OptUniverse Replica BlockId}

/-- **The crown lemma** (`lem:opt-fast-evidence`): if `q_fast` replicas
vote for `L`, every decision-round block of `L`'s slot is fast evidence
for `L`. In the witnessing case the equivocating leader is Byzantine and,
by `leader_excluded`, not among the block's parents, so at most `f − 1`
undetected Byzantine parents remain. -/
theorem isFastEvidence_of_fastCommitOpt {k : ℕ} {L C : BlockId}
    (hL : IsLeaderBlock U.toBlockUniverse k L)
    (h : FastCommitOpt U.toBlockUniverse L (S.slotRound k))
    (hC : C ∈ U.toBlockUniverse.ids)
    (hCr : (U.toBlockUniverse.block C).round = decisionRound Replica k) :
    IsFastEvidence U.toBlockUniverse k C L := by
  set B := U.toBlockUniverse with hB
  set A := authorsOf B.block (B.block C).parents with hA
  set V := supporters B L (S.slotRound k + 1) with hV
  have hq : q Replica ≤ A.card :=
    (B.valid C hC).quorum (by simp only [decisionRound] at hCr; omega)
  have hVc : qFastOpt Replica ≤ V.card := h
  have hinter := Finset.card_union_add_card_inter A V
  have huniv : (A ∪ V).card ≤ Fintype.card Replica := by
    rw [← Finset.card_univ]; exact Finset.card_le_univ _
  have hf := O.card_byzantine
  have hrow1 := qFastOpt_add_q_eq (Replica := Replica)
  have hrow2 := nf_add_tEquiv_le (Replica := Replica)
  refine ⟨fun _ => ?_, fun hw => ⟨?_, ?_⟩⟩
  · -- plain case: (A ∩ V) \ byzantine ⊆ votesFor
    have hsub : (A ∩ V) \ O.byzantine ⊆ votesFor B C L := by
      intro v hv
      obtain ⟨hvin, hvnb⟩ := Finset.mem_sdiff.mp hv
      obtain ⟨hvA, hvS⟩ := Finset.mem_inter.mp hvin
      exact mem_votesFor_of_nonByzantine hC hCr hvA hvS hvnb
    have hcard := Finset.card_le_card hsub
    have hsd := Finset.le_card_sdiff O.byzantine (A ∩ V)
    omega
  · -- witnessing case, lower bound: the leader is excluded
    have hlb := leader_byzantine_of_witnesses hw
    have hlA : S.leader k ∉ A := by
      intro hmem
      obtain ⟨j, hj, hjc⟩ := mem_authorsOf.mp hmem
      exact U.leader_excluded C hC k hCr hw j hj hjc
    have hsub : (A ∩ V) \ (O.byzantine.erase (S.leader k)) ⊆ votesFor B C L := by
      intro v hv
      obtain ⟨hvin, hvnb⟩ := Finset.mem_sdiff.mp hv
      obtain ⟨hvA, hvS⟩ := Finset.mem_inter.mp hvin
      have hvnb' : v ∉ O.byzantine := by
        intro hvb
        apply hvnb
        rw [Finset.mem_erase]
        exact ⟨fun heq => hlA (heq ▸ hvA), hvb⟩
      exact mem_votesFor_of_nonByzantine hC hCr hvA hvS hvnb'
    have hcard := Finset.card_le_card hsub
    have hsd := Finset.le_card_sdiff (O.byzantine.erase (S.leader k)) (A ∩ V)
    have herase := Finset.card_erase_of_mem hlb
    have hbpos : 1 ≤ O.byzantine.card := Finset.card_pos.mpr ⟨_, hlb⟩
    omega
  · -- witnessing case, rivals: their voters are outside V or Byzantine non-leaders
    intro L' hL' hne
    have hlb := leader_byzantine_of_witnesses hw
    have hlA : S.leader k ∉ A := by
      intro hmem
      obtain ⟨j, hj, hjc⟩ := mem_authorsOf.mp hmem
      exact U.leader_excluded C hC k hCr hw j hj hjc
    have hsub : votesFor B C L' ⊆ (A \ V) ∪ O.byzantine.erase (S.leader k) := by
      intro v hv
      obtain ⟨p', hp', hpv, hpc⟩ := mem_votesFor.mp hv
      have hvA : v ∈ A := mem_authorsOf.mpr ⟨p', hp', hpc⟩
      by_cases hvb : v ∈ O.byzantine
      · refine Finset.mem_union_right _ (Finset.mem_erase.mpr ⟨?_, hvb⟩)
        intro heq; exact hlA (heq ▸ hvA)
      · refine Finset.mem_union_left _ (Finset.mem_sdiff.mpr ⟨hvA, ?_⟩)
        intro hvS
        -- v's unique voting block is p', which votes both L and L'
        have hvL := mem_votesFor_of_nonByzantine hC hCr hvA hvS hvb
        obtain ⟨p'', hp'', hpv'', hpc''⟩ := mem_votesFor.mp hvL
        have hpi : p' ∈ B.ids := B.complete C hC p' hp'
        have hpi'' : p'' ∈ B.ids := B.complete C hC p'' hp''
        have hnb : (B.block p').author ∈ (NonByzantine : Finset Replica) := by
          rw [mem_nonByzantine, hpc]; exact hvb
        have hpp : p' = p'' :=
          B.no_equivocation p' hpi p'' hpi'' hnb (by rw [hpc, hpc''])
            (by
              have h1 := round_of_mem_parents hC hp'
              have h2 := round_of_mem_parents hC hp''
              omega)
        subst hpp
        exact hne ((B.valid p' hpi).distinct_authors L' hpv L hpv''
          (by rw [hL'.2.2, hL.2.2]))
    have hcard := Finset.card_le_card hsub
    have hun := Finset.card_union_le (A \ V) (O.byzantine.erase (S.leader k))
    have hsdV : (A \ V).card ≤ Fintype.card Replica - V.card := by
      have h1 : A \ V ⊆ Vᶜ := fun v hv => by
        rw [Finset.mem_compl]; exact (Finset.mem_sdiff.mp hv).2
      have h2 := Finset.card_le_card h1
      rw [Finset.card_compl] at h2
      exact h2
    have hVn : V.card ≤ Fintype.card Replica := Finset.card_le_univ _
    have herase := Finset.card_erase_of_mem hlb
    have hbpos : 1 ≤ O.byzantine.card := Finset.card_pos.mpr ⟨_, hlb⟩
    simp only [tEquiv, qFastOpt] at hVc ⊢
    omega

end Crown

section Seam

variable [S : Slots Replica] {B : BlockUniverse Replica BlockId}

/-- **Exclusivity** (`lem:opt-exclusive`): a decision-round block is fast
evidence for at most one candidate of its slot. -/
theorem isFastEvidence_exclusive {k : ℕ} {C L L' : BlockId}
    (hL : IsLeaderBlock B k L) (hL' : IsLeaderBlock B k L')
    (h : IsFastEvidence B k C L) (h' : IsFastEvidence B k C L') : L = L' := by
  by_cases hw : WitnessesEquivocation B k C
  · by_contra hne
    have h1 := (h.2 hw).2 L' hL' (Ne.symm hne)
    have h2 := (h'.2 hw).1
    omega
  · by_contra hne
    apply hw
    have h1 := h.1 hw
    have h2 := h'.1 hw
    have hpos := tPlain_pos (Replica := Replica)
    obtain ⟨v, hv⟩ := Finset.card_pos.mp (by omega : 0 < (votesFor B C L).card)
    obtain ⟨v', hv'⟩ := Finset.card_pos.mp (by omega : 0 < (votesFor B C L').card)
    obtain ⟨p, hp, hpv, -⟩ := mem_votesFor.mp hv
    obtain ⟨p', hp', hpv', -⟩ := mem_votesFor.mp hv'
    exact ⟨L, L', hL, hL', hne, ⟨p, hp, hpv⟩, ⟨p', hp', hpv'⟩⟩

/-- `EvidenceLinked` is inherited upward along reachability. -/
theorem evidenceLinked_of_reaches {A A' L : BlockId} {k : ℕ}
    (hAA : Reaches B A' A) (h : EvidenceLinked B A L k) : EvidenceLinked B A' L k := by
  obtain ⟨s, hs, hcard⟩ := h
  exact ⟨s, fun b hb =>
    ⟨(hs b hb).1, (hs b hb).2.1, Reaches.trans hAA (hs b hb).2.2⟩, hcard⟩

/-- **Rung 2 is unique** (`lem:opt-evidence-unique`): two evidence quorums
at one anchor share a non-Byzantine author, whose unique decision-round
block would be evidence for both candidates. -/
theorem evidenceLinked_unique {A L L' : BlockId} {k : ℕ}
    (hL : IsLeaderBlock B k L) (hL' : IsLeaderBlock B k L')
    (h : EvidenceLinked B A L k) (h' : EvidenceLinked B A L' k) : L = L' := by
  obtain ⟨s, hs, hcard⟩ := h
  obtain ⟨s', hs', hcard'⟩ := h'
  obtain ⟨b, hb, hb'⟩ := exists_common_mem_of_author_quorums (s := s) (t := s')
    (r := decisionRound Replica k)
    (fun b hb => mem_blocksAt.mp (hs b hb).1)
    (fun b hb => mem_blocksAt.mp (hs' b hb).1)
    (by have := nf_lt_two_qCert (Replica := Replica); omega)
  exact isFastEvidence_exclusive hL hL' (hs b hb).2.1 (hs' b hb').2.1

/-- **Skip clears rung 2** (`lem:opt-commit-excludes-direct-skip`): a
skipped slot's candidates have no evidence quorum anywhere — the
no-evidence quorum and any evidence quorum share a non-Byzantine author. -/
theorem not_evidenceLinked_of_skippedOpt {k : ℕ} {L A : BlockId}
    (hL : IsLeaderBlock B k L) (h : SkippedLeaderOpt B k) :
    ¬ EvidenceLinked B A L k := by
  rintro ⟨s, hs, hcard⟩
  obtain ⟨t, ht, htcard⟩ := h.2
  obtain ⟨b, hb, hbt⟩ := exists_common_mem_of_author_quorums (s := s) (t := t)
    (r := decisionRound Replica k)
    (fun b hb => mem_blocksAt.mp (hs b hb).1)
    (fun b hb => mem_blocksAt.mp (ht b hb).1)
    (by have := nf_lt_two_qCert (Replica := Replica); omega)
  exact (ht b hbt).2 L hL (hs b hb).2.1

/-- **Skip clears rung 1**: a skipped slot's candidates are never certified
— `q_cert` blames against the `q_cert` votes inside a certificate. -/
theorem not_certifiedIn_of_skippedOpt {k : ℕ} {L A : BlockId}
    (hL : IsLeaderBlock B k L) (h : SkippedLeaderOpt B k) :
    ¬ CertifiedIn B A L (S.slotRound k) := by
  rintro ⟨C, hC, -⟩
  obtain ⟨hCi, hCr, hcert⟩ := mem_certificates.mp hC
  have hcard2 : qCert Replica ≤ (supporters B L (votingRound Replica k)).card := by
    have hle := Finset.card_le_card
      (authors_voteBlocks_subset_supporters (L := L) hCi hCr)
    simp only [IsCertificate] at hcert
    have : votingRound Replica k = S.slotRound k + 1 := rfl
    rw [this]
    omega
  have hsub : supporters B L (votingRound Replica k) ∩ blames B k ⊆ O.byzantine := by
    intro v hv
    obtain ⟨hv₁, hv₂⟩ := Finset.mem_inter.mp hv
    exact byzantine_of_votes_and_blames hL hv₁ hv₂
  have h1 := Finset.card_union_add_card_inter
    (supporters B L (votingRound Replica k)) (blames B k)
  have h2 : (supporters B L (votingRound Replica k) ∪ blames B k).card ≤
      Fintype.card Replica := by
    rw [← Finset.card_univ]; exact Finset.card_le_univ _
  have h3 := Finset.card_le_card hsub
  have h4 := O.card_byzantine
  have h5 := nf_lt_two_qCert (Replica := Replica)
  have hb := h.1
  omega

/-- **Starvation, rung 1**: a fast commit leaves no certificate for any
same-slot rival. -/
theorem not_certifiedIn_of_fastCommitOpt {k : ℕ} {L L' A : BlockId}
    (hne : L' ≠ L) (hL : IsLeaderBlock B k L) (hL' : IsLeaderBlock B k L')
    (h : FastCommitOpt B L (S.slotRound k)) : ¬ CertifiedIn B A L' (S.slotRound k) := by
  rintro ⟨C, hC, -⟩
  obtain ⟨hCi, hCr, hcert⟩ := mem_certificates.mp hC
  have hcard2 : qCert Replica ≤ (supporters B L' (S.slotRound k + 1)).card := by
    have hle := Finset.card_le_card
      (authors_voteBlocks_subset_supporters (L := L') hCi hCr)
    simp only [IsCertificate] at hcert
    omega
  have hsub : supporters B L' (S.slotRound k + 1) ∩ supporters B L (S.slotRound k + 1) ⊆
      O.byzantine := by
    intro v hv
    obtain ⟨hv₁, hv₂⟩ := Finset.mem_inter.mp hv
    exact byzantine_of_votes_two hne (by rw [hL'.2.2, hL.2.2]) hv₁ hv₂
  have h1 := Finset.card_union_add_card_inter
    (supporters B L' (S.slotRound k + 1)) (supporters B L (S.slotRound k + 1))
  have h2 : (supporters B L' (S.slotRound k + 1) ∪ supporters B L (S.slotRound k + 1)).card ≤
      Fintype.card Replica := by
    rw [← Finset.card_univ]; exact Finset.card_le_univ _
  have h3 := Finset.card_le_card hsub
  have h4 := O.card_byzantine
  have h5 := nf_lt_qFastOpt_add_qCert (Replica := Replica)
  simp only [FastCommitOpt] at h
  omega

end Seam

section CrownConsequences

variable [S : Slots Replica] {U : OptUniverse Replica BlockId}

/-- **Starvation, rung 2**: a fast commit leaves no evidence quorum for any
same-slot rival — every decision-round block is evidence for the
committed block, hence for nothing else. -/
theorem not_evidenceLinked_of_fastCommitOpt {k : ℕ} {L L' A : BlockId}
    (hne : L' ≠ L) (hL : IsLeaderBlock U.toBlockUniverse k L)
    (hL' : IsLeaderBlock U.toBlockUniverse k L')
    (h : FastCommitOpt U.toBlockUniverse L (S.slotRound k)) :
    ¬ EvidenceLinked U.toBlockUniverse A L' k := by
  rintro ⟨s, hs, hcard⟩
  have hpos := qCert_pos_opt (Replica := Replica)
  obtain ⟨v, hv⟩ :=
    Finset.card_pos.mp (by omega : 0 < (authorsOf U.toBlockUniverse.block s).card)
  obtain ⟨b, hb, -⟩ := mem_authorsOf.mp hv
  obtain ⟨hbi, hbr⟩ := mem_blocksAt.mp (hs b hb).1
  exact hne (isFastEvidence_exclusive hL' hL (hs b hb).2.1
    (isFastEvidence_of_fastCommitOpt hL h hbi hbr))

private theorem evidenceLinked_of_fastCommitOpt_base {k : ℕ} {L : BlockId}
    (hL : IsLeaderBlock U.toBlockUniverse k L)
    (h : FastCommitOpt U.toBlockUniverse L (S.slotRound k)) {A : BlockId}
    (hA : A ∈ U.toBlockUniverse.ids)
    (hAr : (U.toBlockUniverse.block A).round = S.slotRound k + 3) :
    EvidenceLinked U.toBlockUniverse A L k := by
  refine ⟨(U.toBlockUniverse.block A).parents, fun b hb => ?_, ?_⟩
  · have hbi := U.toBlockUniverse.complete A hA b hb
    have hbr := round_of_mem_parents hA hb
    have hbr' : (U.toBlockUniverse.block b).round = decisionRound Replica k := by
      simp only [decisionRound]; omega
    exact ⟨mem_blocksAt.mpr ⟨hbi, hbr'⟩, isFastEvidence_of_fastCommitOpt hL h hbi hbr',
      Reaches.single hb⟩
  · have hq : q Replica ≤
        (authorsOf U.toBlockUniverse.block (U.toBlockUniverse.block A).parents).card :=
      (U.toBlockUniverse.valid A hA).quorum (by omega)
    have := qCert_le_q_opt (Replica := Replica)
    omega

private theorem evidenceLinked_of_fastCommitOpt_aux {k : ℕ} {L : BlockId}
    (hL : IsLeaderBlock U.toBlockUniverse k L)
    (h : FastCommitOpt U.toBlockUniverse L (S.slotRound k)) :
    ∀ d, ∀ A, A ∈ U.toBlockUniverse.ids →
      (U.toBlockUniverse.block A).round = S.slotRound k + 3 + d →
      EvidenceLinked U.toBlockUniverse A L k := by
  intro d
  induction d with
  | zero => exact fun A hA hAr => evidenceLinked_of_fastCommitOpt_base hL h hA hAr
  | succ d ih =>
      intro A hA hAr
      obtain ⟨b, hb⟩ := parents_nonempty hA (by omega)
      have hbi := U.toBlockUniverse.complete A hA b hb
      have hbr := round_of_mem_parents hA hb
      exact evidenceLinked_of_reaches (Reaches.single hb) (ih b hbi (by omega))

/-- **Rung 2 fires** (`lem:opt-fast-propagation`): every block from round
`r + 3` on reaches an evidence quorum for a fast-committed block. -/
theorem evidenceLinked_of_fastCommitOpt {k : ℕ} {L : BlockId}
    (hL : IsLeaderBlock U.toBlockUniverse k L)
    (h : FastCommitOpt U.toBlockUniverse L (S.slotRound k)) {A : BlockId}
    (hA : A ∈ U.toBlockUniverse.ids)
    (hAr : S.slotRound k + 3 ≤ (U.toBlockUniverse.block A).round) :
    EvidenceLinked U.toBlockUniverse A L k :=
  evidenceLinked_of_fastCommitOpt_aux hL h
    ((U.toBlockUniverse.block A).round - (S.slotRound k + 3)) A hA (by omega)

/-! ## At-anchor wrappers -/

variable {V W : View U.toBlockUniverse}

/-- The anchor's round, from its slot and eligibility. -/
theorem anchor_round_opt {k j : ℕ} {A : BlockId}
    (hj : DecidedOpt U W j (some A)) (helig : EligibleAsAnchor Replica k j) :
    S.slotRound k + 3 ≤ (U.toBlockUniverse.block A).round := by
  have hA := isLeaderBlock_of_decidedOpt hj
  rw [hA.2.1]
  exact (eligibleAsAnchor_iff Replica).mp helig

/-- A slow commit in any view is certified at every decided eligible
anchor. -/
theorem certifiedIn_of_slowCommitInView_at_anchor_opt {k j : ℕ} {L A : BlockId}
    (h : SlowCommitInView U.toBlockUniverse V L (S.slotRound k))
    (hj : DecidedOpt U W j (some A)) (helig : EligibleAsAnchor Replica k j) :
    CertifiedIn U.toBlockUniverse A L (S.slotRound k) :=
  certifiedIn_of_slowCommit (slowCommit_of_slowCommitInView h)
    (isLeaderBlock_of_decidedOpt hj).1 (anchor_round_opt hj helig)

/-- A fast commit in any view is evidence-linked at every decided eligible
anchor. -/
theorem evidenceLinked_of_fastCommitOptInView_at_anchor {k j : ℕ} {L A : BlockId}
    (hL : IsLeaderBlock U.toBlockUniverse k L)
    (h : FastCommitOptInView U.toBlockUniverse V L (S.slotRound k))
    (hj : DecidedOpt U W j (some A)) (helig : EligibleAsAnchor Replica k j) :
    EvidenceLinked U.toBlockUniverse A L k :=
  evidenceLinked_of_fastCommitOpt hL (fastCommitOpt_of_fastCommitOptInView h)
    (isLeaderBlock_of_decidedOpt hj).1 (anchor_round_opt hj helig)

end CrownConsequences

end Optimal

end Hydrozoan
