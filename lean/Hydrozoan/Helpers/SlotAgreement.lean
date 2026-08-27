import Hydrozoan.Model.Decided
import Hydrozoan.Helpers.Counting
import Hydrozoan.Helpers.CausalHistory
import Hydrozoan.Helpers.DirectRules
import Hydrozoan.Helpers.IndirectRules

/-!
# The seam toolkit

Generated proof infrastructure for `SlotAgreement`: the "rung fires"
lemmas (an eligible anchor's history contains the evidence of any direct
commit), the starvation and skip negatives (nothing conflicting survives
on either rung), and the abstract anchor-comparison lemma. Nothing here
is part of the audit surface.
-/

namespace Hydrozoan

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [F : Faults Replica]
  {U : BlockUniverse Replica BlockId}

/-! ## Arithmetic feeders -/

/-- `n + f < q + q_slow` — an anchor's parents meet any slow commit
(Phase 2's row 5). Standalone this is an identity of truncated ℕ
arithmetic (both sides of `q + q_slow` sum to `n + f + 1`); its content
materializes in consumers where `q` also bounds a real author-set
cardinality. Same for the starvation row below. -/
theorem nf_lt_q_add_qSlow :
    Fintype.card Replica + F.f < q Replica + qSlow Replica := by
  have := F.card_replicas
  simp only [q, qSlow]
  omega

/-- `n + f < q_fast + q_weak` — a fast commit starves conflicts below
the weak rung (Phase 2's row 3). -/
theorem nf_lt_qFast_add_qWeak :
    Fintype.card Replica + F.f < qFast Replica + qWeak Replica := by
  have := F.card_replicas
  simp only [qFast, qWeak, p]
  omega

/-- The **strengthened** footprint row: `n + q_weak + f ≤ q_fast + q`.
Only the non-Byzantine overlap of an anchor's parent authors with the
fast quorum contributes anchor-linked votes — a Byzantine author's
reachable block may be its non-voting equivocation — so the design
note's `q_fast + q − n ≥ q_weak` must absorb an extra `f`. Still holds
at every `k ≥ 0`; tight at the tight replica count when `c + k` is even
(odd `c + k` leaves one unit of slack). -/
theorem n_add_qWeak_add_f_le_qFast_add_q :
    Fintype.card Replica + qWeak Replica + F.f ≤ qFast Replica + q Replica := by
  have := F.card_replicas
  simp only [q, qFast, qWeak, p]
  omega

/-- `1 ≤ q`. -/
theorem q_pos : 1 ≤ q Replica := by
  have := F.card_replicas
  simp only [q]
  omega

/-! ## Structural plumbing -/

omit [DecidableEq BlockId] in
/-- Non-genesis universe blocks have a parent (`q ≥ 1`). -/
theorem parents_nonempty {i : BlockId} (hi : i ∈ U.ids)
    (hr : 0 < (U.block i).round) : (U.block i).parents.Nonempty := by
  by_contra h
  rw [Finset.not_nonempty_iff_eq_empty] at h
  have hq : q Replica ≤ (authorsOf U.block (U.block i).parents).card :=
    (U.valid i hi).quorum hr
  rw [h] at hq
  simp only [authorsOf, Finset.image_empty, Finset.card_empty] at hq
  have := q_pos (Replica := Replica)
  omega

/-- Whatever route committed it, a verdict names a genuine candidate. -/
theorem isLeaderBlock_of_decided [LinearOrder BlockId] [S : Slots Replica]
    {V : View U} {j : ℕ} {A : BlockId}
    (h : Decided U V j (some A)) : IsLeaderBlock U j A := by
  cases h with
  | directFast hL _ => exact hL
  | directSlow hL _ => exact hL
  | indirectCert _ _ _ _ hL _ => exact hL
  | indirectWeak _ _ _ _ _ hL _ _ => exact hL

/-- A certified-in-reach candidate has a certificate. -/
theorem certificates_nonempty_of_certifiedIn {A L : BlockId} {r : ℕ}
    (h : CertifiedIn U A L r) : (certificates U L r).Nonempty := by
  obtain ⟨C, hC, -⟩ := h
  exact ⟨C, hC⟩

/-- `CertifiedIn` is inherited upward along reachability. -/
theorem certifiedIn_of_reaches {A B L : BlockId} {r : ℕ}
    (hBA : Reaches U B A) (h : CertifiedIn U A L r) : CertifiedIn U B L r := by
  obtain ⟨C, hC, hreach⟩ := h
  exact ⟨C, hC, Reaches.trans hBA hreach⟩

omit [DecidableEq BlockId] in
/-- `WeakLinked` is inherited upward along reachability. -/
theorem weakLinked_of_reaches {A B L : BlockId} {r : ℕ}
    (hBA : Reaches U B A) (h : WeakLinked U A L r) : WeakLinked U B L r := by
  obtain ⟨s, hs, hcard⟩ := h
  exact ⟨s, fun b hb =>
    ⟨(hs b hb).1, (hs b hb).2.1, Reaches.trans hBA (hs b hb).2.2⟩, hcard⟩

/-! ## Rung 1 fires: any eligible anchor reaches a slow commit's
certificate -/

private theorem certifiedIn_of_slowCommit_base {L : BlockId} {r : ℕ}
    (h : SlowCommit U L r) {A : BlockId} (hA : A ∈ U.ids)
    (hAr : (U.block A).round = r + 3) : CertifiedIn U A L r := by
  obtain ⟨C, hC₁, hC₂⟩ :=
    exists_common_mem_of_author_quorums (s := (U.block A).parents)
      (t := certificates U L r) (r := r + 2)
      (fun b hb => ⟨U.complete A hA b hb, by
        have := round_of_mem_parents hA hb; omega⟩)
      (fun b hb => ⟨(mem_certificates.mp hb).1, (mem_certificates.mp hb).2.1⟩)
      (by
        have hq : q Replica ≤ (authorsOf U.block (U.block A).parents).card :=
          (U.valid A hA).quorum (by omega)
        have h5 := nf_lt_q_add_qSlow (Replica := Replica)
        simp only [SlowCommit, certifiers] at h
        omega)
  exact ⟨C, hC₂, Reaches.single hC₁⟩

private theorem certifiedIn_of_slowCommit_aux {L : BlockId} {r : ℕ}
    (h : SlowCommit U L r) :
    ∀ d, ∀ A, A ∈ U.ids → (U.block A).round = r + 3 + d →
      CertifiedIn U A L r := by
  intro d
  induction d with
  | zero => exact fun A hA hAr => certifiedIn_of_slowCommit_base h hA hAr
  | succ d ih =>
      intro A hA hAr
      obtain ⟨b, hb⟩ := parents_nonempty hA (by omega)
      have hbi : b ∈ U.ids := U.complete A hA b hb
      have hbr := round_of_mem_parents hA hb
      exact certifiedIn_of_reaches (Reaches.single hb) (ih b hbi (by omega))

/-- **Rung 1 fires.** A slow commit's certificate lies in the causal
history of every block from round `r + 3` on. -/
theorem certifiedIn_of_slowCommit {L : BlockId} {r : ℕ} (h : SlowCommit U L r)
    {A : BlockId} (hA : A ∈ U.ids) (hAr : r + 3 ≤ (U.block A).round) :
    CertifiedIn U A L r :=
  certifiedIn_of_slowCommit_aux h ((U.block A).round - (r + 3)) A hA (by omega)

/-! ## Rung 2 fires: any eligible anchor sees a fast commit's weak
footprint -/

private theorem weakLinked_of_fastCommit_base {L : BlockId} {r : ℕ}
    (h : FastCommit U L r) {A : BlockId} (hA : A ∈ U.ids)
    (hAr : (U.block A).round = r + 2) : WeakLinked U A L r := by
  refine ⟨(U.block A).parents.filter (fun b => IsVote U b L), ?_, ?_⟩
  · intro b hb
    obtain ⟨hbp, hbv⟩ := Finset.mem_filter.mp hb
    have hbi : b ∈ U.ids := U.complete A hA b hbp
    have hbr := round_of_mem_parents hA hbp
    exact ⟨mem_blocksAt.mpr ⟨hbi, by omega⟩, hbv, Reaches.single hbp⟩
  · have hsub :
        (authorsOf U.block (U.block A).parents ∩ supporters U L (r + 1)) \
            F.byzantine ⊆
          authorsOf U.block
            ((U.block A).parents.filter (fun b => IsVote U b L)) := by
      intro v hv
      obtain ⟨hvin, hvnb⟩ := Finset.mem_sdiff.mp hv
      obtain ⟨hvP, hvS⟩ := Finset.mem_inter.mp hvin
      obtain ⟨p', hp', hpc⟩ := mem_authorsOf.mp hvP
      obtain ⟨b, hbi, hbr, hbv, hbc⟩ := mem_supporters.mp hvS
      have hpi : p' ∈ U.ids := U.complete A hA p' hp'
      have hpr := round_of_mem_parents hA hp'
      have hnb : (U.block p').author ∈ (NonByzantine : Finset Replica) := by
        rw [mem_nonByzantine, hpc]; exact hvnb
      have hpb : p' = b :=
        U.no_equivocation p' hpi b hbi hnb (by rw [hpc, hbc]) (by omega)
      subst hpb
      exact mem_authorsOf.mpr ⟨p', Finset.mem_filter.mpr ⟨hp', hbv⟩, hpc⟩
    have hcard := Finset.card_le_card hsub
    have hsd := Finset.le_card_sdiff F.byzantine
      (authorsOf U.block (U.block A).parents ∩ supporters U L (r + 1))
    have hq : q Replica ≤ (authorsOf U.block (U.block A).parents).card :=
      (U.valid A hA).quorum (by omega)
    have hinter := Finset.card_union_add_card_inter
      (authorsOf U.block (U.block A).parents) (supporters U L (r + 1))
    have huniv : (authorsOf U.block (U.block A).parents ∪
        supporters U L (r + 1)).card ≤ Fintype.card Replica := by
      rw [← Finset.card_univ]; exact Finset.card_le_univ _
    have hf := F.card_byzantine
    have h5 := n_add_qWeak_add_f_le_qFast_add_q (Replica := Replica)
    simp only [FastCommit] at h
    omega

private theorem weakLinked_of_fastCommit_aux {L : BlockId} {r : ℕ}
    (h : FastCommit U L r) :
    ∀ d, ∀ A, A ∈ U.ids → (U.block A).round = r + 2 + d →
      WeakLinked U A L r := by
  intro d
  induction d with
  | zero => exact fun A hA hAr => weakLinked_of_fastCommit_base h hA hAr
  | succ d ih =>
      intro A hA hAr
      obtain ⟨b, hb⟩ := parents_nonempty hA (by omega)
      have hbi : b ∈ U.ids := U.complete A hA b hb
      have hbr := round_of_mem_parents hA hb
      exact weakLinked_of_reaches (Reaches.single hb) (ih b hbi (by omega))

/-- **Rung 2 fires.** A fast commit's weak footprint is visible from
every block at round `r + 2` on. -/
theorem weakLinked_of_fastCommit {L : BlockId} {r : ℕ} (h : FastCommit U L r)
    {A : BlockId} (hA : A ∈ U.ids) (hAr : r + 2 ≤ (U.block A).round) :
    WeakLinked U A L r :=
  weakLinked_of_fastCommit_aux h ((U.block A).round - (r + 2)) A hA (by omega)

/-! ## Starvation: a fast commit clears both rungs of every rival -/

private theorem supporters_capped_of_fastCommit {L L' : BlockId} {r : ℕ}
    (hne : L' ≠ L) (hauthor : (U.block L').author = (U.block L).author)
    (h : FastCommit U L r) :
    (supporters U L' (r + 1)).card + qFast Replica ≤
      Fintype.card Replica + F.f := by
  have hsub : supporters U L' (r + 1) ∩ supporters U L (r + 1) ⊆
      F.byzantine := by
    intro v hv
    obtain ⟨h₁, h₂⟩ := Finset.mem_inter.mp hv
    exact byzantine_of_votes_two hne hauthor h₁ h₂
  have h1 := Finset.card_union_add_card_inter
    (supporters U L' (r + 1)) (supporters U L (r + 1))
  have h2 : (supporters U L' (r + 1) ∪ supporters U L (r + 1)).card ≤
      Fintype.card Replica := by
    rw [← Finset.card_univ]; exact Finset.card_le_univ _
  have h3 := Finset.card_le_card hsub
  have h4 := F.card_byzantine
  simp only [FastCommit] at h
  omega

/-- A fast commit starves every same-author rival (an equivocating
copy — the only kind a slot's candidates can be) off the weak rung, at
every anchor. -/
theorem not_weakLinked_of_fastCommit {L L' : BlockId} {r : ℕ} {A : BlockId}
    (hne : L' ≠ L) (hauthor : (U.block L').author = (U.block L).author)
    (h : FastCommit U L r) : ¬ WeakLinked U A L' r := by
  rintro ⟨s, hs, hcard⟩
  have hsub : authorsOf U.block s ⊆ supporters U L' (r + 1) := by
    intro v hv
    obtain ⟨b, hb, hbc⟩ := mem_authorsOf.mp hv
    obtain ⟨hb1, hb2, -⟩ := hs b hb
    obtain ⟨hbi, hbr⟩ := mem_blocksAt.mp hb1
    exact mem_supporters.mpr ⟨b, hbi, hbr, hb2, hbc⟩
  have h1 := Finset.card_le_card hsub
  have h2 := supporters_capped_of_fastCommit hne hauthor h
  have h5 := nf_lt_qFast_add_qWeak (Replica := Replica)
  omega

/-- A fast commit starves every same-author rival off the certificate
rung too. -/
theorem certificates_eq_empty_of_fastCommit {L L' : BlockId} {r : ℕ}
    (hne : L' ≠ L) (hauthor : (U.block L').author = (U.block L).author)
    (h : FastCommit U L r) : certificates U L' r = ∅ := by
  rw [Finset.eq_empty_iff_forall_notMem]
  intro C hC
  obtain ⟨hCi, hCr, hcert⟩ := mem_certificates.mp hC
  have hle := Finset.card_le_card
    (authors_voteBlocks_subset_supporters (L := L') hCi hCr)
  have h2 := supporters_capped_of_fastCommit hne hauthor h
  have hqc := qWeak_le_qCert (Replica := Replica)
  have h5 := nf_lt_qFast_add_qWeak (Replica := Replica)
  simp only [IsCertificate] at hcert
  omega

/-- Starvation of same-author rivals, rung-1 phrasing. -/
theorem not_certifiedIn_of_fastCommit {L L' : BlockId} {r : ℕ} {A : BlockId}
    (hne : L' ≠ L) (hauthor : (U.block L').author = (U.block L).author)
    (h : FastCommit U L r) : ¬ CertifiedIn U A L' r := by
  rintro ⟨C, hC, -⟩
  rw [certificates_eq_empty_of_fastCommit hne hauthor h] at hC
  exact Finset.notMem_empty C hC

/-! ## Skip-side negatives: a skipped slot has nothing on either rung -/

section Skip

variable [S : Slots Replica]

private theorem supporters_capped_of_skipped {k : ℕ} {L : BlockId}
    (hL : IsLeaderBlock U k L) (h : SkippedLeader U k) :
    (supporters U L (S.slotRound k + 1)).card + qFast Replica ≤
      Fintype.card Replica + F.f := by
  have h' : (supporters U L (votingRound Replica k)).card + qFast Replica ≤
      Fintype.card Replica + F.f := by
    have hsub : supporters U L (votingRound Replica k) ∩ blames U k ⊆
        F.byzantine := by
      intro v hv
      obtain ⟨h₁, h₂⟩ := Finset.mem_inter.mp hv
      exact byzantine_of_votes_and_blames hL h₁ h₂
    have h1 := Finset.card_union_add_card_inter
      (supporters U L (votingRound Replica k)) (blames U k)
    have h2 : (supporters U L (votingRound Replica k) ∪ blames U k).card ≤
        Fintype.card Replica := by
      rw [← Finset.card_univ]; exact Finset.card_le_univ _
    have h3 := Finset.card_le_card hsub
    have h4 := F.card_byzantine
    simp only [SkippedLeader] at h
    omega
  exact h'

/-- A skipped slot's candidates never reach the weak rung. -/
theorem not_weakLinked_of_skipped {k : ℕ} {L : BlockId} {A : BlockId}
    (hL : IsLeaderBlock U k L) (h : SkippedLeader U k) :
    ¬ WeakLinked U A L (S.slotRound k) := by
  rintro ⟨s, hs, hcard⟩
  have hsub : authorsOf U.block s ⊆ supporters U L (S.slotRound k + 1) := by
    intro v hv
    obtain ⟨b, hb, hbc⟩ := mem_authorsOf.mp hv
    obtain ⟨hb1, hb2, -⟩ := hs b hb
    obtain ⟨hbi, hbr⟩ := mem_blocksAt.mp hb1
    exact mem_supporters.mpr ⟨b, hbi, hbr, hb2, hbc⟩
  have h1 := Finset.card_le_card hsub
  have h2 := supporters_capped_of_skipped hL h
  have h5 := nf_lt_qFast_add_qWeak (Replica := Replica)
  omega

/-- A skipped slot's candidates are never certified. -/
theorem certificates_eq_empty_of_skipped {k : ℕ} {L : BlockId}
    (hL : IsLeaderBlock U k L) (h : SkippedLeader U k) :
    certificates U L (S.slotRound k) = ∅ := by
  rw [Finset.eq_empty_iff_forall_notMem]
  intro C hC
  obtain ⟨hCi, hCr, hcert⟩ := mem_certificates.mp hC
  have hle := Finset.card_le_card
    (authors_voteBlocks_subset_supporters (L := L) hCi hCr)
  have h2 := supporters_capped_of_skipped hL h
  have hqc := qWeak_le_qCert (Replica := Replica)
  have h5 := nf_lt_qFast_add_qWeak (Replica := Replica)
  simp only [IsCertificate] at hcert
  omega

/-- Skip-side, rung-1 phrasing. -/
theorem not_certifiedIn_of_skipped {k : ℕ} {L : BlockId} {A : BlockId}
    (hL : IsLeaderBlock U k L) (h : SkippedLeader U k) :
    ¬ CertifiedIn U A L (S.slotRound k) := by
  rintro ⟨C, hC, -⟩
  rw [certificates_eq_empty_of_skipped hL h] at hC
  exact Finset.notMem_empty C hC

end Skip

/-! ## The anchor comparison (abstract, per lean-dag) -/

omit [Fintype Replica] [DecidableEq Replica] [DecidableEq BlockId] in
/-- Two searches for the nearest eligible committed slot above `k`
cannot disagree: whichever anchor is earlier is decided `none` by the
other side's intermediate premise and `some` by its own derivation. No
consensus content — `Dec` and `Elig` are arbitrary. -/
theorem anchor_eq {W : Type*} {Dec : W → ℕ → Option BlockId → Prop}
    {Elig : ℕ → Prop} {k j j₂ : ℕ} {A A₂ : BlockId} {V₂ : W}
    (hkj : k < j) (helig : Elig j) (hkj₂ : k < j₂) (helig₂ : Elig j₂)
    (hj₂ : Dec V₂ j₂ (some A₂))
    (hmid₂ : ∀ i, k < i → i < j₂ → Elig i → Dec V₂ i none)
    (ihj : ∀ V v, Dec V j v → some A = v)
    (ihmid : ∀ i, k < i → i < j → Elig i → ∀ V v, Dec V i v → none = v) :
    j = j₂ ∧ A = A₂ := by
  rcases lt_trichotomy j j₂ with hlt | heq | hgt
  · exact absurd (ihj V₂ none (hmid₂ j hkj hlt helig)) (by simp)
  · subst heq
    exact ⟨rfl, Option.some.inj (ihj V₂ (some A₂) hj₂)⟩
  · exact absurd (ihmid j₂ hkj₂ hgt helig₂ V₂ (some A₂) hj₂) (by simp)

/-! ## At-anchor wrappers (view-level rules against a decided anchor) -/

section AtAnchor

variable [LinearOrder BlockId] [S : Slots Replica]

/-- The anchor's round, from its slot and eligibility. -/
theorem anchor_round {W : View U} {k j : ℕ} {A : BlockId}
    (hj : Decided U W j (some A)) (helig : EligibleAsAnchor Replica k j) :
    S.slotRound k + 3 ≤ (U.block A).round := by
  have hA := isLeaderBlock_of_decided hj
  rw [hA.2.1]
  exact (eligibleAsAnchor_iff Replica).mp helig

/-- A slow commit in any view is certified at every decided eligible
anchor. -/
theorem certifiedIn_of_slowCommitInView_at_anchor {V W : View U} {k j : ℕ}
    {L A : BlockId} (h : SlowCommitInView U V L (S.slotRound k))
    (hj : Decided U W j (some A)) (helig : EligibleAsAnchor Replica k j) :
    CertifiedIn U A L (S.slotRound k) :=
  certifiedIn_of_slowCommit (slowCommit_of_slowCommitInView h)
    (isLeaderBlock_of_decided hj).1 (anchor_round hj helig)

/-- A fast commit in any view is weak-linked at every decided eligible
anchor. -/
theorem weakLinked_of_fastCommitInView_at_anchor {V W : View U} {k j : ℕ}
    {L A : BlockId} (h : FastCommitInView U V L (S.slotRound k))
    (hj : Decided U W j (some A)) (helig : EligibleAsAnchor Replica k j) :
    WeakLinked U A L (S.slotRound k) :=
  weakLinked_of_fastCommit (fastCommit_of_fastCommitInView h)
    (isLeaderBlock_of_decided hj).1 (by have := anchor_round hj helig; omega)

end AtAnchor

end Hydrozoan
