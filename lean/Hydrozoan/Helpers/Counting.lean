import Hydrozoan.Model.DirectRules
import Hydrozoan.Helpers.Faults

/-!
# The quorum-counting toolkit

Generated proof infrastructure: membership unfoldings for the rule
sets, the "guilty replica is Byzantine" collapse lemmas, and the
quorum-intersection arithmetic. Nothing here is part of the audit
surface.
-/

namespace Hydrozoan

section AuthorsOf

variable {Replica BlockId : Type*} [DecidableEq Replica]

/-- Membership in an author image, unfolded. -/
@[simp]
theorem mem_authorsOf {blk : BlockId → Block Replica BlockId}
    {s : Finset BlockId} {v : Replica} :
    v ∈ authorsOf blk s ↔ ∃ i ∈ s, (blk i).author = v :=
  Finset.mem_image

end AuthorsOf

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [F : Faults Replica]
  {U : BlockUniverse Replica BlockId}

omit [DecidableEq BlockId] in
/-- Membership in a round slice, unfolded. -/
@[simp]
theorem mem_blocksAt {i : BlockId} {r : ℕ} :
    i ∈ blocksAt U r ↔ i ∈ U.ids ∧ (U.block i).round = r := by
  simp [blocksAt]

/-- Membership in a supporter set, unfolded. -/
theorem mem_supporters {L : BlockId} {r : ℕ} {v : Replica} :
    v ∈ supporters U L r ↔
      ∃ b ∈ U.ids, (U.block b).round = r ∧ IsVote U b L ∧
        (U.block b).author = v := by
  simp only [supporters, mem_authorsOf, Finset.mem_filter, mem_blocksAt]
  tauto

/-- Membership in a slot's blamer set, unfolded. -/
theorem mem_blames [S : Slots Replica] {k : ℕ} {v : Replica} :
    v ∈ blames U k ↔
      ∃ b ∈ U.ids, (U.block b).round = votingRound Replica k ∧
        (∀ j ∈ (U.block b).parents, ¬ IsLeaderBlock U k j) ∧
        (U.block b).author = v := by
  simp only [blames, mem_authorsOf, Finset.mem_filter, mem_blocksAt]
  tauto

/-- Membership in a certificate set, unfolded. -/
theorem mem_certificates {C L : BlockId} {r : ℕ} :
    C ∈ certificates U L r ↔
      C ∈ U.ids ∧ (U.block C).round = r + 2 ∧ IsCertificate U C L := by
  simp only [certificates, Finset.mem_filter, mem_blocksAt]
  tauto

/-- A certificate's vote block exists, sits at the voting round, and
votes: through `U.complete` and the additive `predecessor`. -/
theorem mem_voteBlocks_spec {C L b : BlockId} {r : ℕ}
    (hC : C ∈ U.ids) (hCr : (U.block C).round = r + 2)
    (hb : b ∈ voteBlocks U C L) :
    b ∈ U.ids ∧ (U.block b).round = r + 1 ∧ IsVote U b L := by
  rw [voteBlocks, Finset.mem_filter] at hb
  obtain ⟨hmem, hvote⟩ := hb
  have hids : b ∈ U.ids := U.complete C hC b hmem
  have hround := (U.valid C hC).predecessor b hmem
  exact ⟨hids, by omega, hvote⟩

/-- A replica voting for two distinct same-author candidates in one
round is Byzantine: a non-Byzantine author has one voting block, and a
valid block never references two blocks by one author. -/
theorem byzantine_of_votes_two {L₁ L₂ : BlockId} {r : ℕ} {v : Replica}
    (hne : L₁ ≠ L₂) (hauthor : (U.block L₁).author = (U.block L₂).author)
    (h₁ : v ∈ supporters U L₁ r) (h₂ : v ∈ supporters U L₂ r) :
    v ∈ F.byzantine := by
  by_contra hv
  obtain ⟨b₁, hb₁, hr₁, hv₁, hc₁⟩ := mem_supporters.mp h₁
  obtain ⟨b₂, hb₂, hr₂, hv₂, hc₂⟩ := mem_supporters.mp h₂
  have hnb : (U.block b₁).author ∈ (NonByzantine : Finset Replica) := by
    rw [mem_nonByzantine, hc₁]; exact hv
  have hb : b₁ = b₂ :=
    U.no_equivocation b₁ hb₁ b₂ hb₂ hnb (by rw [hc₁, hc₂]) (by rw [hr₁, hr₂])
  subst hb
  exact hne ((U.valid b₁ hb₁).distinct_authors L₁ hv₁ L₂ hv₂ hauthor)

/-- A replica voting for a slot's candidate while blaming the slot is
Byzantine: its unique voting block would have to both reference a
candidate and reference none. -/
theorem byzantine_of_votes_and_blames [S : Slots Replica] {k : ℕ}
    {L : BlockId} {v : Replica} (hL : IsLeaderBlock U k L)
    (hs : v ∈ supporters U L (votingRound Replica k)) (hb : v ∈ blames U k) :
    v ∈ F.byzantine := by
  by_contra hv
  obtain ⟨b₁, hb₁, hr₁, hv₁, hc₁⟩ := mem_supporters.mp hs
  obtain ⟨b₂, hb₂, hr₂, hnovote, hc₂⟩ := mem_blames.mp hb
  have hnb : (U.block b₁).author ∈ (NonByzantine : Finset Replica) := by
    rw [mem_nonByzantine, hc₁]; exact hv
  have hbeq : b₁ = b₂ :=
    U.no_equivocation b₁ hb₁ b₂ hb₂ hnb (by rw [hc₁, hc₂]) (by rw [hr₁, hr₂])
  subst hbeq
  exact hnovote L hv₁ hL

/-- A certificate's vote-authors are supporters at the voting round. -/
theorem authors_voteBlocks_subset_supporters {C L : BlockId} {r : ℕ}
    (hC : C ∈ U.ids) (hCr : (U.block C).round = r + 2) :
    authorsOf U.block (voteBlocks U C L) ⊆ supporters U L (r + 1) := by
  intro v hv
  obtain ⟨b, hb, hcb⟩ := mem_authorsOf.mp hv
  obtain ⟨hids, hround, hvote⟩ := mem_voteBlocks_spec hC hCr hb
  exact mem_supporters.mpr ⟨b, hids, hround, hvote, hcb⟩

/-- A slow commit requires at least one certificate (`q_slow ≥ 1`). -/
theorem certificates_nonempty_of_slowCommit {L : BlockId} {r : ℕ}
    (h : SlowCommit U L r) : (certificates U L r).Nonempty := by
  rw [Finset.nonempty_iff_ne_empty]
  intro hempty
  simp only [SlowCommit, certifiers, hempty, authorsOf, Finset.image_empty,
    Finset.card_empty, qSlow] at h
  omega

/-- Two replica sets whose cardinalities sum past `n + f` share a
non-Byzantine member. -/
theorem exists_nonByzantine_mem_inter {A B : Finset Replica}
    (h : Fintype.card Replica + F.f < A.card + B.card) :
    ∃ v ∈ A ∩ B, v ∉ F.byzantine := by
  by_contra hcon
  push Not at hcon
  have hsub : A ∩ B ⊆ F.byzantine := fun v hv => hcon v hv
  have hunion : (A ∪ B).card ≤ Fintype.card Replica := by
    rw [← Finset.card_univ]; exact Finset.card_le_univ _
  have hadd := Finset.card_union_add_card_inter A B
  have hle := Finset.card_le_card hsub
  have hf := F.card_byzantine
  omega

omit [DecidableEq BlockId] in
/-- Two same-round block sets whose author sets meet quorums summing
past `n + f` share a block: their non-Byzantine common author's voting
block is unique. -/
theorem exists_common_mem_of_author_quorums {s t : Finset BlockId} {r : ℕ}
    (hs : ∀ b ∈ s, b ∈ U.ids ∧ (U.block b).round = r)
    (ht : ∀ b ∈ t, b ∈ U.ids ∧ (U.block b).round = r)
    (hcard : Fintype.card Replica + F.f <
      (authorsOf U.block s).card + (authorsOf U.block t).card) :
    ∃ b, b ∈ s ∧ b ∈ t := by
  obtain ⟨v, hv, hvnb⟩ := exists_nonByzantine_mem_inter hcard
  rw [Finset.mem_inter] at hv
  obtain ⟨hvs, hvt⟩ := hv
  obtain ⟨b₁, hb₁, hc₁⟩ := mem_authorsOf.mp hvs
  obtain ⟨b₂, hb₂, hc₂⟩ := mem_authorsOf.mp hvt
  obtain ⟨hb₁i, hb₁r⟩ := hs b₁ hb₁
  obtain ⟨hb₂i, hb₂r⟩ := ht b₂ hb₂
  have hnb : (U.block b₁).author ∈ (NonByzantine : Finset Replica) := by
    rw [mem_nonByzantine, hc₁]; exact hvnb
  have hbeq : b₁ = b₂ :=
    U.no_equivocation b₁ hb₁i b₂ hb₂i hnb (by rw [hc₁, hc₂]) (by rw [hb₁r, hb₂r])
  exact ⟨b₁, hb₁, hbeq ▸ hb₂⟩

/-- `n + f < 2·q_fast` — no two conflicting fast quorums. -/
theorem nf_lt_two_qFast : Fintype.card Replica + F.f < 2 * qFast Replica := by
  have := F.card_replicas
  simp only [qFast, p]
  omega

/-- `n + f < 2·q_cert` — certificate uniqueness. -/
theorem nf_lt_two_qCert : Fintype.card Replica + F.f < 2 * qCert Replica := by
  have := F.card_replicas
  simp only [qCert]
  omega

/-- The rung ordering: the weak quorum never exceeds the certificate
quorum. -/
theorem qWeak_le_qCert : qWeak Replica ≤ qCert Replica := by
  have := F.card_replicas
  simp only [qWeak, qCert, p]
  omega

/-- `n + f < q_fast + q_cert` — the fast path starves every conflicting
certificate. -/
theorem nf_lt_qFast_add_qCert :
    Fintype.card Replica + F.f < qFast Replica + qCert Replica := by
  have := F.card_replicas
  simp only [qFast, qCert, p]
  omega

end Hydrozoan
