import Hydrozoan.Model.Liveness
import Hydrozoan.Helpers.Counting
import Hydrozoan.Helpers.DirectRules

/-!
# Direct-liveness toolkit

Generated proof infrastructure for `DirectLiveness`: the threshold
feeders (`q_cert ≤ q`, `q_slow ≤ q`, the fast-allowance complement
count) and the wave-chain lemmas (populated + synchronised ⟹ the
guaranteed quorum votes, certifies, and slow-commits). Nothing here is
part of the audit surface.
-/

namespace Hydrozoan

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [F : Faults Replica]

/-- `q_cert ≤ q` — Phase 2's "slow path collectible" row, as a lemma. -/
theorem qCert_le_q : qCert Replica ≤ q Replica := by
  have := F.card_replicas
  simp only [qCert, q]
  omega

/-- `q_slow ≤ q` — the guaranteed quorum covers the certificate count. -/
theorem qSlow_le_q : qSlow Replica ≤ q Replica := by
  have := F.card_replicas
  simp only [qSlow, q]
  omega

/-- Under `≤ p` actual faults, the correct pool reaches the fast
quorum. -/
theorem qFast_le_card_correct
    (h : (F.byzantine ∪ F.crashed).card ≤ p Replica) :
    qFast Replica ≤ (Correct : Finset Replica).card := by
  have hcompl : (Correct : Finset Replica).card
      = Fintype.card Replica - (F.byzantine ∪ F.crashed).card :=
    Finset.card_compl _
  have hle : (F.byzantine ∪ F.crashed).card ≤ Fintype.card Replica :=
    Finset.card_le_univ _
  simp only [qFast]
  omega

section Wave

variable [S : Slots Replica] {U : BlockUniverse Replica BlockId}
  {T : Finset Replica} {R k : ℕ}

omit [DecidableEq BlockId] in
/-- The leader's block exists and is a candidate. -/
theorem exists_isLeaderBlock_of_populated
    (hpop : PopulatedOn U T (S.slotRound k)) (hlead : S.leader k ∈ T) :
    ∃ L, IsLeaderBlock U k L := by
  obtain ⟨L, hL, hLr, hLa⟩ := hpop _ hlead
  exact ⟨L, hL, hLr, hLa⟩

/-- Every `T`-member supports the leader block at the voting round. -/
theorem subset_supporters_of_synchronised
    (hs : SynchronisedOn U T R) (hRk : R ≤ S.slotRound k)
    (hpop1 : PopulatedOn U T (S.slotRound k + 1))
    {L : BlockId} (hL : IsLeaderBlock U k L)
    (hLT : (U.block L).author ∈ T) :
    T ⊆ supporters U L (S.slotRound k + 1) := by
  intro v hv
  obtain ⟨b, hb, hbr, hba⟩ := hpop1 v hv
  have hvote : L ∈ (U.block b).parents :=
    hs (S.slotRound k) hRk b hb hbr (by rw [hba]; exact hv)
      L hL.1 hL.2.1 hLT
  exact mem_supporters.mpr ⟨b, hb, hbr, hvote, hba⟩

/-- Every `T`-authored decision block certifies the leader. -/
theorem isCertificate_of_synchronised
    (hcard : q Replica ≤ T.card)
    (hs : SynchronisedOn U T R) (hRk : R ≤ S.slotRound k)
    (hpop1 : PopulatedOn U T (S.slotRound k + 1))
    {L : BlockId} (hL : IsLeaderBlock U k L)
    (hLT : (U.block L).author ∈ T)
    {C : BlockId} (hC : C ∈ U.ids)
    (hCr : (U.block C).round = S.slotRound k + 2)
    (hCa : (U.block C).author ∈ T) :
    IsCertificate U C L := by
  have hsub : T ⊆ authorsOf U.block (voteBlocks U C L) := by
    intro v hv
    obtain ⟨b, hb, hbr, hba⟩ := hpop1 v hv
    have href : b ∈ (U.block C).parents :=
      hs (S.slotRound k + 1) (by omega) C hC hCr hCa
        b hb hbr (by rw [hba]; exact hv)
    have hvote : L ∈ (U.block b).parents :=
      hs (S.slotRound k) hRk b hb hbr (by rw [hba]; exact hv)
        L hL.1 hL.2.1 hLT
    exact mem_authorsOf.mpr
      ⟨b, Finset.mem_filter.mpr ⟨href, hvote⟩, hba⟩
  exact le_trans (qCert_le_q (Replica := Replica))
    (le_trans hcard (Finset.card_le_card hsub))

/-- The guaranteed quorum slow-commits its leader. -/
theorem slowCommit_of_synchronised
    (hcard : q Replica ≤ T.card)
    (hs : SynchronisedOn U T R) (hRk : R ≤ S.slotRound k)
    (hpop1 : PopulatedOn U T (S.slotRound k + 1))
    (hpop2 : PopulatedOn U T (S.slotRound k + 2))
    {L : BlockId} (hL : IsLeaderBlock U k L)
    (hLT : (U.block L).author ∈ T) :
    SlowCommit U L (S.slotRound k) := by
  have hsub : T ⊆ certifiers U L (S.slotRound k) := by
    intro v hv
    obtain ⟨C, hC, hCr, hCa⟩ := hpop2 v hv
    have hcert : IsCertificate U C L :=
      isCertificate_of_synchronised hcard hs hRk hpop1 hL hLT hC hCr
        (by rw [hCa]; exact hv)
    exact mem_authorsOf.mpr
      ⟨C, mem_certificates.mpr ⟨hC, hCr, hcert⟩, hCa⟩
  exact le_trans (qSlow_le_q (Replica := Replica))
    (le_trans hcard (Finset.card_le_card hsub))

end Wave

/-- Certificates are universe members. -/
theorem certificates_subset_ids {U : BlockUniverse Replica BlockId}
    {L : BlockId} {r : ℕ} : certificates U L r ⊆ U.ids :=
  fun _ hC => (mem_certificates.mp hC).1

/-- The eventual view is caught up to every horizon. -/
theorem View.coversUpto_full (U : BlockUniverse Replica BlockId) (N : ℕ) :
    (View.full U).CoversUpto N :=
  fun _ hb _ => hb

/-- Caught up to `N` is caught up to every lower horizon. -/
theorem View.CoversUpto.mono {U : BlockUniverse Replica BlockId}
    {V : View U} {M N : ℕ}
    (h : V.CoversUpto N) (hMN : M ≤ N) : V.CoversUpto M :=
  fun b hb hr => h b hb (le_trans hr hMN)

/-- A view caught up to the decision round holds every certificate, so
a universe-level slow commit is a slow commit in that view. -/
theorem slowCommitInView_of_coversUpto
    {U : BlockUniverse Replica BlockId} {V : View U} {L : BlockId} {r : ℕ}
    (h : SlowCommit U L r) (hcov : V.CoversUpto (r + 2)) :
    SlowCommitInView U V L r := by
  have hsub : certificates U L r ⊆ V.ids := by
    intro C hC
    obtain ⟨hCids, hCr, -⟩ := mem_certificates.mp hC
    exact hcov C hCids (le_of_eq hCr)
  have hinter : certificatesInView U V L r = certificates U L r := by
    simp only [certificatesInView]
    exact Finset.inter_eq_left.mpr hsub
  simp only [SlowCommitInView, certifiersInView, hinter]
  simp only [SlowCommit, certifiers] at h
  exact h

end Hydrozoan
