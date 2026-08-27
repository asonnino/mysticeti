import Hydrozoan.Optimal.Model.DirectRules
import Hydrozoan.Helpers.Counting
import Hydrozoan.Helpers.DirectRules

/-!
# Optimal-Hydrozoan: counting lemmas

Generated proof infrastructure; not part of the audit surface. The two
arithmetic rows the direct-safety proofs consume, in the subtraction-free
form `omega` wants, and the view-to-universe bridges for the Optimal
rules (a view can only under-report a rule).
-/

namespace Hydrozoan

namespace Optimal

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica]

omit [DecidableEq BlockId] in
/-- Two Optimal fast quorums overlap in a non-Byzantine replica, given
`f ≥ 1` (the `FastUniqueness` row). -/
theorem nf_lt_two_qFastOpt (hf : 1 ≤ O.f) :
    Fintype.card Replica + O.f < 2 * qFastOpt Replica := by
  have hn := O.card_replicas
  simp only [qFastOpt, pOpt, p]
  omega

omit [DecidableEq BlockId] in
/-- An Optimal fast quorum and a certificate quorum overlap in a
non-Byzantine replica (the `CertFastExclusion` row). -/
theorem nf_lt_qFastOpt_add_qCert :
    Fintype.card Replica + O.f < qFastOpt Replica + qCert Replica := by
  have hn := O.card_replicas
  have hnt := O.nontrivial
  simp only [qFastOpt, qCert, pOpt, p]
  omega

omit [DecidableEq BlockId] in
/-- With `f = 0` there is no Byzantine replica at all. -/
theorem byzantine_eq_empty_of_f_eq_zero (hf : O.f = 0) :
    (O.byzantine : Finset Replica) = ∅ := by
  have h := O.card_byzantine
  rw [hf] at h
  exact Finset.card_eq_zero.mp (Nat.le_zero.mp h)

variable {U : BlockUniverse Replica BlockId}

/-- A fast commit seen in a view holds in the universe. -/
theorem fastCommitOpt_of_fastCommitOptInView {V : View U} {L : BlockId} {r : ℕ}
    (h : FastCommitOptInView U V L r) : FastCommitOpt U L r :=
  le_trans h (Finset.card_le_card (Finset.image_subset_image Finset.inter_subset_left))

/-- The blame half of a skip seen in a view holds in the universe. -/
theorem qCert_le_blames_of_skippedLeaderOptInView [S : Slots Replica] {V : View U} {k : ℕ}
    (h : SkippedLeaderOptInView U V k) : qCert Replica ≤ (blames U k).card :=
  le_trans h.1 (Finset.card_le_card (Finset.image_subset_image Finset.inter_subset_left))

end Optimal

end Hydrozoan
