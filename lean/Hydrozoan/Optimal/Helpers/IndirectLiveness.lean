import Hydrozoan.Optimal.Model.Decided
import Hydrozoan.Helpers.IndirectLiveness

/-!
# Optimal-Hydrozoan: indirect-liveness lemmas

Generated proof infrastructure; not part of the audit surface. Totality
of the graded rule (a classical case split on the two rung tests — no
least-candidate step, since the evidence rung carries no tie-break), the
descent below a committed run (Hydrozoan's `Nat.find` argument over
`DecidedOpt`), and the view-monotonicity family that the eventual-view
harvest of later phases consumes.
-/

namespace Hydrozoan

namespace Optimal

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica] [S : Slots Replica]
  {U : OptUniverse Replica BlockId}

/-- **The graded rule is total.** Under the shared anchor prefix of the
indirect constructors, some rung fires: a certificate hit
(`indirectCert`), else — rung 1 empty for every candidate — an evidence
hit at any clearing candidate (`indirectEvidence`), else both rungs empty
and the slot skips (`indirectSkip`). -/
theorem decidedOpt_of_anchor {V : View U.toBlockUniverse} {k j : ℕ} {A : BlockId}
    (helig : EligibleAsAnchor Replica k j)
    (hj : DecidedOpt U V j (some A))
    (hmid : ∀ i, k < i → i < j → EligibleAsAnchor Replica k i →
      DecidedOpt U V i none) :
    ∃ v, DecidedOpt U V k v := by
  classical
  have hkj : k < j := lt_of_eligibleAsAnchor helig
  by_cases hc : ∃ L, IsLeaderBlock U.toBlockUniverse k L ∧
      CertifiedIn U.toBlockUniverse A L (S.slotRound k)
  · obtain ⟨L, hL, hcert⟩ := hc
    exact ⟨some L, DecidedOpt.indirectCert hkj helig hj hmid hL hcert⟩
  · push Not at hc
    by_cases he : ∃ L, IsLeaderBlock U.toBlockUniverse k L ∧
        EvidenceLinked U.toBlockUniverse A L k
    · obtain ⟨L, hL, hev⟩ := he
      exact ⟨some L, DecidedOpt.indirectEvidence hkj helig hj hmid hc hL hev⟩
    · push Not at he
      exact ⟨none, DecidedOpt.indirectSkip hkj helig hj hmid hc he⟩

open Classical in
/-- **A committed run decides everything below it** — Hydrozoan's fuel
induction on `b − i`, each slot extracting its nearest eligible committed
anchor by `Nat.find`. -/
theorem decidedOpt_below_of_committed_run {V : View U.toBlockUniverse} {b n : ℕ}
    (hbn : b ≤ n)
    (hspan : ∀ i, i < b → EligibleAsAnchor Replica i n)
    (hrun : ∀ j, b ≤ j → j ≤ n → ∃ B, DecidedOpt U V j (some B)) :
    ∀ i, i < b → ∃ v, DecidedOpt U V i v := by
  have key : ∀ d i, i < b → b - i ≤ d → ∃ v, DecidedOpt U V i v := by
    intro d
    induction d with
    | zero => intro i hi hd; omega
    | succ d ih =>
      intro i hi hd
      have hex : ∃ j, EligibleAsAnchor Replica i j ∧
          ∃ B, DecidedOpt U V j (some B) :=
        ⟨n, hspan i hi, hrun n hbn (le_refl n)⟩
      have hle : Nat.find hex ≤ n :=
        Nat.find_le ⟨hspan i hi, hrun n hbn (le_refl n)⟩
      obtain ⟨helig, B, hB⟩ := Nat.find_spec hex
      have hmid : ∀ i', i < i' → i' < Nat.find hex →
          EligibleAsAnchor Replica i i' → DecidedOpt U V i' none := by
        intro i' h1 h2 h3
        have hnc : ¬ ∃ C, DecidedOpt U V i' (some C) :=
          fun hcom => Nat.find_min hex h2 ⟨h3, hcom⟩
        have hi'b : i' < b := by
          by_contra he
          exact hnc (hrun i' (by omega) (by omega))
        obtain ⟨v, hv⟩ := ih i' hi'b (by omega)
        cases v with
        | none => exact hv
        | some C => exact absurd ⟨C, hv⟩ hnc
      exact decidedOpt_of_anchor helig hB hmid
  intro i hi
  exact key (b - i) i hi (le_refl _)

/-! ## Verdicts persist as a view grows -/

section ViewMono

variable {B : BlockUniverse Replica BlockId}

omit S in
/-- A larger view holds every supporter the smaller one does. -/
theorem fastCommitOptInView_mono {V V' : View B} (hsub : V.ids ⊆ V'.ids)
    {L : BlockId} {r : ℕ} (h : FastCommitOptInView B V L r) :
    FastCommitOptInView B V' L r :=
  le_trans h (Finset.card_le_card (Finset.image_subset_image
    (Finset.inter_subset_inter Finset.Subset.rfl hsub)))

/-- A larger view holds every no-evidence block the smaller one does. -/
theorem noEvidenceQuorumInView_mono {V V' : View B} (hsub : V.ids ⊆ V'.ids)
    {k : ℕ} (h : NoEvidenceQuorumInView B V k) : NoEvidenceQuorumInView B V' k := by
  obtain ⟨s, hs, hcard⟩ := h
  exact ⟨s, fun b hb => ⟨(hs b hb).1, hsub (hs b hb).2.1, (hs b hb).2.2⟩, hcard⟩

/-- A larger view holds every blame and no-evidence block the smaller
one does. -/
theorem skippedLeaderOptInView_mono {V V' : View B} (hsub : V.ids ⊆ V'.ids)
    {k : ℕ} (h : SkippedLeaderOptInView B V k) : SkippedLeaderOptInView B V' k :=
  ⟨le_trans h.1 (Finset.card_le_card (Finset.image_subset_image
    (Finset.inter_subset_inter Finset.Subset.rfl hsub))),
    noEvidenceQuorumInView_mono hsub h.2⟩

end ViewMono

/-- **Verdicts persist as a view grows.** Structural induction on the
derivation; the rung tests (`CertifiedIn`, `EvidenceLinked`) are
universe-level, so the negated premises transport unchanged. -/
theorem decidedOpt_mono {V V' : View U.toBlockUniverse} (hsub : V.ids ⊆ V'.ids)
    {k : ℕ} {v : Option BlockId} (h : DecidedOpt U V k v) : DecidedOpt U V' k v := by
  induction h with
  | directFast hL hfc =>
      exact DecidedOpt.directFast hL (fastCommitOptInView_mono hsub hfc)
  | directSlow hL hsc =>
      exact DecidedOpt.directSlow hL (slowCommitInView_mono hsub hsc)
  | directSkip hsk =>
      exact DecidedOpt.directSkip (skippedLeaderOptInView_mono hsub hsk)
  | indirectCert hkj helig _ _ hL hcert ihj ihmid =>
      exact DecidedOpt.indirectCert hkj helig ihj ihmid hL hcert
  | indirectEvidence hkj helig _ _ hnc hL hev ihj ihmid =>
      exact DecidedOpt.indirectEvidence hkj helig ihj ihmid hnc hL hev
  | indirectSkip hkj helig _ _ hnc hne ihj ihmid =>
      exact DecidedOpt.indirectSkip hkj helig ihj ihmid hnc hne

/-- Any view's verdicts hold at the eventual view. -/
theorem decidedOpt_full {V : View U.toBlockUniverse} {k : ℕ} {v : Option BlockId}
    (h : DecidedOpt U V k v) : DecidedOpt U (View.full U.toBlockUniverse) k v :=
  decidedOpt_mono V.subset_ids h

end Optimal

end Hydrozoan
