import Hydrozoan.Optimal.ThresholdArithmetic.Statement

/-!
# Optimal-Hydrozoan: threshold arithmetic — proof

Generated proof layer; not part of the audit surface. Every row unfolds
to linear arithmetic over `n`, `f`, `c`, `k` with the floor divisions
`p = (c + k) / 2` and `q_cert = (n + f) / 2 + 1`, under the committee
bound and `1 ≤ f + c`; `omega` decides all of it, natural-number
subtraction included.
-/

namespace Hydrozoan

namespace Optimal

namespace ThresholdArithmetic

theorem holds : Statement := by
  intro Replica _ _ O
  have hn := O.card_replicas
  have hnt := O.nontrivial
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_⟩ <;>
    simp only [Hydrozoan.ThresholdArithmetic.CertUniqueness,
      Hydrozoan.ThresholdArithmetic.AnchorSeesSlow,
      Hydrozoan.ThresholdArithmetic.SlowCollectible,
      CertFastExclusion, EvidencePlain, EvidenceEquiv, FastUniqueness,
      qCert, qFastOpt, qSlow, q, pOpt, p, tPlain, tEquiv] <;>
    omega

end ThresholdArithmetic

end Optimal

end Hydrozoan
