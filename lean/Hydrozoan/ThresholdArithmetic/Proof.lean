import Hydrozoan.ThresholdArithmetic.Statement

/-!
# Threshold arithmetic — proof

Generated proof layer; not part of the audit surface. Every row of the
table unfolds to linear arithmetic over `n`, `f`, `c`, `k` with the two
floor divisions `p = (c + k) / 2` and `q_cert = (n + f) / 2 + 1`, all of
which `omega` decides natively (including natural-number subtraction).
-/

namespace Hydrozoan

namespace ThresholdArithmetic

theorem holds : Statement := by
  intro Replica _ _ F
  have hn := F.card_replicas
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩ <;>
    simp only [CertUniqueness, FastUniqueness, FastStarvation, SlowCollectible,
      AnchorSeesSlow, AnchorSeesFast, qCert, qFast, qSlow, qWeak, q, p] <;>
    omega

end ThresholdArithmetic

end Hydrozoan
