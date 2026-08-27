import Hydrozoan.Optimal.Helpers.Grounding

/-!
# Optimal-Hydrozoan: grounding — proof

Generated. Fairness is Hydrozoan's theorem, reused; the two
universe-level conjuncts come from the Optimal helpers.
-/

namespace Hydrozoan

namespace Optimal

namespace Grounding

theorem holds : Statement :=
  ⟨Hydrozoan.Grounding.waveRobinFair, hypothesesRealizable, groundedProgress⟩

end Grounding

end Optimal

end Hydrozoan
