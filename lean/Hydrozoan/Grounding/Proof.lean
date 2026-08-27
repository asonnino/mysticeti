import Hydrozoan.Helpers.Grounding

/-!
# Proof: grounding

Generated. The three conjuncts come from the helpers: the wave-aligned
fairness arithmetic, the horizon-universe construction, and the
capstone composition.
-/

namespace Hydrozoan
namespace Grounding

theorem holds : Statement :=
  ⟨waveRobinFair, hypothesesRealizable, groundedProgress⟩

end Grounding
end Hydrozoan
