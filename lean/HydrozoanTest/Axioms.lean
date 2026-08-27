import Hydrozoan.ThresholdArithmetic.Proof
import Hydrozoan.DirectSafety.Proof
import Hydrozoan.SlotAgreement.Proof
import Hydrozoan.PrefixAgreement.Proof
import Hydrozoan.DirectLiveness.Proof
import Hydrozoan.IndirectLiveness.Proof
import Hydrozoan.EventualDecision.Proof
import Hydrozoan.Grounding.Proof
import Hydrozoan.Optimal.ThresholdArithmetic.Proof
import Hydrozoan.Optimal.DirectSafety.Proof
import Hydrozoan.Optimal.SlotAgreement.Proof
import Hydrozoan.Optimal.PrefixAgreement.Proof
import Hydrozoan.Optimal.DirectLiveness.Proof
import Hydrozoan.Optimal.IndirectLiveness.Proof

/-!
# The axioms tripwire

Build-failing enforcement of the acceptance criterion: every headline
theorem depends on exactly the standard triple `propext`,
`Classical.choice`, `Quot.sound` (the guard compares the full output,
so a *dropped* axiom trips it as loudly as a smuggled one — stricter
than the "at most" criterion, in the safe direction). `#guard_msgs`
fails elaboration — hence `lake build`, hence CI — on any deviation (a
smuggled axiom, a `sorry` anywhere in the dependency tree, or a
toolchain change to the message format, which a pin bump surfaces
loudly).
-/

/--
info: 'Hydrozoan.ThresholdArithmetic.holds' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.ThresholdArithmetic.holds

/--
info: 'Hydrozoan.DirectSafety.holds' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.DirectSafety.holds

/--
info: 'Hydrozoan.SlotAgreement.holds' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.SlotAgreement.holds

/--
info: 'Hydrozoan.PrefixAgreement.holds' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.PrefixAgreement.holds

/--
info: 'Hydrozoan.DirectLiveness.holds' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.DirectLiveness.holds

/--
info: 'Hydrozoan.DirectLiveness.fastLatency' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.DirectLiveness.fastLatency

/--
info: 'Hydrozoan.DirectLiveness.skipLatency' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.DirectLiveness.skipLatency

/--
info: 'Hydrozoan.IndirectLiveness.holds' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.IndirectLiveness.holds

/--
info: 'Hydrozoan.EventualDecision.holds' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.EventualDecision.holds

/--
info: 'Hydrozoan.Grounding.holds' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.Grounding.holds

/--
info: 'Hydrozoan.Optimal.ThresholdArithmetic.holds' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.Optimal.ThresholdArithmetic.holds

/--
info: 'Hydrozoan.Optimal.DirectSafety.holds' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.Optimal.DirectSafety.holds

/--
info: 'Hydrozoan.Optimal.SlotAgreement.holds' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.Optimal.SlotAgreement.holds

/--
info: 'Hydrozoan.Optimal.PrefixAgreement.holds' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.Optimal.PrefixAgreement.holds

/--
info: 'Hydrozoan.Optimal.DirectLiveness.holds' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.Optimal.DirectLiveness.holds

/--
info: 'Hydrozoan.Optimal.DirectLiveness.fastLatency' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.Optimal.DirectLiveness.fastLatency

/--
info: 'Hydrozoan.Optimal.IndirectLiveness.holds' depends on axioms: [propext, Classical.choice, Quot.sound]
-/
#guard_msgs in
#print axioms Hydrozoan.Optimal.IndirectLiveness.holds
