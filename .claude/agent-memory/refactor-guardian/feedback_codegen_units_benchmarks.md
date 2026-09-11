---
name: codegen-units-benchmarks
description: Benchmark comparisons must hold codegen-units fixed and name the setting; default 16 CGU / no LTO swings replay speed ~40% on unrelated edits
metadata:
  type: feedback
---

Compare like with like: same `CARGO_PROFILE_RELEASE_CODEGEN_UNITS` (and bench profile) on
both sides, and say which setting was used. Prefer reporting both cgu16 (the shipped release
profile) and cgu1.

**Why:** the user measured that unrelated code changes swing adaptive-replay speed by about
40% via inlining differences across the 16 default codegen units (no LTO). A single-setting
number can show a "regression" or "win" that is only a partitioning artifact.

**How to apply:** build base/patched in separate target dirs per cgu setting, interleave
runs, report min + median, and cross-check with symbol sizes / call profiles. See
[[direct-verdict-cache-seam]] for the audit method.
