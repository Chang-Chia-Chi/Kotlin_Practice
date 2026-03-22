---
name: jacoco-coverage-gate
description: Parse JaCoCo HTML report, check per-package coverage thresholds, and guide SDET agent to close gaps.
triggers:
  - after test suite execution produces a JaCoCo report
  - before finalizing SDET Output coverage matrix
tools:
  - check_coverage.py
---

## Usage

See @commands/jacoco-coverage.md

Exit 0 = pass. Exit 1 = violations found.

## Threshold Guidance

| Package Character                  | Instruction | Branch |
|------------------------------------|-------------|--------|
| Core engine / DSL / state machine  | 85%         | 80%    |
| Leader election / coordination     | 80%         | 75%    |
| Extension / plugin hooks           | 70%         | 60%    |
| Shutdown / lifecycle               | 80%         | 75%    |

## On Violation — Diagnose and Act

1. **Map** each `[FAIL]` package to the locked interface contract — which public methods and state transitions live there?
2. **Classify** the gap:
    - Low instruction → missing happy-path tests.
    - Low branch → untested conditionals / state transitions / guard clauses.
    - Low method → entire contract methods untouched.
3. **Write tests** per charter constraints (contract-only, mock at boundaries, raise if untestable).
4. **Re-run** and re-invoke the script. Do NOT update your Coverage Matrix until JaCoCo confirms the gap is closed.

## Coverage Matrix vs JaCoCo

JaCoCo = mechanical (what code ran). Your Coverage Matrix = semantic (what was intentionally tested). Watch for:

- ✅ in matrix + low JaCoCo → test exists but doesn't exercise the real path (over-mocked?).
- High JaCoCo + ❌ in matrix → incidental coverage from other tests. Still a gap.