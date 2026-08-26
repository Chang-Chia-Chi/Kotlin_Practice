# Checkpoint P8 - M1 COMPLETE

- ID: P8-2026-08-26
- Phases closed since P7: P5 (040f212, tag p5), P6 (e3cf5fa, tag p6), design
  session (e8b197b - D29 threads knob, consumer-instance ruling, runaway
  acceptance), P8 (this commit, tag p8)
- Status: P8 PHASE COMPLETE (both agents APPROVED cycle 1). M1 - the spec 17.8
  framework acceptance scope - is COMPLETE on this machine; the two Unix-only
  FD assertions first execute on Linux CI. 127 tests, 0 failures, 2 skips.
- Spec 17.6 flips recorded: A3, A7, file-level A1 confirmed; A4 confirmed in
  adapter-guard form. A2/A5/A6/A8 + RSS-trend measurement remain open (D19).
- User decisions this stretch: drain pulled into P8 (plan P8/P9 amended);
  D29 (serving threads knob, runaway accept+observe); consumer instance in
  P9 CDI wiring.
- Next: M2 starts only after the user accepts M1 (plan 3b gate). P9 scope:
  CDI/scheduler/Micrometer/admin endpoint/startup wiring + shutdown hook
  calling cache.shutdown() + interrupt delivery. Reviewer red-flag standing:
  any P9 core diff beyond wiring seams. NOTE: the repo has no host Quarkus
  service (P0 deviation) - P9 needs a user decision on where the service
  lives.

## Files to Re-read on resume

- docs/snapshotcache/progress.md (P8 entry + design session)
- snapshotcache/src/main/kotlin/infra/snapshotcache/core/DefaultSnapshotCache.kt (shutdown)
- snapshotcache/src/test/kotlin/infra/snapshotcache/e2e/ (both files)
- This checkpoint
