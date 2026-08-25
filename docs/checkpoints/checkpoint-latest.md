# Checkpoint P4

- ID: P4-2026-08-25
- Phase: P4 - RefreshCycle: state machine, verify gate, failure paths
- Team: sdet + engineer + reviewer
- Baseline: tag `p3` (= f40bdfd, P3 complete)
- Status at checkpoint: PHASE COMPLETE (sdet APPROVED cycle 1; engineer APPROVED cycle 2 after 1 required change - candidate.connection() DISK_ERROR hoist; DoD gate passed; progress.md P4 entry appended; 82 tests green). P4b (built-in verify-rule tests) remains open as the split's second half.

## Build result

    mvn test  ->  BUILD SUCCESS
    ArchitectureTest 5, DefaultSnapshotCacheTest 17, GenerationRegistryTest 21,
    RefreshCycleTest 7 (new), RefreshCycleFailureTest 9 (new),
    MetricLabelContractTest 3, ConfigDefaults 2, AccountingFixtureTest 10,
    InMemoryGenerationStoreTest 8
    total 82 tests, 0 failures

## Test-diff check

    git diff --stat p3 -- '**/test/**'  ->  empty (no earlier-phase test touched)
    Working tree: core/RefreshCycle.kt + GenerationRegistry.kt +
    DefaultSnapshotCache.kt modified, spi/VerifyGate.kt new (engineer);
    P4TestSupport.kt + RefreshCycleTest.kt + RefreshCycleFailureTest.kt new (sdet).

## Session history (both agents survived infra stream-stalls and were resumed)

1. Both agents' first runs were killed by API infrastructure errors mid-flight;
   lead resumed each from its transcript; all work recovered.
2. Integration found ONE production defect, reported by the sdet with root
   cause: blocked-by-K never auto-resumed (K guard evaluated before any
   reclaim; GC only ran on successful rounds; "0 leases outstanding" yet still
   blocked). Engineer fixed with a 7-line diff: on a tripped guard, run
   reclaimPass then re-check before declaring BLOCKED_BY_K.
3. Final: 82/82 green.

## Lead rulings this session

1. P4b SPLIT INVOKED (plan P4's pre-authorized remedy): detailed built-in
   verify-rule tests (non_empty/key_unique/required_non_null/row_count_delta
   default-off/table discovery) deferred to a P4b session. P4 covers
   verify_failed via caller GenerationCheck. QueryScript heuristics in
   P4TestSupport are ready for P4b reuse.
2. VerifyGate lives in spi, not core (RATIFIED; supersedes the lead's pin 7
   wording): engineer probe-proved java.sql METHOD CALLS from core violate
   immutable ArchUnit rule 4 (P0's probe only planted a field). D28 precedent.
3. Escalation semantics pinned: verifyFailureEscalated fires exactly once when
   the consecutive counter REACHES the threshold; success resets and re-arms.
4. Round sequence deviation (pre-ruled at spawn): verify runs AFTER promote via
   open(gen) (frozen store has no reopen-candidate seam); I1 unaffected.
5. Failure classification (pre-ruled): store exceptions -> DISK_ERROR
   (+ emergency GC), source -> SOURCE_ERROR, interrupt/shutdown ->
   SHUTDOWN_ABORTED - by failing component, not exception inspection.

## Files to Re-read on resume

- docs/snapshotcache/progress.md (P4 entry appended only after the DoD gate)
- snapshotcache/src/main/kotlin/infra/snapshotcache/core/RefreshCycle.kt
- snapshotcache/src/main/kotlin/infra/snapshotcache/spi/VerifyGate.kt
- snapshotcache/src/test/kotlin/infra/snapshotcache/core/RefreshCycleTest.kt,
  RefreshCycleFailureTest.kt, P4TestSupport.kt
- This checkpoint
