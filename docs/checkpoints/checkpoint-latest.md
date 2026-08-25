# Checkpoint P1

- ID: P1-2026-08-25
- Phase: P1 - GenerationRegistry (pure core state machine)
- Team: sdet + engineer + reviewer (composition table: highest-risk phase)
- Baseline: tag `p0` (= 343e2ce, P0 complete)
- Status at checkpoint: PHASE COMPLETE (reviewer APPROVED both agents, 0 refactor cycles; DoD gate passed; progress.md P1 entry appended)

## Build result

    mvn test-compile  ->  OK (first-try integration against the lead-pinned surface)
    mvn test          ->  BUILD SUCCESS
    ArchitectureTest              5 tests, 0 failures
    GenerationRegistryTest       21 tests, 0 failures  (new, P1 sdet)
    MetricLabelContractTest       3 tests, 0 failures
    SnapshotCacheConfigDefaults   2 tests, 0 failures
    total                        31 tests, 0 failures

## Test-diff check

    git diff --stat p0 -- '**/test/**'  ->  empty (no earlier-phase test touched)
    Working tree: only core/GenerationRegistry.kt modified (engineer) and
    src/test/kotlin/infra/snapshotcache/core/ added (sdet).

## Files produced

    src/main/kotlin/infra/snapshotcache/core/GenerationRegistry.kt
        Lifecycle enum (BUILDING/OPENING/LIVE/RECLAIMING/GONE), RegistryLease,
        GenerationRegistry filled from the P0 shell. Single ReentrantLock, zero
        I/O, hooks fire outside the lock.
    src/test/kotlin/infra/snapshotcache/core/GenerationRegistryTest.kt
        21 tests: I2_/I3_/I4_/I6_/I8_ named tests, double-close,
        AFTER_READ_CURRENT acquire-during-swap gate test, Clock-driven deadline
        expiry, waiter release on publish and on shutdown. Helpers: MutableClock,
        GateHooks (latch-based HookRunner). No sleeps.

## Lead decisions this session

1. Pinned a session-local GenerationRegistry integration surface (17 members
   derived from the plan P1 deliverables) so sdet and engineer could build in
   parallel without seeing each other's output. Core internals remain FREE per
   the docs; this is coordination, not a contract change. It held: first-try
   compile of the union.
2. mvn is not on PATH on this machine; use
   C:\Users\maxch\.m2\wrapper\dists\apache-maven-3.9.8-bin\337e6d14\apache-maven-3.9.8\bin\mvn.cmd

## Notable agent-reported items for review / later phases

- Engineer: shutdown does not gate tryAcquire/beginBuild (P3/P4 consult
  isShuttingDown); awaitCurrent propagates InterruptedException raw (P3 owns
  translation); generation counter is a plain Long under the monitor, not
  AtomicLong (plan 2.5 supersedes spec 4.3's implementation hint; I3 unchanged).
- Sdet assumptions: blockedByK() null at live == K, non-null strictly above K
  (spec 6.1 read literally); exactly-at-deadline expiry unspecified; interrupt
  behavior of awaitCurrent not asserted at registry level.

## Files to Re-read on resume

- docs/snapshotcache/progress.md (P1 entry appended only after the DoD gate)
- snapshotcache/src/main/kotlin/infra/snapshotcache/core/GenerationRegistry.kt
- snapshotcache/src/test/kotlin/infra/snapshotcache/core/GenerationRegistryTest.kt
- This checkpoint
