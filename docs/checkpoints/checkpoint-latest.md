# Checkpoint P3

- ID: P3-2026-08-25
- Phase: P3 - SnapshotCache facade + orphan safety net
- Team: sdet + engineer + reviewer (composition table: concurrency-sensitive)
- Baseline: tag `p2` (= f867327, P2 complete)
- Status at checkpoint: PHASE COMPLETE (engineer APPROVED cycle 1; sdet APPROVED cycle 2 after 1 required change - interruptible-wait test; DoD gate passed; progress.md P3 entry appended; 66 tests green)

## Build result

    mvn test  ->  BUILD SUCCESS, first-try integration of parallel work
    ArchitectureTest              5   GenerationRegistryTest      21
    DefaultSnapshotCacheTest     16 (new, P3)
    MetricLabelContractTest       3   SnapshotCacheConfigDefaults  2
    AccountingFixtureTest        10   InMemoryGenerationStoreTest  8
    total                        65 tests, 0 failures
    The orphan warning (D27 first log call site) visibly fired during the run.

## Test-diff check

    git diff --stat p2 -- '**/test/**'  ->  empty (no earlier-phase test touched)
    Working tree: pom.xml + core/DefaultSnapshotCache.kt + core/GenerationRegistry.kt
    modified (engineer), spi/SnapshotHandle.kt new (engineer),
    core/DefaultSnapshotCacheTest.kt new (sdet).

## Files produced

    core/DefaultSnapshotCache.kt   GroupRuntime; pinned 4-arg ctor; withSnapshot/
                                   acquire/copyOut/currentInfo; waitBudget path;
                                   single release path firing events + orphan warning
    core/GenerationRegistry.kt     pinned additions: publish(gen, opened, info)
                                   overload, RegistryLease.opened (+generationInfo),
                                   currentInfo(); P1 members untouched
    spi/SnapshotHandle.kt          NEW - the D28 handle: Snapshot impl from
                                   (OpenGeneration, dataAsOf, callback); shared
                                   Cleaner; connection tracking; idempotent close
    core/DefaultSnapshotCacheTest.kt  16 tests; AccountingFixture registered;
                                   helpers MutableClock, RecordingCacheEvents,
                                   awaitParked, publishGen
    pom.xml                        + org.jboss.logging:jboss-logging:3.6.1.Final

## Lead rulings this session

1. pom.xml dependency (engineer, flagged out-of-boundary): APPROVED as
   BLOCKED-IMPL resolution - D27 mandates the type, plan 2.4 forbids logging
   facades not the mandated library; P4 needs it regardless.
2. sdet awaitParked (polls Thread.state with 10s deadline to establish "waiter
   parked" before publish/shutdown): ACCEPTED as a bounded precondition check,
   not sleep sequencing - cannot pass by timing luck. Reviewer to confirm.
3. Engineer deviation beyond the pin: RegistryLease also carries
   generationInfo (dataAsOf must ride the lease atomically; currentInfo()
   after acquire races a concurrent publish). Defaulted null; P1 tests compile
   unchanged. Reviewer to judge.

## Files to Re-read on resume

- docs/snapshotcache/progress.md (P3 entry appended only after the DoD gate)
- snapshotcache/src/main/kotlin/infra/snapshotcache/core/DefaultSnapshotCache.kt
- snapshotcache/src/main/kotlin/infra/snapshotcache/spi/SnapshotHandle.kt
- snapshotcache/src/test/kotlin/infra/snapshotcache/core/DefaultSnapshotCacheTest.kt
- This checkpoint
