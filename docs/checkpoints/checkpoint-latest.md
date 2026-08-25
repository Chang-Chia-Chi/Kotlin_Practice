# Checkpoint P0

- ID: P0-2026-08-25
- Phase: P0 - Skeleton, API types, quality gates
- Team: engineer + reviewer (composition table: nothing behavioural to test)
- Baseline commit: 1acf5dd (pre-P0)
- Status at checkpoint: PHASE COMPLETE (reviewer APPROVED after 1 refactor cycle)

## Build result

    mvn -pl snapshotcache clean test  ->  BUILD SUCCESS
    ArchitectureTest              5 tests, 0 failures
    MetricLabelContractTest       3 tests, 0 failures
    SnapshotCacheConfigDefaults   2 tests, 0 failures
    total                        10 tests, 0 failures
    production + test sources     639 lines

## Test-diff check

    git diff --stat 1acf5dd..HEAD -- '**/test/**'   ->  empty

No earlier-phase test file exists yet; P0 is the first phase. From P1 onward the
baseline is the `p0` tag.

## Gate-bites verification (lead-run, not delegated)

A deliberate `java.sql.Connection` field was planted in
`infra.snapshotcache.core` and the ArchUnit suite went RED with 3 precise
violations, then green again after removal. The boundary rules enforce, they do
not merely pass. Recorded because all five rules carry `allowEmptyShould(true)`,
which would otherwise mask a broken importer. Review cycle 1 then REQUIRED their removal; all five rules now run without a silent-pass mode and none reported vacuous.

## Files produced

    src/main/kotlin/infra/snapshotcache/api/  SnapshotCache, Values, Producer,
                                              CacheAdmin, CacheEvents,
                                              SnapshotCacheConfig, Hook
    src/main/kotlin/infra/snapshotcache/spi/  GenerationStore
    src/main/kotlin/infra/snapshotcache/core/ GenerationRegistry, RefreshCycle,
                                              DefaultSnapshotCache (shells)
    src/test/kotlin/infra/snapshotcache/      ArchitectureTest,
                                              SnapshotCacheConfigDefaultsTest

## Open items carried out of P0

All three were escalated to the user and DECIDED. Documents updated before code.

1. ArchUnit rule 4 vs P3 - RESOLVED. Rule stands as written, not relaxed.
   `OpenGeneration` (spi) produces the `Snapshot` handle; `core` holds it only as
   `api.Snapshot` and supplies the release callback. plan 2.2 + D28.
   **P3 must not implement `Snapshot` inside `core`.**
2. spec 12.2 result labels - RESOLVED. Grew to seven: `disk_error` and
   `shutdown_aborted` added. spec 9.2 gained the missing shutdown-abort row.
   D26. Enum and label test updated.
3. Lease-owner logging - RESOLVED. `org.jboss.logging.Logger`, never
   `io.quarkus.logging.Log`. Host stack is Kotlin + Quarkus + DuckDB + JDBI,
   now recorded in CLAUDE.md. D27. No call sites yet.

## Files to Re-read on resume

- docs/snapshotcache/spec.md
- docs/snapshotcache/plan.md
- docs/snapshotcache/progress.md
- snapshotcache/src/main/kotlin/infra/snapshotcache/api/
- snapshotcache/src/main/kotlin/infra/snapshotcache/spi/GenerationStore.kt
