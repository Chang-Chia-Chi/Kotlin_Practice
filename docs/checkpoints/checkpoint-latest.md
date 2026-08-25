# Checkpoint P2

- ID: P2-2026-08-25
- Phase: P2 - Test kit: fake storage + accounting fixture
- Team: sdet + reviewer (composition table: test-only phase)
- Baseline: tag `p1` (= 0b5ebdc, P1 complete)
- Status at checkpoint: PHASE COMPLETE (reviewer APPROVED, 0 refactor cycles; DoD gate passed; progress.md P2 entry appended)

## Build result

    mvn test  ->  BUILD SUCCESS
    ArchitectureTest              5 tests, 0 failures
    GenerationRegistryTest       21 tests, 0 failures
    MetricLabelContractTest       3 tests, 0 failures
    SnapshotCacheConfigDefaults   2 tests, 0 failures
    AccountingFixtureTest        10 tests, 0 failures  (new, P2)
    InMemoryGenerationStoreTest   8 tests, 0 failures  (new, P2)
    total                        49 tests, 0 failures

## Test-diff check

    git diff --stat p1 -- '**/test/**'  ->  empty (no earlier-phase test touched)
    Working tree: ONLY src/test/kotlin/infra/snapshotcache/testkit/ added
    (untracked); zero tracked files modified. No production sources touched.

## Files produced (all under src/test/kotlin/infra/snapshotcache/testkit/)

    InMemoryGenerationStore.kt   fake spi store: ordered thread-safe call
                                 recording (StoreOp/StoreCall), strict lifecycle
                                 guards, one-shot scripted failures
                                 failOnNth(op, n) / failOnGen(op, gen)
    ConnectionTracker.kt         dynamic-proxy Connection stub (close/isClosed
                                 only), creation stack per issued connection
    AccountingFixture.kt         JUnit AfterEachCallback asserting the four
                                 spec 17.3 equations + 17.6 unclosed-connection
                                 check; currentGeneration/refCounts supplier
                                 seams for registry-side facts
    InMemoryGenerationStoreTest.kt  8 store self-tests
    AccountingFixtureTest.kt        10 fixture self-tests incl. seeded-leak
                                    acceptance tests (equations 1/3/4 +
                                    connection leak with creation stack)

## Notable sdet-reported design points for review

- Equations count effects, not attempts: scripted-failure calls recorded with
  failed=true but excluded from the equations.
- Strict store guards (ISE on out-of-order transitions); under them equation 2
  is structurally unviolatable in isolation but still asserted verbatim.
- OpenGeneration.connection() issues a fresh tracked connection per call;
  store.close(gen) does NOT auto-close issued connections (would hide leaks).
- Fake connections stub only close/isClosed; query behavior deliberately not
  prebuilt (P4's need, YAGNI).
- Registry-side facts for equations 3-4 arrive via mutable supplier properties.

## Files to Re-read on resume

- docs/snapshotcache/progress.md (P2 entry appended only after the DoD gate)
- snapshotcache/src/test/kotlin/infra/snapshotcache/testkit/ (all five files)
- This checkpoint
