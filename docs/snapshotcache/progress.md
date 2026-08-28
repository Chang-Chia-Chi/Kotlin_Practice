# Implementation progress

One entry per completed phase, appended in order. Keep entries short - this
is a handover note for the next session, not a report.

Format:

    ## <PHASE ID> - <title>  (<YYYY-MM-DD>)

    ### Delivered
    - <classes added, tests added - one line each>

    ### Deviations from the documents
    - <anything done differently from spec.md or plan.md, and why>
    - <write "none" if there were none>

    ### Notes for later phases
    - <stubs left behind, assumptions not yet verified, awkward seams>

The deviations section matters most. Implementation always meets details
the documents did not anticipate. An unrecorded deviation leaves the next
session looking at code that disagrees with the documents, which it will
"correct" back - silently undoing a deliberate decision.

---

## P0 - Skeleton, API types, quality gates  (2026-08-25)

### Delivered
- `api/` - SnapshotCache + Snapshot, GenerationSource + BuildContext, GenerationCheck + VerifyResult, CacheAdmin, CacheEvents + NoOpCacheEvents, GenerationInfo, CopyOutSpec, CopyOutResult, GenerationState, LeaseInfo, SnapshotCacheConfig, NotReadyException, ShuttingDownException, Hook + HookRunner + NoOpHooks.
- `spi/` - GenerationStore (7 methods), Candidate, OpenGeneration.
- `core/` - GenerationRegistry, RefreshCycle, DefaultSnapshotCache. Shells only; every body is `TODO(...)` tagged with the phase that fills it.
- `ArchitectureTest.kt` - the five plan 2.2 boundary rules.
- `SnapshotCacheConfigDefaultsTest.kt` - spec 13 table asserted verbatim.
- `MetricLabelContractTest.kt` - whole-set equality of `name.lowercase()` against the spec 12.2/12.3 label sets. `RefreshResult` carries seven constants after the D26 change.
- Build: 10 tests, 0 failures. Classpath carries no DuckDB, Quarkus or Micrometer.

### Deviations from the documents
- **Module, not an `infra/` subtree (plan 2.2 / 2.4).** There is no host ETL service in this repository, so the framework lives in a dedicated Maven module `snapshotcache`. Kotlin package names are unchanged (`infra.snapshotcache.{api,spi,core,duckdb}`) and every ArchUnit rule in plan 2.2 applies verbatim. plan 2.4's "no separate Maven module" is read as "no second module, no extracted library".
- **`GenerationStore` has 7 methods, not spec 17.1's 6.** `copyOut(opened, spec)` added, mandated by plan P3 so the facade's copy stays out of core.
- **Static analysis is ArchUnit only.** plan P0 says "static-analysis config"; no detekt or ktlint was added, because no document references any other gate.
- **Value types spec 5 names without a body were defined here:** `GroupId` (value class over String), `RefreshOutcome(result, generation?, detail?)`, `GcOutcome(reclaimed, deferred)`, `Candidate` (AutoCloseable; generation + connection()), `OpenGeneration` (generation, connection(), fileBytes(); not closeable - `store.close(gen)` is the single detach path), the 10-method `CacheEvents` set, `RefreshPhase` and `AcquireUnavailableReason`, `HookRunner`.
- **`nonEmpty` / `readable` are computed `val`s, not constructor params**, so spec 8.1's "cannot be disabled" holds at the type level.
- **`VerifyResult` is a `sealed interface` and `BuildContext` a data class**, where spec 5.2 writes `sealed class` / plain class. Fixed shape preserved; idiomatic Kotlin spelling.

### Notes for later phases
- **P1 owns the `Clock` seam.** Spec 17.8's "GenerationStore, Clock and the scheduler trigger are injectable" is co-owned by P0/P1/P2. P0 froze GenerationStore (spi) and the scheduler trigger (`CacheAdmin.triggerRefresh`). No `java.time.Clock` appears in the code yet, correctly - nothing in `api` needs one, and it becomes a `GenerationRegistry` constructor dependency at P1.
- **`DefaultSnapshotCache` takes only `config` today.** P3 will widen the constructor to registry + store + events + hooks. The current signature is not a frozen contract.
- **RESOLVED - ArchUnit rule 4 stands, and P3 has a route.** The collision was real (a `Connection` field planted in `core` fails the rule with 3 violations), but the user ruled that `core` should not hold `java.sql.Connection` at all. `OpenGeneration` (spi) produces the `Snapshot` handle; `core` holds it only as the `api.Snapshot` type and supplies the release callback. Recorded in plan 2.2 and D28. **P3 must not implement `Snapshot` inside `core`.**
- **RESOLVED - spec 12.2's result label set grew to seven.** `disk_error` and `shutdown_aborted` added; `RefreshResult` and `MetricLabelContractTest` updated to match. The question also exposed a gap in spec 9.2: shutdown-aborting an in-flight refresh was described in 10.2 step 3 but had no row in the failure table, so P4 had nothing to enumerate. That row now exists. Recorded as D26.
- **RESOLVED - logging is `org.jboss.logging.Logger`.** The host service is Kotlin + Quarkus + DuckDB + JDBI, which no document previously recorded. `io.quarkus.logging.Log` is forbidden in api/spi/core: it would break the plan 2.2 boundary rule and force the core suite to boot a framework. JBoss Logging is what Quarkus is built on, so `quarkus.log.*` config applies unchanged. Recorded as D27 and in CLAUDE.md. No call sites exist yet; P4 owns the blocked-by-K owner dump, P9 the drain-timeout list.
- **`Candidate.close()` must not throw when a candidate is abandoned.** close() is where CHECKPOINT happens, so a throwing close inside `use {}` would mask the exception that triggered spec 9.2's abort path.
- **Empty shells kept.** `GenerationRegistry`/`RefreshCycle` have no members. Review flagged them as scaffolding under plan 2.4; kept because plan P0 explicitly permits "core classes may exist as empty shells", and P1 fills GenerationRegistry.
- Untested: the five `require` guards in `SnapshotCacheConfig`/`VerifyConfig`.

## P1 - GenerationRegistry  (2026-08-25)

### Delivered
- `core/GenerationRegistry.kt` filled from the P0 shell: `Lifecycle` enum (BUILDING/OPENING/LIVE/RECLAIMING/GONE), `RegistryLease`, `GenerationRegistry(maxLive, leaseDeadline, clock, hooks)`. Single ReentrantLock, zero I/O, zero `GenerationStore` references; hooks fire outside the lock. 190 lines.
- `core/GenerationRegistryTest.kt`: 21 tests - `I2_`/`I3_`/`I4_`/`I6_`/`I8_` named tests, double-close (inside I6), `acquireDuringSwap_afterReadCurrentHook_leaseRemainsLiveReadable` (parks at AFTER_READ_CURRENT, runs full publish + beginReclaim, asserts the lease's gen never marked RECLAIMING - verified discriminating against a split-critical-section impl), Clock-driven deadline expiry, waiters released by both publish and shutdown. Helpers: `MutableClock`, `GateHooks`. Zero sleeps. 347 lines.
- Build: 31 tests, 0 failures. No earlier-phase test file changed (diff vs tag `p0` empty).

### Deviations from the documents
- **Registry surface is a session-pinned coordination artifact, not a frozen contract.** Core internals are FREE per plan 1; the lead pinned 17 members (beginBuild/discardBuild/beginPublish/publish, tryAcquire/release/awaitCurrent, blockedByK/beginReclaim/reclaimed/deferReclaim, current/liveGenerations/expiredLeases, beginShutdown/isShuttingDown) so sdet and engineer could build in parallel. Later phases may widen it (P3 will need publish to carry dataAsOf/rowCounts or an OpenGeneration reference for `currentInfo` and handles).
- **Generation counter is a plain Long under the monitor, not spec 4.3's AtomicLong.** Plan 2.5 (all mutable state under one monitor) supersedes the implementation hint; I3 is unchanged.
- **`blockedByK` triggers strictly above K** - spec 6.1's table read literally ("<= K proceeds normally"): at exactly K refresh proceeds, and the pass after exceeding reports blocked.
- **BEFORE_DETACH fires unconditionally after `beginReclaim`'s marking**, even when nothing was marked, so hook-driven tests are not order-dependent on candidate presence.
- **Consecutive-failure counter absent.** Plan 2.5 lists it as registry state; nothing in P1's scope exercises it. P4 must add it to the registry, not grow state in RefreshCycle.

### Notes for later phases
- **P3 owns shutdown gating and interrupt translation.** `tryAcquire`/`beginBuild` do not check `isShuttingDown()`; `awaitCurrent` propagates `InterruptedException` raw. The facade maps these to `ShuttingDownException`/`NotReadyException`.
- **`tryAcquire`'s discarded first read is deliberate.** The pre-hook `lock.withLock { currentGen }` exists only to make AFTER_READ_CURRENT honest (hook outside the lock, atomicity in the second critical section, which re-reads). Do not "clean it up" - deleting it would move the hook inside the real critical section and deadlock latch tests.
- `expiredLeases` boundary at exactly-deadline is unspecified (tests pin -1s/+1s only).
- `liveGenerations` includes BUILDING/OPENING/RECLAIMING records (fileBytes 0 until publish) as the admin diagnostic view; tests only pin LIVE presence and post-reclaim absence.
- Reviewer suggestions left open (non-blocking): comment on the deliberate discarded read; move release's refcount check before mutation; exact-sequence hook assertion for P5; an IllegalStateException-on-invalid-transition test.

## P2 - Test kit: fake storage + accounting fixture  (2026-08-25)

### Delivered
- `src/test/kotlin/infra/snapshotcache/testkit/` (502 lines, all test sources, depends on P0 only):
  - `InMemoryGenerationStore.kt` - fake spi store; ordered thread-safe call recording (`StoreOp`/`StoreCall`), strict lifecycle guards (ISE on out-of-order transitions), one-shot scripted failures `failOnNth(op, n)` / `failOnGen(op, gen)` throwing `ScriptedFailureException`.
  - `ConnectionTracker.kt` - dynamic-proxy `Connection` stub (close/isClosed only, everything else throws), creation stack captured per issued connection, `unclosed()` listing (spec 17.6 JVM-side detector).
  - `AccountingFixture.kt` - JUnit `AfterEachCallback` asserting the four spec 17.3 equations verbatim plus the unclosed-connection check; `currentGeneration`/`refCounts` supplier seams feed registry-side facts; every violation names the exact generation and operation.
  - Self-tests (18): store recording/guards/scripts/copyOut, and the seeded-leak acceptance tests - fixture demonstrably fails on deliberate leaks for equations 1, 3 (both directions), 4, and the connection detector.
- Build: 49 tests, 0 failures. Earlier suites unchanged (diff vs tag `p1` empty); zero tracked files modified.

### Deviations from the documents
- **Equations count effects, not attempts.** A scripted-failure call is recorded with `failed=true` but excluded from the equations, since it mutated no state - required for the equations to hold on spec 9.2 abort paths (a failed promote renamed nothing). Pinned by `scriptedFailedCalls_doNotCountInTheEquations`.
- **Equation 2 has no seeded-leak test.** The store's strict guards make opens != closes structurally unviolatable in isolation (still-open is equation 3's job); seeding it would require weakening the guards. It is still asserted verbatim in every `verify()`.
- **Connection tracking uses direct bookkeeping, not PhantomReference.** Spec 17.6's PhantomReference wording targets the real-profile detector; for a fake, direct tracking is deterministic and strictly stronger. Creation stacks and unclosed reporting are as specified.

### Notes for later phases
- **P3-P6 review checklist item: every suite using the kit MUST register `AccountingFixture` via `@RegisterExtension`.** P2 has no mechanism to force registration, so "asserted at the end of every test" (spec 17.8) is enforced by each phase's review. `InMemoryGenerationStoreTest` itself is exempt by design (instrument self-tests end mid-lifecycle).
- `store.close(gen)` does NOT auto-close connections issued from that generation - auto-closing would hide leaks from the detector. `OpenGeneration.connection()` issues a fresh tracked connection per call; the caller closes it.
- Fake connections support only close/isClosed. P4's verify rules will need query behavior stubbed at the spi boundary - deliberately not prebuilt.
- `Candidate.close()` is idempotent, never throws, closes the lazily issued write connection (P0 progress note honored).
- Before P5's stress suite: set `fileBytesPerGeneration`/`copyOutRows` before spawning threads, or mark them `@Volatile` (reviewer note).
- Reviewer suggestions left open (non-blocking): EngineTestKit case proving JUnit invokes the callback; comment on equation 1's best-effort leaked-set under gen-number reuse; comment protecting the nullable `afterEach` parameter from cleanup.

## P3 - SnapshotCache facade + orphan safety net  (2026-08-25)

### Delivered
- `core/DefaultSnapshotCache.kt` - `GroupRuntime(registry, store)`; ctor `(config, groups, events, clock)`; withSnapshot/acquire/copyOut/currentInfo; full waitBudget path (zero fast-fail, interruptible bounded wait, shutdown beats timeout classification); single release path firing leaseReleased/leaseOrphaned; the D27 orphan warning is the first log call site. CacheAdmin methods remain `TODO("P4")`.
- `spi/SnapshotHandle.kt` (new) - the D28 handle: `Snapshot` built from `(OpenGeneration, dataAsOf, callback)`; one shared `Cleaner`; issued-connection tracking; idempotent close via `Cleanable.clean()` at-most-once + `@Volatile explicitClose` + `reachabilityFence` against mid-close GC misclassification.
- `core/GenerationRegistry.kt` - P3 additions only: `publish(gen, opened, info)` overload (fileBytes evaluated outside the lock), `RegistryLease.opened` + `generationInfo`, `currentInfo()`. P1 members and semantics untouched (21/21 green).
- `core/DefaultSnapshotCacheTest.kt` - 17 tests incl. the 2-thread-pool zero-budget scenario, interruptible-wait test (added in review cycle 1), shutdown-releases-waiter, orphan exactly-once via bounded Cleaner await, copyOut lineage + scripted failure recovery; AccountingFixture registered with suppliers wired.
- `pom.xml` - `org.jboss.logging:jboss-logging:3.6.1.Final` (lead-approved; D27 mandates the type, no document had provisioned the artifact).
- Build: 66 tests, 0 failures. Review: engineer APPROVED cycle 1; sdet APPROVED cycle 2 (one required change: the interruptible-wait test).

### Deviations from the documents
- **`RegistryLease` carries `generationInfo` in addition to the session-pinned `opened`.** `dataAsOf` must ride the lease atomically - reading `currentInfo()` after acquire races a concurrent publish and could stamp a handle with another generation's lineage (an I8 violation). Reviewer-confirmed necessary.
- **`Snapshot.close()` also closes every connection the handle issued** (lead decision; spec silent). Consequence: DETACH-in-use (A4/spec 9.2) arises only from raw non-handle connections, which is exactly how spec 17.7 step 4 stages it.
- **Non-shutdown interrupt of a waiting acquire maps to `NotReadyException(TIMEOUT)`** after re-setting the interrupt flag; shutdown-originated interrupts map to `ShuttingDownException`. The FIXED reason label set (spec 12.3) has no "interrupted" value; TIMEOUT is the least-wrong classification.
- **Negative `waitBudget` treated as zero.** Spec defines only zero and positive.
- **Test methodology: `awaitParked`** (poll of `Thread.state` under a 10s deadline) accepted as a bounded precondition check - it observes a definite JVM state and cannot produce a false pass; the FIXED Hook enum has no seam on the wait path. The 100ms real-wait expiry test accepted: `Condition.awaitNanos` runs on nanoTime, which an injected `Clock` cannot drive.

### Notes for later phases
- **P4 must publish via `publish(gen, opened, info)`.** The fileBytes-only overload exists solely for P1-test compatibility; a facade served from it trips `checkNotNull` at consumer time. Suggestion open: mark it as such.
- `Snapshot.connection()` after close throws IllegalStateException (fail-fast; unasserted).
- Shutdown gating now lives at the facade (`ShuttingDownException` on every entry point); lease drain (spec 10.2 step 4) is P9's.
- Reviewer suggestions left open (non-blocking): log swallowed connection-close failures in Cleaner cleanup; comment the interrupt-to-TIMEOUT mapping in code; pin negative-budget, connection-after-close ISE, and copyOut targetConnection ownership in tests.
- P3 total ~670 lines incl. tests - slightly over the "roughly 200-600" guidance; driven by the D26-style breadth of the waitBudget/shutdown/orphan acceptance list, not speculative code.

## P4 - RefreshCycle: state machine, verify gate, failure paths  (2026-08-26)

### Delivered
- `core/RefreshCycle.kt` - runOnce()/reclaimPass(); full round sequence (tryBeginRound -> K guard with reclaim-then-recheck -> beginBuild -> createCandidate -> source.refresh -> candidate.close/CHECKPOINT -> beginPublish -> promote -> open -> verify -> AFTER_VERIFY -> BEFORE_POINTER_SWAP -> publish(gen, opened, info) -> GC -> endRound in finally); RoundAbort + single abort() epilogue (record GONE before cleanup I/O; emergency GC iff DISK_ERROR; best-effort close/delete never masks the original failure); blocked-by-K owner dump via jboss Logger.
- `spi/VerifyGate.kt` (new) - all built-in rules (readable, non_empty, key_unique, required_non_null, row_count_delta default-off), caller GenerationCheck composition, all SQL, verify-connection lifecycle. rowCounts feed GenerationInfo.
- `core/GenerationRegistry.kt` - P4 additions: tryBeginRound/endRound (overlap state under the one monitor), recordVerifyFailure/resetVerifyFailures (counter in registry per the P1 note), discardBuild widened to BUILDING|OPENING.
- `core/DefaultSnapshotCache.kt` - GroupRuntime gains defaulted `cycle` param; triggerRefresh/gc/liveGenerations wired; null cycle -> ISE.
- Tests: `RefreshCycleTest.kt` (7), `RefreshCycleFailureTest.kt` (9), `P4TestSupport.kt` (QueryScript pattern/heuristic SQL stubbing - never exact-string; QueryStubGenerationStore delegating every call to the recording fake; AccountingFixture in both suites). I1_/I5_/I7_ named tests; every P4-scope spec 9.2 row asserts return-to-usable.
- Build: 82 tests, 0 failures. Review: sdet APPROVED cycle 1; engineer APPROVED cycle 2.

### Deviations from the documents
- **Verify runs AFTER promote, via open(gen) read-only.** The frozen GenerationStore has no reopen-candidate seam (spec 4.2's "reopen the candidate" as written is unimplementable against it). I1 unaffected: the gen sits in OPENING (invisible to acquire) through promote/open/verify and the pointer swaps only after the gate passes. Crash window leaves an unattached unverified file; startup wipe (D10) removes it.
- **Failure classification is by failing component, not exception inspection**: GenerationStore ops (incl. `candidate.connection()`, hoisted in review cycle 1) -> `disk_error` + emergency GC; source.refresh -> `source_error`; InterruptedException or shuttingDown at a stage boundary -> `shutdown_aborted`.
- **VerifyGate lives in `spi`, not core.** Probe-proved: any java.sql METHOD CALL from core violates ArchUnit rule 4 (P0's probe only planted a field). D28 precedent; core keeps policy (when to verify, escalation, publish), spi keeps SQL. No frozen file modified.
- **`config.allowOverlap` is a no-op knob.** Spec 4.4 says overlapping runs are forbidden, flatly; honoring `true` would double resource envelopes. The spec 13 knob remains asserted in config defaults but is never consulted.
- **Escalation fires exactly once when the consecutive counter REACHES the threshold**; success resets and re-arms (spec 8.5 "once ... reach" read literally).
- **`dataAsOf` = clock.instant() at round start.** BuildContext is frozen with a framework-supplied dataAsOf; true txn-start capture needs source cooperation and is P10's.
- **Phase timings emitted: CHECKPOINT/VERIFY/PUBLISH only.** QUERY/FETCH/APPEND are source-side and land with P10's source (plan P10 lists per-step timing there).
- **P4b split invoked** (plan P4's pre-authorized remedy; production ~470 + tests ~810 lines): detailed built-in-rule tests deferred to P4b. verify_failed coverage in P4 rides caller checks.
- **Defect found and fixed in-phase**: blocked-by-K never auto-resumed (K guard before any reclaim; GC only on success). Sdet's suite caught it; fix = reclaimPass on tripped guard, re-check. Reviewer re-verified race-free (never reclaims current or leased gens).

### Notes for later phases
- **P4b scope**: built-in rule behavior (non_empty, key_unique, required_non_null table.column/bare forms, row_count_delta default-off publishes on wild deltas, BASE TABLE discovery filter / union-view key_unique exemption), candidate-connection()-throws -> DISK_ERROR test, round-entry shutdown short-circuit assertion. QueryScript heuristics in P4TestSupport are ready for reuse. LEAD RULING: P4TestSupport.kt may be EXTENDED add-only in P4b (it is the second half of P4 itself); RefreshCycleTest/RefreshCycleFailureTest assertions remain immutable.
- All five hooks now wired (AFTER_READ_CURRENT/AFTER_POINTER_SWAP/BEFORE_DETACH in P1, AFTER_VERIFY/BEFORE_POINTER_SWAP in P4) - P5 has its full seam set.
- A registry `check` failure inside the round bypasses the abort epilogue by design (invariant violation, not a 9.2 row).
- `reclaimPass` treats InterruptedException as an ordinary reclaim failure (defer) without re-setting the flag; P9 owns interrupt delivery.
- Reviewer suggestions open: comment the epilogue-bypass; testkit-toString coupling in one BuildContext assertion.

## P4b - Built-in verify-rule tests  (2026-08-26)

### Delivered
- `core/VerifyRulesTest.kt` (12 tests, ~280 lines, extends RefreshCycleTestBase so the AccountingFixture rides along): per-rule pass/fail/gating for non_empty (zero-row table + no-tables-at-all), key_unique (+ keyUnique=false publishes the same duplicates), required_non_null (table.column scoping, bare-column checked against every table, default-empty list publishes NULLs), row_count_delta (default-off publishes -90%/+900%; enabled: decrease and increase ratios proven independent in both directions; cold-start previous=null skips the gate), BASE TABLE discovery (union view exempt from key_unique and absent from rowCounts - discriminates an unfiltered implementation both ways). Plus the two P4-review follow-ups: candidate.connection()-throws -> DISK_ERROR + emergency GC (via CandidateConnectionRefusingStore, the seam failOnNth cannot script) and round-entry shutdown short-circuit (zero store calls).
- Shared assertVerifyRejected (exact 8.1/12.2 rule label, non-generic detail naming the offender per spec 8.5) and assertCandidateCleaned in every fail case.
- P4TestSupport.kt untouched (add-only permission unused); no production change.
- Build: 102 tests, 0 failures, 1 skipped (P7's Unix-only FD assertion). Review: APPROVED cycle 1.

### Deviations from the documents
- none.

### Notes for later phases
- **Open gap, assigned as a P5 rider: the `readable` rule's failure path** (verify-connection open or discovery query throwing -> Fail("readable")) is the one spec 8.1 row unasserted anywhere. ~15 lines reusing the wrapper pattern on open()'s OpenGeneration. Note: a real corrupt-file ATTACH failure classifies as disk_error (open() throws before the gate); the readable rule covers the verify-connection path specifically.
- non_empty/readable non-disableable flags need no runtime gating test: no knob exists by construction (P0's computed vals + config defaults test).
- QueryScript resolution order (queue -> patterns -> heuristics) verified against the real VerifyGate SQL; nullCounts keys match as SQL substrings (table-name scoping).

## P7 - DuckDB storage adapter  (2026-08-26)

### Delivered
- `duckdb/DuckDbGenerationStore.kt` (~250 lines) - the only DuckDB-touching production code. Plain JDBC (no JDBI). File layout spec 3.1 verbatim (gen_<10-digit>.db / .db.tmp, per-group dir); promote via Files.move ATOMIC_MOVE; ATTACH READ_ONLY onto one store-owned in-memory serving instance (single memory_limit per spec 11.1); OpenGeneration.connection() = serving.duplicate() + USE; copyOut = target-instance file ATTACH + CTAS, counter-suffixed alias, current_database restored; Candidate.close = CHECKPOINT then close, idempotent, never throws; delete removes .db/.tmp/.wal (the D10 startup-wipe primitive); listOnDisk reports promoted and leftover .tmp.
- `duckdb/DuckDbGenerationStoreTest.kt` (9 tests on real DuckDB 1.1.3): A3 (READ_ONLY rejects INSERT, same connection still reads), A4 (close throws IllegalStateException "still has" while a tracked connection is open; reader untouched; succeeds after close; file gone), memory_limit/temp_directory via current_setting, 20-rotation loop (zero files, zero tracked connections always; FD baseline Unix-only via UnixOperatingSystemMXBean + assumeTrue - runs on Linux CI, skipped on Windows dev), abort-shaped rounds (delete without close) leave tracking flat, copyOut rows/lineage/no catalog residue, crashed-build .tmp listed.
- `pom.xml` - org.duckdb:duckdb_jdbc:1.1.3 (pinned; pre-authorized).
- `spi/VerifyGate.kt` - one-line cross-phase defect fix (see deviations).
- Build: 103 tests, 0 failures, 1 skipped (Unix-only FD). Review: REVISE cycle 1 (2 required changes), APPROVED cycle 2.

### Deviations from the documents
- **A4 is partially FALSE at engine level (probe-verified by both engineer and reviewer independently).** DuckDB 1.1.3 DETACH succeeds under an idle reader and invalidates the reader's next query. The spec 9.2 defer safeguard is enforced by adapter bookkeeping: the store tracks every connection it issues per generation and close(gen) throws while any is open. Deterministic and strictly stronger than the engine behavior the spec hoped for. Spec 17.6 A4 row updated. **Consequence for P8: E2E step 4 stages its "raw connection" through OpenGeneration.connection() and verifies the adapter guard.**
- **VerifyGate discovery was instance-wide (cross-phase defect in P4-approved code, found by the reviewer's probe).** information_schema.tables on the shared serving instance returns other attached generations' identically-named tables. Fixed: discovery SQL now ends `AND table_catalog = current_database()` (probe-verified to return exactly the candidate's tables). P4b's discovery test tolerated the amendment unmodified, as designed.
- **Review cycle 1 defect: `issued` tracking map leaked one entry per aborted round** (abort reaches delete without close). Fixed: delete(gen) drops the entry (safe per the SPI sequencing contract); discriminating abort-shaped test added.
- Adapter close-in-use guard has a benign TOCTOU closed by the SPI's per-generation sequencing contract (commented in code).

### Notes for later phases
- P8: step 4 staging consequence above; A3/A7 and file-level A1 now have adapter-level coverage, E2E confirms them end to end; memory_limit display format on 1.1.3 is MiB text ("476.8 MiB" for 500MB).
- P9: DuckDbGenerationStore is AutoCloseable (teardown); startup wipe = listOnDisk + delete per gen.
- FD-count assertion first executes on Linux CI; the tracked-connection + zero-files assertions cover the leak class everywhere.

## P5 - Deterministic concurrency suite  (2026-08-26)

### Delivered
- `core/HookDriver.kt` (60 lines) - the plan-P5 latch driver: one HookRunner serving registry and cycle hooks, selective parking per armed Hook, arm-once CAS, throwing 10s proceed-checks (broken interleavings fail loudly, never hang).
- `core/ConcurrencySuiteTest.kt` (9 tests) - the six spec 17.4 rows VERBATIM at integration level (facade + cycle + registry + fake store), incl. the flagship mid-acquire case: acquirer parked at AFTER_READ_CURRENT, complete publish + GC while parked (gen1 DELETE asserted mid-park), released acquirer holds the post-swap LIVE gen - discriminates a split-critical-section implementation. Plus both plan-P5 shutdown interleavings and the N=20/M=100 stress with a per-round invariant sweep (I2 registry-vs-store cross-check, I3, I4 with the blocked-state exception via a bounded corrective gc-and-recheck loop, I6, I8 per handle) and exact end-state accounting.
- `core/ReadableRuleFailureTest.kt` (P4b rider) - both readable-failure shapes pinned: verify connection unobtainable AND unqueryable classify as VERIFY_FAILED rule=readable (matches spec 8.1's observable); store-level open() failure stays disk_error per the P4 component ruling. The last unasserted spec 8.1 row is closed.
- Flakiness acceptance: 20/20 repeat runs green (one-time check, surefire-XML confirmed). Build: 114 tests, 0 failures, 1 Unix-only skip. Review: APPROVED cycle 1.

### Deviations from the documents
- Size 753 lines vs the ~400-600 guidance - the overage is the FIXED 17.4 table breadth + the two plan-mandated shutdown interleavings + the assigned rider, declared and reviewer-accepted (same shape as P3's recorded overrun).
- Shutdown initiated via registry.beginShutdown() - the facade has no shutdown entry until P9 (P3 precedent).

### Notes for later phases
- Reviewer suggestions open (non-blocking): progress assertion inside the I4 sweep's corrective loop; pin events.unavailable empty in the mid-acquire case.
- HookDriver is reusable; P6's model test may consume it read-only.

## P6 - Randomized model test  (2026-08-26)

### Delivered
- `core/RandomizedModelTest.kt` (~400 lines) - spec 17.5 verbatim: per-sequence real integration stack (P5 wiring, K=3) vs an independent in-test model; 7-op weighted generator (acquire/close incl. deliberate double-close picks/refresh-success/refresh-failure/verify-failure/gc/orphan); I1-I8 checked after EVERY op in observable forms (I2 as exact openedGenerations == live-set equality; I7 as disk == live-set after every op; I4 with one corrective gc pass for the close-no-gc-yet transient); I5 + the 17.3 equations at every sequence end via AccountingFixture.verify(). Fixed seed 20260826, per-sequence seed = SEED+i, failure header prints seed + full op log + replay instruction. 5000 sequences x 40 ops (~5s) + a 10-sequence orphan-dedicated run (bounded Cleaner await).
- Mandated sanity check executed under the strict protocol: dropped beginReclaim's refCount==0 filter -> caught in sequence #0 in 0.28s (leased gen reclaimed under its handle, model-vs-registry divergence named); revert proven byte-clean (git diff vs p5 on src/main EMPTY, lead-verified independently).
- Build: 116 tests, 0 failures, 1 Unix-only skip. Review: REVISE cycle 1 (dead accounting-backstop extension removed - the explicit per-sequence verify() is the sole, documented mechanism), APPROVED cycle 2.

### Deviations from the documents
- Test names are `model_*`, not `I<n>_`: the spec 17.2 naming convention belongs to the pre-existing targeted per-invariant tests; spec 17.5 mandates no name.
- Single-threaded by design: spec 17.5 sequences operations; interleavings are P5's contract.
- Orphan determinism choice: bulk run excludes the orphan op; a dedicated 10-sequence run guarantees at least one per sequence (Cleaner nondeterminism kept out of the 5000-sequence budget).

### Notes for later phases
- The model's K-guard mirror is derived from spec 6.1 semantics over model state (reviewer-verified independent of production logic); the bug-injection run is standing evidence the oracle discriminates.

## Design session - concurrent DuckDB reads  (2026-08-26, user-decided)

Grill session on concurrent-read handling. Three decisions, all recorded in the documents:

1. **`duckdb.serving.threads` knob added** (spec 13 + D29): nullable, null = engine default; capped on CPU-limited pods. Wired in SnapshotCacheConfig + DuckDbGenerationStore (serving instance only, not candidate builds); one ADDED assertion in the P0 config defaults test under the user-decided-document-change rule.
2. **Runaway readers: accept + observe** (D29). No watchdog (would re-decide D8), no kill switch yet; the threads cap bounds the blast radius, lease-deadline diagnostics name the culprit. Kill switch deferred to P9+, only if lease-duration histograms demand it.
3. **Shared consumer instance lives in P9's CDI wiring** (plan P9 amended): an @ApplicationScoped producer with consumerMemoryLimit + threads; CopyOutSpec.targetConnection stays caller-supplied. No new api surface; plan 2.3's five-interface budget intact.

Confirmed already-covered (no action): release safety via identity-based idempotent leases (I2/I6/I8 + the adapter's per-generation connection guard); multiple simultaneous readers via serving.duplicate() per connection() call; withSnapshot as the consumer abstraction. Per-handle connection issuance deliberately unbounded (jobs are scheduler-bounded; plan 2.4).

## P8 - E2E feasibility test + shutdown drain  (2026-08-26) - M1 COMPLETE

### Delivered
- Shutdown drain (user decision, pulled forward from P9; plan P8/P9 amended first): `GenerationRegistry.awaitQuiescence(budget)` (bounded interruptible wait on the existing `published` condition, signalled by release's effective path - orphans included; zero/negative budget = immediate snapshot) and `DefaultSnapshotCache.shutdown(): List<LeaseInfo>` (spec 10.2 steps 1+4: beginShutdown on every group, ONE nanoTime-based `leaseDrainTimeout` deadline across groups, per-lease WARN with owner + hold duration outside the lock, returns the outstanding list; stateless idempotency). ~55 lines. Steps 2+3 (stop scheduling, interrupt delivery) remain P9 wiring.
- `e2e/SyntheticSource.kt` - real org.duckdb Appender path; t_a 2000 / t_b 3000 rows; spec 3.3 union view (source column, aligned names, typed NULLs); generation-stamped values make I8 content-provable.
- `e2e/EndToEndFeasibilityTest.kt` (~640 lines, @Tag("e2e"), PER_CLASS ordered) - the seven spec 17.7 steps VERBATIM on the full real stack, 22 real build->publish->reclaim rotations + steady-state loop, RecordingStoreSpy asserting all four 17.3 equations on the real store, WarnCapture (JUL fallback) pinning the drain WARN, second never-published group making step 7's waitBudget-waiter clause reachable, FD baseline Unix-gated (P7 ruling).
- Build: 127 tests, 0 failures, 2 Unix-only skips; E2E ~2s (stable across 3 runs), far under the 2-minute budget. Review: both agents APPROVED cycle 1.
- Spec 17.6 table updated at the gate: A3, A7, file-level A1 confirmed; A4 confirmed in its adapter-guard form (per the P7 record).

### Deviations from the documents
- **Drain pulled into P8 from P9** (user decision 2026-08-26; plan P8/P9 amended before code). `shutdown()` lives on the concrete facade, not a frozen interface.
- **Step 1's wipe is performed by the test through the store primitives** (listOnDisk + delete): startup orchestration (spec 10.1) is P9's; the E2E proves the primitives clean a dirty directory including WAL siblings.
- **K=1 for step 3** (K=3 makes the blocked state unreachable with one held lease - per-round GC holds live at 2; spec 6.1 semantics are K-invariant; P5 precedent).
- Self-managed temp root instead of @TempDir: JUnit 5.11 re-injects non-static @TempDir per method even under PER_CLASS, and per-method cleanup collides with still-ATTACHed files on Windows.
- WarnCapture assumes the jboss-logging JUL fallback (no other backend on the test classpath); breaks if a logging backend is ever added there.
- A second shutdown() call under still-stuck leases drains again (bounded) rather than returning instantly - lead-ruled acceptable; no drain-state invented.

### Notes for later phases
- **P9's remaining scope**: CDI producers (incl. the D29 consumer instance), @Scheduled adapter, Micrometer binder, admin endpoint, startup sequence (wipe orchestration + readiness), shutdown HOOK wiring steps 2+3 to cache.shutdown(), grace-period alignment. Reviewer red-flag: any P9 diff touching core beyond wiring seams.
- The `published` condition now carries two predicates (publish/shutdown and quiescence) - reviewer suggestion open to comment it.
- Other open suggestions: assert step 4's deferral warning via WarnCapture; per-rotation openIssuedConnections()==0 in the steady-state loop.
- M1 (spec 17.8 framework acceptance) is complete on this machine except the two Unix-only FD assertions, which first execute on Linux CI. Deferred by design (D19): RSS-trend leak measurement, perf baselines (spec 16.3), data-correctness validation (P10).

## Review fix pass 1 - 2026-08-28 code review  (2026-08-28)

Applied the pass-1 findings of `docs/snapshotcache/code-review-2026-08-28.md` (the ones
needing no plan ruling): H1, H2, H3, M1, M2, M3, M4, M6, and the main-source half of L1.
Build: 134 tests, 0 failures, 2 Unix-only skips. No earlier-phase test file changed.

### Fixed
- **H1 - reclaim wedged forever after close-succeeded-then-delete-failed.** `close(gen)` is
  now idempotent at the store boundary: the DuckDB adapter tracks the attached set and
  no-ops a DETACH of an unattached alias, so the next pass's retry of the close + delete
  unit completes instead of failing on a catalog error every time. `abort()` now uses the
  same protocol as `reclaimPass` - detach first, delete only once detached - so a failed
  detach no longer leaves a dangling alias over a deleted file.
- **H2 - a throwing `CacheEvents` sink skipped `abort()` and leaked a zombie record.**
  Every `events.*` call in `RefreshCycle` goes through a `notify {}` guard that logs and
  swallows; reporting is best-effort by contract. `candidate.close()` in the round now runs
  inside `storeOp` (a third-party store's close failing is a disk error, not an escape
  hatch) and inside `runCatching` on the abort path. `spi.Candidate.close`'s
  idempotent-and-never-throws guarantee is now in the interface KDoc, not only in the
  adapter and a P0 note. Hooks are deliberately NOT guarded: a `HookDriver` throw is a
  broken interleaving and must stay loud.
- **H3 - unquoted identifiers in the verify gate.** Table names come verbatim from
  `information_schema` and columns from caller config, so a table named `order` made every
  round VERIFY_FAILED for valid data (probe-verified on 1.1.3: `SELECT COUNT(*) FROM order`
  is a parse error, quoted is fine). All three gate query builders now quote.
- **M1 - one orphaned `.tmp` leaked per round whose `configure()` threw.** `abort()` calls
  `store.delete(gen)` whenever the generation was detached, `candidate == null` included:
  `createCandidate` creates the file before it can fail, and delete is deleteIfExists-safe.
- **M2 - a lease could be granted after `beginShutdown`.** `GenerationRegistry.tryAcquire`
  returns null under the shutting-down flag, decided inside its existing lock; the facade's
  refusal moved after it, so neither of the two call sites has a window any more.
- **M3 - unbounded tracking growth while one generation stays current.**
  `DuckDbGenerationStore.track` and `SnapshotHandle.CleanupState.issue` drop closed entries
  before appending.
- **M4 - shutdown across the seconds-long verify still swapped the pointer.** One
  `isShuttingDown()` check between the gate verdict and `registry.publish`, raising
  `RoundAbort(SHUTDOWN_ABORTED)`.
- **M6 - `copyOut` cleanup masked the real failure and swallowed a failed DETACH.** The
  `USE <home>` restore is best-effort (`runCatching` + warn) so a connection that died
  mid-CTAS still reports the CTAS error; a DETACH failure on the success path now
  propagates - the lease still pins the generation there, so failing the copyOut is safe,
  whereas a surviving attach becomes a dangling alias once the next reclaim deletes the file.
- **L1, main-source items.** `spi/Internals.kt` holds the shared `ident` / `literal`,
  the `Statement` scalar-query helpers (four hand-rolled copies collapsed into one), and
  `Throwable.describe()` (was duplicated in `RefreshCycle` and `VerifyGate`).
  `key_unique`'s two per-table scans became one `SELECT COUNT(id), COUNT(DISTINCT id)`.

### Regression tests added
- `core/ReviewFixRegressionTest.kt` (4): H1's reclaim retry, H2's throwing sink, M2's
  post-shutdown `tryAcquire`, M4's shutdown across the verify window (HookDriver-parked).
- `duckdb/DuckDbStoreReviewFixTest.kt` (3, real DuckDB 1.1.3): H1's idempotent detach,
  H3's reserved-word (`order`) and mixed-case table names, M3's pruning.
- Discrimination proven per test, the P6 way: each fix reverted in place, the suite run,
  all seven confirmed failing, then restored. `store.trackedConnections(gen)` was added
  next to the two existing internal leak-evidence accessors so M3's test can see the list.

### Deviations from the documents
- **`GenerationStore.close` is now contractually idempotent** (spec 9.2 gained a
  delete-fails row, spec 17.1 the sentence). Written into the documents before the code, as
  required: H1 is unfixable without it, because reclaim retries close + delete as a unit.
- **`InMemoryGenerationStore.close` follows the new contract** - a second close of an
  already-detached generation is a no-op - while a close of a *never*-opened generation
  still throws, so P2's frozen `guards_rejectOutOfOrderTransitions` assertion is untouched.
  Additive change to the fake, no test assertion modified.
- **`key_unique`'s scan collapse is 2 queries per table, not 1.** Merging non_empty's
  `COUNT(*)` in too needs a three-aggregate query, and P4's frozen `QueryScript` heuristic
  answers a multi-count shape with exactly two columns. That last merge needs a documented
  P4-test ruling; the two-scan version needed none and is where the review's per-table
  saving mostly is.
- **No `RefreshResult` for an internal error.** The review's alternative H2 fix - a
  catch-all around `round()` - was not taken: spec 9.2's classification is fixed and every
  existing label would be a lie in `snapshot_refresh_total`. The event sink, the only
  caller-supplied callback the review proved reachable, is guarded at its source instead.

### Not fixed in this pass
- **M5** (lease-deadline diagnostics dead) needs the plan ruling the review asks for: does
  P9 wiring poll `expiredLeases()` on the schedule tick, or does the core fire
  `CacheEvents.leaseExpired` itself.
- **L1's nullable test-only seams** (`publish(gen, fileBytes)`, nullable `GroupRuntime.cycle`)
  and the **test-side consolidation** - both need a recorded ruling first, because frozen
  earlier-phase tests depend on the seams. Pass 2.
- M1 and M6 have no dedicated regression test: staging a `configure()` failure or a dead
  target connection needs a fault-injection seam neither the fake nor the adapter has.
  Both fixes are small and their surrounding paths are covered.
## Design session - archive & diff layer  (2026-08-28, user-decided)

Grill session on persisting snapshots for cross-restart diffing. Outcome: spec
Sec 18 added, D30-D36 recorded, plan M3 (P11-P14) added. No code; docs only,
per the docs-first rule. Framework itself untouched: D10/D11/D22/D24, the
five-interface budget and all frozen contracts stand.

Decisions (rationale in the decision log):

1. **Consumer-land layer** in `infra.snapshotarchive`, one-way ArchUnit
   boundary; same Maven module (D30). Explicitly NOT the Sec 14.2 extension -
   cross-ref added there.
2. **Durable identity = Oracle sequence** in a manifest table; generation
   numbers stay in-process; `data_as_of` is the only join key, with an
   archiver-enforced monotonicity guard (D31).
3. **Checkpoints only, no delta files** (D32). Hourly full Parquet per table
   (~1M rows => tens of MB). An ETL always diffs `checkpoint(watermark)` vs
   the LIVE snapshot - one download per run, no checkpoint-to-checkpoint
   diffs. Over-report safe (D25), under-report impossible; revisit at ~50M
   rows by layering deltas on top.
4. **Intent-first publish** (D33, user-specified): PENDING row with full
   inventory before any upload; conditional transitions; watchdog converges
   PENDING -> COMPLETE/FAILED; ghost files impossible, no LIST sweep. Crash
   and graceful shutdown share the watchdog recovery path.
5. **Retention = fixed window sized to slowest ETL + keep-newest-COMPLETE**
   (D34); full compare vs live snapshot is the designed fallback.
6. **Watermark is ETL-owned** (D35): `max(version) WHERE status='COMPLETE'
   AND data_as_of <= snapshot.dataAsOf`, committed with the ETL's output; the
   predicate closes the long-running-job under-report race.
7. **PK-required tables only; Parquet, download-then-read** (D36). Unkeyed
   tables cut. Parquet decouples the archive from the DuckDB 1.1.3 pin.

Rejected in session: restore-and-serve at startup (driver is diff-chain
survival only; D10 stands), per-refresh deltas, consumer registration for
retention, archiving `.db` files, MVCC-style in-store history.

Open before P11 (spec 18.6): COPY-TO-parquet-on-read-only-attach spike (else
stage via public copyOut, D16 instance); checkpoint size/duration measurement
at 1M rows; watchdog timeout vs real upload time.

## Review fix pass 2 - deep-module pass on the core seams  (2026-08-29)

Seam review of `api` / `core` / `spi` / `duckdb` (deep-module criteria: leverage per
unit of interface, one adapter = hypothetical seam, the interface is the test surface).
Verdicts: `api.SnapshotCache` deep and correct; `spi.GenerationStore` a real seam - two
adapters, and it is what keeps the core suite DuckDB-free; `GenerationSource` /
`GenerationCheck` correctly shaped. Two findings were acted on.

### H2's guard extended to the consumer side, closing a permanent lease leak

Pass 1 wrapped every `events.` call in `RefreshCycle` and stopped there. The five fires
in `DefaultSnapshotCache` stayed unguarded, so the same contract - a caller-supplied sink
must never break the path that fired it - held on the refresh side only, with nothing at
the interface saying so.

The gap at `acquireWaited` was a live defect, not just an asymmetry: `tryAcquire` has
already incremented the refcount and registered the lease when the sink fires, so a
throwing sink escaped before the lease reached the `SnapshotHandle` that owns its
release. Nothing could ever release it - the generation was pinned for the process
lifetime and refresh eventually wedged at the K guard (spec 6.1). `leaseOrphaned` /
`leaseReleased` escaped `Snapshot.close()` and masked the block's own result;
`acquireUnavailable` substituted the sink's exception for `NotReadyException`.

Fix: `core/Internals.kt` (mirroring `spi/Internals.kt`) holds one `emit(group) { ... }`,
used by both classes. `RefreshCycle`'s private `notify` is gone. The guard now lives in
one place instead of at each call site's discretion.

- `ReviewFixRegressionTest.throwingEventSink_onAWaitedAcquire_isIgnored_andTheLeaseIsNotLeaked`
  proves it: with the `acquireWaited` guard removed the acquire returns the sink's
  `RuntimeException` instead of a `Snapshot`, and the test fails. Verified by reverting
  that one line.

### L1's `publish(gen, fileBytes)` seam removed (the pass-2 ruling that was owed)

Ruled: delete it. The overload existed only for two `GenerationRegistryTest` setup lines
and forced `RegistryLease.opened` and `.generationInfo` nullable through production code,
paid for by `openedOf` / `dataAsOfOf` `checkNotNull` helpers on the acquire path.

- `GenerationRegistry`: one `publish(gen, opened, info)`; `publishInternal` folded into
  it; both lease fields non-null. The LIVE-implies-both invariant is now checked once
  inside `tryAcquire`, under the lock where it actually lives, instead of twice in the
  facade.
- `DefaultSnapshotCache`: both helpers deleted, call sites read `lease.opened` and
  `lease.generationInfo.dataAsOf` directly.
- No earlier-phase assertion or scenario line was touched. `GenerationRegistryTest` gains
  a private `GenerationRegistry.publish(gen, bytes)` extension adapting to the real
  overload with a stub `OpenGeneration`, so all 18 existing call sites are unchanged.

Nullable `GroupRuntime.cycle` was deliberately left alone: unlike the publish overload it
has a non-test justification - E2E's `coldGroup` is a group that never refreshes.

135 tests green (2 pre-existing skips).

### Still open

- **`RefreshPhase.QUERY` / `FETCH` / `APPEND` are never emitted.** Only a
  `GenerationSource` could time them and `BuildContext` hands it no events handle. Three
  label values a binder must handle and will never see.
- **`CacheEvents.leaseExpired` is never fired** and `GenerationRegistry.expiredLeases()`
  has test callers only. Spec 6.2's lease-deadline diagnostic is declared at two seams and
  wired at neither - P9 owes the poll, or both go.
- **`GenerationRegistry.current()`** has no production caller; `currentInfo()` covers it.
- **`tryBeginRound` / `endRound` could collapse** into one `inRound { }` that makes the
  protocol unrepresentable rather than `check()`-enforced, without moving I/O under the
  lock. Not done: it reshapes `runOnce`'s control flow.
- **`Hook` / `HookRunner` sit in `api`**, widening the consumer seam with five test-only
  constants. `spi` is the honest home; `core` may depend on it either way.
- **`CacheEvents` is 11 methods where one `on(event: CacheEvent)` would do**, which would
  also make the guard above a single wrapper rather than a helper each call site must
  remember. Blocked: the signature is FIXED and pinned to spec 12.2 labels plus
  `MetricLabelContractTest`, so it needs a spec change first.

## M3 ticket 01 - Parquet export spike  (2026-08-29)

M3 was broken into five tickets under `docs/snapshotcache/m3-tickets/`, mapping onto
plan P11-P14 with the spec 18.6 spike promoted ahead of them as its own ticket. This entry
covers ticket 01 only.

### Delivered
- `duckdb/ParquetExportSpikeTest` (3 tests) - the whole deliverable. Exports from the real
  store's READ_ONLY-attached duplicate connection, asserts the attach still rejects an
  INSERT afterwards, and reads the file back through a separate instance to prove it is
  portable Parquet. The third test carries the 18.6 item-2 measurement.
- Spec 18.6 items 1 and 2 closed in the spec; item 3 left open and re-scoped.
- **No main source.** The spike's deliverable is the answer, and the answer is the spec
  entry plus this test. A production `exportTable` would have had one caller - a test -
  until ticket 03 unblocks, so it was written, reviewed, then deleted rather than parked in
  `infra.snapshotarchive` as scaffolding. Ticket 03 places it knowing what it needs.

### The answer
`COPY ... TO '<f>.parquet'` works directly on a read-only attached snapshot connection.
The `copyOut` staging fallback (D16) is not needed and was not built - that was the fork
this ticket existed to resolve, and it collapses ticket 03's export step to one statement
under the lease the archiver already holds.

Measured on the pinned duckdb_jdbc 1.1.3, three runs: 1M rows -> 14,180,166 bytes in
39/41/52 ms. Retention storage sizes off ~14 MB per million rows per table. The
lease-vs-K question the spec flagged answers itself at 40 ms.

Two smaller findings, both folded in: `executeUpdate` returns the COPY row count, so the
inventory's `row_count` needs no second scan (guarded with a `check`, since a driver
returning -1 would put a negative count into a manifest row the watchdog later verifies);
and A3 survives the export, which is what lets the archiver run against the live serving
instance at all, so it is asserted rather than assumed.

### Deviations from the documents
- **None on contracts.** No main source was modified at all; no frozen interface,
  invariant, equation, enum, or earlier test was touched. Full suite 138 tests, 0 failures
  (135 before, plus these 3; the 2 aborted are the pre-existing Unix-only skips).
- **The test lives in `infra.snapshotcache.duckdb`, not `infra.snapshotarchive`** (user
  ruling, 2026-08-29, after the first placement was committed). What it pins is a DuckDB
  adapter capability - `COPY ... TO parquet` is the neighbour of `copyOut`, not archive
  policy, which is manifests, versions, retention and watermarks. Siting it here also lets
  it reuse `spi`'s `ident`/`literal` instead of duplicating them, and leaves
  `infra.snapshotarchive` to be created cleanly by ticket 02 together with its ArchUnit
  rules rather than existing unguarded in the meantime.

### Notes for later tickets
- **Row count comes from `COUNT(*)`, not from COPY's update count.** 1.1.3 does report it,
  but an empty table and a driver that stopped classifying COPY as DML both return 0, and
  nothing downstream could tell them apart - a 0 recorded for a 1M-row table would be
  committed into the PENDING inventory and then "verified" against the real object. Probed
  before deciding: `executeQuery` is rejected outright for COPY, so there is no third option.
- **`ArchitectureTest` imports only `infra.snapshotcache`.** When ticket 02 creates
  `infra.snapshotarchive`, adding that package to `importPackages` is part of the same
  change - otherwise its two new rules are declared but never evaluated, and the package
  could reach into `spi`, `core`, or `org.duckdb` with a green build.
- **Spec 18.6 item 3 is still open and is ticket 04's input.** The spike sizes the payload
  (~14 MB) but there is no MinIO link on this machine, so the worst-case upload time behind
  the watchdog timeout T was not measured. T must not be picked from the export number.
- **M2 is not implemented.** No P9 wiring, no P10 Oracle source, and the module pom has no
  JDBI, Oracle driver, or MinIO client. Tickets 02-05 need all of those; plan 3c gates M3 on
  M2 being accepted. Ticket 01 was the only one that could run today - it needs DuckDB alone.
- Maven is not on PATH in this environment. The suite was compiled and run directly against
  the cached `.m2` jars (Kotlin 2.2.0 compiler, `-Xfriend-paths` for the `internal` test
  access, main and test into `target/classes` / `target/test-classes` so ArchUnit's
  DO_NOT_INCLUDE_TESTS still excludes tests). Anyone re-running should just use `mvn test`.
