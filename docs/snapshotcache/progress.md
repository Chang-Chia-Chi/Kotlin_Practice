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
