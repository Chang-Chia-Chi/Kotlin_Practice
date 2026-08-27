# Snapshot Cache Code Review — 2026-08-28

**Status (2026-08-28, fix pass 1):** H1, H2, H3, M1, M2, M3, M4, M6 and the main-source
half of L1 are **FIXED**, with seven discriminating regression tests
(`core/ReviewFixRegressionTest.kt`, `duckdb/DuckDbStoreReviewFixTest.kt`). 134 tests, 0
failures. **Open for pass 2, ruling first:** M5, L1 nullable test-only seams, L1
test-side consolidation. Details and deviations: progress.md, Review fix pass 1.

---

Scope: all main and test sources under `snapshotcache/src/`, reviewed against
`docs/snapshotcache/spec.md`, `docs/snapshotcache/plan.md`, and the deviations
recorded in `docs/snapshotcache/progress.md`. Method: 8 independent finder
angles (line-by-line, spec-contract, cross-file tracing, conventions,
efficiency, simplification, reuse, altitude), 23 raw candidates deduplicated,
each survivor adversarially verified against the full main sources. 10 findings
survived; the conventions angle found no CLAUDE.md violations.

Severity: **HIGH** = an invariant (spec 17.2) violated, the cache wedged
permanently, or a spec-legal input unreachable in production. **MEDIUM** =
races and cleanup-protocol gaps with operator-visible effects, resource leaks,
and mandated diagnostics that cannot fire. **LOW** = cleanups the fix pass can
batch.

Each finding notes its verification verdict: CONFIRMED (defect established by
reading the code; failure scenario reachable) or PLAUSIBLE (facts verified but
the defect is latent, timing-dependent, or judgment-dependent).

**For the fixing session:** read `progress.md` before touching anything — a
deviation may already cover it. Findings **H1, H2, M1, M3** are CONFIRMED
against source. **M5** needs a plan ruling before it is a code fix. **L1**
requires a documented ruling because frozen earlier-phase tests depend on the
seams (CLAUDE.md: never modify an earlier phase's tests; update the document
first, then the code). Fix once at the root — several findings share a
boundary (H1's close/delete split also appears in `abort()`; H3's quoting
helper also collapses the duplicated JDBC scalar helpers).

---

## HIGH

### H1. reclaimPass treats close+delete as one retryable unit, but close is not idempotent — a close-succeeded-then-delete-failed generation wedges in a permanent defer loop
`snapshotcache/src/main/kotlin/infra/snapshotcache/core/RefreshCycle.kt:224` — CONFIRMED

`store.close(gen)` DETACHes and drops the issued-tracking entry
(`DuckDbGenerationStore.kt:90-99`); if `store.delete(gen)` then throws
transiently (Windows AV/indexer file lock), `deferReclaim` returns the
generation to LIVE. Every later pass re-runs `store.close(gen)`, which now
fails unconditionally — DETACH of a no-longer-attached alias is a catalog
error, and the testkit fake's not-opened check behaves the same — so the
generation is deferred forever even after the file lock clears.

Failure scenario: the file is never deleted; each incident adds one undeletable
LIVE generation; past K, `blockedByK()` reports blocked with **zero** blocking
leases and refresh stalls permanently (violates I5). The same protocol split
exists in `abort()` (`RefreshCycle.kt:198-205`), which conversely DELETEs the
file even when the detach just failed, leaving a dangling attached alias on the
serving instance.

Suggested fix, once at the store boundary: make `close(gen)` idempotent (no-op
if not attached) and/or track detach separately from delete in `reclaimPass`,
and route `abort()`'s opened-generation cleanup through the same
close-then-delete-or-defer sequence.

### H2. round() catches only InterruptedException and RoundAbort — a RuntimeException from a caller-supplied CacheEvents callback skips abort() and leaks a zombie generation record
`snapshotcache/src/main/kotlin/infra/snapshotcache/core/RefreshCycle.kt:172` — CONFIRMED

A metrics-backed `CacheEvents.refreshPhase` (lines 118/135/161) or
`events.verifyFailed` (line 144) that throws `RuntimeException` propagates past
both catch clauses; `discardBuild` never runs.

Failure scenario: a zombie `GenRecord` stays in `records` for the process
lifetime (poisoning `liveGenerations` and the 17.3 accounting), the
candidate/promoted file stays on disk, and no `refreshFinished` event fires.
Related unguarded edge inside `abort()` itself: `candidate?.close()` (line 192)
is bare, so a third-party `GenerationStore` whose `Candidate.close()` throws
aborts the abort before `discardBuild` — the never-throws guarantee lives only
in the DuckDB adapter and a progress note, not the `spi.Candidate` contract.

Suggested fix: catch `Exception` (rethrowing RoundAbort/Interrupted first)
around `round()`'s body or wrap events/hook calls; `runCatching` the candidate
close in `abort()`; document never-throws on `spi.Candidate.close`.

### H3. Verify-gate SQL interpolates discovered table names and requiredNonNull columns unquoted — a reserved-word or mixed-case table name makes every verify round fail and the cache go permanently stale
`snapshotcache/src/main/kotlin/infra/snapshotcache/spi/VerifyGate.kt:74` — PLAUSIBLE

Table names come verbatim from `information_schema`; `SELECT COUNT(*) FROM
order` is a parse error, which `rule()` converts into a non_empty failure.

Failure scenario: a source table named `order` (or mixed-case via quoted DDL)
makes every round VERIFY_FAILED for valid data; the 8.5 escalation fires and no
new generation ever publishes. The quoting helper already exists as
`DuckDbGenerationStore.ident()` (line 231) but is private to the adapter.

Suggested fix: quote identifiers in VerifyGate's query builders — hoist
`ident()` to a shared internal helper at the spi boundary, which also collapses
the four duplicated `queryLong`/`queryString` JDBC helpers
(`VerifyGate.kt:147-172`, `DuckDbGenerationStore.kt:233-241`; see L1).

---

## MEDIUM

### M1. createCandidate creates the .tmp file before configure() can throw, but abort() skips store.delete when candidate == null — one orphaned .tmp leaked per failed round
`snapshotcache/src/main/kotlin/infra/snapshotcache/duckdb/DuckDbGenerationStore.kt:68` — CONFIRMED

`DriverManager.getConnection("jdbc:duckdb:<tmp>")` creates `gen_NNN.db.tmp` on
disk; if `configure()` throws (bad memory_limit/temp dir/disk fault),
`createCandidate` closes the connection and rethrows; `storeOp` classifies
DISK_ERROR with `candidate == null`, and `abort()`'s guard at
`RefreshCycle.kt:202` (`candidate != null || opened != null`) skips
`store.delete(gen)`.

Failure scenario: a persistent fault leaks one `.tmp` per 10-minute round until
restart. The `InMemoryGenerationStore`'s scripted CREATE_CANDIDATE failure
mutates no state, so core tests never see the divergence.

Suggested fix: call `store.delete(gen)` unconditionally in `abort()` — delete
is deleteIfExists-safe for both paths — which also removes the guard's
unreachable `opened != null` disjunct (`opened` is only ever assigned after
`candidate`).

### M2. isShuttingDown() and tryAcquire() are separate critical sections and tryAcquire never checks the flag — a lease can be granted after beginShutdown, even after the drain reported clean
`snapshotcache/src/main/kotlin/infra/snapshotcache/core/DefaultSnapshotCache.kt:147` — PLAUSIBLE

Thread A passes `isShuttingDown() == false` at line 147 and is preempted;
`shutdown()` runs beginShutdown + awaitQuiescence (no leases → returns empty;
P9 wiring proceeds to close the store); A resumes and `tryAcquire` (line 149;
the post-wait one at line 161 has the same window after the check at 159)
succeeds because `currentGen` is still set.

Failure scenario: a Snapshot over a store being closed, invisible to the
finished drain — violates spec 10.2 step 1 ("new acquires throw
ShuttingDownException immediately").

Suggested fix at the root: have `GenerationRegistry.tryAcquire` return null (or
throw) when shuttingDown, inside its existing lock — one line, closes both
call-site windows.

### M3. track() appends every issued connection to a per-generation CopyOnWriteArrayList never pruned of closed entries — unbounded heap growth while one generation stays current
`snapshotcache/src/main/kotlin/infra/snapshotcache/duckdb/DuckDbGenerationStore.kt:221` — CONFIRMED

Entries are removed only at `close(gen)`/`delete(gen)`, which never run for the
current generation.

Failure scenario: refresh stalls (source outage, repeated verify failures,
BLOCKED_BY_K) so one generation stays current for hours/days while consumers
acquire per request; every `Snapshot.connection()` appends (~86k entries/day at
1 read/s), each `CopyOnWriteArrayList.add` copying the whole array. The same
pattern exists consumer-side in `SnapshotHandle.CleanupState.issued`
(`SnapshotHandle.kt:65-72`): connections a long-lived `withSnapshot` job
already closed stay listed until the lease ends.

Suggested fix: `removeIf { it.isClosed }` before append in `track()` and in
`CleanupState.issue()`.

### M4. No shutting-down check at the VERIFYING→PUBLISHING boundary — a shutdown that begins during the seconds-long verify still swaps the current pointer and reclaims mid-drain
`snapshotcache/src/main/kotlin/infra/snapshotcache/core/RefreshCycle.kt:158` — PLAUSIBLE

Cooperative shutdown checks exist only at round entry (line 93) and before
promote (line 120). `shutdown()` marks shutting-down while `gate.verify()` runs
full-table scans; the round then proceeds through BEFORE_POINTER_SWAP,
`registry.publish()`, and `reclaimPass()`.

Failure scenario: publishes a generation no consumer can ever acquire and
detaches/deletes the old current during exit — contradicts spec 10.2 step 3 /
9.2 ("never promote…, current pointer untouched") and never counts
`shutdown_aborted` for this window. The P4 progress deviation covers failure
classification at stage boundaries, not this missing boundary check.

Suggested fix: one `registry.isShuttingDown()` check between the gate verdict
and `registry.publish`, throwing `RoundAbort(SHUTDOWN_ABORTED)`.

### M5. Spec 6.2 lease-deadline diagnostics are dead code — nothing calls expiredLeases() in production and CacheEvents.leaseExpired never fires
`snapshotcache/src/main/kotlin/infra/snapshotcache/core/GenerationRegistry.kt:272` — PLAUSIBLE

Spec 6.2 mandates "record snapshot_lease_expired_total; log the owner" as the
designed early warning before generations pile up to K, but `expiredLeases()`
is called only from `GenerationRegistryTest` and `leaseExpired`
(`CacheEvents.kt:64`) is defined but never fired. Not recorded as a deviation
in progress.md.

Failure scenario: a consumer holds a lease past the 5-minute deadline and the
operator's first signal is the much later blocked_by_K alert.

**Needs a ruling before fixing:** confirm against the plan whether P9 wiring is
supposed to poll `expiredLeases()` on the schedule tick — if yes, this drops to
a documentation gap; if the core event sink is supposed to fire it (plan 2.3
reading), wire an expiry sweep into the refresh round or admin path.

### M6. copyOut's cleanup can mask the real failure and leave a dangling attach
`snapshotcache/src/main/kotlin/infra/snapshotcache/duckdb/DuckDbGenerationStore.kt:140` — PLAUSIBLE

(a) Target connection dies mid-CTAS: `CREATE TABLE` throws the informative
error, then the finally's `USE ident(home)` throws on the same dead connection
and replaces it — logs show only "USE failed", root cause lost. (b) `DETACH
$alias` fails (line 143): `runCatching` logs and returns normally,
`DefaultSnapshotCache.copyOut`'s finally releases the lease, the next
`reclaimPass` detaches from serving and deletes the file while the caller's
target instance still holds the READ_ONLY attach — later target queries hit a
dangling alias/deleted file.

Suggested fix: wrap the finally-cleanup statements in `runCatching` so the CTAS
failure propagates, and on DETACH failure propagate instead of swallowing (the
lease is still held at that point, so failing the copyOut is safe and keeps the
file-pinned semantics honest).

---

## LOW

### L1. Cleanup batch (altitude, efficiency, reuse)
Various files — cleanups the fix pass can batch. The first item **requires a
documented ruling** because frozen earlier-phase tests depend on the seams.

- **Test-only nullable seams leak into production types**
  (`GenerationRegistry.kt:127`): the `publish(gen, fileBytes)` overload forces
  nullable `RegistryLease.opened`/`generationInfo` (consumer-time
  `checkNotNull` crashes in `DefaultSnapshotCache.openedOf`/`dataAsOfOf`,
  `DefaultSnapshotCache.kt:202-207`), and `GroupRuntime.cycle` is nullable only
  for registry-only tests (runtime ISE in `cycleOf`,
  `DefaultSnapshotCache.kt:137`). Any future wiring using these compiles
  cleanly and blows up far from the wiring bug. Both are used by frozen
  earlier-phase tests, so per CLAUDE.md this needs a recorded ruling: propose
  deleting the fileBytes-only overload and making `cycle` non-null with tests
  wiring stubs over the existing `InMemoryGenerationStore`; update
  plan/progress first.
- **VerifyGate scans each table up to 3 times per refresh**
  (`VerifyGate.kt:83`): non_empty's `COUNT(*)` plus key_unique's `COUNT(id)`
  and `COUNT(DISTINCT id)` collapse into one `SELECT COUNT(*), COUNT(id),
  COUNT(DISTINCT id)` per table — this directly shortens the
  attached-but-unpublished window. DuckDB 1.1.3 handles multi-aggregate.
- **`Throwable.describe()` duplicated** in `RefreshCycle.kt:279` and
  `VerifyGate.kt:174`.
- **Test-side duplication belongs in testkit**: `MutableClock`
  (`DefaultSnapshotCacheTest.kt:461`) is a byte-for-byte copy of
  `MutableTestClock` (`P4TestSupport.kt:337`); five private recording
  `CacheEvents` implementations (`P4TestSupport.kt:299`,
  `ConcurrencySuiteTest.kt:541`, `DefaultSnapshotCacheTest.kt:471`,
  `RandomizedModelTest.kt:385`, `EndToEndFeasibilityTest.kt:607`) where one
  testkit recorder replaces four (the counter-based stress variant may stay);
  `awaitParked`/`joinOrFail` copied across three files
  (`DefaultSnapshotCacheTest.kt:443`, `ConcurrencySuiteTest.kt:510/520`,
  `P4TestSupport.kt:415`, `EndToEndFeasibilityTest.kt:583/598`);
  `ConcurrencySuiteTest.kt:46` re-declares `RefreshCycleTestBase`'s fixture
  wiring instead of extending it (VerifyRulesTest already extends the base per
  progress.md); the JDBC scalar-query helper is hand-rolled four times (see
  H3's fix, which covers the two main-side copies; one testkit helper covers
  `DuckDbGenerationStoreTest.kt:264` and `EndToEndFeasibilityTest.kt:562`);
  `EndToEndFeasibilityTest.kt:302` re-implements the adapter's
  generation-file naming rule (`gen_` + `padStart(10,'0')` + `.db`) inline —
  use a literal like the file's other spec-3.1 assertions, or expose the
  adapter's naming as an internal helper.

Note: modifying frozen earlier-phase test files to deduplicate them is an
earlier-test modification under CLAUDE.md. Test-side consolidation should be
proposed as its own recorded pass (documents first), or limited to the newest
phase's own test sources.

---

## Fix-pass ordering

1. **Pass 1, no ruling needed:** H1, H2, M1, M2, M3, M4, M6, and the
   main-source L1 items (verify-gate single scan, `describe()` dedup) — plus
   H3 once the shared `ident()` helper location is chosen.
2. **Pass 2, ruling/document first:** M5 (plan reading on who fires lease
   expiry), L1's nullable-seam removal, and any test-side consolidation.
3. After each pass: run the full suite (`mvn test`), verify no earlier-phase
   test changed, and append a progress.md entry describing what was fixed and
   any deviation recorded.
