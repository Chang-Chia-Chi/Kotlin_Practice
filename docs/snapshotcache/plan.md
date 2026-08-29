# DuckDB Generational Snapshot Cache - Implementation Plan

Version: v1.0
Companion to: `duckdb-snapshot-cache-spec.en.md` (v1.0, Sec 1-Sec 18)
Purpose: break the framework into steps small enough for an AI agent to implement in one session each, with fixed contracts and fixed acceptance criteria so implementations may vary but assertions may not.

---

## 1. Ground Rules for Agent-Driven Implementation

These apply to every step.

1. **Fixed vs free.** Interfaces, value types, invariants (spec Sec 17.2), accounting equations (Sec 17.3), and test assertions are FIXED by this plan and the spec. Internal class structure, algorithms, and naming of private members are free. An agent that believes a fixed contract is wrong must stop and flag it, not silently adapt it.
2. **One step, one PR.** Each step lands as a single reviewable change, roughly 200-600 lines including tests. If a step grows past that, it was scoped wrong; stop and split.
3. **Tests are deliverables, not afterthoughts.** A step without its listed tests is not done. Test names for invariants follow the `I<n>_...` convention (Sec 17.2).
4. **No scope creep.** A step must not implement anything listed under a later step, even "while I'm here." Stubs that throw `NotImplementedError` are acceptable placeholders for later-step seams.
5. **No sleeps in tests.** Deterministic interleaving via hooks only (Sec 17.4). A `Thread.sleep` in a concurrency test fails review.
6. **Build tooling.** Maven. Follow the existing team quality gates (static analysis, architecture tests) from step 0 onward, not retrofitted at the end.

---

## 2. Architecture Overview

### 2.1 Design stance

Follow the deep-module principle: a small number of interfaces with simple signatures, each hiding substantial complexity. Do not introduce an interface unless (a) it is part of the public API, or (b) it is a genuine test seam identified in spec Sec 17.1. Everything else is a concrete internal class.

Clean-architecture layering applies in its minimal form: the core knows nothing about DuckDB, Oracle, Quarkus, or Micrometer. Adapters point inward. That is the whole rule; no additional layers, mappers, or DTO translation between layers.

### 2.2 Package layout

The framework lives **inside the existing service project**, not as a separate library or Maven module. It gets its own package subtree under the team's `infra` layer, which acts as an enforceable boundary rather than a step toward extraction:

```
<team-root>
+-- infra/
|   +-- snapshotcache/
|       +-- api/      Public surface: SnapshotCache, Snapshot, GenerationSource,
|       |             GenerationCheck, CacheEvents, GenerationInfo, CopyOutSpec/Result,
|       |             VerifyResult, GenerationState, LeaseInfo, SnapshotCacheConfig,
|       |             NotReadyException, ShuttingDownException.
|       |             Depends on: JDK + kotlin-stdlib only.
|       +-- spi/      GenerationStore, Candidate, OpenGeneration.
|       |             Depends on: api.
|       +-- core/     GenerationRegistry, RefreshCycle, ReclaimPass, OrphanDetector,
|       |             DefaultSnapshotCache. All Kotlin `internal`.
|       |             Depends on: api, spi.
|       +-- duckdb/   DuckDbGenerationStore, copy-out mechanics, DuckDB config plumbing.
|                     Depends on: spi (+ api value types), duckdb_jdbc.
+-- etl/              Business: consumer jobs
+-- source/           Business: Oracle extraction (implements GenerationSource)
+-- ...               Other existing business packages
```

Why a dedicated subtree even though it is never extracted:

- **The boundary becomes one ArchUnit rule** (`infra.snapshotcache..` must not depend on business packages) instead of a list of class names that goes stale on every addition. This is the rule that stops an agent from importing a business type into the framework for convenience.
- **Stack traces localize the problem.** "Framework bug or my `GenerationSource` bug" is answered by which package the frame is in.
- **Agent briefings get a clean fence**: "only touch `infra/snapshotcache/`."

`spi` is a separate package (not folded into `core`) because `GenerationStore` genuinely has two implementations - the DuckDB one and the in-memory test double - and because the Sec 14.2 object-storage extension is an already-planned second adapter. `infra` already conveys "this touches the outside world", so no additional `io` layer is introduced.

Time is `java.time.Clock`, injected. No custom time abstraction.

ArchUnit rules (enforced from P0):

- `infra.snapshotcache..` must not depend on any business package (`etl..`, `source..`, etc.).
- `infra.snapshotcache.{api,spi,core}..` must not depend on `infra.snapshotcache.duckdb..`, `org.duckdb..`, `io.quarkus..`, or `io.micrometer..`.
- `api` must not depend on `spi`, `core`, or `duckdb`.
- `java.sql..` is permitted only in `api` signatures (`Connection`), `spi`, and `duckdb`.
  This rule stands as written and is **not** relaxed for P3. The consequence is that the
  `Snapshot` handle implementation may not live in `core`: naming `Connection` as a field,
  parameter or return type there violates the rule (verified empirically at P0 - a planted
  `Connection` field produced three violations). `OpenGeneration` already owns the
  connection, so it produces the handle and `core` holds it only as the `api.Snapshot`
  type. Lease bookkeeping is unaffected: `core` supplies the release callback. See D28.
- Logging in `api`, `spi` and `core` uses `org.jboss.logging.Logger`. `io.quarkus.logging.Log`
  is forbidden there by the rule above and would force the core suite to boot Quarkus. The
  host service is Quarkus, so `quarkus.log.*` configuration governs the output either way.
  See D27.
- Nothing outside `core` reaches into `core` internals (Kotlin `internal` plus an ArchUnit rule).

### 2.3 Public surface budget

Exactly five interfaces. This is a hard budget.

| Interface | Role | Why it exists |
|---|---|---|
| `SnapshotCache` (+ `Snapshot`) | Consumer API: withSnapshot / copyOut / acquire / currentInfo | The product |
| `GenerationSource` | Caller-injected "how to pull" | Spec Sec 5.2; delta upgrade path |
| `GenerationCheck` | Caller-extensible verify gate | Spec Sec 5.2; built-in rules provided |
| `GenerationStore` (spi) | The only component touching files/DuckDB | Test seam Sec 17.1; keeps core pure |
| `CacheEvents` | Single event sink: refresh outcomes, step timings, lease expiry/orphan, blocked-by-K | Keeps core free of Micrometer; one place to wire metrics and logs. Default no-op implementation ships in api |

Gauge-style metrics (current generation, data_as_of, rows, live generations, active leases, file bytes) are NOT events; they are polled from `SnapshotCache.currentInfo` and `CacheAdmin.liveGenerations` by the caller-land metrics binder. The listener carries only discrete occurrences.

`CacheAdmin` (spec Sec 5.3) is implemented as additional methods on the same core object that implements `SnapshotCache`; split it into a second interface only if P9 shows a real need to hand consumers a narrower type. Default: one concrete class, two small interfaces max.

### 2.4 Do-not-build list (anti-over-engineering)

Agents must NOT introduce any of the following, even as "extensibility":

- A `LeaseManager`, `GcStrategy`, `GenerationNumberProvider`, `RetryPolicy`, or similar single-implementation interface.
- An event bus, observer registry, or annotation-driven listener discovery. One listener, injected in the constructor.
- A generic "pipeline" or "plugin" framework around verify rules. Built-in rules are a hardcoded list; caller extension is the single `GenerationCheck` injection point.
- Async/coroutine orchestration. The refresh loop is one thread; consumers bring their own threads.
- Persistence of registry state. Spec D10: startup wipes and rebuilds.
- A separate Maven module or extracted library. The framework lives in the service project; packages + ArchUnit are the boundary.
- A custom time abstraction. Use `java.time.Clock`, injected.
- A logging facade or adapter of any kind. `org.jboss.logging.Logger` directly.
- JDBI anywhere in `api`, `spi` or `core`. The host service uses Kotlin + Quarkus + DuckDB
  + JDBI, but `api` depends on the JDK and kotlin-stdlib only; JDBI is confined to the
  `duckdb` adapter and to caller-land `GenerationSource` implementations.
- Speculative multi-group scheduling policies. A `Map<GroupId, ...>` and a per-group loop is the design.

### 2.5 Concurrency architecture rule

This is the one structural rule that prevents the classic failure modes. It is fixed.

- All mutable state (generation table, current pointer, refcounts, leases, consecutive-failure counter) lives inside `GenerationRegistry`, guarded by a single monitor.
- **No I/O ever executes while holding the registry lock.** Storage calls (createCandidate, promote, open, close, delete) are decided under the lock but executed outside it.
- To keep invariants across that gap, the registry uses explicit transitional states per generation: `BUILDING`, `OPENING`, `LIVE`, `RECLAIMING`, `GONE`. Example: GC marks a generation `RECLAIMING` under the lock (making it invisible to acquire), then detaches/deletes outside the lock, then marks `GONE` under the lock. `acquire` only ever hands out `LIVE` generations, preserving I2 without holding the lock across DETACH.
- Registry methods are therefore short, non-blocking, and trivially testable; the orchestrator sequences the I/O.
- Shutdown is part of the same state: a `shuttingDown` flag lives in the registry, and the publish condition variable is signalled on shutdown so every waiter is released at once (spec Sec 10.2 step 1). Waiting must use interruptible condition await, never an uninterruptible lock.
- ArchUnit or a targeted review checklist item: no `GenerationStore` call inside a `synchronized` block of the registry.

---

## 3. step Plan

Work is split into two milestones. **M1 is the framework acceptance scope** (spec Sec 17.8); nothing in it touches Oracle or Quarkus, so it can run end to end against synthetic data. **M2 is integration**, and is only started once M1 is accepted - if the concurrency model turns out to be wrong, M1 fails before any integration effort is spent on it.

```
M1 (framework)

  P0 --+--> P1 --> P3 --> P4 --> P5 --> P6     core logic + concurrency tests
       |          ^        |
       +--> P2 ---+        |                   test kit (needed from P3 on)
       |                   v
       +--> P7 ----------> P8                  DuckDB adapter, then synthetic E2E

M2 (integration)  -- starts only after M1 is accepted

  P9 --> P10                                   service wiring, then Oracle source

M3 (archive & diff layer, spec Sec 18)  -- starts only after M2 is accepted

  P11 --> P12 --> P13
            |
            +---> P14                          manifest, archiver, watchdog/purge, diff helper
```

Three tracks run in parallel after P0: core logic (P1, P3-P6), test kit (P2), and the DuckDB adapter (P7).

---

## 3a. M1 - Framework

---

### P0 - Skeleton, API types, quality gates

- **Goal:** compilable module with the entire fixed public surface, so every later step codes against final signatures.
- **Deliverables:** package subtree under `infra/snapshotcache/`; `api` complete (all five interfaces including `waitBudget` parameters, value types, `SnapshotCacheConfig` with Sec 13 defaults, `NotReadyException`, `ShuttingDownException`, no-op `CacheEvents`); `spi` complete (`GenerationStore`, `Candidate`, `OpenGeneration`); `Hooks` enum with a no-op hook runner; ArchUnit rules of Sec 2.2; static-analysis config.
- **Out of scope:** any behavior. Core classes may exist as empty shells.
- **Acceptance:** compiles; ArchUnit tests pass; config defaults match spec Sec 13 exactly (assert in a test).
- **Size:** small.

### P1 - GenerationRegistry (pure core state machine)

- **Goal:** the deep module. All bookkeeping and all concurrency-critical decisions, zero I/O.
- **Deliverables:** generation lifecycle states (Sec 2.5), monotonic numbering, atomic acquire (read current + refcount++ in one critical section), release with idempotent close semantics, lease records (owner, acquiredAt, deadline via injected `Clock`), K enforcement (report blocked, never throw), GC candidate selection (`RECLAIMING` marking), state snapshot for `liveGenerations`, the `shuttingDown` flag and the publish/shutdown condition variable. Hook points wired at `AFTER_READ_CURRENT`, `AFTER_POINTER_SWAP`, `BEFORE_DETACH`.
- **Fixed contracts:** invariants I2, I3, I4 (registry half), I6, I8; the Sec 2.5 rule.
- **Acceptance:** named tests `I2_`, `I3_`, `I4_`, `I6_`, `I8_`; double-close test; acquire-during-swap test using the `AFTER_READ_CURRENT` hook (registry-level: swap + mark old gen `RECLAIMING`, assert handle's gen still `LIVE`-readable per I2 rules); deadline expiry via `Clock.fixed`/`Clock.offset` with no real waiting; waiters released by both publish and shutdown signal.
- **Size:** medium. This is the step where correctness lives; give it the review attention.

### P2 - Test kit: fake storage + accounting fixture

- **Goal:** the instrument every later test uses.
- **Deliverables:** `InMemoryGenerationStore` recording every call with arguments and order; scripted failure injection ("Nth close throws", "promote of gen X throws"); a shared JUnit fixture/extension asserting the Sec 17.3 accounting equations at the end of every test automatically; a tracking wrapper for issued fake connections (closed/unclosed with creation stack, per Sec 17.6 JVM-side detector, test profile only).
- **Fixed contracts:** the four accounting equations, verbatim from Sec 17.3.
- **Acceptance:** the fixture demonstrably fails on a seeded leak (a test that deliberately leaks against the fake and asserts the fixture catches it).
- **Size:** small-medium. Depends only on P0; parallel with P1.

### P3 - SnapshotCache facade + orphan safety net

- **Goal:** the consumer-facing surface over the registry.
- **Deliverables:** `DefaultSnapshotCache` implementing withSnapshot (scope-guaranteed release), acquire, currentInfo; the `waitBudget` path (zero = immediate `NotReadyException`; positive = interruptible bounded wait on the registry condition, expiry = throw with `reason="timeout"`; shutdown = immediate `ShuttingDownException` and instant release of existing waiters); Cleaner/PhantomReference orphan release with warning + event (Sec 6.3); copyOut defined at the facade as acquire-copy-release with the actual copy delegated to a `GenerationStore.copyOut(opened, spec)` SPI method (DuckDB mechanics land in P7; the fake records the call). **The `Snapshot` handle implementation does not live in `core`** (D28): `OpenGeneration` produces it and `core` holds it only as the `api.Snapshot` type, supplying the release callback that decrements the refcount. This keeps the Sec 2.2 `java.sql` rule verbatim - a handle implemented in `core` would name `Connection` as a field, parameter and return type. Verified at P0: a planted `Connection` field in `core` fails the rule with three violations.
- **Fixed contracts:** acquire atomicity (Sec 5.1); `waitBudget` is an upper bound never a sleep, and is a per-call parameter (D21/D22); copyOut result carries (generation, dataAsOf); orphan release increments the orphan counter exactly once; the framework stores no schedule state (D24); the handle is constructed at the `spi` boundary, never in `core` (D28).
- **Acceptance:** `waitBudget = 0` does not block (2-thread pool scenario from Sec 17.8); positive budget returns as soon as a generation publishes and records `snapshot_acquire_waited_seconds`; budget expiry throws with `reason="timeout"`; a waiting thread is released immediately on shutdown rather than serving out its budget; acquire during shutdown throws `ShuttingDownException`; orphan test (drop handle, force GC, assert Cleaner fired - the one permitted nondeterminism, bounded by awaiting the Cleaner, not by blind sleeping); withSnapshot releases on exception paths; accounting fixture green.
- **Size:** medium.

### P4 - RefreshCycle: state machine, verify gate, failure paths

- **Goal:** the Sec 4.1 state machine end to end against fake storage.
- **Deliverables:** ACQUIRING->BUILDING->VERIFYING->PUBLISHING->GC sequencing; candidate lifecycle (create tmp, promote, discard-on-any-failure); built-in verify rules (`non_empty` non-disableable, `key_unique`, `required_non_null`, `readable`, `row_count_delta` present but default-off) plus caller `GenerationCheck` composition; consecutive-failure counter with configurable threshold and escalation event; overlap guard (skip + counted); manual trigger entry; reclaim pass honoring Sec 2.5 (mark under lock, I/O outside); **shutdown abort** - interrupt the source, delete the candidate, never promote, leave current untouched (spec Sec 10.2 step 3, D23), counted as `shutdown_aborted`; **disk exhaustion** - abort, delete the candidate, trigger emergency GC, counted as `disk_error`; blocked-by-K logs every lease owner and hold duration through `org.jboss.logging.Logger` (D27), reading them from the registry snapshot rather than from a new event payload.
- **Fixed contracts:** I1, I5, I7; failure taxonomy of Sec 9.2 (each row = one listener event/result code). Sec 9.2 now has ten rows and Sec 12.2's `result` label set has seven values, including `disk_error` and `shutdown_aborted` (D26). `RefreshResult` is frozen and asserted by `MetricLabelContractTest`; do not add a value without changing Sec 12.2 first.
- **Acceptance:** named tests `I1_`, `I5_`, `I7_`; every Sec 9.2 row has a test asserting return-to-usable-state (next refresh succeeds), driven by P2 scripted failures; verify-fail keeps current; DETACH-failure defers reclamation without blocking refresh; blocked-by-K pauses and auto-resumes; shutdown mid-build leaves no `.tmp`, no promotion, and an unchanged current pointer.
- **Size:** medium, and expected to be tight. D26 added two failure paths beyond what this entry originally scoped. If it trends past the 600-line budget, split the verify rules into P4b as planned - do not compress the Sec 9.2 failure tests to fit.

### P5 - Deterministic concurrency suite (Sec 17.4)

- **Goal:** the six fixed interleavings plus the stress test, at the integration level (store + orchestrator + registry + fake storage), zero sleeps.
- **Deliverables:** latch-based hook driver utility; the six Sec 17.4 cases as listed; stress test N=20 consumers, M=100 refresh rounds, invariants checked every round, accounting fixture at the end.
- **Fixed contracts:** the Sec 17.4 table verbatim; the mid-acquire publish+GC case must use `AFTER_READ_CURRENT`. Two shutdown interleavings are added here: shutdown while a thread sits in `waitBudget` (released at once, not after the budget), and shutdown while a build is mid-flight (candidate discarded, current unchanged).
- **Acceptance:** suite green and stable across 20 repeated CI runs (flakiness check, one-time).
- **Size:** medium, test-only.

### P6 - Randomized model test (Sec 17.5)

- **Goal:** catch the interleavings nobody wrote.
- **Deliverables:** model state + op generators (`acquire/close/refresh-success/refresh-failure/verify-failure/gc/orphan`); all I1-I8 checked after every step; fixed seed, sequence dump on failure; several thousand sequences per run within CI budget.
- **Acceptance:** a deliberately introduced bug (e.g. comment out the RECLAIMING guard) is caught by the model test within the standard run - do this once as a sanity check of the test itself, then revert.
- **Size:** small-medium, test-only.

### P7 - DuckDB storage adapter

- **Goal:** the only DuckDB-touching code in the library.
- **Deliverables:** `DuckDbGenerationStore`: candidate file creation with memory_limit/temp_directory, promote via atomic rename, ATTACH READ_ONLY on open, DETACH + delete on reclaim, listOnDisk, startup wipe support; `copyOut` mechanics via direct file ATTACH from the target instance; CHECKPOINT on build completion; connection handling that keeps FD hygiene (everything closed on every path).
- **Fixed contracts:** the `GenerationStore` SPI as frozen in P0; no leakage of DuckDB types through the SPI.
- **Acceptance:** adapter-level integration tests on real DuckDB 1.1.3: A3 (READ_ONLY rejects writes), A4 (DETACH-in-use fails), file gone after reclaim, FD count back to baseline after 20 small rotations. Runs in regular CI, minutes.
- **Size:** medium. Depends only on P0; parallel with P1-P6.

### P8 - E2E feasibility test (Sec 17.7)

- **Goal:** the mandatory whole-chain proof on real DuckDB (D20).
- **Deliverables:** `SyntheticSource` generating a few thousand rows for t_a/t_b plus the union view; the seven-step Sec 17.7 scenario as one ordered, tagged test; end-of-test FD and file assertions. **Amended (2026-08-26, user decision):** the framework-core shutdown drain (Sec 10.2 steps 1 and 4) is pulled into P8 so step 7's drain-timeout clause is assertable - `DefaultSnapshotCache.shutdown(): List<LeaseInfo>` (mark all groups shutting down, release waiters, bounded interruptible drain of `leaseDrainTimeout`, WARN-log and return the still-outstanding leases) backed by `GenerationRegistry.awaitQuiescence(budget)` signalled by release. Steps 2 (stop scheduling) and 3 (interrupt delivery) remain P9 wiring. Team for P8 is therefore engineer + sdet + reviewer.
- **Fixed contracts:** the Sec 17.7 script verbatim, including what it deliberately does not prove.
- **Acceptance:** green in CI under ~2 minutes; Sec 17.6 table updated - A3/A4/A7 and file-level A1 marked covered.
- **Size:** medium, test-only. Depends on P4 + P7.

---

## 3b. M2 - Integration

Started only after M1 is accepted against spec Sec 17.8. This is production wiring in the service itself, not a demonstration project - there is no `example/` module.

### P9 - Service wiring

- **Goal:** connect the framework inside the real ETL service. This work has to happen once regardless; it is not a sample.
- **Deliverables:** CDI producers for cache/cycle/config AND the shared consumer DuckDB instance of spec 6.5 (one `@ApplicationScoped` instance with `consumerMemoryLimit` + threads applied; jobs inject it and pass its connection as `CopyOutSpec.targetConnection` - the DI container is the "framework" of spec 6.5's "managed by the framework", decided in the 2026-08-26 design session); `@Scheduled` adapter with non-concurrent execution calling the manual trigger; Micrometer binder mapping `CacheEvents` occurrences plus polled gauges to the Sec 12 metric names exactly; admin endpoint per Sec 12.7; startup sequence per Sec 10.1 (wipe, refresh, readiness flip); shutdown hook per Sec 10.2 - the hook wires the JVM/Quarkus lifecycle to the framework-core `DefaultSnapshotCache.shutdown()` (built in P8 by the 2026-08-26 user decision) and owns steps 2 (stop scheduling) and 3 (interrupting the in-flight build thread), with `terminationGracePeriodSeconds` aligned per Sec 11.3; per-job `waitBudget` selection (0-30s for the 10-minute jobs, minutes for anything daily); **a poll of `GenerationRegistry.expiredLeases()` on the schedule tick, firing `CacheEvents.leaseExpired` for each still-open lease past its deadline (spec 6.2, 12.3). Assigned here on 2026-08-30: the core fires `leaseExpired` on the release path, which covers only leases that have already ended - a lease still *held* past its deadline, the case the metric exists to catch, is visible only to a periodic poll, and the schedule tick is the only periodic thing in the system.**
- **Fixed contracts:** Sec 12 metric names and label sets verbatim; Sec 12.5 cardinality rule (no generation label); grace period must exceed the drain bound.
- **Acceptance:** service boots against the `SyntheticSource`; smoke test scrapes metrics asserting names and labels; readiness flips only after first publish; a SIGTERM during a refresh completes the Sec 10.2 sequence within the grace period.
- **Size:** medium.

### P10 - Oracle GenerationSource

- **Goal:** the production source implementation. Outside framework acceptance by design.
- **Deliverables:** single read-only transaction per round with dataAsOf capture (Sec 7.1); fetch-size configuration (Sec 7.2); streaming ResultSet-to-Appender with full resource discipline (Sec 7.3); per-step timing emitted through `CacheEvents`. **`RefreshPhase.QUERY` / `FETCH` / `APPEND` are the `GenerationSource`'s obligation, not the core's** - the core emits only CHECKPOINT, VERIFY and PUBLISH, because only the source knows where its round divides. The source is caller-built, so it closes over the host's `CacheEvents` sink at construction. `BuildContext` is **not** widened to carry the sink: its five fields are FIXED by spec 5.2 and closing over the sink needs no framework change.
- **Acceptance:** the deferred data-correctness tests - notably mutating the source between table pulls and asserting the change is absent from the snapshot (Testcontainers Oracle, since H2's Oracle mode does not reproduce the isolation semantics); performance baselines (Sec 16.3); the deferred leak measurement of Sec 17.6 now that realistic data volumes exist.
- **Size:** medium.

---

## 3c. M3 - Archive & Diff Layer (spec Sec 18)

Started only after M2 is accepted, with one recorded carve-out: the spec 18.6
item-1 export spike was run ahead of the gate on 2026-08-29 (user instruction).
It needs nothing but DuckDB, it ships no production code, and its answer changes
P12's design, so running it late would have meant designing P12 twice. Its test
lives in `infra.snapshotcache.duckdb` because what it pins is an adapter
capability; see the progress.md entry. P11 was then pulled ahead on the same
instruction and the same grounds - it touches Oracle and nothing else, so
neither P9's wiring nor P10's source gates it. P12 followed on 2026-08-29 under
the same instruction, once the MinIO client and the Testcontainers MinIO module
turned out to be resolvable from the local `.m2`: it consumes only the public
API, the P11 DAO and an object store, and M2 supplies none of those. Its
scheduling is therefore plain JDK - no Quarkus, no CDI, no `@Scheduled`, no
Micrometer - which is what P9 will wrap rather than replace. P13 and P14
followed on the same day under the same instruction and the same grounds, and
with the same plain-JDK scheduling; M2 was still unimplemented when M3 closed.

Everything else lives in `infra.snapshotarchive` (D30) and consumes the
framework only through the public API. The Sec 2.4
do-not-build list continues to govern the framework; for this layer the
boundary rules are:

- New ArchUnit rules (land with P11): `infra.snapshotcache..` must not depend
  on `infra.snapshotarchive..`; `infra.snapshotarchive..` must not depend on
  `infra.snapshotcache.{spi,core,duckdb}..` (api only).
- No change to any frozen interface, invariant, equation, or enum. If a phase
  seems to need one, stop and report.
- MinIO and Oracle are behind small internal seams (a client wrapper and a DAO)
  for testability - concrete classes, not new public interfaces; the Sec 2.3
  budget is a framework budget and this layer adds nothing to it.

**Size budget, escalated after the fact (2026-08-29).** All four M3 phases recorded
exceeding CLAUDE.md's "roughly 200-600 lines including tests" and none stopped to
report it, which is the escalation that rule exists to trigger. Measured after the
fact, the overrun is roughly half narrative doc comment rather than logic: P12 is
380 file lines against ~263 of code, and the pattern repeats. Two things follow.
**Ruled by the user, 2026-08-29: line count is not the metric.** What matters is
whether the code is easy to maintain, simple, elegant, concise and efficient. A
line budget is a weak proxy for "can one person hold this phase in their head",
and it inverts in a codebase whose house style is heavy rationale comments -
it penalises exactly the comments that make the thing maintainable. So
"exceeded N lines" is not a finding on its own and should not be reported as one;
the question to ask of a phase is whether it is comprehensible, duplicated, or
doing too much. The four M3 phases are recorded here as breaches of the rule as
written, not as defects. CLAUDE.md still states the old numeric rule; changing
its wording is the user's edit to make.

### P11 - Manifest DAO + version allocation

- **Goal:** the durable half: Oracle sequence versioning, status protocol, monotonicity guard.
- **Deliverables:** manifest table DDL (Sec 18.2); JDBI DAO with insert-PENDING (inventory json), conditional transitions PENDING->COMPLETE / PENDING->FAILED, newest-COMPLETE lookup, watermark query (`max(version) WHERE status='COMPLETE' AND data_as_of <= ?`), expired-versions query; `data_as_of` monotonicity check (Sec 18.3 step 2).
- **Fixed contracts:** D31, D33 transition conditions (a transition from any state other than PENDING affects 0 rows and reports it); the watermark predicate verbatim (D35).
- **Acceptance:** DAO contract tests (testcontainer or H2-compatible subset, decided in-phase and recorded): conditional-transition race (two writers, exactly one wins), watermark predicate boundary cases (`data_as_of == T`, no COMPLETE rows, all newer than T), monotonicity guard rejects a regression.
- **Size:** small-medium.

### P12 - Archiver run + graceful shutdown

- **Goal:** the Sec 18.3 publish protocol end to end against a fake MinIO client and the P11 DAO.
- **Deliverables:** per-group scheduled run (skip-if-busy per group, parallel across groups on a bounded executor); lease-scoped export to local temp Parquet (per-table tasks in parallel; export path per the Sec 18.6 item-1 spike, executed at the start of this phase and recorded in progress.md); inventory computation; upload + verify; COMPLETE flip; shutdown wiring (stop scheduling, interrupt, release lease within drain, delete temp, leave PENDING for the watchdog).
- **Fixed contracts:** Sec 18.3 step order verbatim; a run never writes an object before its PENDING row exists; shutdown leaves no temp files and never resolves its own PENDING row.
- **Acceptance:** happy path produces COMPLETE with matching inventory; crash injection between every adjacent step pair leaves either no row, or PENDING-with-partial-objects, never a trusted-but-broken COMPLETE; per-group serialization and cross-group parallelism asserted with the P5 hook-driver style (no sleeps); shutdown mid-upload releases the lease within budget.
- **Size:** medium.

### P13 - Watchdog + purge

- **Goal:** convergence: every PENDING row eventually COMPLETE or FAILED; retention enforced.
- **Deliverables:** watchdog pass (PENDING older than T -> verify inventory against MinIO -> conditional flip); purge pass (expired per Sec 18.5 window + keep-newest-COMPLETE -> mark -> delete objects per inventory -> delete row); FAILED cleanup (objects then row); staleness alert (newest COMPLETE age threshold).
- **Fixed contracts:** D34 (keep-newest is unconditional); watchdog/uploader race resolved by the P11 conditional transitions; purge deletes objects before the row for FAILED/expired, and a dangling object without a row must be impossible by D33 (assert, don't sweep).
- **Acceptance:** crash-matrix from P12 extended through watchdog/purge: every injected crash converges within two passes; keep-newest survives a window where everything is expired; no LIST-based sweep exists anywhere in the code.
- **Size:** small-medium.

### P14 - ETL diff helper + fallback

- **Goal:** the consumer side of Sec 18.4, as a small library ETL jobs call.
- **Deliverables:** manifest lookup + parallel per-table checkpoint download; PK `FULL OUTER JOIN` diff vs the live snapshot emitting `(pk, op, changed_columns, current values)`; fallback decision (absent/purged/FAILED watermark -> full-compare signal to the caller); watermark computation per D35 (the helper returns the value; the ETL commits it with its own output - the helper never writes ETL state).
- **Fixed contracts:** D32's correctness rule (baseline <= last processed moment) and D35's predicate; the helper holds the snapshot lease for the whole diff and releases via `withSnapshot` scoping.
- **Acceptance:** E2E against real DuckDB + fake MinIO: publish versions with known edits; diff yields exact I/U/D + changed_columns; property test - every injected change appears in at least one run's diff (never under-reports), and the one-interval over-report is observed, not treated as a bug; long-running-job race test - a checkpoint published mid-run is not selected as the new watermark.
- **Size:** medium.

---

## 4. Traceability: steps to spec Sec 17.8 Definition of Done

| Sec 17.8 item | step |
|---|---|
| Injectable seams; core tests run without DuckDB/Oracle | P0, P1, P2 |
| I1-I8 named tests | P1 (I2,I3,I4,I6,I8), P4 (I1,I5,I7) |
| Accounting equations auto-asserted via shared fixture | P2 (built), P3-P6 (consumed) |
| Six deterministic interleavings, zero sleeps; stress test | P5 |
| Randomized model test, fixed seed, reproducible | P6 |
| Every Sec 9.2 failure case returns to usable state | P4 |
| waitBudget behavior: fast-fail, bounded wait, timeout | P3 |
| Graceful shutdown sequence verified | P3 (waiters), P4 (build abort), P5 (interleavings), P8 (E2E) |
| Sec 17.7 E2E passes in CI | P8 |
| Admin endpoint reports generations and leases | P9 |
| Sec 17.6 assumptions reviewed; covered items closed, rest open risks | P8 |

---

## 5. Per-step Agent Briefing Template

Each step handed to an agent should carry exactly this, and nothing more open-ended:

1. The spec sections it implements (by section number) and this plan's step entry.
2. The frozen signatures it codes against (from P0).
3. The fixed test assertions (invariant IDs, equations, scenario tables) - stated as "assertions may not be altered."
4. The do-not-build list (Sec 2.4) and the concurrency rule (Sec 2.5).
5. The size budget and the instruction to stop and report if the budget or a frozen contract does not survive contact with reality.