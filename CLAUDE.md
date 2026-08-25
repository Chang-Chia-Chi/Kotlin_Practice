# Snapshot Cache Framework

Generational snapshot cache inside the ETL service. Keeps a local DuckDB copy of
Oracle data, refreshed every 10 minutes, and hands in-process ETL jobs an
immutable, internally consistent snapshot to read from.

One generation = one standalone DuckDB file. Build a candidate, verify it, swap
the current pointer, reclaim old generations once their leases release.

## Commands

```bash
mvn package                  # build
mvn test                     # all tests
mvn test -pl <module>        # single module

# First check when reviewing any phase - did it touch earlier tests?
git diff --stat <prev-phase-tag>..HEAD -- '**/test/**'
```

## Documents

Read all three before writing code. Paths are relative to the repo root.

- `docs/snapshotcache/spec.md` - design, invariants, semantics, acceptance criteria
- `docs/snapshotcache/plan.md` - phases, architecture rules, per-phase contracts
- `docs/snapshotcache/progress.md` - what previous sessions did, and every deviation

When code and documents disagree, the documents win unless progress.md records a
deliberate deviation.

## How work is done

One phase per session. The opening message names the phase; its deliverables,
contracts, and acceptance criteria are in the plan entry for that phase.

- Implement only the named phase. A stub throwing `NotImplementedError` is the
  correct placeholder for a later seam.
- Modify only `infra/snapshotcache/` and that phase's own test sources.
- Never modify or weaken a test written by an earlier phase. A failing earlier
  test means the new change is wrong. Stop and report.
- No `Thread.sleep` in tests. Deterministic interleaving uses the declared hook
  points only. Invariant tests are named `I<n>_<description>`.
- Append a `progress.md` entry when the phase is done.

### Fixed vs free

FIXED, may not be altered: interface signatures, invariant definitions
(spec 17.2), accounting equations (spec 17.3), test assertions and scenario
tables, the do-not-build list (plan 2.4), the concurrency rule (plan 2.5).

FREE: internal class structure, algorithms, private naming.

### Stop and report

- A fixed contract does not survive contact with reality.
- The phase exceeds its size budget (roughly 200-600 lines including tests).
- Work requires touching code outside the boundary above.

Update the document first, then continue. Deciding unilaterally is how code and
documents diverge, after which every later session works from a wrong map.

## Constraints an agent cannot infer

- **DuckDB is pinned to 1.1.3** (CI Linux component constraint). No statement
  timeout, no file-shrinking vacuum. Do not reach for newer APIs.
- **One generation = one file.** `DROP TABLE` does not shrink a DuckDB file and
  1.1.3 has no vacuum, so rotating inside one file leaves a high-water mark.
  Attach read-only, detach, delete.
- **Single lock, no I/O inside it.** All mutable state lives in
  `GenerationRegistry`; storage calls are decided under the lock and executed
  outside it, using the transitional states in plan 2.5.
- **Time is `java.time.Clock`, injected.** No custom time abstraction.
- **The host service is Kotlin + Quarkus + DuckDB + JDBI.** None of that reaches the
  framework core: `api` is JDK + kotlin-stdlib only, JDBI is confined to the `duckdb`
  adapter and caller-land sources, and Quarkus appears only in the P9 wiring layer.
- **Logging is `org.jboss.logging.Logger`, not `io.quarkus.logging.Log`.** Quarkus is
  built on JBoss Logging, so `quarkus.log.*` config applies unchanged, but naming the
  Quarkus type in `core` breaks the ArchUnit boundary and forces the core suite to boot
  a framework.
- Rationale for these and the rest is in spec 15 (D1-D25).

## Boundaries

Package boundaries are enforced by ArchUnit, not by convention. The rules live in
`ArchitectureTest.kt`. If `core` imports DuckDB the build fails, and that is what
keeps the core test suite runnable in milliseconds without a database.