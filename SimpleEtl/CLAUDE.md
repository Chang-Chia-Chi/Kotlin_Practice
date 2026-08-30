# YAML-Driven ETL Framework (SimpleEtl)

Scoped house rules for this module. The repository root `CLAUDE.md` describes the **snapshot
cache** - it names `docs/snapshotcache/*` as its documents and says "modify only
`infra/snapshotcache/`". Read literally that forbids work here, which is a documentation gap
eight phases of this module ran against before anyone named it. This file closes it: for work
inside `SimpleEtl/`, this file's documents and boundaries apply, and the root file's *process*
rules - one phase per session, fixed vs free, stop-and-report, no unrecorded deviations - apply
unchanged.

A Kotlin ETL framework in two layers. Layer 1 moves rows between JDBC datasources and DuckDB and
is consumed on its own by the snapshot cache. Layer 2 turns a YAML file into a scheduled,
retrying, observable task. Layer 2 depends on Layer 1; nothing depends on Layer 2.

## Commands

```bash
mvn -pl SimpleEtl -am test          # the default: excludes spike and oracle groups
mvn -pl SimpleEtl -am test -DexcludedGroups=none -Dtest=*OracleTest -Dsurefire.failIfNoSpecifiedTests=false   # the three Testcontainers Oracle classes; needs Docker
mvn -pl SimpleEtl -am test -DexcludedGroups=none -Dtest=*Spike -Dsurefire.failIfNoSpecifiedTests=false    # spikes; one appends 6.2M rows ten times - run deliberately

# First check when reviewing any phase - did it touch earlier tests?
git diff --stat <prev-phase-tag>..HEAD -- '**/test/**'
```

`-am` is required: the reactor builds `snapshotcache` first. The `*OracleTest` classes need
Docker; where Docker is absent they cannot run and any claim about them is inherited from an
earlier phase rather than re-measured.

Spikes carry `@Tag("spike")` and the three `*OracleTest` classes carry `@Tag("oracle")`; surefire's
`excludedGroups` in `SimpleEtl/pom.xml` excludes both by default. This replaced a naming-convention
trick (spikes named `*Spike` to dodge surefire's default include pattern) that any `-Dtest=`
silently defeated - `-Dgroups=` opts back into either group deliberately and survives a `-Dtest=`
alongside it.

## Documents

Read all three before writing code. Paths are relative to the repo root.

- `docs/simpleetl/spec.md` - schema, type contract, execution semantics, public API
- `docs/simpleetl/plan.md` - phases, public surface per phase, per-phase acceptance criteria
- `docs/simpleetl/progress.md` - what previous sessions did, and every deviation

When code and documents disagree, the documents win unless progress.md records a deliberate
deviation.

**Phases here are `P0`-`P9` and then `E10` onwards.** The repository holds a second plan,
`docs/snapshotcache/plan.md`, whose own phases run to `P14`, so a bare "P11" names a phase in both
documents. The letter is what disambiguates: `P` is snapshotcache's, `E` is this module's. A phase
named without a document is not a phase name - ask which plan.

## Boundaries

Package boundaries are a dependency contract enforced by ArchUnit in `ArchitectureTest.kt`, not a
filing scheme. The decision procedure, shared with the snapshotcache module:

1. A module shipping more than one independently consumable unit splits by unit first. SimpleEtl
   ships two - Layer 1 without Layer 2 is spec 2.1's whole reason for existing.
2. Within a unit: `api` / `spi` / `core`, applied when a unit outgrows roughly a dozen files.
3. Every technology adapter is its own package named for the technology.
4. Rules are written as dependency sentences and enforced by ArchUnit.

| Package | Holds |
|---|---|
| `infra.etl.pipe` | Layer 1: CanonicalType, Row, RowMapper, RowWriter, RowPipe |
| `infra.etl.duckdb` | DuckDbTableWriter, ScratchDb, DatasetNamer |
| `infra.etl.jdbc` | JdbcWriters |
| `infra.etl.task` | Layer 2: the definition model, engine, loader, runner, scheduler, admin |
| `infra.etl.micrometer` | the metric binding (P8b) |

**"Adapters are leaves" is a statement about dependency direction, not about naming.** `duckdb`
and `jdbc` implement `RowWriter`, a seam defined in `pipe`, so they never name `task`.
`micrometer` implements `TaskMetrics`, a seam defined in `task`, so it names `task` and nothing
else. The invariant that holds for all three: **nothing in `infra.etl` depends on an adapter, and
an adapter depends only on the package defining the seam it implements.** The existing
`adapters do not depend on task` rule names `duckdb` and `jdbc` literally and therefore says
nothing about `micrometer`; the rules that constrain `micrometer` are its own.

## Constraints an agent cannot infer

- **DuckDB is pinned to 1.1.3** (spec 1.4, CI glibc constraint). No newer API.
- **Quarkus is deliberately absent.** Spec 8.6 makes the cron binding, the HTTP resource and the
  `etl-admin` role check host obligations. Measured: Quarkus does not read
  `application.properties` from a dependency jar, so shipping the binding here would have been a
  green test for a production failure.
- **Logging is `org.jboss.logging.Logger`, not `io.quarkus.logging.Log`.** Quarkus is built on
  JBoss Logging, so `quarkus.log.*` applies unchanged, but naming the Quarkus type drags a
  framework into a module that boots none.
- **Time is `java.time.Clock`, injected.** No custom time abstraction, no `System.nanoTime()` in
  the engine. This is what makes duration assertions exact without a test that sleeps.
- **Every dataset written inside scratch gets an attempt-suffixed physical name and a stable
  view** (spec 5.5). `DROP TABLE` does not shrink a DuckDB file and 1.1.3 has no vacuum; the whole
  run directory is deleted instead.
- **A single DuckDB `Connection` used from two threads crashes the JVM** rather than raising an
  error (spec 7.2). A concurrent reader takes a `duplicate()`.

## Concurrency idiom

The coroutine boundary is `TaskRunner`: one `Dispatchers.IO.limitedParallelism(1)` view per task,
tagged with a `CoroutineName`. Everything below it - `TaskEngine` and down - is blocking JDBC by
design (spec 8.3), and `TaskEngine.run`'s signature is frozen by spec 11.2 as an ordinary
function.

So `ScratchDb` guards its state with `synchronized`, not `kotlinx.coroutines.sync.Mutex`, and that
is forced rather than preferred: no frame in `run -> execute -> pipe -> ScratchDb.connection()` is
`suspend`, so `Mutex.withLock` is unreachable without making the frozen signature suspending. Two
further reasons it would be wrong even where reachable - DuckDB's hazard is a **thread**
constraint while a `Mutex` is coroutine-scoped and can resume the holder on another thread, and
`synchronized` is reentrant while `Mutex` is not (`duplicate()` calls `open()` inside the lock).

`TaskRunner`'s self-concurrency guard is an `AtomicBoolean`, not a lock: it is claimed on the
triggering thread and released from `invokeOnCompletion` on whatever thread finished the
coroutine. A `Mutex` locked by one party and unlocked by another is a misuse of the abstraction.
It is a claim/release token modelling a state machine, which is what a CAS flag is for.

A **notification** mechanism is a plain listener interface called on the run's own thread, not a
`SharedFlow`. P8c built the flow and P8c's revert removed it: `tryEmit` buys "never suspends", not
"never blocks", and an `Unconfined` collector was measured running 300 ms inline in the producer -
so the back-pressure guarantee the flow was added for did not exist. Isolation is the listener
call site's job (`ForwardingListener`, and the engine catching what a listener throws). Do not
rebuild the flow.
