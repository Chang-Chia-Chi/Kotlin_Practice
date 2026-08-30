# CONTEXT — SimpleEtl

Orientation for agents. Caches only what is expensive to rediscover; the documents in the
pointer table are the authority and win over this summary.

## What it is

A YAML-driven ETL framework in two layers. **Layer 1** (`pipe`, `duckdb`, `jdbc`) moves typed
rows between JDBC datasources and DuckDB and is consumed alone by the snapshot cache. **Layer 2**
(`task`) turns a YAML file into a scheduled, retrying, observed task; nothing depends on it.
Five step types: `pipe` (rows through the JVM), `materialize`, `sql`, `export` (task variables),
`cacheCopy` (file-to-file out of a snapshot-cache generation).

## Design principles

1. **Boot-loud beats run-silent.** Spec §10's validation rules turn mid-run failures into
   startup rejections; task-shaped rules live once in `TaskRules`, enforced on both the file
   path and the code-built path with identical wording.
2. **Scratch is disposable.** Every run gets a private DuckDB file; every scratch dataset is
   written attempt-suffixed and published as a stable view only on success (`ScratchDatasets`);
   the whole run directory is deleted at run end.
3. **Blocking below `TaskRunner`, by design.** One `limitedParallelism(1)` view per task;
   `TaskEngine.run` is a frozen ordinary function; `synchronized` not `Mutex` (DuckDB's hazard
   is a thread constraint); an ArchUnit rule bans `suspend` on the engine and `duckdb`.
4. **Reject, never queue.** A firing during a run is skipped (`AlreadyRunning`); retries are
   per step, transient-only, and off-scratch require the author's `idempotent: true`.
5. **The host carries what a library cannot** — spec §8.6's table, each row with its symptom:
   cron binding, auth, readiness path, metrics registry + `seed`, statement timeouts.

## How to use

```kotlin
val wired = EtlWiring(scratchDir, cron, datasources, transforms, hooks, caches,
        listener = ..., metrics = binding, onTasksLoaded = binding::seed,
    ).start(taskDirectory)          // or .start(definitions) for code-built tasks
// WiringResult.Wired(admin) -> trigger/list/run/reload ; .close() is terminal
```

Task YAML: see `../etl-host/example-tasks/` (loader-validated by a test). Working hosts:
`../composed-host-example/`, `../etl-host/`.

## Load-bearing facts (tier 1 — each cost a day to learn)

- **One DuckDB `Connection` from two threads crashes the JVM.** Scratch statements share the
  write connection; concurrent readers take `duplicate()`.
- **DuckDB pinned 1.1.3: `DROP TABLE` shrinks nothing, no vacuum** — hence attempt suffixes and
  whole-directory deletion. Tightening `scratchMemoryLimitMb` converts RAM into spill on the
  spec-7.2 volume (measured: 192 MB saved → 3 GB spill).
- **A non-scratch publish must be an idempotent MERGE** — a plain table target appends a full
  copy per firing (measured unbounded in soak). Non-scratch `materialize` binds no variables
  (Oracle rejects binds in DDL).
- **`Wired.close()` is terminal**; reload afterwards reports it instead of resurrecting a dead
  schedule. Trigger-after-close answers `AlreadyRunning` — the host distinguishes busy from
  dying with its own readiness flag, raised *before* close.
- **Heavy tests (`spike`, `oracle`) are excluded via a pom *property*** — opt in with
  `-DexcludedGroups=none`; a `-Dgroups=` opt-in alone runs zero tests.

## Pointers

| Need | Read |
|---|---|
| Schema §3, type contract §4, semantics §5–6, public API §11, host table §8.6 | `../docs/simpleetl/spec.md` |
| Validation rules and where each lives | spec §10 + `TaskRules.kt` |
| Why anything is this way; every decline/deviation — search before proposing | `../docs/simpleetl/progress.md` |
| Commands, concurrency idiom, phase process | `CLAUDE.md` (this directory) + root `../CLAUDE.md` |
| Layer 1 alone (rows, writers, types) | `infra/etl/pipe/` KDoc, spec §4 |
