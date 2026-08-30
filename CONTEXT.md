# CONTEXT — the snapshot-cache + ETL frameworks

Orientation for agents. This file caches what is expensive to rediscover; everything else lives
behind the pointers at the bottom, and those documents win over this summary.

## The shape

Two frameworks, one composition:

```
Oracle ──GenerationSource──▶ snapshotcache ──CacheBinding──▶ SimpleEtl ──▶ target DBs
         (host-written)      (generations of  (cacheCopy      (YAML tasks,
                              DuckDB files)    step)            scratch DuckDB)
```

- **snapshotcache** keeps a local, refreshed copy of Oracle: each refresh builds a whole new
  DuckDB file (a *generation*), verifies it, atomically promotes it, and reclaims old ones when
  their *leases* release. Consumers get an immutable, consistent snapshot.
- **SimpleEtl** turns a YAML file into a scheduled, retrying, observed task. Layer 1 (rows
  between JDBC and DuckDB) is usable alone; Layer 2 adds the task engine. Every task run gets a
  private *scratch* DuckDB file, deleted whole at run end.
- **shape-D** is the canonical composed task: `cacheCopy` a generation subset into scratch →
  `materialize` over it → publish out via an idempotent MERGE.

## Design principles (each generates many rules; the documents carry the rules)

1. **Documents are the memory.** Specs/plans/progress record every decision with reasoning;
   proposals are judged against three tiers (root `CLAUDE.md`): measured facts are never voted
   down, design decisions are appealable document-first, internals are free.
2. **Measured, not assumed.** Claims cite a probe or a test. A green suite is evidence only
   about what it exercises — booting, soaking, and operating found what review could not.
3. **Disposable data, loud failure.** Generations and scratch are rebuildable copies: publish
   only on success, never repair in place, delete whole directories. Failures reject rather
   than queue, and prefer boot-time-loud over run-time-silent.
4. **Boundaries are ArchUnit rules, not convention.** `api` innermost; adapters depend only on
   their seam; `bootstrap`/hosts are leaves nothing depends on.
5. **Hosts carry what libraries cannot.** Scheduling, auth, readiness, metrics binding, config:
   each spec's §8.6/§5.4 table lists the obligations with the symptom of missing each. The
   frameworks make what they can unrepresentable; the rest is a named host duty.

## How to use (the two front doors)

Cache first, then ETL — the binding needs a live cache:

```kotlin
val managed = openSnapshotCache(config, mapOf(GroupId("wip") to source), events, clock)
val wired = EtlWiring(scratchDir, cron, datasources,
        caches = mapOf("wip" to CacheBinding(managed.cache, GroupId("wip"))),
        listener = ..., metrics = binding, onTasksLoaded = binding::seed,
    ).start(taskDirectory) // WiringResult.Wired(admin) | Invalid(report)
// shutdown, in order: raise your readiness flag → wired.close() → managed.close()
```

Copy from the working exemplars instead of reassembling: `composed-host-example/` (plain, 11
scenarios) and `etl-host/` (full Quarkus host: auth, health, archive, Docker staging stack,
`example-tasks/`).

## Load-bearing facts (tier 1 — relearning any of these costs a day)

- **One DuckDB `Connection` touched from two threads crashes the JVM** (no exception). Hence
  `synchronized` not `Mutex`, the non-suspend engine (ArchUnit-enforced), and per-reader
  `duplicate()`.
- **DuckDB is pinned 1.1.3; nothing shrinks a live file.** Hence attempt-suffixed names, views,
  and whole-directory reclamation. Tightening a scratch `memory_limit` converts RAM into spill
  on the same volume (measured 192 MB saved → 3 GB spill).
- **Lease attribution exists only under `-Dkotlinx.coroutines.debug=on`.** Tests always show it
  working (`-ea` auto-enables it); production silently loses it without the flag.
- **On Linux, `ATOMIC_MOVE` silently replaces and unlink-while-open succeeds**; on Windows both
  differ. Per-group directories and the startup wipe exist because of this.
- **A non-scratch publish target must be an idempotent MERGE** — a plain table target appends a
  full copy per firing (measured unbounded in soak).
- **Heavy tests are tag-excluded via a pom *property*; opt in with `-DexcludedGroups=none`.**
  Composed pod budget: `N_concurrent × scratchMemoryLimitMb + servingMemoryLimit`.

## Pointers

| When you need | Read |
|---|---|
| Commands, budgets, concurrency idiom, phase process | root `CLAUDE.md`, `SimpleEtl/CLAUDE.md` |
| Cache design, invariants, D1–D36 decisions, host table §5.4 | `docs/snapshotcache/spec.md` (+ `plan.md` §2.4 do-not-build) |
| ETL schema, validation rules, public API §11, host table §8.6 | `docs/simpleetl/spec.md` |
| Why anything is the way it is; every decline and deviation | `docs/*/progress.md` — search before proposing |
| Wiring recipes that run | `composed-host-example/`, `etl-host/` (+ its `staging/`) |
| Soak evidence and how to repeat it | `soak/README.md` |
