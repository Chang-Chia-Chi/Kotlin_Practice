# CONTEXT — composing the two frameworks

Per-framework orientation lives with each framework: **`snapshotcache/CONTEXT.md`** and
**`SimpleEtl/CONTEXT.md`**. This file holds only what neither owns — the composition — and the
documents win over any summary here.

## The shape

```
Oracle ──GenerationSource──▶ snapshotcache ──CacheBinding──▶ SimpleEtl ──▶ target DBs
         (host-written)      (generations of  (cacheCopy      (YAML tasks,
                              DuckDB files)    step)            scratch DuckDB)
```

**shape-D** is the canonical composed task: `cacheCopy` a generation subset into scratch →
`materialize` over it → publish out via an idempotent MERGE.

## Composition rules (each measured on a real composed host)

- **Order: cache first.** `openSnapshotCache(...)` before `EtlWiring(...)` — the
  `CacheBinding` needs a live cache. Shutdown reverses it: raise your readiness flag →
  `wired.close()` → `managed.close()`.
- **A `cacheCopy` lease lasts exactly the copy**, seconds not the run — reclamation is never
  blocked by a long task, only by the copy itself.
- **Pod budget**: `N_concurrent × EtlWiring.scratchMemoryLimitMb + servingMemoryLimit`.
  SimpleEtl opens one scratch DuckDB per run, so `consumerMemoryLimit` is inert here; and
  tightening the memory term inflates the spill term on the same volume.
- **A task fired before the first generation** pins its dispatcher slot for the cache's full
  `defaultWaitBudget` (30 s default) and fails as `TIMEOUT` — gate task scheduling on cache
  readiness.
- **Lease attribution across the boundary needs `-Dkotlinx.coroutines.debug=on`** — SimpleEtl's
  shared-pool dispatcher defeats thread naming, and every test JVM masks the gap (`-ea`
  auto-enables the flag).

## How code is structured (one procedure, three silhouettes)

The shared layout law (stated in full in `SimpleEtl/CLAUDE.md`, enforced by each module's
`ArchitectureTest`): split by independently consumable **unit** first; apply `api`/`spi`/`core`
within a unit only once it outgrows **roughly a dozen files**; every technology adapter is its
own package named for the technology; every boundary is an ArchUnit dependency sentence.

So the three trees differ because their inputs do: `snapshotcache` (17 files, one unit) is
layered; `SimpleEtl` splits into `pipe`/`task` (two units) with flat insides; `snapshotarchive`
(6 files, a *consumer* of the cache per D30) is flat. Consistency lives in the procedure, not
the silhouette. One watched threshold: `task` sits at 13 files, held flat because spec 11.2 +
`internal` + ArchUnit already do `api`'s job — if it grows further, the layering rule fires.

## Copy from, in order of ceremony

| | |
|---|---|
| `composed-host-example/` | plainest working composition, 11 scenarios, no framework booted |
| `etl-host/` | full Quarkus host: auth, health, archive layer, Docker staging stack, validated `example-tasks/` |
| `soak/README.md` | the 93-minute soak harness and its measurements, repeatable |

Process, tiers of authority, and commands: root `CLAUDE.md`. Every past decision and decline:
`docs/*/progress.md` — search before proposing.
