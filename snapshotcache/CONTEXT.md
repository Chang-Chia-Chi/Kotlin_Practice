# CONTEXT — snapshotcache

Orientation for agents. Caches only what is expensive to rediscover; the documents in the
pointer table are the authority and win over this summary.

## What it is

A generational snapshot cache: keeps a local DuckDB copy of Oracle, refreshed on the host's
schedule. Each refresh builds a whole new DuckDB file (a *generation*), verifies it, atomically
promotes it, and reclaims old ones once their *leases* release. Consumers acquire an immutable,
internally consistent snapshot; a slow consumer delays reclamation, never staleness.

## Design principles

1. **One generation = one file.** Build candidate → verify → promote → reclaim. Publish only on
   success; never repair in place; reclaim by deleting whole files. Data is a disposable copy.
2. **Single lock, no I/O inside it.** All mutable state in `GenerationRegistry`; storage calls
   decided under the lock, executed outside it. Time is injected `java.time.Clock`.
3. **`api` is the innermost layer** (ArchUnit-enforced); everything implementing it is
   `internal`. Construction happens only through the `bootstrap` composition root.
4. **The host carries what a library cannot**: the refresh tick, the `expiredLeases`/pinning
   poll, readiness, metrics binders, thread/lease attribution. Spec §5.4 lists each with the
   symptom of missing it.

## How to use

```kotlin
val managed = openSnapshotCache(config, mapOf(GroupId("wip") to source), events, clock)
managed.cache.withSnapshot(GroupId("wip")) { snap -> /* read; lease auto-released */ }
managed.admin  // refresh trigger, gc, liveGenerations
managed.close() // drains leases; call it LAST at shutdown
```

`GenerationSource` is yours: fill the candidate via `BuildContext`. Working exemplars:
`../composed-host-example/`, `../etl-host/`.

## Load-bearing facts (tier 1 — each cost a day to learn)

- **One DuckDB `Connection` touched from two threads crashes the JVM** — no exception. Readers
  take `duplicate()`; the store sweep in `close()` is guarded by a clean drain for this reason.
- **DuckDB pinned 1.1.3: nothing shrinks a live file** — hence whole-file generations and the
  startup wipe. `listOnDisk`+`delete` are the wipe primitives; numbering seeds from disk when
  the wipe is off.
- **Lease `owner` is real only under `-Dkotlinx.coroutines.debug=on`** for coroutine-dispatched
  consumers. Tests always show attribution (`-ea` auto-enables it); production silently loses
  it without the flag.
- **On Linux `ATOMIC_MOVE` silently replaces; unlink-while-open succeeds** (space frees at last
  close). Windows differs on both. Per-group directories exist because numbering restarts at 1
  per group.
- **A single stuck lease is invisible at default K=3** by design (D7): poll
  `liveGenerations`' refCounts on the host tick — `gc()`'s empty outcome cannot distinguish
  "nothing to do" from "pinned".

## Pointers

| Need | Read |
|---|---|
| Invariants, semantics, D1–D36 decisions, host table §5.4/§10 | `../docs/snapshotcache/spec.md` |
| Boundary rules, do-not-build list §2.4 | `../docs/snapshotcache/plan.md`, `ArchitectureTest.kt` |
| Why anything is this way; every decline/deviation — search before proposing | `../docs/snapshotcache/progress.md` |
| Process, tiers of authority, commands | `../CLAUDE.md` |
| Archive layer (M3, parquet checkpoints to MinIO) | `infra/snapshotarchive/` - a **consumer** of the cache, never part of it (D30; ArchUnit-enforced both ways, `api`-only access). Flat, not layered: six files, and layering is earned at ~a dozen. Wiring: `../etl-host/` |
