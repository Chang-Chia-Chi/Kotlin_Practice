# composed-host-example

Both frameworks in one host, through their two public front doors. This is the module to copy
from when writing a real host (P9), and the executable form of the two specs' host-obligation
tables — snapshotcache spec §5.4 and SimpleEtl spec §8.6.

```
Oracle (simulated)                    the host owns everything in this column
      │
      ▼
GenerationSource ──► openSnapshotCache(config, sources, events, clock)   snapshotcache §5.4
                            │
                            ▼
                     ManagedSnapshotCache ── cache ──► CacheBinding(cache, GroupId("wip"))
                            │                                │
                            │                                ▼
                            │        EtlWiring(scratch, cron, datasources, caches, listener…)
                            │                                │        SimpleEtl §11.2
                            │                                ▼
                            │                  .start(definitions) ─► WiringResult.Wired
                            │                                │
                            │             shape-D task: cacheCopy ─► materialize ─► pipe out
                            │                                │
      shutdown order:       ▼                                ▼
      1. wired.close()   ◄──┴── 2. managed.close()      target DuckDB
```

## The recipe, in the order that matters

1. **Cache first** — `openSnapshotCache` before `EtlWiring`, because `CacheBinding` needs a live
   `SnapshotCache`.
2. **Each host obligation is a named class here**: `ManualCron` *throws on an unparseable cron*
   (§8.6 row 5 — a scheduler that accepts garbage makes atomic reload a lie);
   `RecordingCacheEvents` is the metrics seam; `ThreadRecordingListener` is §9.2's listener,
   without which a 30-minute run emits nothing.
3. **Shutdown in the documented order**: `wired.close()` (stops scheduling, §10.2 steps 2–3),
   *then* `managed.close()` (drains leases, closes stores). Both are terminal; recovery is a new
   `EtlWiring.start`.

## What the test proves (10 scenarios, no doubles on any seam)

- The full path: refresh → generation → `cacheCopy` → scratch → materialize → rows in the target.
- **Reclaimability** — SimpleEtl §8.6 calls this "not testable in this repository"; here it is
  tested: `refCount` drops to 0 the instant the copy returns, the file is gone after a successor
  publishes.
- The cross-boundary failure seams: `NotReadyException` before the first generation (fails the
  step, no retry, full `defaultWaitBudget` spent — §3.6), close-mid-copy (clean drain lets the
  copy finish), reload-during-refresh.
- **The owner-attribution trap, kept measured**: under surefire's default `-ea`,
  `LeaseInfo.owner` reads `DefaultDispatcher-worker-1 @wip-summary#5`; re-run with
  `-DextraArgs=-da` and it degrades to the bare worker name — the production behaviour both
  specs now warn about (`-Dkotlinx.coroutines.debug=on` is the fix).

## Run

```bash
mvn -pl composed-host-example -am test          # the 10 scenarios
mvn -pl composed-host-example test -DextraArgs=-da   # the production-JVM owner measurement
```

Nothing may depend on this module. It publishes no API; copying from it is its intended use.
Provenance: the composed-host adoption dry-run of 2026-08-30, promoted into the reactor so its
measurements stay measured.
