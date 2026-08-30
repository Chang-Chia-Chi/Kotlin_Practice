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
   `EtlWiring.start`. `ComposedHost.shutdownEtl()` is the first half with the readiness flag raised
   **before** it — see M3.

## What the test proves (11 scenarios, no doubles on any seam)

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
  specs now warn about (`-Dkotlinx.coroutines.debug=on` is the fix, and **M1 below prices it**).
- **Busy is not dying, and the host already knows which** (scenario 11) — `ReadinessProbe`, nine
  lines, discharges the `ShuttingDown`-vs-`AlreadyRunning` deferral. See M3.

## The measurements (`MeasurementsTest`)

Three numbers this module owns because nowhere else can measure them. **Absolute figures are
machine-relative** — one Windows 11 laptop, Java 21, duckdb_jdbc 1.1.3, medians over repeated
rounds in repeated JVMs. The ratios and the yes/no answers travel; the microseconds do not.

### M1 — what `-Dkotlinx.coroutines.debug=on` costs

Both specs recommend the flag so `LeaseInfo.owner` names the task. It is charged per dispatch and
per resume (a `CoroutineId` element, a `Thread.setName`), and this framework dispatches about twice
per run. Measured through the real `EtlWiring` → `TaskRunner` path, on a shape-A task so DuckDB
does not bury the signal — 3 JVMs per state, 3,000 warm-up runs, then 7 × 1,000 timed:

| state | median of round medians | pooled median | pooled min |
|---|---|---|---|
| `debug=off` | **301.7 µs/task** | 302.9 | 209.2 |
| `debug=on` | **308.1 µs/task** | 307.1 | 206.6 |

**+6.4 µs/task, +2.1% — and that is an upper bound, not a reading.** Round medians spread ~90 µs
within a single JVM and the pooled *minimum* moves the other way, so the effect sits at or below
this harness's resolution. What three JVMs a side do establish: **under 10 µs and under 3% of a
task run.**

> **Recommend the flag unconditionally.** Two thread renames per run cannot matter at spec 8.1's
> one-run-per-ten-minutes. The condition worth stating is not cost: `-ea` turns the flag on too, so
> the one configuration where attribution silently degrades is a JVM with neither — which is
> exactly what production is.

### M2 — the pod-budget formula's two premises

`N × EtlWiring.scratchMemoryLimitMb + servingMemoryLimit` (the D16 reconciliation) rests on
enforcement and on additivity. Both now measured; both hold.

**Enforcement — yes, by spilling.** One hash aggregate over N distinct keys, run inside a real
scratch instance whose `memory_limit` came from the task file:

| memory_limit | distinct keys | outcome | peak spill |
|---|---|---|---|
| 64 MB | 10 M | ok, spilled | 3,717 MB (58× the limit) |
| 256 MB | 10 M | ok, spilled | 737 MB (2.9×) |
| 1024 MB | 10 M | ok, in memory | 0 |
| 64 MB | 40 M | ok, spilled | 14,874 MB (232×) |

DuckDB 1.1.3 honours the setting and spills into `ScratchDb`'s wired temp directory rather than
raising or growing. Not one run failed. **The surprise is the second column:** shrinking the limit
does not shrink the run, it converts RAM into scratch-volume bytes at a poor rate — 192 MB of pod
memory saved cost 3 GB of extra spill on the same query. Tuning `scratchMemoryLimitMb` *down* to
fit more tasks per pod trades against a term the formula does not contain. Spec 7.2 already sizes
that volume as file plus spill; this is the exchange rate between the two budgets.

**Additivity — yes, per instance.** Two concurrent tasks, 256 MB and 512 MB, read back `244.1 MiB`
and `488.2 MiB` from their own scratch connections (DuckDB reads `MB` as 10⁶ and echoes binary
units, so those *are* the requested values), each readback timestamped inside the other run's
window, with two live scratch run directories observed at once.

**Still unmeasured, and unmeasurable here: the operating point.** What N, at what limits, fits a
given pod is a statement about a memory request, a page cache, a JVM heap and a real concurrency
level. It is **configuration, not framework fact** — no test in this repository is evidence about
it. What is measured is only that the two terms are real and do not interfere.

### M3 — busy vs dying (scenario 11)

`TriggerResult.AlreadyRunning` is answered both while a run is in flight and after
`WiringResult.Wired.close()`; spec 11.2 deferred a fifth sealed case on the claim that *the host
can tell them apart, because the host is the one that called `close()`*. Scenario 11 is that claim
executed: `ReadinessProbe` holds one `@Volatile shuttingDown`, raised **before** `close()`, and
`classify()` maps the same framework answer to `409 busy` or `503 gone`. It sees only the sealed
result and its own flag — exactly what a real `AdminResource` has.

**Verdict: the deferral holds, and the reopen trigger did not fire.** The only thing that has to be
got right is ordering — raise the flag first, or there is a window where a cancelled runner refuses
a trigger while the probe still says "retry later".

## Run

```bash
mvn -pl composed-host-example -am test          # the 11 scenarios; measurements are opt-in
mvn -pl composed-host-example test -DexcludedGroups=none   # the 3 measurements (forks JVMs, writes GBs of spill)

# the production-JVM owner measurement (scenario 4)
mvn -pl composed-host-example test -DextraArgs=-da

# M1's A/B. -da on BOTH sides: surefire's default -ea turns the coroutines flag on by itself,
# so a plain run and a -da run differ in two variables rather than one.
mvn -pl composed-host-example surefire:test -Dtest='MeasurementsTest#M1*' \
    -DexcludedGroups=none -Dsurefire.failIfNoSpecifiedTests=false -DextraArgs='-da -Dkotlinx.coroutines.debug=off'
mvn -pl composed-host-example surefire:test -Dtest='MeasurementsTest#M1*' \
    -DexcludedGroups=none -Dsurefire.failIfNoSpecifiedTests=false -DextraArgs='-da -Dkotlinx.coroutines.debug=on'

# M2's spill sweep
mvn -pl composed-host-example surefire:test -Dtest='MeasurementsTest#M2 - a small*' \
    -DexcludedGroups=none -Dsurefire.failIfNoSpecifiedTests=false -Dm2.limitMb=256 -Dm2.groups=10000000
```

Knobs: `-Dm1.warmup -Dm1.n -Dm1.rounds`, `-Dm2.limitMb -Dm2.groups -Dm2.burn`. The M2 enforcement
scenario writes several GB of spill by design; `-Dm2.groups` scales it.

Nothing may depend on this module. It publishes no API; copying from it is its intended use.
Provenance: the composed-host adoption dry-run of 2026-08-30, promoted into the reactor so its
measurements stay measured.
