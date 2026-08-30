# DuckDB Generational Snapshot Cache - Design Spec

Version: v1.0 (finalized)
Scope: single ETL pod, in-process consumers
Status: ready for implementation

---

## 1. Background and Goals

### 1.1 Problem

Multiple ETL jobs need to read the same set of Oracle data repeatedly. The source has no CDC mechanism, and downstream wants the data as fresh as possible. Letting every ETL hit Oracle on its own means duplicate queries, load on the source, and different jobs seeing data from different points in time.

### 1.2 Goals

- Maintain a local cache of the Oracle data inside the ETL pod, backed by DuckDB in file mode.
- Refresh every 10 minutes. The swap must be invisible to consumers - nobody ever reads a half-built dataset.
- At any moment, a consumer can obtain an **internally consistent** snapshot and know exactly what point in time it represents.
- Old versions must genuinely release their resources (memory and disk). Long-running processes must not grow.
- A slow consumer must not be able to block the swap indefinitely (which would silently make the data stale).

### 1.3 Non-goals

- No CDC / incremental update (interface leaves room for it - see Sec 14.1).
- No multi-replica synchronization (single replica - see Sec 11.2).
- No cross-process sharing (future extension via manifest + object storage - see Sec 14.2).

---

## 2. Terminology

| Term | Definition |
|---|---|
| Group | A set of tables that must stay mutually consistent. All tables in a group are captured at the same source consistency point and swap together. |
| Generation | One immutable version of a group, identified by a monotonically increasing integer. |
| Current | The generation being served right now. This is what `acquire` hands out. |
| Lease | A consumer's declaration that it is using a generation. While a lease exists, that generation cannot be reclaimed. |
| Refresh | The full flow of pulling from Oracle and building a candidate generation. |
| Publish | Moving the current pointer to a new generation. |
| Verify | Quality checks run on a candidate generation before publishing. |
| dataAsOf | The source data point in time for that generation (start of the read transaction). |
| K | Maximum number of generations allowed to be alive at once. |

---

## 3. Overall Model

### 3.1 Core decision: one generation = one standalone DuckDB file

We are **not** using the "LEFT / RIGHT two-slot rotation" model. We use a generational model instead.

**Why not two slots:**

With two slots, the slot that refresh N+1 needs to write into is exactly the slot a consumer may still be reading. This couples refresh frequency to your slowest consumer, leaving only two bad options: wait for the reader to finish (data goes stale), or force-drop it (the consumer blows up mid-query).

**Why not create/drop tables inside a single .db file:**

DuckDB's `DROP TABLE` marks blocks reusable but **does not shrink the file**. DuckDB 1.1.3 has no vacuum that can shrink a database file. Repeatedly creating and dropping generations inside one file leaves the file sitting at its historical high-water mark, with fragmentation you have no way to clean up.

**The model we're using:**

```
/data/cache/<group>/
    gen_0000000123.db        <- old generation, still leased
    gen_0000000124.db        <- current
    gen_0000000125.db.tmp    <- being built
```

- Build writes into a `.tmp` file; on successful verify it's renamed to the final name.
- Publishing attaches it read-only: `ATTACH '.../gen_0000000124.db' AS g124 (READ_ONLY)`.
- Reclaiming means `DETACH g124` followed by **deleting the file**.

What this buys us:

- Memory and disk are genuinely returned. No high-water mark, no fragmentation.
- All tables in a group live in one file, so the swap is atomic by construction.
- Read-only attach prevents consumers from accidentally writing.
- An immutable generation file is naturally the right unit to upload to object storage later (Sec 14.2).

### 3.2 How to draw group boundaries

**Group by "does this need to be consistent with that", not by convenience.**

Every table in a group is refreshed together, so the slowest table holds up the whole group. Tables that don't need mutual consistency should live in separate groups with independent refresh cadences.

**For this project:** the two source tables are used together in a union, so they belong to **one group** and are pulled inside a single Oracle read transaction.

### 3.3 Handling the union

Keep two separate physical tables underneath and expose a union view on top. **Do not merge them into a single physical table at write time.**

Reasons:

- The two schemas differ somewhat. Merging means filling NULLs for missing columns, and consumers can no longer tell "this value is genuinely empty" from "this source doesn't have this column."
- After merging, ids may collide, forcing uniqueness checks onto a composite key. Two tables let each validate its own id - much simpler.
- When a source schema changes, the two-table approach only needs the view definition adjusted.
- A DuckDB view costs nothing physically and filter pushdown still works, so consumers still get the "it's just one table" experience.

Structure inside a generation file:

```sql
CREATE TABLE t_a (...);
CREATE TABLE t_b (...);

CREATE VIEW t_unified AS
  SELECT 'A' AS source, id, <aligned column list> FROM t_a
  UNION ALL
  SELECT 'B' AS source, id, <aligned column list> FROM t_b;
```

The view definition lives in the group spec and is created with every generation.

**To be filled in: the column mapping table.** Before implementation, list the A/B column correspondence and clearly distinguish two cases:

- **Same concept, different column name** -> align (rename) inside the view.
- **That source genuinely lacks the concept** -> fill `NULL::<type>` and note why in the mapping table.

Put this mapping under version control. Without it, nobody can reconstruct the reasoning later.

---

## 4. Refresh Flow

### 4.1 State machine

```
        +---------+
        |  IDLE   |<-------------------------------+
        +----+----+                                |
             | scheduled / manual trigger          |
             v                                     |
      +--------------+                             |
      |  ACQUIRING   |  open source read txn       |
      |              |  fetch -> Appender write    |
      +------+-------+                             |
             |                                     |
             v                                     |
      +--------------+                             |
      |  BUILDING    |  create view / CHECKPOINT   |
      +------+-------+                             |
             |                                     |
             v                                     |
      +--------------+   verify failed             |
      |  VERIFYING   +-------------+               |
      +------+-------+             |               |
             | verify passed       v               |
             v            +----------------+       |
      +--------------+    |    DISCARD     |       |
      |  PUBLISHING  |    | delete candidate+------>+
      | swap current |    | current stays  |       |
      +------+-------+    +----------------+       |
             |                                     |
             v                                     |
      +--------------+                             |
      |      GC      |  reclaim refcount==0 gens   |
      +------+-------+                             |
             +-------------------------------------+
```

**Failure at any stage leaves the current pointer untouched.** The candidate file is simply deleted and the next round starts fresh. The refresh flow is stateless: a failure leaves nothing behind that needs repairing.

### 4.2 Stage details

**ACQUIRING**

1. Allocate a generation number (monotonic - see Sec 4.3).
2. Create candidate file `gen_NNNN.db.tmp`; set `memory_limit` and `temp_directory`.
3. Open a read-only transaction against Oracle; record `dataAsOf`.
4. Pull each table in the group in order, streaming into DuckDB via the Appender.
5. Close the Oracle transaction.

**BUILDING**

1. Create the union view.
2. Run `CHECKPOINT` to fold the WAL into the main file.
3. Close the candidate file's write connection.

**VERIFYING**

Reopen the candidate file read-only and run verification (Sec 8). Reopening is deliberate - it confirms the file itself is complete and readable.

**PUBLISHING**

1. Rename `.tmp` to the final filename (rename within one filesystem is atomic).
2. `ATTACH` it with `READ_ONLY` onto the serving DuckDB instance.
3. Update the current pointer and generation registry inside a single critical section.

**GC**

Scan all non-current generations; for those with `refcount == 0`, DETACH and delete the file.

### 4.3 Generation numbering

- Monotonically increasing integer, held in an `AtomicLong` within the process.
- **Do not use a timestamp as the number.** Timestamps aren't monotonic across restarts, clock corrections, or a future multi-node setup.
- After a restart, numbering starts over at 1 (startup wipes all leftover files - see Sec 10.1), so nothing needs persisting.

### 4.4 Scheduling

- Use "N minutes after the previous round **finishes**", not a fixed cron. This way a slow round never causes a backlog.
- **Overlapping runs are forbidden.** Two concurrent builds double both memory and disk requirements.
- If the previous round is still running, skip this trigger and count `snapshot_refresh_total{result="skipped_overlap"}`.
- Provide a manual trigger endpoint for operations.

---

## 5. Interface Definition

Spec-level only; no implementation.

### 5.1 Consumer side

```
interface SnapshotCache {

    // Short lease: framework controls the lifecycle. Preferred usage.
    fun <T> withSnapshot(
        group: GroupId,
        waitBudget: Duration = config.defaultWaitBudget,
        block: (Snapshot) -> T
    ): T

    // Specialized short lease: copy a subset out, then release immediately
    fun copyOut(
        group: GroupId,
        spec: CopyOutSpec,
        waitBudget: Duration = config.defaultWaitBudget
    ): CopyOutResult

    // Long lease: caller is responsible for close()
    fun acquire(group: GroupId, waitBudget: Duration = config.defaultWaitBudget): Snapshot

    // No lease created; status only (for metrics / health checks / caller-side policy)
    fun currentInfo(group: GroupId): GenerationInfo?
}

interface Snapshot : AutoCloseable {
    val generation: Long
    val dataAsOf: Instant
    fun connection(): Connection      // read-only connection bound to this generation
    override fun close()              // idempotent; repeated calls are harmless
}

data class GenerationInfo(
    val generation: Long,
    val dataAsOf: Instant,
    val publishedAt: Instant,
    val rowCounts: Map<String, Long>
)

data class CopyOutSpec(
    val sql: String,
    val targetTable: String,
    val targetConnection: Connection
)

data class CopyOutResult(
    val generation: Long,
    val dataAsOf: Instant,
    val rowsCopied: Long
)
```

**Atomicity requirement for `acquire()`:** reading the current generation and incrementing its refcount must happen inside one critical section. Split into two steps, a swap plus reclaim in between hands the consumer a generation that has already been detached.

**Prefer `withSnapshot` / `copyOut`.** `acquire()` stays available but is an advanced path; callers must wrap it in try-finally.

**`waitBudget` semantics.** It is an upper bound, not a sleep. When a generation is already available, every acquire returns immediately regardless of the budget. The budget only applies before the first successful publish (Sec 9.3) - in steady state it is never consumed, because a failed refresh keeps serving the previous generation.

- `Duration.ZERO` - fail fast, throw `NotReadyException` immediately.
- `> 0` - wait, interruptibly, until a generation is available or the budget expires, then throw.

Setting the budget generously costs nothing; setting it too low costs a missed run. Callers therefore pick by the cost of missing a run, not by expected latency:

| Caller cadence | Suggested budget | Reasoning |
|---|---|---|
| Every 10 minutes | 0 - 30s | Missing one run costs 10 minutes |
| Once per day | 15m | Missing one run costs a day; comfortably covers cold start and rolling deploy |

**Caller-side scheduling policy (deliberate non-feature).** The framework never persists or reasons about run schedules, last-success times, or catch-up windows. That is scheduling, not caching. Two seams let a caller implement any such policy itself:

- `waitBudget` is a **per-call parameter, not configuration**, so a caller may compute it at call time from whatever state it keeps.
- `currentInfo()` returns null when nothing is published yet, and otherwise reports `generation` and `dataAsOf`, so a caller can inspect availability and freshness without taking a lease and decide to skip, wait, or proceed.

A caller that wants "skip this run if my last success was recent" implements it against those two seams. See D24.

### 5.2 Producer side (caller-injected)

```
interface GenerationSource {
    fun refresh(ctx: BuildContext)
}

class BuildContext {
    val group: GroupId
    val generation: Long
    val target: Connection        // write connection to the candidate generation file
    val dataAsOf: Instant
    val previous: Snapshot? // reserved for delta mode (Sec 14.1)
}

interface GenerationCheck {
    fun verify(candidate: Connection, previous: GenerationInfo?): VerifyResult
}

sealed class VerifyResult {
    object Pass : VerifyResult()
    data class Fail(val rule: String, val detail: String) : VerifyResult()
}
```

"How to pull the data" is injected by the caller. The framework only owns generation management, leases, the verify gate, and reclamation. Switching to delta mode later means swapping the `GenerationSource` implementation and nothing else.

### 5.3 Admin side

```
interface CacheAdmin {
    fun triggerRefresh(group: GroupId): RefreshOutcome
    fun gc(group: GroupId): GcOutcome
    fun liveGenerations(group: GroupId): List<GenerationState>
}

data class GenerationState(
    val generation: Long,
    val isCurrent: Boolean,
    val refCount: Int,
    val fileBytes: Long,
    val leases: List<LeaseInfo>
)

data class LeaseInfo(
    val owner: String,          // identifier of whoever took the lease
    val acquiredAt: Instant,
    val deadline: Instant
)
```

### 5.4 Construction (added 2026-08-30)

Sec 5.1-5.3 describe what a host *uses*; this is how a host *obtains* it. Everything implementing
those interfaces is `internal`, so without an entry point the API is unreachable from another
module. The entry point lives in `infra.snapshotcache.bootstrap`, the composition root added by the
plan 2.2 amendment - not in `api`, which would have to depend on `core` and `duckdb` and would break
two FIXED boundary rules.

```
fun openSnapshotCache(
    config: SnapshotCacheConfig,
    sources: Map<GroupId, GenerationSource>,
    events: CacheEvents = NoOpCacheEvents,
    checks: List<GenerationCheck> = emptyList(),
    clock: Clock = Clock.systemUTC()
): ManagedSnapshotCache

class ManagedSnapshotCache : AutoCloseable {   // a holder, not a seam - plan 2.4 bans
    val cache: SnapshotCache                  // single-implementation interfaces, and the
    val admin: CacheAdmin                     // 2.3 budget counts interfaces, not types
    override fun close()        // Sec 10.2 step 1, then 4; the stores only on a clean drain
}
```

- **One store per group, derived.** The store directory for a group is
  `config.storagePath.resolve(group.value)`, which is Sec 3.1's `/data/cache/<group>/` layout. It is
  derived rather than configured because generation numbering restarts at 1 per group: two groups
  aimed at one directory collide on `gen_0000000001.db`, and a derived path makes that
  misconfiguration unrepresentable.
- **Sec 10.1 startup, steps 1 and 2** happen here: the stale-file wipe when
  `startup.clearStaleFiles` is true - every `gen_*` file under `storage.path`, in that directory
  itself and in every first-level subdirectory, whether or not a group of that name is still served
  - and the serving instance's `memory_limit`, `temp_directory` and thread cap. With the wipe
  disabled, generation numbering instead starts above the highest number found on disk, so a
  survivor is neither overwritten by `promote` nor stranded outside the registry forever. Steps 3-5
  (readiness, the first refresh, the readiness flip) are the host's - readiness is the host's health
  surface and the framework has no scheduler.
- **`close()` splits on the drain.** After Sec 10.2 step 4 the store connections are closed only if
  the drain came back clean. An outstanding lease means a consumer thread may be mid-query on a
  connection the store issued, and a DuckDB connection used from two threads crashes the process
  rather than raising - so on a dirty drain the stores are left open and the files are left to step
  5, "connections die with the process", which the next startup's wipe then clears.
- **What stays the host's**, unchanged by this entry point: refresh scheduling (Sec 4.4 - the host's
  tick calls `CacheAdmin.triggerRefresh`), the `expiredLeases()` poll on that same tick (Sec 6.2,
  12.3), the metrics binder (Sec 12), lease attribution (below), and Sec 10.2 steps 2 and 3 - stop
  scheduling, interrupt the in-flight build - because only the host owns the scheduler and the build
  thread.
- **The host's tick reads `liveGenerations` as well as `expiredLeases()`.** `gc()` answers
  `GcOutcome(reclaimed = [], deferred = [])` for two different worlds - there was nothing to
  reclaim, and a consumer is pinning a generation that is not current - and the outcome alone
  cannot tell them apart. The refCount and lease list on each `GenerationState` can, so the same
  tick that polls `expiredLeases()` reads `liveGenerations(group)` and reports the non-current
  generations whose refCount is above zero. Without it, "a job is holding a generation open" is
  invisible until the live count reaches K and Sec 6.1 pauses refresh - which is the alert firing
  one stage after the fact it describes.
- **Lease attribution needs a JVM flag on a coroutine-dispatched host.** `LeaseInfo.owner` is the
  acquiring thread's name (Sec 5.3), and the remedy this section carried until 2026-08-30 - "the
  host names its pools" - is not available to the primary consumer. SimpleEtl dispatches every run
  on `Dispatchers.IO.limitedParallelism(1)`, a *view* over kotlinx-coroutines' shared IO pool: there
  is no `ThreadFactory` to hand a name to, and renaming a shared worker would misattribute every
  other coroutine on it. What supplies the attribution instead is
  **`-Dkotlinx.coroutines.debug=on`**, which appends the coroutine's name and id to the worker's
  thread name for the duration of each dispatch. Measured on the first composed host:

  | JVM | `LeaseInfo.owner` |
  |---|---|
  | assertions on (`-ea`) | `DefaultDispatcher-worker-1 @wip-summary#5` |
  | production defaults | `DefaultDispatcher-worker-1` |

  The flag is JVM-global and costs a small per-dispatch thread rename, which is the price of the
  only attribution a coroutine-dispatched host can get.

  **The trap is that no test can catch its absence.** kotlinx-coroutines' debug mode is `AUTO`,
  which switches itself *on* whenever assertions are enabled - and surefire enables them. So every
  test JVM in this repository shows the attributed form above while a production JVM silently shows
  the bare worker name, and a test asserting on `owner` is evidence only about the flag surefire
  happens to set. The flag has to be set in the deployment, and the check is reading it off a
  running pod's command line, not a green suite.
- `jdbc.fetchSize` is read by the caller's `GenerationSource` (Sec 7.2) and `refresh.interval` by the
  host's scheduler (Sec 4.4). They remain in `SnapshotCacheConfig` as one manifest of Sec 13, but the
  entry point does not act on them and says so.

---

## 6. Concurrency and Lifecycle

### 6.1 Generation cap K

| Condition | Behavior |
|---|---|
| Live generations <= K | Refresh proceeds normally, unaffected by leases |
| Live generations > K | **Pause refresh and alert**, wait for leases to release |

Default `K = 3` (current plus two awaiting reclamation).

This is the key to avoiding starvation. Under normal conditions a slow consumer only keeps an old generation around a bit longer - it **does not make the data stale**. Only when leases genuinely go out of control does the system degrade to "resource safety first", and that degradation is explicit and alerted, not a silent slowdown.

### 6.2 What the lease deadline is for

**The deadline is diagnostic, not enforcement.**

There is no safe way to yank the underlying data out from under a query that is mid-execution. Therefore:

- Past deadline -> record `snapshot_lease_expired_total`; log the owner and how long it has been held.
- Because leases record an owner, you can immediately identify which job is stuck, instead of only seeing memory climb.
- The real resource guardrail is K + pause refresh + alert.

Default deadline 5 minutes, configurable.

### 6.3 Orphaned lease protection

An interrupted thread, or an exception path that skips close, leaves a refcount that never returns to zero. Defenses:

1. **Offer scoped APIs first** (`withSnapshot`) so callers have no opportunity to forget.
2. **Cleaner / PhantomReference as a backstop**: when the handle object is garbage collected, force the release and log a warning. This is a bug signal, not a normal path, and must be visible.
3. `close()` must be idempotent - repeated calls must never drive refcount negative.

### 6.4 Consistency limit of short leases

`copyOut()` takes whatever is current at that moment. If one job calls it several times, **the results may come from different generations** and won't be consistent with each other.

Rules:

| Situation | Usage |
|---|---|
| One round needs one fetch (can be expressed in a single SQL) | `copyOut()` |
| Needs repeated fetches, or computes as it queries | `withSnapshot()`, pinning one generation for the whole round |

`copyOut()` returns `(generation, dataAsOf)`; the consumer must record these as data lineage.

### 6.5 The consumer's second DuckDB instance

Consumers may copy a subset into another DuckDB instance for processing. This has a real benefit: lease duration shrinks from "the whole ETL run" to "the few seconds of the copy", which is very friendly to reclamation.

But it must be managed by the framework, not left to each job:

- **Share a single consumer instance where the consumer can share one.** Don't open one per job -
  several unbounded instances will add up and eat the pod's memory budget.
- That instance also needs a `memory_limit`, counted in the overall budget (Sec 11.1).
- For cross-instance copies, attach the other file directly and read from it; don't serialize through the application.

**The primary consumer cannot share one, and D16 is amended rather than enforced.** SimpleEtl's own
spec 5.5 and 7.2 require a per-run scratch DuckDB in its own run directory - DuckDB 1.1.3 has no
vacuum and `DROP TABLE` does not shrink a file, so the run directory is deleted whole at the end of
the run, and a shared instance has no such end. Measured on the first composed host: two concurrent
tasks are two DuckDB instances, each at SimpleEtl's `EtlWiring.scratchMemoryLimitMb` default of
4096 MB. Both reasonings survive contact - D16's arithmetic is right about what unbounded instances
cost, and SimpleEtl's file-per-run is right about reclaiming disk on 1.1.3 - so neither is
overturned and the budget below is what changes.

**`duckdb.consumer.memoryLimit` is inert when the consumer is SimpleEtl.** A `cacheCopy` step
passes *scratch's own write connection* as `CopyOutSpec.targetConnection`, so the copy lands in an
instance whose `memory_limit` was set by `EtlWiring.scratchMemoryLimitMb`. The config field remains
in Sec 13 as the knob for a host that does own a shared consumer instance; for a SimpleEtl host it
is a number nothing reads, and the live knob is `scratchMemoryLimitMb`.

---

## 7. Source-side Extraction

### 7.1 Consistency

All tables in a group must be read inside **one Oracle read-only transaction**. Reading them separately produces a torn snapshot: the union may show duplicates or gaps, intermittently, and it's extremely hard to reproduce after the fact.

- Record `dataAsOf` when the transaction starts.
- Pull every table on the same connection, in the same transaction, before committing.
- If stricter guarantees become necessary, switch to a flashback query at an explicit SCN.

### 7.2 JDBC settings

**Fetch size must be tuned.** Oracle JDBC defaults to a fetch size of 10; pulling a million rows means over a hundred thousand round trips.

Set it to 2000 as a starting point and tune from measurements. This single setting affects total time more than any other optimization, and it costs no complexity.

### 7.3 Streaming writes

**Never read the whole ResultSet into a List before writing.** A million rows means hundreds of MB of temporary objects on the JVM heap.

Fetch and append in a stream, writing through the DuckDB Appender as rows arrive.

Appender discipline:

- Every Appender must be closed. It holds an internal buffer; not closing it is a leak.
- Wrap it in the try-with-resources equivalent so exception paths close it too.
- **Do not append to the same table from multiple threads** - it will produce write transaction conflicts.

### 7.4 Update strategy: full reload

**Decision: full reload. No incremental.**

The source tables do have an update-time column, but the savings from incremental aren't on the DuckDB side:

| Step | Full reload | Incremental |
|---|---|---|
| Oracle query + JDBC transfer | 1M rows, tens of seconds to minutes | changed rows, sub-second |
| DuckDB write | append 1M, roughly 5-15s | copy previous gen + small delta, roughly 3s |

The DuckDB-side difference is single-digit seconds - not worth the complexity. And incremental carries real costs:

- **Delete detection.** An update-time column tells you what changed, never what was deleted. Deleted rows stay in the cache forever. Fixing that requires periodically pulling all keys for reconciliation - meaning you never actually escape the full pull.
- **Drift doesn't self-heal.** A full reload is stateless: break it once and the next round repairs itself. Incremental is stateful: one mistake persists until the next reconciliation.
- **Time-boundary problems.** Timestamp precision, and commit order not matching update-time assignment order, both cause missed rows.

**Upgrade trigger:** if measured round time consistently exceeds 30% of the refresh interval (i.e. 3 minutes), reevaluate delta mode. At that point only the `GenerationSource` implementation changes.

### 7.5 Performance expectations

For 1M rows, no CLOBs, single-column id key, on an internal network:

| Stage | Estimate |
|---|---|
| Oracle query execution | seconds to tens of seconds (depends on the query) |
| JDBC fetch transfer | 30s - 2min (with fetch size tuned) |
| DuckDB Appender write | 3 - 15s |
| CHECKPOINT + verify | a few seconds |

Both tables together should land in the 1-4 minute range.

**Per-stage timing metrics are mandatory, not optional** (Sec 12). If you only measure total time, you have no way to tell "Oracle got slower" from "local writes got slower" when it regresses.

### 7.6 Capacity estimate

Assuming ~30 columns, mostly numeric and short strings:

- Roughly 150-250 bytes per row uncompressed
- 1M rows ~ 150-250 MB raw
- After DuckDB compression, roughly 50-150 MB per generation file

Two tables x K=3 generations ~ 300-800 MB on disk. A 5-10 GB volume leaves comfortable headroom.

---

## 8. Verify Gate

Verification runs **before** the swap. Any failing rule aborts the candidate; current stays put.

### 8.1 Rules

| Rule | Description | Default |
|---|---|---|
| `non_empty` | Any table with zero rows fails | **On, cannot be disabled** |
| `key_unique` | id is unique within its own table | On |
| `required_non_null` | Designated critical columns must not be NULL | On, column list configurable (grammar below) |
| `row_count_delta` | Fail if row count differs from previous by more than a ratio | **Off by default** |
| `readable` | Candidate file can be reopened and queried | On, cannot be disabled |

Each `required_non_null` entry is either a qualified `table.column`, checked in that one
table, or a bare `column`, checked in **every** table of the group. A bare name is therefore
an all-tables assertion: if the column is absent from any one table the rule errors, the whole
gate fails, and the group goes permanently stale - so qualify the name unless the column
genuinely exists everywhere.

### 8.2 On `non_empty`

Zero rows is almost never a real data state; it usually means a permission, connection, or predicate problem. Once published, every downstream job quietly computes "there's nothing" and writes that result. This class of error is the most expensive to recover from, so the check is non-disableable.

### 8.3 On `row_count_delta`

**Off by default. Observe first.**

Data volume may legitimately spike, and there isn't enough history yet to pick a sensible threshold. The plan:

1. Collect actual variation through the `snapshot_rows` metric.
2. After two to three weeks of data, decide whether to enable it and at what threshold.
3. Make the threshold configurable, with separate limits for decrease and increase (a drop generally warrants more suspicion than a rise).

Until it's enabled, row-count movement can be an alert rather than a gate - it notifies without blocking the swap.

### 8.4 Verification cost

For a million rows all of the above run in sub-second to a few seconds. That's negligible against the cost of pulling from Oracle, so there's no reason to weaken the checks for performance.

### 8.5 Consecutive failures

- Escalate to critical once consecutive failures reach the threshold (**default 3, configurable**).
- Keep serving the old data throughout; `snapshot_data_as_of` naturally reflects the growing staleness.
- Every failure must log which rule failed and the details - never just "verification failed."

---

## 9. Failure Handling and Degradation

### 9.1 Principle: serve stale data, but be loud about it

On refresh failure, keep serving the existing generation. But stale must be **visibly** stale, never silently stale.

- Every handle carries `dataAsOf` so consumers can judge for themselves.
- The `snapshot_data_as_of` metric lets monitoring judge independently.
- Consecutive failures escalate.

### 9.2 Failure cases

| Failure | Handling |
|---|---|
| Oracle connection failure / query timeout | Abort round, delete candidate, count `source_error` |
| Verification failure | Abort round, delete candidate, count `verify_failed`, escalate to critical at threshold |
| Insufficient disk space | Abort round, delete candidate, trigger emergency GC, count `disk_error`, alert |
| Live generations exceed K | Pause refresh, alert, log all lease owners and hold durations |
| DETACH fails (connection still in use) | Leave the generation for the next GC pass, log warning |
| File delete fails after a successful DETACH | Leave the generation for the next GC pass, log warning; the retry re-runs DETACH + delete as one unit, so `GenerationStore.close` must be idempotent (Sec 17.1) |
| No generation yet, `waitBudget == 0` | `acquire()` **throws `NotReadyException` immediately**; does not block |
| No generation yet, `waitBudget > 0` | Wait interruptibly up to the budget; on expiry throw `NotReadyException`, count `reason="timeout"` |
| Shutdown in progress | `acquire()` throws `ShuttingDownException` immediately; threads already waiting are interrupted and released at once (Sec 10.2) |
| In-flight refresh aborted by shutdown | Interrupt the source, delete the candidate, never promote, current pointer untouched, count `shutdown_aborted` (Sec 10.2 step 3, D23) |
| Lease drain times out at shutdown | Log warning listing every outstanding lease owner and hold duration, then proceed to exit |

### 9.3 On acquire before readiness

This applies only before the first successful publish. In steady state a refresh failure keeps serving the previous generation, so acquire always has something to hand out.

**Why the default is not "block indefinitely":** a blocking acquire occupies a scheduler thread. Several jobs blocking together exhaust the scheduler pool and stall unrelated jobs, producing the hard-to-diagnose symptom "nothing scheduled is running after startup."

**Why the default is not zero either:** a job that runs once a day loses a whole day by failing fast during a cold start that would have finished in two minutes.

The resolution is the bounded, caller-chosen `waitBudget` of Sec 5.1, defaulting to 30 seconds - long enough to absorb a normal cold start or rolling deploy, short enough that it cannot exhaust a scheduler pool. Callers that genuinely cannot wait pass `Duration.ZERO`; callers whose runs are expensive to miss pass minutes.

Implementation requirements:

- The wait must be **interruptible** and must release immediately on shutdown (Sec 10.2 step 1). A non-interruptible wait will be SIGKILLed by Kubernetes before graceful shutdown can run.
- Waiters are signalled by a condition variable on publish, never by polling.
- Record `snapshot_acquire_waited_seconds` (how long waits actually took) and `snapshot_acquire_unavailable_total{reason}` (gave up).

Note that readiness probes (Sec 10.1) already keep external traffic away until the first publish. The wait/fail behavior here exists for **internal scheduled jobs**, which can fire before readiness flips. The two mechanisms are complementary, not redundant.

---

## 10. Startup and Shutdown

### 10.1 Startup

1. **Delete every `gen_*.db` and `.tmp` file under the cache directory.**

   If the pod was OOMKilled or crashed, orphaned generation files are left on disk. Since the current pointer is not persisted, all such files are unowned. Wiping and rebuilding is the cleanest option - a cold start has to rebuild anyway.

2. Initialize the serving DuckDB instance; set `memory_limit` and `temp_directory`.
3. readiness = false.
4. Start the first refresh immediately (don't wait for the schedule).
5. readiness = true once the first generation publishes successfully.

**Why wait for the first generation before ready:** consumers waiting is far better than consumers reading an empty table. An empty table makes ETL quietly produce "nothing at all" and write that downstream - the hardest kind of error to notice and the most expensive.

### 10.2 Shutdown

Every step is bounded. The ordering matters: releasing waiters first is what makes the rest of the sequence reachable at all.

1. **Mark shutting down.**
    - New acquires throw `ShuttingDownException` immediately; they no longer enter the wait path.
    - Threads already waiting on `waitBudget` are interrupted and released **at once**, without consuming the remainder of their budget.

2. **Stop scheduling.** No new refresh cycle starts.

3. **Abort any in-flight refresh.**
    - Interrupt the source connection.
    - Delete the candidate `.tmp` file. **Never promote it**, even if it was nearly complete.
    - The current pointer is untouched.

   The candidate is an isolated file that was never promoted, never attached, and never visible through any handle, so discarding it is unobservable to every consumer. This is a direct benefit of one-file-per-generation: no long-running transaction is needed to protect the build. Letting a nearly-finished round complete would make shutdown duration unpredictable for no gain, since the build is stateless and the next startup rebuilds anyway.

4. **Drain leases**, bounded by `shutdown.leaseDrainTimeout` (default 30s).
    - On timeout, log a warning naming every outstanding lease owner and its hold duration, then proceed. This log is the only way to identify what is delaying shutdown.

5. **Exit.** No delicate cleanup: generation files are wiped by the next startup, and connections die with the process.

**Consumer responsibility.** If drain times out, in-flight consumer work is cut off. The framework guarantees that a snapshot a consumer holds never changes underneath it; it does not guarantee the consumer finishes. **Consumer writes must therefore be transactional or idempotent**, so an interrupted run is safely re-executed on the next cycle. Interruption risk lives on the consumer's output side, not in the cache - the cache's own state machine is unaffected by shutdown at any point.

**Grace period alignment.** The total of steps 1-4 must be less than the pod's `terminationGracePeriodSeconds`, or Kubernetes SIGKILLs the process mid-sequence and the design has no effect. With the 30s default drain, set the grace period to 45-60s (Sec 11.3).

---

## 11. Deployment and Resources

### 11.1 Memory budget (8 GB pod limit)

| Purpose | Allocation |
|---|---|
| JVM heap (`-Xmx`) | 2 GB |
| Serving DuckDB `memory_limit` | 3 GB |
| Consumer instances - `N_concurrent x` each instance's `memory_limit` | 1 GB for one shared instance |
| OS page cache, JDBC buffers, JVM non-heap, allocator fragmentation | remaining ~2 GB |

**The consumer row is a product, not a constant** (2026-08-30, first composed host). A consumer
that opens one DuckDB instance per unit of work contributes one term per *concurrently running*
unit, so the composed budget is

```
servingMemoryLimit + N_concurrent x <the consumer's per-instance memory_limit>
```

With SimpleEtl as the consumer that reads
`servingMemoryLimit + N_concurrent x EtlWiring.scratchMemoryLimitMb`, and
`scratchMemoryLimitMb` defaults to **4096 MB** - so two concurrent tasks alone are 8 GB of
consumer-side limit against a 3 GB serving instance in an 8 GB pod. `N_concurrent` is the number of
SimpleEtl tasks whose crons can overlap, which is the host's to bound; the framework caps nothing
(Sec 6.5, D16). Sizing a composed pod off the one-shared-instance row is the arithmetic that gets
it OOMKilled.

**`memory_limit` must not approach the pod limit.** DuckDB only accounts for its own buffers; JVM heap, JDBC staging, and glibc allocator fragmentation are all outside its view. Setting it too high means DuckDB thinks it's healthy right up until the pod is OOMKilled.

**And it must not be tuned down without pricing the disk term** (measured, composed-host-example
M2, 2026-08-30): shrinking a scratch instance's limit does not shrink the run, it converts RAM into
spill on the very volume 7.2 sizes as file-plus-spill, at a poor exchange rate - on one 10M-key
aggregate, saving 192 MB of pod memory (256 -> 64 MB) cost 3 GB of extra peak spill (737 MB ->
3,717 MB). The budget formula above has a memory term and, implicitly, a disk term, and they trade
against each other; fitting more tasks per pod by tightening `scratchMemoryLimitMb` is spending the
scratch volume to buy the memory request.

**`temp_directory` must be set** and pointed at a volume with real space. Without it there's nowhere to spill and you go straight to OOM.

In practice the data is only a few hundred MB, so it will sit entirely in the buffer pool - query performance equals in-memory mode.

### 11.2 Single replica

- Set the Deployment to `strategy: Recreate`, or `maxSurge: 0`.

  The default RollingUpdate briefly runs old and new pods together. We deliberately skip leader election: for a read-only cache, an overlap merely means pulling Oracle one extra time and cannot produce wrong data. Solving it with a deployment setting is far cheaper than writing election logic.

- No standby pod. A standby would have to keep pulling Oracle to stay fresh, which isn't worth the cost. A few minutes of cold-start rebuild is acceptable.

### 11.3 Probes

| Probe | Setting |
|---|---|
| startupProbe | `failureThreshold` wide enough to cover **10 minutes**. The first refresh can take minutes; defaults will declare startup failed and restart the pod repeatedly. |
| readinessProbe | Checks that a current generation exists. Takes over after startupProbe passes. |
| livenessProbe | Process liveness only. **Do not** tie it to data freshness - stale data is not something a restart fixes, and tying it in just creates a useless restart loop. |
| `terminationGracePeriodSeconds` | **45-60s** with the default 30s lease drain. Must exceed the total of Sec 10.2 steps 1-4, otherwise the graceful shutdown sequence is SIGKILLed partway and has no effect. |

---

## 12. Metrics and Alerting

### 12.1 Version and freshness

| Metric | Type | Labels | Description |
|---|---|---|---|
| `snapshot_current_generation` | gauge | group | Current generation number |
| `snapshot_data_as_of_seconds` | gauge | group | Source point in time, as an **absolute Unix-seconds value** |
| `snapshot_published_at_seconds` | gauge | group | When the swap completed |
| `snapshot_rows` | gauge | group, table | Row count per table |

**`data_as_of` must store an absolute timestamp, not "how old it is."** Alert rules compute `time() - snapshot_data_as_of_seconds > X`, which means thresholds can be adjusted any time without a code change.

### 12.2 Refresh process

| Metric | Type | Labels | Description |
|---|---|---|---|
| `snapshot_refresh_duration_seconds` | histogram | group, phase | phase = `query` / `fetch` / `append` / `checkpoint` / `verify` / `publish` |
| `snapshot_refresh_total` | counter | group, result | result = `success` / `verify_failed` / `source_error` / `disk_error` / `shutdown_aborted` / `skipped_overlap` / `blocked_by_k` |
| `snapshot_verify_failed_total` | counter | group, rule | Broken down by failing rule |
| `snapshot_last_success_seconds` | gauge | group | Time of last successful publish |

Per-phase timing is required, not optional. When performance regresses later, this is the only thing that distinguishes "Oracle got slower" from "local writes got slower."

### 12.3 Lifecycle health

| Metric | Type | Labels | Description |
|---|---|---|---|
| `snapshot_live_generations` | gauge | group | **The most important leak indicator.** Steady state should be 1 |
| `snapshot_active_leases` | gauge | group | Current lease count |
| `snapshot_lease_duration_seconds` | histogram | group | Distribution of lease hold times |
| `snapshot_lease_expired_total` | counter | group | Leases that passed their deadline |
| `snapshot_lease_orphaned_total` | counter | group | Leases force-released by the Cleaner. **Any non-zero value is a bug** |
| `snapshot_acquire_waited_seconds` | histogram | group | How long acquires actually waited before a generation became available. Empty in steady state |
| `snapshot_acquire_unavailable_total` | counter | group, reason | reason = `not_ready` (budget was zero) / `timeout` (budget expired) / `shutting_down` |

### 12.4 Resources

| Metric | Type | Labels | Description |
|---|---|---|---|
| `snapshot_db_file_bytes` | gauge | group | Total size of all live generation files |
| `snapshot_gc_deleted_total` | counter | group | Generations reclaimed |

Plus the existing JVM / process RSS metrics.

**How to verify there's no leak:** run 100 consecutive generation rotations and watch whether process RSS and `snapshot_db_file_bytes` converge to a flat line. A continued climb means something isn't being released.

### 12.5 Label cardinality

**Never use `generation` as a metric label.** It increases monotonically and would grow time series without bound. Generation-level detail belongs in logs and the admin endpoint, not in metrics.

### 12.6 Starting alert rules

| Alert | Condition | Severity |
|---|---|---|
| Data stale | `time() - snapshot_data_as_of_seconds > 3 x interval` | warning |
| Data badly stale | `time() - snapshot_data_as_of_seconds > 5 x interval` | critical |
| Consecutive verify failures | consecutive count >= threshold (default 3) | critical |
| Generations piling up | `snapshot_live_generations > 1` sustained for 15 minutes | warning |
| Refresh blocked | `snapshot_refresh_total{result="blocked_by_k"}` increasing | critical |
| Orphaned leases | `snapshot_lease_orphaned_total` increasing | warning (treat as a bug) |
| Acquire giving up | `snapshot_acquire_unavailable_total{reason="timeout"}` increasing outside a deploy window | warning (a job is missing runs) |

### 12.7 Admin endpoint

Expose an internal endpoint returning full state for manual investigation:

```
GET /internal/snapshot/{group}
{
  "current": { "generation": 124, "dataAsOf": "...", "rowCounts": {...} },
  "liveGenerations": [
    { "generation": 123, "isCurrent": false, "refCount": 1, "fileBytes": 118000000,
      "leases": [ { "owner": "etl-job-x", "acquiredAt": "...", "deadline": "..." } ] },
    { "generation": 124, "isCurrent": true,  "refCount": 0, "fileBytes": 119000000,
      "leases": [] }
  ],
  "lastRefresh": { "result": "success", "durations": { "query": 8.2, "fetch": 71.5, ... } }
}
```

---

## 13. Configuration

| Parameter | Default | Description |
|---|---|---|
| `refresh.interval` | 10m | Gap after the previous round finishes |
| `refresh.allowOverlap` | false | Overlapping runs forbidden |
| `generation.maxLive` (K) | 3 | Live generation cap |
| `acquire.defaultWaitBudget` | 30s | Default upper bound for acquire before first publish; overridable per call |
| `lease.deadline` | 5m | Diagnostic threshold (no forced reclamation) |
| `verify.nonEmpty` | true | Cannot be disabled |
| `verify.keyUnique` | true | |
| `verify.requiredNonNull` | (column list) | Entries are `table.column` (that table only) or a bare `column` (every table); see Sec 8.1 |
| `verify.rowCountDelta.enabled` | **false** | Observe before enabling |
| `verify.rowCountDelta.maxDecreaseRatio` | 0.20 | Applies once enabled |
| `verify.rowCountDelta.maxIncreaseRatio` | 1.00 | Applies once enabled |
| `verify.consecutiveFailureThreshold` | 3 | Failures before escalating to critical |
| `jdbc.fetchSize` | 2000 | |
| `duckdb.serving.memoryLimit` | 3GB | |
| `duckdb.consumer.memoryLimit` | 1GB | The host's shared consumer instance (Sec 6.5). **Inert when the consumer is SimpleEtl**, whose `cacheCopy` hands scratch's own connection as `CopyOutSpec.targetConnection`; the live knob there is `EtlWiring.scratchMemoryLimitMb`. Kept as a Sec 13 row rather than removed - a host that does own a shared instance reads it |
| `duckdb.serving.threads` | (null = engine default) | Optional cap on the serving instance's DuckDB thread pool. The engine default equals hardware concurrency, which oversubscribes CPU-limited pods; a cap also bounds how much of the pod a runaway reader can occupy (D29) |
| `duckdb.tempDirectory` | (required) | |
| `storage.path` | (required) | Generation file directory |
| `startup.clearStaleFiles` | true | Wipe leftovers on startup |
| `shutdown.leaseDrainTimeout` | 30s | Bound on Sec 10.2 step 4; keep pod `terminationGracePeriodSeconds` above this plus headroom |

---

## 14. Known Limitations and Future Extensions

### 14.1 Incremental update (delta)

Currently full reload. If refresh time starts approaching the interval, delta mode is available:

```
1. Copy the full previous generation into the new file
   (DuckDB-internal copy of 1M rows is roughly a second)
2. Pull only rows past the update-time watermark from Oracle
3. DELETE the changed keys, then append the new values
```

**Must be solved before switching:**

- **Delete detection**: requires periodic full key reconciliation (anti-join); frequency TBD.
- **Watermark**: needs a safety overlap window, or switch to SCN as the watermark.
- **Periodic full rebuild**: even under delta, rebuild fully on a schedule (e.g. daily) to clear accumulated drift.

Interface-wise this only replaces `GenerationSource`; `BuildContext.previous` is already reserved.

### 14.2 Cross-process sharing (manifest + object storage)

Currently limited to in-process consumers. Since generation files are immutable and self-contained, the extension path is direct:

```
1. On successful publish, upload the generation file to object storage
2. Maintain a manifest: { group, generation, dataAsOf, objectUrl, rowCounts, checksum }
3. Other processes read the manifest, download the generation, and ATTACH it
```

This turns the current pointer into a globally consistent manifest version, solving both cross-replica version skew and duplicate Oracle pulls.

Additional design needed: manifest storage and update atomicity, local cache and cleanup on the download side, retention period for old generations.

Note: Sec 18 (archive & diff layer) is a **different, decided** extension that shares the "manifest + object storage" vocabulary but not the goal. Sec 18 persists Parquet checkpoints so in-process ETLs can diff across restarts; this section (14.2) is about distributing `.db` generation files for cross-process *serving*, and remains an open sketch. Sec 18 deliberately archives Parquet, not `.db` files, precisely so it neither depends on nor preempts this extension.

### 14.3 Multiple replicas

Not supported. Replicas refreshing independently would mean:

- Different replicas serving different generations, so consecutive consumer calls could see the version go backwards.
- Nx the pull load on Oracle.

If it becomes necessary, go the Sec 14.2 route (single leader pulls, manifest distributes) rather than having each replica pull for itself.

### 14.4 Schema changes

When a source schema changes, the new generation fails to build and the system keeps serving the old one. That behavior is safe, but **verification failure must produce a visible alert** - otherwise the symptom is "everything looks fine but the data stopped updating."

No automatic schema evolution for now.

### 14.5 DuckDB version constraint

Pinned to 1.1.3 (Linux component compatibility in the CI environment). Consequences:

- No statement timeout, so query timeouts can't be set at the SQL level. Killing a runaway query requires interrupting the connection from the API layer.
- No vacuum that shrinks files. The one-file-per-generation design sidesteps this entirely.

---

## 15. Decision Log

| ID | Decision | Rationale |
|---|---|---|
| D1 | Generational model, not LEFT/RIGHT two-slot rotation | Two slots couple refresh frequency to the slowest consumer, forcing a choice between stale data and unsafe forced reclamation |
| D2 | One generation = one standalone .db file | DuckDB 1.1.3's DROP TABLE doesn't shrink files and there's no vacuum; rotating within one file leaves a high-water mark and fragmentation |
| D3 | ATTACH generation files READ_ONLY | Prevents accidental consumer writes; a failed DETACH also serves as an extra safety signal |
| D4 | Both source tables in one group | They're used in a union and must come from the same consistency point |
| D5 | Two physical tables + union view, not one merged table | Preserves column semantics, avoids id collisions, and limits schema-change impact to the view |
| D6 | Full reload, no incremental | Incremental saves Oracle/JDBC time, not DuckDB time; delete detection and drift cost more than the savings. Upgrade path preserved in the interface |
| D7 | K = 3; pause refresh and alert when exceeded | Slow consumers affect resource usage rather than freshness; genuine loss of control degrades explicitly and loudly |
| D8 | Lease deadline is diagnostic, not enforcement | No safe way to pull data out from under a running query; recording the owner makes problems locatable |
| D9 | Scoped API preferred, `acquire()` is advanced usage | Orphaned leases are the main leak source; eliminate them at the interface level |
| D10 | Wipe all leftover files on startup | The current pointer isn't persisted, so leftovers are unowned; a cold start rebuilds anyway |
| D11 | Readiness waits for the first generation | Consumers waiting beats consumers reading an empty table, which produces hard-to-notice downstream errors |
| D12 | Single replica + `Recreate` strategy, no leader election | A read-only cache can't produce wrong data on overlap; a deployment setting is far cheaper than election logic |
| D13 | `non_empty` is a non-disableable gate | Zero rows is almost always a fault, and publishing it makes every downstream quietly compute an empty result |
| D14 | `row_count_delta` off by default, collect metrics first | Volume may legitimately spike; there's no basis yet for a sensible threshold |
| D15 | Escalate to critical after 3 consecutive verify failures, configurable | Tolerate a single failure; sustained failure indicates a systemic problem |
| D16 | Consumers share one DuckDB instance **where the consumer can share one**; where it cannot, the pod budget is sized as `N_concurrent x <per-instance limit> + servingMemoryLimit` (amended 2026-08-30) | Multiple unbounded instances would add up and consume the pod's memory budget - which is why the amendment is arithmetic and not permission. SimpleEtl cannot share one: its spec 5.5/7.2 need a per-run file so the run directory can be deleted whole, DuckDB 1.1.3 having no vacuum. Both reasonings hold, neither is overturned, and Sec 11.1 carries the product |
| D17 | `data_as_of` metric stores an absolute timestamp | Alert thresholds become adjustable without code changes |
| D18 | `generation` is never a metric label | Monotonic growth would blow up time series cardinality |
| D19 | DuckDB behavioral assumptions (A1-A8) documented but empirical leak/GC measurement deferred | No time budget for a pre-implementation spike; assumptions and pass criteria are recorded in Sec 17.6 so measurement can run later without redesigning the tests |
| D20 | A real-DuckDB E2E feasibility test is mandatory before the framework is accepted | Fake-storage tests prove the framework's bookkeeping, not that DuckDB 1.1.3 actually behaves as assumed; a small synthetic-data E2E covers the functional subset cheaply |
| D21 | Acquire uses a bounded, per-call `waitBudget` (default 30s) rather than fail-fast or block-forever | Fail-fast loses a whole run for daily jobs; blocking forever exhausts scheduler pools. A bound is the only option that serves both, and only the caller knows the cost of a missed run |
| D22 | `waitBudget` is a call parameter, not configuration, and is not derived from historical statistics | It is an upper bound, not a sleep, so setting it generously is free while setting it low has a real cost; a one-sided cost function should be given headroom, not estimated. Rolling averages would also need persistence, and would be empty at cold start - the only moment the budget is used |
| D23 | Shutdown aborts any in-flight refresh and discards the candidate rather than finishing the round | The candidate is an unpromoted, unattached file that no consumer can observe, so discarding it is invisible; finishing it would make shutdown duration unbounded for a build that is stateless and rebuilt on next start |
| D24 | The framework holds no schedule state (last-success times, catch-up windows); callers implement such policy against `waitBudget` and `currentInfo()` | Scheduling is not caching. Persisting run history would add a store and new failure modes, and would be empty at cold start when it would be needed |
| D26 | Sec 12.2's `result` label set grows to seven, adding `disk_error` and `shutdown_aborted` | Every row of Sec 9.2 must be distinguishable in metrics and in the Sec 17.8 "returns to a usable state" tests. Folding disk exhaustion and shutdown-abort into `source_error` made two distinct operational conditions indistinguishable on a dashboard, and left a P4 acceptance test unable to tell which failure it had exercised. The label is bounded and enumerable, so growth costs no cardinality |
| D27 | `core` logs through `org.jboss.logging.Logger`, never `io.quarkus.logging.Log` | The host is a Quarkus service, so its log manager is already present and all `quarkus.log.*` configuration applies unchanged. Naming the Quarkus type in `core` would break the Sec 2.2 boundary rule and force the core suite to boot a framework, losing the millisecond feedback loop that makes Sec 17.4/17.5 affordable. JBoss Logging is the API Quarkus itself is built on, so the output is identical |
| D28 | The `Snapshot` handle is constructed at the `spi` boundary, not in `core` | Sec 2.2 confines `java.sql` to api signatures, spi and duckdb. A handle implementation living in `core` would name `Connection` in its bytecode as a field, parameter and return type. `OpenGeneration` already owns the connection, so it produces the handle and `core` holds it only as the `api.Snapshot` type, keeping the rule verbatim and the lease bookkeeping unchanged |
| D25 | Consumers are responsible for transactional or idempotent writes | Lease drain is bounded, so an interrupted consumer is possible by design. The cache guarantees snapshot stability, not consumer completion; the interruption risk lives on the consumer's output side |
| D29 | `duckdb.serving.threads` knob added (nullable, null = engine default); runaway readers are accepted-and-observed, not enforced | DuckDB's thread default equals hardware concurrency, which throttles CPU-limited pods and lets one runaway reader starve the shared serving instance (1.1.3 has no statement timeout, Sec 14.5). The cap bounds the blast radius; detection stays with the D8 lease-deadline diagnostics. An admin kill switch (interrupting a lease's connections - the Sec 14.5-sanctioned path, made safe by D25 idempotency) is deferred to P9+, to be added only if lease-duration histograms show real abuse |
| D30 | The archive & diff layer (Sec 18) is a consumer of the public API in a sibling package `infra.snapshotarchive`, never part of the framework | It needs only `withSnapshot`/`copyOut`/`currentInfo` plus Oracle and MinIO. Keeping it outside `infra.snapshotcache` leaves D10/D22/D24 and the five-interface budget untouched, and the one-way ArchUnit rule (framework never imports archive) makes the boundary mechanical. Plan 2.4's module rule still holds: same Maven module, packages are the fence |
| D31 | Durable archive versions come from an Oracle sequence in the manifest; generation numbers never leave the process; `data_as_of` is the only ephemeral-to-durable join key | Generation numbering restarts at 1 on every boot (Sec 4.3), so it cannot identify anything durable, and most generations are never archived. The archiver enforces `data_as_of` monotonicity at publish (skip + alert on regression) - the same distrust of timestamps as Sec 4.3, applied at the one place time is load-bearing |
| D32 | Checkpoints only - hourly full Parquet export per table; no precomputed delta files. An ETL's diff is always `checkpoint(watermark)` vs the live snapshot | At ~1M rows a checkpoint is tens of MB and the PK full-outer-join is seconds of local DuckDB; deltas would add a format, composition semantics and dual retention for no payoff. Diffing an older baseline can only over-report (safe: D25 idempotent consumers); under-reporting is impossible for monotone columns, because a baseline is always a manifest-recorded checkpoint taken at or before the ETL's last processed moment - but NOT in general: a value returning to its baseline's exact value inside one archive interval is missed (open item 18.6 #4). Revisit at ~50M rows: add deltas as an optimization on top, checkpoints stay the source of truth |
| D33 | Publish protocol is intent-first: INSERT manifest row PENDING (with full file inventory + checksums) -> upload -> verify -> conditional UPDATE to COMPLETE; a watchdog resolves stale PENDING rows to COMPLETE or FAILED against the inventory | Every MinIO object is preceded by a covering manifest row, so ghost files are impossible and no LIST-based orphan sweep exists. All status transitions are conditional (`WHERE status='PENDING'`), so an uploader racing the watchdog resolves to exactly one winner. Readers trust only COMPLETE. Crash and graceful shutdown converge on the same watchdog recovery path |
| D34 | Retention is a fixed window sized to the slowest ETL cadence plus margin, with an unconditional keep-newest-COMPLETE rule; full compare against the live snapshot is a first-class fallback, not an error | The fallback must exist anyway (new ETLs, FAILED gaps, watermark purged), which is what lets retention stay a dumb window instead of consumer registration/refcounting. Keep-newest guarantees a broken archiver can never purge the last good baseline |
| D35 | The watermark is consumer state: each ETL records `max(version) WHERE status='COMPLETE' AND data_as_of <= snapshot.dataAsOf` transactionally with its own output | D24 again: the framework tracks no per-consumer state. The `data_as_of <= T` predicate closes the long-running-job race - a checkpoint published mid-run describes state the ETL did not process and must never become its baseline (under-report); the predicate can only err toward an older version, which merely over-reports |
| D36 | Archived tables must declare stable primary keys; the archive format is Parquet, downloaded then read locally | Unkeyed tables have no update semantics and were cut from scope. Parquet decouples the archive from the pinned DuckDB 1.1.3 file format (a `.db` archive would be unreadable the day the pin moves), is per-table, and is natively diffable by DuckDB. No httpfs: download-then-read keeps the 1.1.3 surface minimal |

---

## 16. Open Items Before Implementation

1. **Column mapping table for tables A and B** - explicitly separate "same concept, different name" from "this source lacks the concept"; put it under version control.
2. **The `requiredNonNull` column list** - which columns being NULL indicates broken data.
3. **Baseline measurements** - per-stage timing, to confirm the 10-minute interval is comfortable and to tune fetch size. Deferred along with data-correctness validation; not part of framework acceptance (Sec 17).
4. **Leak verification** - deferred; assumptions and pass criteria recorded in Sec 17.6 so the measurement can be executed later without redesign.

---

## 17. Acceptance Plan (Framework)

Scope: this section covers acceptance of the **snapshot cache framework itself** - generation lifecycle, leases, K enforcement, GC, refresh orchestration, and failure handling. Data correctness (source transaction consistency, `dataAsOf` semantics, view column alignment) and performance baselines are explicitly out of scope for this phase and will be validated after implementation lands.

The plan has three layers:

1. Fast, deterministic tests against a **fake storage layer** - this is where correctness is actually proven.
2. **Documented DuckDB behavioral assumptions** with pass criteria - measurement deferred (D19).
3. A **real-DuckDB E2E feasibility test** with synthetic data - mandatory (D20), covering the functional subset of the assumptions.

### 17.1 Test seams (prerequisite)

The framework's real responsibilities - generation numbering, the current pointer, refcounts/leases, K enforcement, GC, refresh orchestration, state reporting - are all independent of DuckDB. To make them testable deterministically and at millisecond speed, three seams are required in the design:

```
interface GenerationStore {
    fun createCandidate(gen: Long): Candidate
    fun promote(gen: Long)                   // .tmp -> final name
    fun open(gen: Long): OpenGeneration    // attach read-only
    fun close(gen: Long)                     // detach
    fun delete(gen: Long)                    // remove file
    fun listOnDisk(): List<Long>
}
```

- **`GenerationStore`** - the only component that touches DuckDB files. Production implements it with ATTACH/DETACH/delete; tests use an in-memory fake that records every call and can be scripted to fail on specific operations. `close(gen)` is **idempotent**: detaching a generation that is not attached is a no-op, because the GC pass of Sec 9.2 retries DETACH + delete as one unit after a failed delete.
- **Injectable `Clock`** - deadline, expiry, and staleness tests must not sleep for real minutes.
- **Manually triggerable scheduler** - overlap prevention and skip logic must be testable without real `@Scheduled` timing.

Acceptance requires that all Sec 17.2-Sec 17.5 tests run without DuckDB or Oracle on the classpath's runtime path.

### 17.2 Invariants and named tests

Each invariant must have a test named after its ID (e.g. `I3_generationStrictlyIncreasing`). This table is the contract; a future change that breaks a guarantee must break a correspondingly named test.

| ID | Invariant |
|---|---|
| I1 | current only ever points to a verified generation; never to a candidate or a failed build |
| I2 | A generation with refcount > 0 is always in the opened state; it is never closed or deleted |
| I3 | Generation numbers are strictly increasing; no duplicates, no regression |
| I4 | Live generations <= K, except in an explicit, recorded blocked state |
| I5 | A generation that is non-current with refcount == 0 is eventually deleted |
| I6 | refcount is never negative |
| I7 | After a failed refresh, current is unchanged and no candidate resources remain |
| I8 | A handle observes the same generation number for its entire lifetime |

### 17.3 Resource accounting equations

With the fake storage recording every call, leak detection at the framework level becomes exact arithmetic rather than trend observation. These equations are asserted automatically at the end of **every** test (in a shared fixture, not per-test):

```
count(createCandidate) == count(promote) + count(delete of candidates)
per generation: count(open) == count(close)      // except still-live ones
at test end: opened generations == { current } U { gens with refcount > 0 }
at test end: generations on disk == opened generations
```

Any violated equation identifies the exact generation and operation that leaked - strictly more informative than observing RSS trends. The fake storage must also support scripted failures (e.g. "the 3rd close throws") to drive the Sec 9.2 failure-path tests.

### 17.4 Deterministic concurrency tests

**No `Thread.sleep`-based timing anywhere.** Sleep-tuned interleavings are flaky and end up disabled. Instead, the framework exposes test-only hooks at the dangerous points:

```
enum class Hook {
    AFTER_READ_CURRENT,      // acquire has read the pointer, refcount++ not yet done
    BEFORE_POINTER_SWAP,
    AFTER_POINTER_SWAP,      // published, GC not yet run
    BEFORE_DETACH,
    AFTER_VERIFY
}
```

Tests use latches at hook points to let another thread complete a full operation before releasing, making every interleaving deterministic and repeatable.

The single most important case: **at `AFTER_READ_CURRENT`, force a complete publish + GC cycle, then assert the returned handle is still queryable.** This is the seam the Sec 5.1 atomicity requirement exists for, and it must be exercised deterministically, not left to stress-test luck.

Required interleavings:

| Case | Assertion |
|---|---|
| publish + GC occurs mid-acquire | handle valid and queryable; I2 holds |
| lease held on an old gen while refreshing up to K | refresh blocks with explicit state; auto-resumes after release |
| `close()` called twice | refcount decremented once; I6 holds |
| handle garbage-collected without close | Cleaner force-releases; orphan counter +1 |
| overlapping schedule trigger | second run skipped; never two candidates at once |
| one handle spans two publishes | generation number unchanged; I8 holds |

Plus a stress test: N consumer threads randomly acquire/query/close while refresh runs M rounds (suggested N=20, M=100), with all invariants checked after every round. This runs in CI against the fake storage, so it stays fast.

### 17.5 Randomized model test

Targeted cases cover the interleavings we can think of; a model test covers the ones we can't.

- Model state: set of live generations, current pointer, per-generation refcount.
- Randomly generated operation sequences: `acquire` / `close` / `refresh-success` / `refresh-failure` / `verify-failure` / `gc` / `orphan`.
- **All of I1-I8 checked after every step.**
- Fixed seed for reproducibility; on failure, the full operation sequence must be printed.

Run several thousand sequences per CI execution - cheap against the fake storage. This class of test typically catches "three events in the wrong order" bugs that no one writes a targeted case for.

### 17.6 DuckDB behavioral assumptions - documented, measurement deferred

The design rests on assumptions about DuckDB 1.1.3 behavior. Empirical leak/GC measurement is **deferred** (D19). The assumptions and their pass criteria are recorded here so the measurement can be executed later exactly as specified, and so it is explicit which risks remain open until then.

| ID | Assumption | Verification method (when executed) | Impact if false | Status |
|---|---|---|---|---|
| A1 | DETACH + file delete returns RSS to baseline | 50+ rotations, RSS trend per criteria below | **D2 collapses; the whole model must be rethought** | RSS trend still open (deferred, D19); **file-level subset confirmed by the Sec 17.7 E2E (P8)**: files deleted and disk == live set across 22 real rotations |
| A2 | DROP TABLE does not shrink the file | create/drop repeatedly, measure file size | If it does shrink, a simpler single-file model becomes viable | open |
| A3 | READ_ONLY attach rejects writes | attempt INSERT | One protection layer gone; discipline only | **confirmed by the Sec 17.7 E2E (P8)**: INSERT through a handle connection rejected, reads intact |
| A4 | DETACH fails while a connection is in use | open connection, attempt DETACH | The Sec 9.2 safeguard doesn't exist and must be removed | **partially false at engine level (P7, probe-verified)**: on 1.1.3, DETACH succeeds under an idle reader and breaks that reader's next query instead. The Sec 9.2 defer safeguard is therefore enforced by adapter-side connection bookkeeping (`DuckDbGenerationStore.close` throws while a store-issued connection into the generation is open). Sec 17.7 step 4 must stage its "raw connection" through `OpenGeneration.connection()` and verifies the adapter guard, not engine behavior. **Confirmed in that adapter-guard form by the Sec 17.7 E2E (P8)**: reclamation deferred while the tracked connection is open, reader untouched, file gone after close + GC. |
| A5 | `memory_limit` is effective in file mode and spills to temp | load beyond the limit | Sec 11.1 budget math must change | open |
| A6 | An unclosed Appender leaks | deliberately leave open, watch FD/memory | Confirms the discipline requirement | open |
| A7 | Cross-instance ATTACH of another file works | prerequisite for copyOut | Sec 6.5 must be redesigned | **confirmed by the Sec 17.7 E2E (P8)**: rows copied into and read back from a second real instance with correct lineage |
| A8 | 1M-row append time and file size match Sec 7.5/Sec 7.6 | measure | Estimates must be corrected | open (deferred with perf work) |

**Measurement methodology (for when the deferred run happens):**

- **Fixed sampling point**: sample at the same phase of every rotation - after GC completes, before the next build starts. Random sampling catches build peaks and fakes a leak trend.
- **Skip warmup**: the first 5-10 rotations include JIT, buffer pool fill, and allocator arena growth; judge only the later portion.
- **Compare medians, not maxima**: `(median of last 10 rounds - median of rounds 10-20) / baseline < 5%`, or linear regression slope over the second half < 1 MB/round.
- **Four signals together**:

| Signal | Source | Pass criterion |
|---|---|---|
| FD count | `/proc/self/fd` entry count | **exactly zero growth after warmup - hard, no tolerance** |
| Files in cache dir | filesystem | stable <= K - hard |
| Cache dir total bytes | filesystem | later median ~ earlier median - hard |
| JVM heap after forced GC | MXBean | growth < 5% - hard |
| Process RSS | `/proc/self/status` VmRSS | growth < 5% - interpretive (see below) |

- **RSS caveat**: glibc keeps freed memory in arenas. Set `MALLOC_ARENA_MAX=2` to reduce noise; if RSS alone exceeds the criterion, call `malloc_trim(0)` and re-measure before concluding a leak. FD count is the more sensitive and more decisive signal - unclosed connections/appenders/result sets show up there before RSS moves.
- **JVM-side leak detector (test profile only)**: wrap the connection factory so every issued Connection/Appender is tracked via `PhantomReference` with its creation stack. At test end, assert all tracked objects were closed; print the creation stack of any that weren't. This pinpoints the leaking line instead of leaving a number to investigate.

**Deferred executions** (documented now, run later):

- Full leak regression: 200 rotations x 500k rows, nightly, all criteria above.
- 24-hour soak at production cadence: once before go-live and after major changes.

### 17.7 E2E feasibility test - real DuckDB, synthetic data (mandatory)

One end-to-end test against real DuckDB 1.1.3, with a `SyntheticSource` injected in place of the Oracle-backed one (the `GenerationSource` seam makes this free). It generates a few thousand rows per table and writes them through the **real Appender path** into real generation files. No Oracle involved.

Target: 20-30 rotations, total runtime under ~2 minutes, tagged so it runs in regular CI.

Scenario script (single test, ordered):

1. **Dirty startup** - pre-create leftover `gen_*.db` and `.tmp` files in the cache dir; start the framework; assert the directory is wiped, that `acquire(waitBudget = ZERO)` throws `NotReadyException` immediately, and that an `acquire(waitBudget = 30s)` issued before the first publish returns successfully once gen 1 lands, with `snapshot_acquire_waited_seconds` recorded (Sec 10.1, Sec 9.3).
2. **First generation** - build gen 1 with synthetic rows for `t_a` / `t_b` plus the union view; verify passes; publish; readiness flips. Acquire and query through the union view; assert expected row counts and `source` values. Attempt an INSERT through the handle's connection; assert it is rejected (**A3**).
3. **K enforcement with a held lease** - hold a lease on gen 1; run refreshes until live generations reach K; assert the next refresh records `blocked_by_k` and current data keeps serving; assert the held handle still queries gen 1 with unchanged results (I8). Release the lease; assert GC reclaims, files on disk drop back, and refresh resumes automatically.
4. **DETACH-in-use** - open a raw connection to an old generation, trigger GC; assert reclamation is deferred with a warning (**A4**); close the connection, trigger GC again; assert the generation is detached and **its file is gone from disk** (file-level subset of **A1**).
5. **Failure paths** - one round where the refresher throws mid-build: assert candidate file deleted, current unchanged (I7). One round producing 0 rows: assert `non_empty` rejects it, counter incremented, current unchanged.
6. **copyOut across instances** - copy a subset into a second DuckDB instance via direct file ATTACH (**A7**); assert the result carries the correct `(generation, dataAsOf)` and the lease is released immediately after the copy.
7. **Graceful shutdown** - start a refresh, and while it is mid-build, initiate shutdown with a lease still held. Assert: the candidate `.tmp` is deleted and never promoted, the current pointer is unchanged, an acquire issued during shutdown throws `ShuttingDownException`, a thread already inside `waitBudget` is released immediately rather than serving out its budget, and drain timeout logs the outstanding lease owner before exit (Sec 10.2).
8. **End-of-test resource assertions** - FD count equals the post-warmup baseline; files on disk correspond exactly to live generations; no `.tmp` files remain; if the real storage is wrapped with a recording spy, the Sec 17.3 accounting equations hold.

What this test proves: the full chain - build -> verify -> publish -> serve -> block-at-K -> GC -> delete - actually works on DuckDB 1.1.3, and assumptions A3/A4/A7 plus the file-level part of A1 hold. What it deliberately does **not** prove: absence of slow memory leaks (deferred, Sec 17.6) and performance at production scale (deferred, Sec 16.3).

### 17.8 Definition of Done

- [ ] `GenerationStore`, `Clock`, and the scheduler trigger are injectable; all Sec 17.2-Sec 17.5 tests run without DuckDB/Oracle
- [ ] I1-I8 each have a named test
- [ ] Accounting equations (Sec 17.3) asserted automatically at the end of every test via a shared fixture
- [ ] At least the six deterministic interleavings of Sec 17.4, with zero sleeps; stress test in CI
- [ ] Randomized model test in CI with fixed seed and reproducible failure output
- [ ] Every Sec 9.2 failure case has a test asserting **return to a usable state**, not merely that an error was thrown
- [ ] `acquire()` honors `waitBudget`: zero fails fast without blocking (verified under a 2-thread scheduler pool scenario), a positive budget waits interruptibly and returns on publish, and budget expiry throws with `reason="timeout"` recorded
- [ ] Graceful shutdown sequence (Sec 10.2) verified: waiters released immediately, in-flight refresh aborted with candidate deleted and current unchanged, acquire during shutdown throws `ShuttingDownException`, drain timeout logs outstanding lease owners
- [ ] The Sec 17.7 E2E test passes in CI
- [ ] Admin endpoint (Sec 12.7) correctly reports all live generations and leases (it is the only investigation entry point later, so it is acceptance-relevant)
- [ ] Sec 17.6 assumptions table reviewed: A3/A4/A7 and file-level A1 confirmed by the E2E; remaining items explicitly acknowledged as open risks

A note on authorship of these tests: if the concurrency and accounting tests are delegated to an AI agent, the failure mode is tests that pass without testing anything - sleep-based interleavings go green, and leak tests without the accounting assertions go green. The invariant table (Sec 17.2), the equations (Sec 17.3), and the criteria in this section are therefore fixed by the spec; implementations may vary, assertions may not. Coverage percentage is not an acceptance signal here; invariant verification is.
---

## 18. Archive & Diff Layer (M3, decided 2026-08-28)

### 18.1 Problem and goals

Consuming ETLs mostly process **diffs** ("what changed since the version I last
processed"). The framework persists nothing (D10), so a pod restart destroys all
lineage and forces every ETL back to a full compare. This layer makes the diff
baseline durable.

Scope, confirmed in the design session:

- **In scope:** diff-chain survival across restarts. Hourly cadence. Tables of
  ~1M rows with typical hourly change volume ~100k. Oracle-side change tracking
  (audit columns, CDC) is unavailable by DBA policy, so snapshot comparison is
  the mechanism, not a workaround.
- **Out of scope:** cold-start speed (the first refresh already starts
  immediately, Sec 10.1), serving stale data when Oracle is down, cross-process
  snapshot sharing (Sec 14.2, unaffected), unkeyed tables (D36), and any change
  to startup/shutdown semantics of the framework itself (D10/D11 untouched).

### 18.2 Components

All in `infra.snapshotarchive` (D30), consuming only the public API + JDBI
(caller-land) + a MinIO client:

1. **Archiver** - hourly scheduled run per group (runs for different groups in
   parallel on a bounded executor; runs for the same group serialized - a run
   that finds its group busy skips and logs). Holds one lease for the run.
2. **Manifest** - one Oracle table; versions allocated by an Oracle sequence:

   ```sql
   SNAPSHOT_ARCHIVE_MANIFEST (
     group_id     VARCHAR,
     version      NUMBER,       -- Oracle sequence; PK with group_id
     data_as_of   TIMESTAMP,    -- from the archived snapshot's GenerationInfo
     created_at   TIMESTAMP,
     uri_prefix   VARCHAR,      -- <bucket>/snapshots/<group>/v<version>/
     inventory    CLOB,         -- json: [{table, object_key, bytes, checksum, row_count}]
     status       VARCHAR,      -- PENDING | COMPLETE | FAILED
     generation   NUMBER,       -- diagnostic only, never a key (D31)
     updated_at   TIMESTAMP
   )
   ```

3. **Watchdog** - resolves PENDING rows older than a timeout T against their
   inventory (D33).
4. **Purge job** - deletes expired versions: mark, delete objects per
   inventory, delete row (D34). Two rules found while implementing P13 and
   promoted here from its code: purge never reclaims a PENDING version, and
   reclaims a FAILED one only once it has been FAILED for longer than the
   watchdog timeout T. Both close the same hole - a version whose uploader may
   still be running must keep its row, or that uploader's remaining `put` calls
   land behind a row that no longer covers them, which is exactly the dangling
   object D33 forbids. "Objects before the row" is true without these and still
   not sufficient.
5. **ETL diff helper** - manifest lookup, checkpoint download, PK diff vs the
   live snapshot, fallback decision (D32/D35).

### 18.3 Archiver run (publish protocol, D33)

1. `acquire(group)`; export each table to a local temp dir as Parquet
   (per-table tasks in parallel); compute the inventory (keys, checksums,
   sizes, row counts).
2. Refuse to publish (skip + alert) if `data_as_of` is not strictly greater
   than the newest COMPLETE version's (D31 monotonicity guard).
3. `INSERT` manifest row `status=PENDING` with the inventory; commit.
4. Upload all objects under `v<version>/`; verify against the inventory.
5. Conditional `UPDATE ... SET status='COMPLETE' WHERE status='PENDING'`;
   commit. Release the lease; delete the temp dir.

First run ever, or after a FAILED gap: identical - a version is
self-contained; there is no chain to stitch.

**Shutdown:** stop scheduling, interrupt in-flight runs, release the lease
within the framework's bounded drain, delete temp files. A leftover PENDING
row is deliberately NOT cleaned at shutdown - the watchdog resolves it, so
crash and graceful shutdown converge on one recovery path (D33), mirroring the
framework's own "no delicate cleanup" stance (Sec 10.2).

### 18.4 ETL protocol (D32/D35)

Per ETL run:

1. Acquire the live snapshot; note `dataAsOf = T`.
2. Look up `watermark` (the version this ETL recorded last run) in the
   manifest. If COMPLETE: download that one checkpoint, `FULL OUTER JOIN` on
   PK vs the live snapshot in local DuckDB, emitting
   `(pk, op IN (I,U,D), changed_columns, current values)`; apply.
3. Fallback (watermark absent / purged / FAILED): full compare against the
   live snapshot - the same anti-join shape that answers "which snapshot rows
   are missing from my tables", which needs nothing from this layer.
4. Set `watermark = max(version) WHERE status='COMPLETE' AND data_as_of <= T`,
   committed in the same transaction as the ETL's output.

**The correctness rule that must never be weakened:** the baseline checkpoint
must have been taken at or before the ETL's last processed moment. Hence the
watermark is a recorded version, never "the latest checkpoint now" (taken
after the last run; using it silently under-reports). Over-reporting is
bounded by one archive interval and is safe (D25).

Under-reporting is impossible for any column that moves in one direction, which
is what this rule buys. It is **not** impossible in general: a value that
returns to the exact value its baseline holds, before a newer checkpoint is
published, is not reported at all. That is a real hole, found while building the
P14 property test, and it is stated in full as open item 18.6 #4 - read it
before relying on this paragraph.

This is the standard single-baseline incremental sync with full-resync
fallback (cf. ZFS incremental send: the common ancestor snapshot must exist,
otherwise full send).

### 18.5 Retention (D34)

Fixed window >= slowest ETL cadence + margin (config; e.g. 24-48h), plus
unconditional keep-newest-COMPLETE. A PENDING version is never reclaimed, and a
FAILED one only after it has been FAILED for longer than the watchdog timeout T
(see 18.2 item 4 for why); FAILED versions are reclaimed on that timeout rather
than on the retention window, so a broken uploader cannot park a window's worth
of unreadable objects. Not "keep latest only": an ETL slower than
the archive cadence would full-compare every run. Optional alert when the
newest COMPLETE checkpoint's age exceeds a threshold (archiver broken) -
purely operational; diffs stay correct (over-report only).

### 18.6 Open items (originally: before M3 implementation)

1. **CLOSED 2026-08-29 (spike, ticket 01).** DuckDB 1.1.3 runs
   `COPY (SELECT ...) TO '<file>.parquet'` directly on a read-only attached
   snapshot connection. Export therefore streams from the serving instance; the
   `copyOut` staging fallback is not needed and is not built. Writing a file out
   of a READ_ONLY attached database is not a write into it, and the attach stays
   read-only afterwards (A3 verified in the same spike). The inventory's
   `row_count` must come from a `COUNT(*)`, not from `COPY`'s own update count:
   1.1.3 does report one, but an empty table and a driver that stopped
   classifying `COPY` as DML both yield 0 and nothing downstream can tell them
   apart - and that value is committed into the PENDING manifest row that the
   watchdog later verifies a real object against.
2. **CLOSED 2026-08-29 (spike, ticket 01).** 1M rows exported to 14.2 MB in
   ~40 ms on the pinned 1.1.3 (three runs, byte-identical). Size matches the
   "tens of MB" expectation; duration beats "seconds" by two orders of
   magnitude, so a lease held across an export cannot interact with the K
   ceiling in any way worth designing around.
3. **CONFIG TO TUNE, not a gate** (re-scoped 2026-08-29 on the user's ruling;
   originally filed as an open item). T is the watchdog timeout and is set per
   deployment like any other operational value - no code waits on it, and a
   wrong value cannot corrupt a version. Too high only delays a repair nothing
   downstream can observe, since readers already ignore anything that is not
   COMPLETE. Too low makes the watchdog inspect a row whose upload is still
   running, find objects missing and mark it FAILED - and the uploader's
   conditional `markComplete` then returns false, because the transitions
   guarantee exactly one winner (D33). The cost is one wasted archive cycle,
   which the next run replaces. That bounded, one-sided failure is what makes
   this config rather than a blocker.

   What remains genuinely unmeasured is the number itself: worst-case upload
   time on the real MinIO link. There is no such link on this machine,
   only a MinIO container on loopback: 14,180,166 bytes uploaded in
   1170/840/502/546/217 ms over five runs, which measures a local socket and a
   container filesystem, not a network. That is a floor and nothing more, so
   the item stays open.

   Ticket 04 ships **T = 15 minutes as a policy floor, not a derived value**,
   with the rationale on `ArchiveMaintenance.DEFAULT_WATCHDOG_TIMEOUT`. The
   cost function is one-sided the way D22's `waitBudget` is: too low throws
   away checkpoints that were seconds from publishing (correct, but wasteful
   and invisible), too high only delays a repair that nothing downstream can
   observe, since readers already ignore anything that is not COMPLETE. A
   one-sided cost gets headroom rather than an estimate. 15 minutes is four
   times under the hourly cadence, so a stale row is always resolved before the
   next run for its group, and three orders of magnitude above the loopback
   number. Tighten it by measuring the deployment's real link when there is one;
   until then it is the margin, not the estimate, that makes it safe.
4. **NEW, OPEN 2026-08-29 (ticket 05).** The "under-reporting is impossible by
   construction" claim in Sec 18.4 has one exception, found while building the
   P14 property test, and it is recorded here rather than argued away.

   A consumer applies the *live* snapshot's values, so its target is newer than
   any baseline it can record: the watermark is `data_as_of <= T`, and only by
   luck is there a checkpoint at exactly T. The diff therefore answers "how do I
   get from the baseline to live", while the target sits somewhere between them.
   For every column that moves in one direction that is a superset and only
   over-reports, as claimed. For a column that returns to the exact value the
   baseline holds, before any newer checkpoint is published, it is not:

   - 10:00 checkpoint v1, `balance = 100`. ETL run applies it; watermark = v1.
   - 10:20 `balance = 200`. ETL run diffs v1 vs live, applies 200. Still no newer
     checkpoint, so the watermark stays v1.
   - 10:30 `balance = 100` again. ETL run diffs v1 (100) vs live (100), finds
     nothing, and leaves 200 in its target - permanently, since every later
     checkpoint also reads 100.

   The window is one archive interval, and the shape needed is a value that
   returns to a previous value inside it - a status flag toggling back, not a
   monotone counter or an audit timestamp. `EtlDiffTest` pins the behaviour in
   `a value that returns to its baseline inside one archive interval is not
   reported` so it is a known boundary rather than a surprise.

   Nothing in the helper can close it: the helper reports exactly what separates
   the two states it is given. Closing it means changing what a consumer records
   or what the archiver publishes - e.g. publishing a checkpoint per refresh so a
   baseline at exactly T always exists, or having consumers diff checkpoint to
   checkpoint and accept the lag. Neither is in M3's scope; the item is open so
   the choice is made deliberately rather than discovered in production.
