---
name: fenced-leader-election
description: |
  Implement the Fenced Leader Election pattern using Kotlin, Quarkus, and Kubernetes.
  Use this skill whenever the user needs: leader election in K8s, fencing tokens for
  zombie prevention, exactly-once task execution across replicated pods, distributed
  locking with database-level safety, or preventing split-brain data corruption.
  Also trigger when the user mentions: K8s Lease, resourceVersion as epoch, fabric8
  LeaderElector, SELECT FOR UPDATE with leader guards, GC-pause protection, or
  "zombie leader". This skill covers the full stack from RBAC to DB fence.
---

# Fenced Leader Election

## The Problem

Multiple pods compete for leadership to run critical jobs (schedulers, pollers,
outbox processors). A standard K8s Lease gives you leader election, but it does NOT
prevent **zombie leaders** — pods that lost their lease during a GC pause or network
partition but still think they're leading. When they wake up, they write stale data
and corrupt what the new leader has already done.

Leader election alone is necessary but not sufficient. The database must also
participate in rejecting stale writes.

## The Pattern: Fencing Tokens

Three independent layers, each sufficient on its own against zombies:

1. **K8s Lease** — the lock. Use fabric8's built-in `LeaderElector`, not a
   hand-rolled implementation. See `references/leader-lifecycle.md`.

2. **resourceVersion as Epoch** — the token. Every Lease mutation in etcd
   increments `metadata.resourceVersion`. Read it after each leadership
   transition and treat it as a strictly increasing fencing epoch.

3. **Database Fence** — the final gate. Every write includes the epoch in
   both SET and WHERE. A zombie with a stale epoch gets 0 rows affected,
   and the application throws immediately.

Read `references/pattern.md` for the theory behind why this works and what
invariants must hold.

## Architecture Overview

```
Scheduler tick (every N seconds)
  └─► CDI Interceptor (@FencedLeader)
        ├── PRE-CHECK:   is this pod the leader?
        ├── PROPAGATE:   epoch → ThreadLocal + CoroutineContext
        ├── EXECUTE:     business logic + fenced repository calls
        └── POST-CHECK:  still leader? epoch unchanged?

Repository layer (FencedRepository base)
  └─► SQL:  UPDATE ... SET last_epoch = :epoch
            WHERE id = :id AND last_epoch <= :epoch
            → 0 rows = StaleEpochException (zombie caught)
```

The interceptor is defense-in-depth. Even if the pre/post checks race with a
leadership transition, the DB fence is the authoritative gate.

## Component Map

Read each reference file for the concepts and design rationale behind that layer.

| Layer | Reference | Core Concept |
|-------|-----------|-------------|
| Why fencing works | `references/pattern.md` | Fencing token theory, invariants, safety proof |
| Leader lifecycle | `references/leader-lifecycle.md` | fabric8 LeaderElector behavior, epoch extraction, threading |
| Token propagation | `references/token-propagation.md` | Dual-channel design: ThreadLocal + CoroutineContext |
| Database fence | `references/db-fence.md` | SQL invariant, correct vs incorrect fence, edge cases |
| Failure scenarios | `references/failure-modes.md` | GC pause, API down, network split, pod crash — per-layer response |
| Operations | `references/operational.md` | RBAC, health probes, metrics, deployment, timing tuning |

## Implementation Checklist

1. **K8s RBAC** — ServiceAccount + Role granting `leases` verbs (get, list, watch,
   create, update, patch) + RoleBinding. Without this, the pod can't touch Lease objects.

2. **DB migration** — Add a `last_epoch` column (BIGINT/NUMBER(19), default 0, NOT NULL)
   to every table that receives leader-only writes.

3. **Config** — SmallRye `@ConfigMapping` with lease-name, namespace, identity (defaults
   to HOSTNAME = pod name), and the three timing knobs.

4. **LeaderManager** — `@ApplicationScoped` bean wrapping fabric8's `LeaderElector`.
   Exposes `isActive(): Boolean` and `getToken(): Long`. Runs the election in a
   dedicated daemon thread with a restart loop.

5. **FencingContext / FencingTokenHolder** — Dual-channel token propagation. The
   interceptor sets both; downstream code reads whichever fits (sync vs suspend).

6. **@FencedLeader interceptor** — CDI `@AroundInvoke`. Pre-check → propagate →
   proceed → post-check. Throws `NotLeaderException` on failure.

7. **FencedRepository base class** — Provides `fencedUpdate()` / `fencedBatch()`.
   Injects epoch from ThreadLocal, checks affected rows, throws `StaleEpochException`
   on 0 rows.

8. **Health checks** — Liveness: election thread alive? Readiness: is leader?

9. **Scheduled job** — `@Scheduled` calls a `@FencedLeader` method. Catch
   `NotLeaderException` silently on follower pods.

## Common Mistakes

1. **Forgetting the WHERE fence** — `SET last_epoch = :epoch` alone doesn't prevent
   stale writes. The `WHERE last_epoch <= :epoch` is the actual gate.

2. **Using strict less-than** (`< :epoch`) — This prevents same-epoch re-processing.
   Use `<=` unless you specifically want at-most-once per epoch.

3. **Not catching NotLeaderException in scheduler** — Follower pods call `tick()` too.
   The exception is expected, not an error.

4. **Hand-rolling lease logic** — fabric8's `LeaderElector` handles create, renew,
   conflict (409), expiry, and step-down. Don't reimplement it.

5. **Caching the epoch outside the fenced block** — Always read from the propagation
   channel (ThreadLocal/CoroutineContext) inside the fenced method. Never capture it
   in a field.

## When NOT to Use

- Leader work is fully idempotent and harmless to repeat → simpler K8s Job or
  `@IfBuildProfile("leader")` suffices.
- Single-replica deployment → no election needed.
- Write-free leader work (read-only aggregation) → standard leader election without
  fencing is fine.


# Fencing Token Pattern

## Origin

The pattern comes from distributed systems literature (Chubby, ZooKeeper,
Martin Kleppmann's "Designing Data-Intensive Applications" Ch. 8). The core
insight: a lock alone cannot prevent a process that *held* the lock from
acting after it *lost* the lock, because the process doesn't know it lost
the lock until it tries to renew.

## The Zombie Leader Problem

Timeline of a GC pause disaster without fencing:

```
Time    Pod-A (leader)         Pod-B (follower)       K8s Lease
─────   ──────────────         ────────────────       ─────────
T=0     Holds lease            Standing by            holder=A, rv=100
T=1     Starts processing
T=2     ── GC PAUSE ──
T=3     (frozen)               Lease expired
T=4     (frozen)               Acquires lease         holder=B, rv=101
T=5     (frozen)               Writes order #42
T=6     Wakes up!
T=7     Writes order #42 ← CORRUPTION (overwrites B's work)
```

Pod-A's write at T=7 is the problem. It still holds a stale reference
to its "leadership" and has no way to know that B has already processed
order #42.

## How Fencing Solves It

Attach a **monotonically increasing token** to every leadership transition.
The resource (database) tracks the highest token it has seen and rejects
any write carrying a lower token.

Same timeline WITH fencing:

```
Time    Pod-A (epoch=100)      Pod-B (epoch=101)      Database
─────   ──────────────         ────────────────       ────────
T=0     Holds lease            Standing by            last_epoch=100
T=5     (frozen)               Writes #42 (101)       last_epoch=101
T=7     Wakes, writes #42
        WHERE last_epoch<=100  ← 0 rows! (101 > 100)
        StaleEpochException    ← zombie caught
```

## Safety Invariant

The fence is safe if and only if these two properties hold:

1. **Monotonicity**: Every new leader gets a token strictly greater than
   all previous leaders. In K8s, `resourceVersion` is incremented by etcd
   on every Lease mutation, guaranteeing this.

2. **Compare-and-guard**: The database rejects writes where the presented
   token is less than the stored token. The SQL `WHERE last_epoch <= :epoch`
   encodes this — it succeeds only if no higher epoch has written to that row.

Together, these ensure that **at most one leader's writes are accepted for
any given row at any point in time**.

## Why Three Layers

Each layer catches zombies at a different point in the timeline:

| Layer | When it catches the zombie | Latency |
|-------|---------------------------|---------|
| Pre-check (interceptor) | Before any work begins | Immediate |
| Post-check (interceptor) | After work completes, before returning | Immediate |
| DB fence (SQL WHERE) | At the moment of each write | Atomic |

The pre/post checks are optimizations — they avoid wasted work. The DB fence
is the **only layer that provides actual safety**, because it's the only one
that's atomic with the data mutation. The interceptor checks can race with
leadership transitions; the SQL WHERE cannot.

## Token Source: Why resourceVersion

K8s Lease's `metadata.resourceVersion` is ideal because:

- **Strictly increasing** in etcd-backed clusters (it's the etcd MVCC revision).
- **Automatically bumped** on every Lease update (acquire, renew).
- **Free** — no extra infrastructure needed beyond the Lease itself.
- **Consistent** — read from the API server right after the leadership callback.

Caveat: `resourceVersion` is documented as "opaque" and Kubernetes doesn't
formally guarantee it's numeric. In practice, every etcd-backed cluster
(which is all production K8s) uses a monotonic integer. Parse with a
`toLongOrNull()` fallback for safety.

## Scope of the Fence

The epoch is scoped to **one Lease object**. If your application uses multiple
independent leader elections (e.g., one for order processing, one for
notifications), each has its own Lease and its own epoch sequence. The
`last_epoch` columns in different tables are fenced by different leases —
they are NOT comparable across leases.


# Leader Lifecycle

## Use fabric8's LeaderElector

Do NOT hand-roll lease acquisition and renewal logic. The fabric8 kubernetes-client
ships a production-grade leader election implementation in the package
`io.fabric8.kubernetes.client.extended.leaderelection`. It handles:

- Lease creation (if it doesn't exist).
- Optimistic acquire (409 Conflict on race → back off and retry).
- Periodic renewal within the lease duration.
- Step-down when renewal fails past the renew deadline.
- Callback notification for start-leading, stop-leading, and new-leader events.

## Key Classes

- **`LeaseLock`** — Resource lock backed by a `coordination.k8s.io/v1` Lease object.
  Constructor takes namespace, lease name, and identity (use pod name / HOSTNAME).

- **`LeaderElectionConfigBuilder`** — Configures timings and callbacks.
  Required settings: lock, leaseDuration, renewDeadline, retryPeriod, leaderCallbacks.

- **`LeaderCallbacks`** — Three callbacks:
    - `onStartLeading()` — you won the election.
    - `onStopLeading()` — you lost the lease (renewal failed, deadline exceeded).
    - `onNewLeader(identity: String)` — any leader change (including yourself).

- **`LeaderElector`** — Built from config. Call `run()` to enter the election loop.

## Threading Model

`LeaderElector.run()` **blocks** the calling thread. It loops internally:
try-acquire → hold-and-renew → lose → return. When it returns, the pod is
no longer leading.

Design implications:

1. **Dedicated thread**: Run the election in a single-threaded `ExecutorService`
   with a daemon thread. Do not use the Quarkus worker pool.

2. **Restart loop**: After `run()` returns (leadership lost), sleep for one
   `retryPeriod`, then call `run()` again to re-enter the election. This ensures
   the pod automatically tries to re-acquire after losing.

3. **Shutdown**: On `@ShutdownEvent`, call `executor.shutdownNow()` to interrupt
   the election thread. The `run()` method respects interruption.

## Timing Parameters

```
 leaseDuration (15s)
├───────────────────────────────┤
 renewDeadline (10s)
├──────────────────────┤
 retryPeriod (2s)
├──┤  ├──┤  ├──┤  ├──┤
```

- **leaseDuration** (default 15s): How long the Lease is valid. If the leader
  doesn't renew within this window, other pods can acquire it.

- **renewDeadline** (default 10s): Maximum time the leader retries renewal
  before giving up and calling `onStopLeading()`. Must be < leaseDuration.

- **retryPeriod** (default 2s): How often the election loop attempts to
  acquire or renew. Applies to both leaders and followers.

Tuning rules of thumb:
- `leaseDuration` should be > GC pause budget of your JVM.
- `renewDeadline` should be ~2/3 of `leaseDuration`.
- `retryPeriod` should be ~1/5 to 1/7 of `leaseDuration`.
- Lower values = faster failover, more K8s API load.

## Extracting the Fencing Epoch

fabric8's `LeaderElector` does NOT expose the Lease's `resourceVersion`
through its API. This is the one thing we add on top.

After `onStartLeading()` fires, make a separate API call to read the
Lease object:

```
k8s.leases().inNamespace(ns).withName(leaseName).get()
  → lease.metadata.resourceVersion
  → parse to Long (toLongOrNull with hashCode fallback)
  → store in AtomicLong
```

Also refresh in `onNewLeader()` when the new leader is ourselves (covers
the renewal case where resourceVersion bumps).

## State Management

The LeaderManager exposes state to the interceptor and health checks via
lock-free atomics:

- `AtomicBoolean` for `isLeader` — read by interceptor pre-check.
- `AtomicLong` for `epoch` — read by interceptor for token propagation.
- `AtomicReference<Instant>` for `acquiredAt` / `renewedAt` — read by health.
- `@Volatile Instant` for `lastHeartbeat` — read by liveness probe.

No synchronized blocks needed. The callbacks run on the election thread;
the readers are on Quarkus worker threads and the health probe thread.
AtomicBoolean/AtomicLong give visibility guarantees without contention.

## Lifecycle in Quarkus

- Start: observe `@StartupEvent` → create LeaseLock, build config, submit
  election loop to executor.
- Stop: observe `@ShutdownEvent` → set leader=false, shutdownNow() the executor.

The `@ApplicationScoped` bean is created eagerly by Quarkus because it
observes `StartupEvent`.


# Token Propagation

## The Problem

The interceptor knows the fencing epoch. The repository needs it for SQL
writes. These are separated by multiple layers of business logic. How does
the token travel from interceptor to repository without polluting every
method signature with an `epoch: Long` parameter?

## Dual-Channel Design

Two propagation channels cover both calling conventions in a Kotlin/Quarkus app:

### Channel 1: ThreadLocal (for synchronous JDBI)

A `FencingTokenHolder` object wrapping a `ThreadLocal<Long>`.

- **Set by**: the interceptor, before `ctx.proceed()`.
- **Read by**: `FencingRepository.fencedUpdate()` via `FencingTokenHolder.require()`.
- **Cleared by**: the interceptor, in a `finally` block after `ctx.proceed()`.

Why ThreadLocal works: JDBI's `withHandle` executes on the calling thread.
As long as the interceptor sets the token before proceeding and the repository
reads it within the same call stack, the value is available.

Key methods:
- `set(epoch)` / `clear()` — raw access.
- `require(): Long` — returns epoch or throws if not set (fail-fast for
  programming errors).
- `get(): Long?` — nullable variant for optional fencing.
- `withToken(epoch) { ... }` — sets, executes block, clears in finally.
  This is what the interceptor should use.

### Channel 2: CoroutineContext Element (for suspend functions)

A `FencingContext` class extending `AbstractCoroutineContextElement`.

- **Set by**: the interceptor, wrapping `ctx.proceed()` in
  `runBlocking(FencingContext(epoch)) { ... }`.
- **Read by**: any suspend function via `FencingContext.current()`.
- **Lifetime**: scoped to the coroutine — no manual cleanup needed.

Why a CoroutineContext element: if business logic uses `suspend` functions
or launches child coroutines, ThreadLocal won't propagate across suspension
points. A CoroutineContext element travels with the coroutine automatically.

Key design:
- Companion object implements `CoroutineContext.Key<FencingContext>` for
  type-safe lookup.
- `current()` is a `suspend` function that reads from `coroutineContext[Key]`
  and throws if absent.

## Why Both Channels?

| Scenario | ThreadLocal | CoroutineContext |
|----------|-------------|-----------------|
| Synchronous JDBI call | ✅ works | ❌ not in a coroutine |
| Suspend function chain | ❌ lost at suspension | ✅ propagated |
| `runBlocking` bridge | ✅ same thread | ✅ in scope |
| `withContext(Dispatchers.IO)` | ❌ different thread | ✅ propagated |

The interceptor sets BOTH, so downstream code picks whichever fits. In
practice, most Quarkus/JDBI repository code is synchronous and uses the
ThreadLocal path. The coroutine path is there for reactive or Flow-based
processing pipelines.

## Interceptor Wiring (Conceptual)

The `@FencedLeader` interceptor's `@AroundInvoke` method does this:

1. Pre-check `manager.isActive()` → throw `NotLeaderException` if false.
2. Read `epoch = manager.getToken()`.
3. Execute the method body inside `FencingTokenHolder.withToken(epoch)` AND
   `runBlocking(FencingContext(epoch))`.
4. Post-check `manager.isActive()` → throw if leadership was lost during execution.
5. Post-check `manager.getToken() == epoch` → warn if epoch changed (but don't
   throw, because the DB fence already protected individual writes).

The post-check detects GC pauses: if the JVM was frozen for 15+ seconds, the
Lease expired, a new leader was elected, and `isActive()` is now false. The
interceptor discards the result and throws, preventing the caller from trusting
stale output. But the real safety comes from the DB fence on each individual
write — the post-check is a courtesy.

## Anti-Patterns

- **Passing epoch as a method parameter** — Pollutes every interface. The
  ThreadLocal/CoroutineContext approach keeps it invisible to business logic.

- **Storing epoch in a CDI RequestScoped bean** — Works for JAX-RS requests
  but not for `@Scheduled` methods, which have no request scope.

- **Using MDC** — Technically works (it's a ThreadLocal), but MDC is for
  logging context, not application logic. Semantic mismatch.

- **Forgetting to clear the ThreadLocal** — If `ctx.proceed()` throws, the
  ThreadLocal must still be cleared. Always use `withToken()` which has a
  `finally` block, or wrap in try/finally manually.

# Database Fence

## The Invariant

Every leader-only write must satisfy:

> The row is modified **only if** the presented epoch is ≥ the epoch
> already stored in the row.

This is encoded in SQL as two parts that must ALWAYS appear together:

- **SET**: `last_epoch = :epoch` — stamps the row with the writer's epoch.
- **WHERE**: `AND last_epoch <= :epoch` — gates the write on epoch freshness.

The SET without the WHERE is useless (stamps but doesn't guard). The WHERE
without the SET is useless (guards once but doesn't update the watermark
for future guards).

## Column Design

- **Name**: `last_epoch` (or any consistent name across tables).
- **Type**: `NUMBER(19)` (Oracle), `BIGINT` (PostgreSQL/DB2).
- **Default**: `0` — allows the first leader (any epoch > 0) to write.
- **Nullable**: `NOT NULL` — a null epoch is a bug.
- **Index**: NOT needed. The column is only checked in row-level UPDATEs
  that already locate the row by primary key. The PK index does the work.

## Correct SQL Patterns

### Single-row update

```
UPDATE orders
SET status     = :status,
    updated_at = SYSTIMESTAMP,
    last_epoch = :epoch
WHERE id         = :id
  AND last_epoch <= :epoch
```

### Conditional claim (status guard + fence)

```
UPDATE orders
SET status     = 'PROCESSING',
    claimed_by = :claimedBy,
    last_epoch = :epoch
WHERE id         = :id
  AND status     = 'PENDING'
  AND last_epoch <= :epoch
```

When combining with other WHERE conditions (status checks, optimistic locking),
the fence is an additional AND — it doesn't replace existing guards.

### Fenced INSERT (upsert pattern)

For Oracle MERGE or PostgreSQL ON CONFLICT, include the epoch in both
the INSERT values and the UPDATE SET/WHERE:

```
MERGE INTO results r
USING (SELECT :id AS id FROM dual) s ON (r.id = s.id)
WHEN MATCHED THEN
  UPDATE SET value = :value, last_epoch = :epoch
  WHERE r.last_epoch <= :epoch
WHEN NOT MATCHED THEN
  INSERT (id, value, last_epoch) VALUES (:id, :value, :epoch)
```

## Incorrect Patterns

### Missing WHERE fence

```
-- ❌ WRONG: stamps epoch but doesn't guard against stale writers
UPDATE orders SET status = :status, last_epoch = :epoch WHERE id = :id
```

A zombie with epoch=95 overwrites a row that epoch=101 already processed.

### Strict less-than

```
-- ⚠️ CAREFUL: prevents same-epoch re-processing
WHERE last_epoch < :epoch
```

With `<` (strict), the leader cannot re-process a row it already touched
in the current epoch. This is correct for at-most-once semantics but breaks
retry logic where the leader needs to re-attempt after a transient failure
within the same epoch. Use `<=` unless you specifically want at-most-once.

### Fence in application code instead of SQL

```
-- ❌ WRONG: checking in app code then writing is a TOCTOU race
if (row.lastEpoch <= myEpoch) {
    UPDATE orders SET status = :status WHERE id = :id
}
```

Between the read and the write, a new leader could have committed. The
check and the write must be **atomic** — which means they must be in the
same SQL statement.

## Affected Rows = The Signal

After executing a fenced UPDATE, check the affected row count:

- **1 (or more)**: Write succeeded. The epoch was fresh.
- **0**: Write rejected. Either the row doesn't exist, or a higher epoch
  already wrote to it. In the fencing context, treat 0 as
  `StaleEpochException` — the zombie is caught.

The repository base class (`FencedRepository`) should encapsulate this
check. Provide two helpers:

- `fencedUpdate(sql, binder)` — executes one fenced statement, reads epoch
  from ThreadLocal, throws StaleEpochException on 0 rows.
- `fencedBatch(items, sql, binder)` — iterates items, fails fast on first
  0-row result.

## Reads Don't Need Fencing

SELECT queries are safe without the epoch. A zombie reading stale data
and then trying to write will be caught by the fence. The read itself
causes no corruption. Don't add `WHERE last_epoch <= :epoch` to SELECTs —
it would filter out rows the current leader needs to see.

## Multi-Table Consistency

If a fenced operation writes to multiple tables, each table has its own
`last_epoch` column, and each UPDATE carries the same epoch. If the leader
loses its lease mid-transaction, some tables may be stamped and others not.
Options:

1. **Wrap in a DB transaction** — all-or-nothing. If any fenced write
   throws StaleEpochException, roll back the entire transaction.

2. **Idempotent per-row** — design each row's update to be idempotent
   so partial application is harmless on retry by the new leader.

Option 1 is simpler and recommended unless the transaction would be
very long-lived.

## Migration Strategy for Existing Tables

When adding fencing to tables that already have data:

1. Add `last_epoch` with DEFAULT 0 — existing rows get epoch=0.
2. The first leader (any epoch > 0) can immediately write to all rows
   because `0 <= any_positive_epoch` is always true.
3. No backfill needed.

# Failure Modes

## How to Read This Document

Each scenario describes what goes wrong, what each layer does in response,
and the net outcome. The key takeaway: no single layer is sufficient alone,
but the DB fence is the only layer that provides **hard safety**. The other
layers are optimizations that reduce wasted work.

---

## Scenario 1: GC Pause > leaseDuration

**What happens**: The leader JVM freezes for a full GC pause longer than
the lease duration (default 15s). During the pause, the lease expires
and another pod acquires it with a higher resourceVersion.

**Layer responses**:

| Layer | Response |
|-------|----------|
| K8s Lease | Expires. fabric8's renewal loop can't run during GC. |
| fabric8 LeaderElector | When the JVM unfreezes, the next renewal attempt fails → `onStopLeading()` fires → `isActive()` returns false. |
| Interceptor pre-check | If the scheduler ticks after `onStopLeading()`, the pre-check catches it. But if the method was **already executing** when GC started, the pre-check already passed. |
| Interceptor post-check | After the method returns, checks `isActive()`. If leadership was lost during execution, throws `NotLeaderException`. |
| DB fence | The zombie's epoch (e.g., 100) is lower than the new leader's epoch (e.g., 101). `WHERE last_epoch <= 100` fails because the new leader already stamped the row with 101. **Write rejected.** |

**Net outcome**: Safe. The zombie's writes are rejected at the DB level even
if the interceptor checks race. The worst case is wasted computation (the
zombie does work that gets discarded).

---

## Scenario 2: K8s API Server Down

**What happens**: The K8s API server becomes unreachable. The leader can't
renew its lease. Followers can't acquire it.

**Layer responses**:

| Layer | Response |
|-------|----------|
| K8s Lease | Frozen — no renewals or acquisitions possible. |
| fabric8 LeaderElector | Leader retries renewal for `renewDeadline` (10s). After exhausting retries, calls `onStopLeading()` and `run()` returns. |
| LeaderManager | `isActive()` becomes false. The restart loop tries to re-enter the election, but `run()` will throw/fail because the API is down. |
| Liveness probe | `lastHeartbeat` stops updating. After `renewDeadline + retryPeriod` seconds without a heartbeat, liveness fails → K8s restarts the pod. |
| DB fence | If the ex-leader tries to write before stepping down, the fence still works — its epoch is valid as long as no new leader has written. But no new leader CAN write (API is down, so no new epoch). Writes may succeed with a "stale" epoch that happens to still be the highest. |

**Net outcome**: The leader steps down conservatively even though no new leader
exists. This means a **leadership gap** — no pod is leading until the API
recovers. This is by design: it's safer to do nothing than to risk two leaders.

---

## Scenario 3: Network Partition (Split Brain)

**What happens**: A network partition separates Pod-A (current leader) from
the K8s API server, but Pod-A can still reach the database. Pod-B can reach
the API server and acquires the lease.

**Layer responses**:

| Layer | Response |
|-------|----------|
| K8s Lease | Pod-A can't renew → lease expires. Pod-B acquires → new resourceVersion. |
| Pod-A | fabric8 calls `onStopLeading()` after renewDeadline. But there's a window of up to `leaseDuration` where Pod-A still thinks it's leading (it hasn't failed renewal yet). |
| Pod-B | `onStartLeading()` fires. Starts working with a higher epoch. |
| DB fence | Both pods may write concurrently during the partition window (<15s). Only the writes with the higher epoch (Pod-B) will "stick". Pod-A's writes to rows that Pod-B has already touched will get 0 rows affected. Pod-A's writes to rows Pod-B hasn't touched yet will succeed (last_epoch is still ≤ Pod-A's epoch). |

**Net outcome**: Safe at the row level, but there may be a brief window where
both pods process different rows. This is acceptable if each row's processing
is independent. If you need strict total ordering across all rows, you need
additional coordination (e.g., a serial queue table with its own fence).

**Duration of overlap**: At most `leaseDuration` seconds. In practice, fabric8
detects renewal failure within `renewDeadline` (10s), and the overlap is
typically under 5 seconds.

---

## Scenario 4: Pod Crash

**What happens**: The leader pod crashes (OOM kill, segfault, node failure).

**Layer responses**:

| Layer | Response |
|-------|----------|
| K8s Lease | The lease has a remaining TTL of up to `leaseDuration`. |
| Followers | Cannot acquire until the lease expires. Election resumes after `leaseDuration`. |
| DB fence | Not relevant — the crashed pod isn't running. No zombie writes. |

**Net outcome**: Safe, but there's a leadership gap of up to `leaseDuration`
(15s) while the lease expires. Reduce `leaseDuration` for faster failover at
the cost of more frequent API calls.

---

## Scenario 5: Interceptor Post-Check Race

**What happens**: The leader finishes executing a fenced method. Between the
last DB write and the interceptor's post-check, leadership transitions to
a new pod.

**Layer responses**:

| Layer | Response |
|-------|----------|
| Interceptor post-check | Detects `isActive()` is false. Throws `NotLeaderException`. |
| DB fence | All writes already committed with the (at-the-time valid) epoch. |

**Net outcome**: The writes are already committed and valid. The post-check
throwing just means the caller won't see a successful return. This is a
**false alarm** — the work was done correctly. The caller (scheduler) should
treat this as a retryable failure. The new leader may re-process the same
items, which is fine if the fenced writes made them non-retryable (e.g.,
status changed from PENDING to PROCESSED).

---

## Scenario 6: Database Unreachable

**What happens**: The database goes down while the leader is processing.

**Layer responses**:

| Layer | Response |
|-------|----------|
| K8s Lease | Unaffected — lease renewal continues. |
| Leader | JDBI throws. Business logic fails. |
| DB fence | Not relevant — can't write at all. |

**Net outcome**: The leader stays leader (lease is fine) but can't do work.
The scheduler will retry on the next tick. If the DB is down for a long time,
the liveness probe still passes (election loop is healthy). The readiness
probe may need custom logic if you want to stop routing traffic during DB
outages.

---

## Summary Table

| Scenario | Overlap Window | Data Safety | Leadership Gap |
|----------|---------------|-------------|----------------|
| GC Pause > 15s | None (zombie wakes after new leader) | ✅ DB fence rejects | Brief (~retryPeriod) |
| K8s API Down | None (everyone steps down) | ✅ No new leader to conflict | Until API recovers |
| Network Split | Up to leaseDuration | ✅ Per-row fence | None (both think they lead) |
| Pod Crash | None | ✅ Crashed pod can't write | Up to leaseDuration |
| Post-Check Race | None | ✅ Writes already committed | None |
| DB Down | None | ✅ No writes possible | None (leader is healthy) |


# Operational Reference

## K8s RBAC

The pod's ServiceAccount needs a Role granting these verbs on Lease resources:

- `get`, `list`, `watch` — observe current lease state.
- `create` — first pod creates the Lease object.
- `update`, `patch` — acquire and renew the lease.

Scope this to a single namespace with a Role + RoleBinding (not ClusterRole)
to follow least-privilege. The Lease object lives in the `coordination.k8s.io`
API group.

Deployment must set `serviceAccountName` on the pod spec.

## Health Probes

### Liveness: Is the Election Loop Alive?

Purpose: detect a hung election thread (deadlock, infinite loop, thread death).

Logic: the LeaderManager updates a `lastHeartbeat` timestamp on every
`onNewLeader` callback and at the start of each `run()` loop iteration.
The liveness check compares `now - lastHeartbeat` against a threshold of
`renewDeadline + retryPeriod` (12s default). If exceeded → unhealthy →
K8s restarts the pod.

K8s probe config:
- Path: `/q/health/live`
- `initialDelaySeconds`: 15 (allow first election cycle)
- `periodSeconds`: 10
- `timeoutSeconds`: 5
- `failureThreshold`: 3 (restart after 3 consecutive failures = 30s)

### Readiness: Is This Pod the Leader?

Purpose: route traffic only to the leader pod (optional).

Logic: simply returns `manager.isActive()`. If you expose leader-only REST
endpoints (e.g., a diagnostic `/leader` status page), a K8s Service selecting
on readiness ensures only the leader receives requests.

This is optional. Many setups don't need it because the leader work is
triggered by `@Scheduled`, not by inbound HTTP.

## Prometheus Metrics

Four useful metrics for dashboarding and alerting:

1. **`leader_election_is_leader`** (gauge, 0/1) — which pod is leading.
   Alert if no pod has value=1 for > leaseDuration (leadership gap).

2. **`leader_election_epoch`** (gauge) — current fencing token. Useful for
   correlating with DB audit queries.

3. **`leader_election_fenced_writes_total`** (counter) — total fenced write
   attempts. Shows throughput.

4. **`leader_election_fenced_writes_rejected`** (counter) — fenced writes
   rejected (0 rows). A non-zero value means a zombie was caught. Alert on
   any increment — it indicates a split-brain event occurred.

Requires `quarkus-micrometer-registry-prometheus`. Register gauges that
read from `LeaderManager.isActive()` and `getToken()`. Register counters
that the `FencedRepository` increments.

## Deployment

- **Replicas**: 3 is typical. One leads, two stand by. More replicas don't
  improve availability (only one can lead) but provide redundancy.

- **Pod identity**: `HOSTNAME` env var is set by K8s to the pod name
  (e.g., `order-processor-7d8f9b6c4-xk2lp`). This is unique per pod and
  stable for the pod's lifetime. Use it as the LeaderElector identity.

- **Namespace**: inject via downward API (`metadata.namespace` fieldRef).

- **Resource limits**: the election loop is lightweight (one HTTP call every
  retryPeriod). CPU/memory limits should be based on the business logic,
  not the election.

## Timing Tuning Guide

The three timing knobs trade off failover speed vs API load vs GC tolerance:

### Fast Failover (aggressive)

```
leaseDuration: 10s
renewDeadline: 7s
retryPeriod: 1s
```

Leadership gap on pod crash: ≤ 10s. But generates ~1 API call/second per
pod. Use when failover latency matters more than API load (e.g., real-time
order processing).

### Balanced (default)

```
leaseDuration: 15s
renewDeadline: 10s
retryPeriod: 2s
```

Leadership gap: ≤ 15s. ~0.5 API calls/second per pod.

### Relaxed (conservative)

```
leaseDuration: 30s
renewDeadline: 20s
retryPeriod: 5s
```

Leadership gap: ≤ 30s. ~0.2 API calls/second per pod. Use when the leader
job runs infrequently (e.g., hourly batch) and fast failover isn't critical.

### GC Consideration

If your JVM has GC pauses approaching the `leaseDuration`, the leader will
lose its lease during GC. Options:

- Increase `leaseDuration` to exceed worst-case GC pause. Downside: slower
  failover.
- Tune GC to reduce pause times (ZGC/Shenandoah for sub-10ms pauses).
- Accept the fencing pattern will catch the zombie — that's what it's for.

## Diagnostic Endpoint

A REST endpoint at `/leader` returning JSON with `isLeader`, `epoch`,
`holder`, `acquiredAt`, `renewedAt` is useful for debugging. Not for
production traffic routing (use the readiness probe for that).

## Graceful Shutdown

On `SIGTERM` (K8s pod termination):

1. Quarkus fires `@ShutdownEvent`.
2. LeaderManager sets `isActive()=false` and shuts down the election executor.
3. Any in-flight `@FencedLeader` method sees the post-check fail and throws.
4. The lease is NOT explicitly released — it expires naturally after
   `leaseDuration`. This is intentional: an explicit release during a
   network partition could cause the lease to be released on the API server
   while the pod is still finishing work. Letting it expire is safer.

If you want faster handover during rolling deploys, you CAN explicitly
delete/release the lease in `onStop()`. But understand the trade-off:
faster handover vs risk of releasing while still processing.