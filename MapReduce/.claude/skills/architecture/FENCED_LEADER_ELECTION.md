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

**Version:** 2.0  
**Date:** 2026-03-15  
**Status:** Updated — integrates Graceful Shutdown, Event Bus, and Health Probe Registry  
**Changelog:**
- v2.0 (2026-03-15): Explicit lease release on planned shutdown. Event Bus integration
  for lifecycle events. Health probes migrated to HealthContributor interface. Shutdown
  Coordinator integration for Phase 1 leader teardown.
- v1.0 (original): Initial design.

---

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
LeaderManager (K8s Lease-based election)
  └─► Exposes: isActive (Boolean), token (Long/epoch)
      Backed by MutableStateFlow — thread-safe, lock-free reads

Repository layer (direct injection)
  └─► Reads epoch via leaderManager.token
  └─► SQL:  UPDATE ... SET last_epoch = :epoch
            WHERE id = :id AND last_epoch <= :epoch
            → 0 rows = zombie caught (stale epoch rejected)
```

The DB fence is the authoritative safety gate. Repositories inject
LeaderManager directly and read the epoch — no interceptor, no ThreadLocal,
no intermediate propagation layer needed.

## Integration Points (v2.0)

This component integrates with three other framework components. These
integrations are called out inline throughout this document, but summarized
here for orientation:

| Component | Integration | Section |
|-----------|-------------|---------|
| **Event Bus** | LeaderManager fires `LeadershipAcquired` and `LeadershipLost` CDI events on every leadership transition. Other subsystems (shutdown coordinator, orchestration loops, health probes, metrics) observe these events instead of polling `isActive()`. | §Leader Lifecycle, §Event Bus Integration |
| **Shutdown Coordinator** | On planned shutdown (SIGTERM), the shutdown coordinator's Phase 1 calls `LeaderManager.teardown()` which stops orchestration loops, then explicitly releases the K8s Lease for fast handover. | §Graceful Shutdown |
| **Health Probe Registry** | LeaderManager implements `HealthContributor`, providing liveness (election thread alive?) and optional readiness (is leader?) checks to the unified health aggregator. | §Health Probes |

## Component Map

Read each reference file for the concepts and design rationale behind that layer.

| Layer | Reference | Core Concept |
|-------|-----------|-------------|
| Why fencing works | `references/pattern.md` | Fencing token theory, invariants, safety proof |
| Leader lifecycle | `references/leader-lifecycle.md` | fabric8 LeaderElector behavior, epoch extraction, threading |
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
   dedicated daemon thread with a restart loop. Fires CDI events on transitions.
   Implements `HealthContributor` for unified health reporting. Exposes `teardown()`
   for the shutdown coordinator.

5. **Direct epoch injection** — Repositories inject `LeaderManager` and read
   `leaderManager.token` directly. No interceptor, ThreadLocal, or base class
   needed. Use `optionalEpoch()` pattern for writes that work with or without
   a leader context.

8. **Health checks** — Implement `HealthContributor` interface. Liveness: election
   thread alive? Readiness: is leader? (optional). Registered with the Health Probe
   Registry automatically via CDI.

9. **Scheduled job** — `@Scheduled` methods should check `leaderManager.isActive`
   before doing leader-only work. Skip silently on follower pods.

## Common Mistakes

1. **Forgetting the WHERE fence** — `SET last_epoch = :epoch` alone doesn't prevent
   stale writes. The `WHERE last_epoch <= :epoch` is the actual gate.

2. **Using strict less-than** (`< :epoch`) — This prevents same-epoch re-processing.
   Use `<=` unless you specifically want at-most-once per epoch.

3. **Hand-rolling lease logic** — fabric8's `LeaderElector` handles create, renew,
   conflict (409), expiry, and step-down. Don't reimplement it.

4. **Caching the epoch in a field** — Always read from `leaderManager.token` at
   the point of use. The StateFlow gives a fresh, thread-safe read each time.

5. **Calling LeaderManager.shutdown() directly** — Only the shutdown coordinator
   calls it via the ShutdownParticipant interface. Direct calls bypass the phase
   ordering that prevents data races.

## When NOT to Use

- Leader work is fully idempotent and harmless to repeat → simpler K8s Job or
  `@IfBuildProfile("leader")` suffices.
- Single-replica deployment → no election needed.
- Write-free leader work (read-only aggregation) → standard leader election without
  fencing is fine.


---

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
        0 rows → zombie caught!
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

## Safety Layers

| Layer | When it catches the zombie | Mechanism |
|-------|---------------------------|-----------|
| `leaderManager.isActive` check | Before work begins | Fast-fail (optional, in caller code) |
| Status guards (`WHERE status = 'CLAIMED'`) | At the moment of each write | Idempotency |
| Version CAS (`WHERE version = :expected`) | At the moment of each write | Optimistic locking |
| DB fence (`WHERE last_epoch <= :epoch`) | At the moment of each write | Epoch monotonicity |

The DB fence, status guards, and version CAS are all atomic with the data
mutation. Each is independently sufficient for correctness. Together they
provide defense-in-depth.

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


---

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

3. **Shutdown**: On `@ShutdownEvent`, the shutdown coordinator calls
   `LeaderManager.teardown()`, which shuts down the executor. The `run()`
   method respects interruption. See §Graceful Shutdown for the full sequence.

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

## Event Bus Integration (v2.0)

The LeaderManager fires CDI events on every leadership transition. This
replaces direct method calls and polling as the primary mechanism for other
subsystems to react to leadership changes.

```kotlin
@ApplicationScoped
class LeaderManager(
    private val leadershipAcquiredEvent: Event<LeadershipAcquired>,
    private val leadershipLostEvent: Event<LeadershipLost>,
    private val config: LeaderElectionConfig,
    private val k8sClient: KubernetesClient
) : HealthContributor {

    private val isLeader = AtomicBoolean(false)
    private val epoch = AtomicLong(0)
    private val lastHeartbeat = AtomicReference(Instant.now())
    private val acquiredAt = AtomicReference<Instant?>(null)

    private lateinit var executor: ExecutorService
    private val podId = System.getenv("HOSTNAME") ?: "unknown"

    // ── Public API ──────────────────────────────────────────

    fun isActive(): Boolean = isLeader.get()
    fun getToken(): Long = epoch.get()

    // ── Callbacks (run on election thread) ──────────────────

    private fun onStartLeading() {
        val newEpoch = readEpochFromLease()
        epoch.set(newEpoch)
        isLeader.set(true)
        acquiredAt.set(Instant.now())
        lastHeartbeat.set(Instant.now())

        leadershipAcquiredEvent.fire(
            LeadershipAcquired(
                epoch = newEpoch,
                podId = podId,
                acquiredAt = Instant.now()
            )
        )
    }

    private fun onStopLeading() {
        val lastEpoch = epoch.get()
        isLeader.set(false)

        leadershipLostEvent.fire(
            LeadershipLost(
                lastEpoch = lastEpoch,
                podId = podId,
                lostAt = Instant.now()
            )
        )
    }

    private fun onNewLeader(identity: String) {
        lastHeartbeat.set(Instant.now())
        // If we're the new leader (e.g., lease renewal bumped resourceVersion),
        // refresh the epoch
        if (identity == podId && isLeader.get()) {
            val refreshed = readEpochFromLease()
            epoch.set(refreshed)
        }
    }

    // ── Lifecycle ───────────────────────────────────────────

    fun onStartup(@Observes event: StartupEvent) {
        executor = Executors.newSingleThreadExecutor { r ->
            Thread(r, "leader-election").apply { isDaemon = true }
        }
        executor.submit { electionLoop() }
    }

    /**
     * Called by ShutdownCoordinator Phase 1 — NOT directly by @ShutdownEvent.
     * The coordinator controls the ordering: stop orchestration loops first,
     * then call this to release the lease.
     */
    fun teardown() {
        isLeader.set(false)
        releaseLease()
        executor.shutdownNow()
    }

    // ── Internals ───────────────────────────────────────────

    private fun electionLoop() {
        while (!Thread.currentThread().isInterrupted) {
            try {
                val lock = LeaseLock(
                    config.namespace(), config.leaseName(), podId
                )
                val electionConfig = LeaderElectionConfigBuilder()
                    .withLock(lock)
                    .withLeaseDuration(config.leaseDuration())
                    .withRenewDeadline(config.renewDeadline())
                    .withRetryPeriod(config.retryPeriod())
                    .withLeaderCallbacks(LeaderCallbacks(
                        ::onStartLeading,
                        ::onStopLeading,
                        ::onNewLeader
                    ))
                    .build()

                LeaderElector(k8sClient, electionConfig).run()
                // run() returned → leadership lost. Sleep and retry.
                Thread.sleep(config.retryPeriod().toMillis())
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                break
            } catch (e: Exception) {
                // API server unreachable, transient error, etc.
                // Log and retry after retryPeriod.
                logger.error("Election loop error: {}", e.message, e)
                try {
                    Thread.sleep(config.retryPeriod().toMillis())
                } catch (ie: InterruptedException) {
                    Thread.currentThread().interrupt()
                    break
                }
            }
        }
        logger.info("Election loop exited.")
    }

    private fun readEpochFromLease(): Long {
        val lease = k8sClient.leases()
            .inNamespace(config.namespace())
            .withName(config.leaseName())
            .get()
        return lease?.metadata?.resourceVersion?.toLongOrNull()
            ?: lease?.metadata?.resourceVersion?.hashCode()?.toLong()
            ?: 0L
    }

    private fun releaseLease() {
        try {
            val lease = k8sClient.leases()
                .inNamespace(config.namespace())
                .withName(config.leaseName())
                .get()
            if (lease?.spec?.holderIdentity == podId) {
                lease.spec.holderIdentity = null
                lease.spec.acquireTime = null
                k8sClient.leases()
                    .inNamespace(config.namespace())
                    .withName(config.leaseName())
                    .patch(lease)
                logger.info("Lease released explicitly. New leader can acquire immediately.")
            }
        } catch (e: Exception) {
            logger.warn("Failed to release lease explicitly: {}. " +
                "Lease will expire naturally after leaseDuration.", e.message)
        }
    }
}
```

**Who observes these events:**

| Observer | Event | Reaction |
|----------|-------|----------|
| Orchestration loops (barrier monitor, stale reaper, cleanup) | `LeadershipAcquired` | Start loops under a new `SupervisorJob` scope with the new epoch |
| Orchestration loops | `LeadershipLost` | Cancel the `SupervisorJob` scope, stopping all loops |
| Shutdown coordinator | `LeadershipLost` | Informational only during normal operation. During shutdown, the coordinator drives teardown via `teardown()`, not via this event. |
| Health probe registry | Both | Updates the leader election health contributor's state |
| Metrics (pipeline) | Both | Updates `leader_election_is_leader` gauge and `leader_election_epoch` gauge |

**Ordering guarantee:** `LeadershipAcquired` is fired AFTER `isActive()` returns
true and `getToken()` returns the new epoch. `LeadershipLost` is fired AFTER
`isActive()` returns false. Observers can rely on these atomics being consistent
with the event.

## Lifecycle in Quarkus

- Start: observe `@StartupEvent` → create LeaseLock, build config, submit
  election loop to executor.
- Stop: **NOT via `@ShutdownEvent` directly.** The shutdown coordinator calls
  `LeaderManager.teardown()` during Phase 1 (leader teardown). This ensures
  orchestration loops are stopped before the lease is released.

The `@ApplicationScoped` bean is created eagerly by Quarkus because it
observes `StartupEvent`.


---

# Token Propagation

## Design: Direct Injection

Repositories that need the fencing epoch inject `LeaderManager` directly
and read `leaderManager.token`. No interceptor, ThreadLocal, or intermediate
propagation layer is needed.

```kotlin
@ApplicationScoped
class TaskGroupRepository(
    private val jdbi: Jdbi,
    private val leaderManager: LeaderManager,
) {
    private fun optionalEpoch(): Long? =
        if (leaderManager.isActive) leaderManager.token else null
}
```

`LeaderManager.token` is backed by a `MutableStateFlow<Long>` — thread-safe,
lock-free reads from any thread or coroutine, no propagation concerns.

## Why Direct Injection Over ThreadLocal/Interceptor

The previous design used a CDI interceptor (`@FencedLeader`) to set the epoch
in a ThreadLocal, which repositories then read. This was removed because:

1. **ThreadLocal breaks coroutines** — suspend functions can resume on a
   different thread, losing the value.
2. **Unnecessary indirection** — `LeaderManager` is an `@ApplicationScoped`
   bean. Any repository can inject it directly.
3. **The interceptor was never used** — `@FencedLeader` was never applied
   to any method in production. The safety came from status guards, version
   CAS, and execution_generation fencing, not the interceptor.

## Optional Fencing Pattern

Use `optionalEpoch()` for writes that should include the epoch guard when
available but still work without it (e.g., methods called by both leader
and non-leader paths):

```kotlin
val epoch = optionalEpoch()
val epochClause = if (epoch != null) " AND last_epoch <= :epoch" else ""
val epochSet = if (epoch != null) ", last_epoch = :epoch" else ""
// ... build SQL with conditional epoch guards
if (epoch != null) update.bind("epoch", epoch)
```


---

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
  already wrote to it. In the fencing context, 0 rows means a zombie
  was caught.

The repository should check affected rows after each fenced write. For
leader-only writes, 0 rows means a stale epoch was rejected. For writes
that use `optionalEpoch()`, 0 rows may also indicate a status guard
rejection (normal idempotent behavior).

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
   a fenced write gets 0 rows, roll back the entire transaction.

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


---

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
| `isActive` check | If the scheduler ticks after `onStopLeading()`, the check catches it. If the method was **already executing** when GC started, the check already passed. |
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
| LeaderManager | `isActive()` becomes false. Fires `LeadershipLost`. The restart loop tries to re-enter the election, but `run()` will throw/fail because the API is down. |
| Liveness probe | `lastHeartbeat` stops updating. After `renewDeadline + retryPeriod` seconds without a heartbeat, the `HealthContributor.liveness()` returns DOWN → K8s restarts the pod. |
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
| Pod-A | fabric8 calls `onStopLeading()` after renewDeadline. `LeadershipLost` fires. But there's a window of up to `leaseDuration` where Pod-A still thinks it's leading (it hasn't failed renewal yet). |
| Pod-B | `onStartLeading()` fires. `LeadershipAcquired` fires. Starts working with a higher epoch. |
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
| Event bus | No events fire — the pod is dead. Other pods detect the new leadership opportunity via the normal election cycle. |
| DB fence | Not relevant — the crashed pod isn't running. No zombie writes. |

**Net outcome**: Safe, but there's a leadership gap of up to `leaseDuration`
(15s) while the lease expires. Reduce `leaseDuration` for faster failover at
the cost of more frequent API calls. Note: graceful shutdown (SIGTERM) avoids
this gap by explicitly releasing the lease — see §Graceful Shutdown.

---

## Scenario 5: Leadership Lost During Execution

**What happens**: The leader finishes executing a fenced method. Between the
last DB write and returning, leadership transitions to a new pod.

**Layer responses**:

| Layer | Response |
|-------|----------|
| DB fence | All writes already committed with the (at-the-time valid) epoch. |
| `isActive` | Returns false — caller may detect leadership loss. |

**Net outcome**: The writes are already committed and valid. The new leader
may re-process the same items, which is fine if the fenced writes made them
non-retryable (e.g., status changed from PENDING to PROCESSED).

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
the liveness probe still passes (election loop is healthy). The Oracle
`HealthContributor` will report readiness as DOWN, removing the pod from
the K8s Service — but the pod remains alive and will resume when the DB
recovers.

---

## Scenario 7: Planned Shutdown (v2.0)

**What happens**: The pod receives SIGTERM (rolling deployment, scale-down,
node drain). The shutdown coordinator drives Phase 1 leader teardown.

**Layer responses**:

| Layer | Response |
|-------|----------|
| Shutdown coordinator | Sets state to DRAINING. Calls `LeaderManager.teardown()`. |
| LeaderManager.teardown() | Sets `isActive()=false`. Fires `LeadershipLost`. Explicitly releases the K8s Lease by clearing holderIdentity. Shuts down the election executor. |
| Orchestration loops | Observe `LeadershipLost` event (or are cancelled by the shutdown coordinator cancelling the leader scope). Stop within the 5s leader teardown timeout. |
| Other pods | Detect the released Lease within one `retryPeriod` (~2s). A new leader acquires almost immediately. No leadership gap from planned shutdowns. |
| In-flight fenced writes | Any writes still executing will be protected by the DB fence. The epoch guard ensures stale writes are rejected regardless of shutdown timing. |

**Net outcome**: Near-zero leadership gap. The new leader acquires within
seconds of the old leader releasing, compared to up to `leaseDuration` (15s)
for unplanned crashes.

**Why explicit release is safe here**: The concern with explicit release is
that the pod might still be processing leader work when the lease drops. The
shutdown coordinator's phase ordering eliminates this:

1. Phase 0: Claim loop stops → no new tasks claimed.
2. Phase 1: Orchestration loops cancelled → no new leader-driven work.
3. Phase 1: `teardown()` releases lease → new leader can acquire.
4. Phase 2: Worker drain → in-flight tasks (including fenced methods) finish.

By the time the lease is released, all leader-specific orchestration has
stopped. The only remaining work is worker-side task execution, which is
fenced by the DB epoch and safe to overlap with a new leader.

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
| **Planned Shutdown** | **None** | **✅ Phase ordering + DB fence** | **~2s (one retryPeriod)** |


---

# Operational Reference

## K8s RBAC

The pod's ServiceAccount needs a Role granting these verbs on Lease resources:

- `get`, `list`, `watch` — observe current lease state.
- `create` — first pod creates the Lease object.
- `update`, `patch` — acquire, renew, and explicitly release the lease.

Scope this to a single namespace with a Role + RoleBinding (not ClusterRole)
to follow least-privilege. The Lease object lives in the `coordination.k8s.io`
API group.

Deployment must set `serviceAccountName` on the pod spec.

## Health Probes

### Integration with Health Probe Registry (v2.0)

The LeaderManager implements the `HealthContributor` interface from the
Health Probe Registry (see Core Infrastructure Components doc, Component 5).
This replaces standalone health check beans with a unified pattern.

```kotlin
// Inside LeaderManager (which implements HealthContributor)

override val name = "leader-election"

override fun liveness(): HealthCheckResult {
    val heartbeatAge = Duration.between(lastHeartbeat.get(), Instant.now())
    val threshold = config.renewDeadline() + config.retryPeriod()
    return if (heartbeatAge < threshold) {
        HealthCheckResult(
            status = HealthStatus.UP,
            details = mapOf(
                "heartbeatAge" to heartbeatAge.seconds,
                "isLeader" to isLeader.get(),
                "epoch" to epoch.get()
            )
        )
    } else {
        HealthCheckResult(
            status = HealthStatus.DOWN,
            details = mapOf(
                "heartbeatAge" to heartbeatAge.seconds,
                "reason" to "Election loop heartbeat stale for ${heartbeatAge.seconds}s"
            )
        )
    }
}

override fun readiness(): HealthCheckResult? {
    // Leadership is NOT required for readiness by default.
    // The pod can serve config API traffic and execute worker tasks
    // without being the leader. Return null = no opinion on readiness.
    //
    // Override this in deployments where only the leader should
    // receive traffic (e.g., leader-only REST endpoints).
    return null
}
```

### Liveness: Is the Election Loop Alive?

Purpose: detect a hung election thread (deadlock, infinite loop, thread death).

Logic: the LeaderManager updates `lastHeartbeat` on every `onNewLeader`
callback and at the start of each `run()` loop iteration. The `liveness()`
method compares `now - lastHeartbeat` against `renewDeadline + retryPeriod`
(12s default). If exceeded → DOWN → the Health Probe Registry aggregator
reports the pod as not-live → K8s restarts it.

K8s probe config:
- Path: `/q/health/live`
- `initialDelaySeconds`: 15 (allow first election cycle)
- `periodSeconds`: 10
- `timeoutSeconds`: 5
- `failureThreshold`: 3 (restart after 3 consecutive failures = 30s)

### Readiness: Is This Pod the Leader? (Optional)

Purpose: route traffic only to the leader pod.

By default, the LeaderManager returns `null` for readiness (no opinion),
because most pods need to be ready regardless of leadership status (they
serve the config API and run the worker loop). Only enable leader-based
readiness if you have leader-only REST endpoints.

## Prometheus Metrics

Four useful metrics for dashboarding and alerting:

1. **`leader_election_is_leader`** (gauge, 0/1) — which pod is leading.
   Alert if no pod has value=1 for > leaseDuration (leadership gap).

2. **`leader_election_epoch`** (gauge) — current fencing token. Useful for
   correlating with DB audit queries.

Requires `quarkus-micrometer-registry-prometheus`. Gauges are registered
in `LeaderManager.registerMetrics()` reading from `isActive` and `token`.

**Integration with Handler Execution Pipeline (v2.0):** The metrics middleware
in the pipeline records per-handler execution metrics. Leader election metrics
are separate — they measure the election and fencing subsystem, not task
execution. Both feed into the same Prometheus/Grafana stack.

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

This endpoint is supplemented by the Health Probe Registry's
`/q/health/detail` endpoint, which includes leader election state alongside
all other subsystem health (see Core Infrastructure Components doc, §5.4).

## Graceful Shutdown (v2.0 — REVISED)

**Previous behavior (v1.0):** The lease was NOT explicitly released on
shutdown. It expired naturally after `leaseDuration`. This was conservative
but caused a leadership gap of up to 15 seconds on every planned shutdown.

**Current behavior (v2.0):** On planned shutdown (SIGTERM), the lease IS
explicitly released. The shutdown coordinator's phase ordering ensures this
is safe.

### Planned Shutdown Sequence

```
SIGTERM
  │
  ├── Phase 0: ShutdownCoordinator sets state = DRAINING
  │   └── Worker claim loop stops (no new tasks claimed)
  │
  ├── Phase 1: ShutdownCoordinator calls LeaderManager.teardown()
  │   ├── 1a. Cancel orchestration loops (LeadershipLost event fires)
  │   │   ├── Barrier monitor loop exits
  │   │   ├── Stale reaper loop exits
  │   │   └── Cleanup loop exits
  │   ├── 1b. Await loop termination (up to 5s)
  │   ├── 1c. Set isActive() = false
  │   └── 1d. Release K8s Lease (clear holderIdentity via patch)
  │       └── Other pods can acquire within ~retryPeriod (2s)
  │
  ├── Phase 2: Worker drain (up to 60s)
  │   └── In-flight tasks (including fenced methods) complete normally.
  │       DB fence protects any writes against the new leader's epoch.
  │
  ├── Phase 3: Release uncompleted tasks back to PENDING
  └── Phase 4: Close connections, emit metrics, exit
```

### Why Explicit Release Is Safe

The v1.0 concern was: if the pod releases the lease while still executing
leader work, a new leader could start conflicting operations.

The Phase 1 ordering eliminates this risk:

1. **Orchestration loops are cancelled BEFORE the lease is released.** No
   new fan-outs, barrier transitions, or stale reclaims will be initiated
   by this pod after step 1a.

2. **The claim loop is already stopped (Phase 0).** No new tasks are claimed
   after shutdown begins.

3. **In-flight tasks that remain (Phase 2) are worker-side execution.**
   These are fenced by the DB epoch. If the new leader processes the same
   rows, the fence ensures only the higher epoch's writes succeed. Worker
   tasks and leader orchestration are independent — a worker completing a
   map task doesn't conflict with the new leader's barrier detection.

4. **The `teardown()` method is only callable by the ShutdownCoordinator.**
   It is not exposed via `@ShutdownEvent` directly, preventing accidental
   out-of-order invocation.

### Unplanned Shutdown (SIGKILL, OOM, Node Failure)

No shutdown hooks run. The lease expires naturally after `leaseDuration`.
Other pods acquire after the TTL. This is the baseline behavior — graceful
shutdown improves upon it, not replaces it.

### Trade-Off Summary

| Shutdown Type | Lease Release | Leadership Gap | Safety |
|---------------|---------------|----------------|--------|
| Planned (SIGTERM) | Explicit (v2.0) | ~2s (one retryPeriod) | ✅ Phase ordering + DB fence |
| Unplanned (SIGKILL) | Natural expiry | Up to leaseDuration (15s) | ✅ DB fence (no zombie writes from dead pod) |
| Network partition during shutdown | Explicit release may fail | Falls back to natural expiry | ✅ `releaseLease()` catches exception, logs warning |

The `releaseLease()` method is wrapped in try-catch. If the K8s API is
unreachable (network partition during shutdown), the release fails silently
and the lease expires naturally. This preserves v1.0 safety as a fallback
while providing v2.0 speed in the common case.