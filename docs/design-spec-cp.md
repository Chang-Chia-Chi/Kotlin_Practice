# DynaCache CP Subsystem — Design Spec

**Date:** 2026-04-19
**Project:** DynaCache — CP subsystem addition
**Complements:** `docs/design-spec.md` (the Dynamo-style AP spec — unchanged)
**Status:** Design pending user approval

---

## 0. What This Document Is

A constraints document for the **CP subsystem** — a Raft-backed subset of DynaCache that provides linearizable primitives (distributed locks with fencing tokens, atomic counters, semaphores, latches, CAS-on-reference) alongside the existing AP engine.

Like the AP spec, this defines properties, invariants, and semantics — not interfaces, class names, or project layout. Those emerge during implementation planning (P5).

---

## 1. Goals & Non-Goals

### Goal

Add a **Hazelcast-style CP subsystem** to DynaCache: a small Raft group (a subset of cluster nodes) that backs a handful of linearizable primitives. Clients reach it either through explicit `CP.*` RESP verbs or through standard Redis commands on keys prefixed `cp:*`. The existing AP engine is unchanged; the two engines run side-by-side on every node.

### In Scope

| Feature | Why |
|---|---|
| Raft-based consensus via **MicroRaft** library | Proven library, authored by the engineer behind Hazelcast's CP Subsystem — our reference architecture |
| **FencedLock** — distributed mutex with monotonic fencing tokens | Mutual exclusion that survives GC pauses and network delays (Kleppmann-safe) |
| **AtomicLong** — linearizable counter | CAS, INCR, DECR on strongly-consistent counters |
| **Semaphore** — linearizable counting semaphore | Bounded-concurrency coordination |
| **CountDownLatch** — linearizable countdown barrier | One-time event fan-in |
| **AtomicReference** — linearizable CAS-on-bytes | General-purpose compare-and-set |
| Session lifecycle — heartbeats, auto-release on death | Prevents stuck locks from crashed clients |
| TTL on every primitive — deterministic across replicas | Primary mechanism to bound CP state |
| Redis-compat routing (`SET NX PX`, `INCR`, `EXPIRE` on `cp:*`) | Drop-in Redis client usability for the basic lock/counter patterns |
| Explicit `CP.*` verbs | Typed return values (fencing tokens, CAS booleans), primitives Redis can't express |
| Single Raft group for all primitives | Simplification — defer multi-group sharding to a future phase |

### Non-Goals (this phase)

- **Implementing Raft from scratch** — we use MicroRaft. We read the Raft paper for conceptual understanding, not for implementation reference.
- **Multiple Raft groups / hot-lock sharding** — Hazelcast supports this as a scaling technique; out of scope here.
- **Server-side blocking ops** (`awaitUntil`, `tryAcquire(timeout)`) — first pass uses polling. Stretch goal.
- **Dynamic CP-member reconfiguration** — CP members fixed at startup. Runtime membership changes deferred.
- **TLS, auth, ACLs** — not a learning priority.
- **Cross-engine transactions** (mixing AP and CP writes atomically) — explicitly out of scope. Each engine is independent.

---

## 2. Architecture

### 2.1 Two Engines, One Node

Every DynaCache node runs two independent engines behind a single RESP2 port:

- **AP engine** (existing, Dynamo-style): consistent hash ring, SWIM gossip, DVVs, sloppy quorum, hinted handoff, Merkle anti-entropy, W-TinyLFU eviction, WAL, RDB, Chandy-Lamport snapshots. Responsible for all keys *not* in the `cp:*` namespace and all commands *not* prefixed `CP.*`.
- **CP engine** (new): MicroRaft-backed. A single Raft group running five state machines (FencedLock, AtomicLong, Semaphore, CountDownLatch, AtomicReference). Responsible for all `cp:*` keys and all `CP.*` verbs.

A **command dispatcher** at the front of each node inspects every incoming command:

1. If command begins with `CP.` → CP engine
2. Else if first key argument begins with `cp:` → CP engine (only if command is in the Redis-compat subset: `SET`, `GET`, `DEL`, `EXISTS`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `SETEX`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `TYPE`)
3. Else → AP engine

The two engines share **nothing** except the command dispatcher and the gRPC transport.

### 2.2 CP Member Topology

A subset of the cluster's nodes are designated **CP members** at startup. The subset size must be odd and ≥3 (typically 3 or 5). In a 3-node cluster, all 3 are CP members. In a 5- or 7-node cluster, exactly 3 or 5 are CP members; the rest are AP-only.

- **CP members** run MicroRaft, hold the replicated log, and apply state machine entries. They also run the AP engine.
- **AP-only members** do *not* run Raft. They accept `CP.*` commands from clients and forward them over gRPC to the current Raft leader. On leader change, they re-discover via `CP.INFO`.

CP membership is fixed at startup. Adding/removing CP members requires a planned operation (deferred — not in P5).

### 2.3 Module Placement

New Maven module: **`dynacache-cp`** — sits between `dynacache-cluster` and `dynacache-server`.

```
dynacache-engine   → kotlin-stdlib ONLY
dynacache-cluster  → engine + coroutines + gRPC
dynacache-cp       → cluster + MicroRaft                  ← NEW
dynacache-server   → cp + Netty + LuaJ
```

The engine module remains pure. Compile-time dependency enforcement prevents the AP engine from accidentally importing CP or Raft symbols.

### 2.4 Transport

CP operations between nodes reuse the existing **gRPC transport** from the cluster module. New Protobuf service: `CpService`, carrying:

- `Apply(request) → (response, leader_hint)` — forward a CP op to the leader
- `GetInfo() → (leader, members, log_state)` — introspection
- `Heartbeat(session_id) → ok` — session keepalive (also proxy-forwarded)

MicroRaft's internal inter-member traffic (AppendEntries, RequestVote, InstallSnapshot) uses a separate Protobuf service `RaftService` embedded in the same gRPC server.

---

## 3. State Machines

Each primitive type is its own state machine inside the single Raft group. All state machines are keyed by a string inside the `cp:*` namespace, conventionally prefixed by primitive type (`cp:lock:`, `cp:counter:`, `cp:sem:`, `cp:latch:`, `cp:ref:`).

### 3.1 FencedLock

**State per key:**
```
{
  owner: SessionId | null
  token: Long                  // monotonically increasing per key
  lease_expiry: LogTimestamp   // see §5
  reentrance_count: Int
}
```

**Log ops:**
- `LOCK_TRY(key, session, ttl_ms)` → `(ok: Bool, token: Long)`. If already held by same session, increments reentrance, returns existing token.
- `LOCK_UNLOCK(key, session, token)` → `ok: Bool`. Rejects if session/token mismatch. Decrements reentrance; releases at 0.
- `LOCK_RENEW(key, session, token, ttl_ms)` → `ok: Bool`. Extends lease for holder only.
- `LOCK_STATE(key)` → full state (linearizable read).
- `LOCK_FORCE_UNLOCK(key)` → admin override, bypasses ownership check.

### 3.2 AtomicLong

**State per key:** `value: Long`

**Log ops:**
- `LONG_GET(key)` → `Long`
- `LONG_SET(key, value)`
- `LONG_INCR(key) / LONG_DECR(key)` → new value
- `LONG_ADD(key, delta)` → new value
- `LONG_CAS(key, expected, new)` → `Bool`
- `LONG_GETADD(key, delta)` → old value

### 3.3 Semaphore

**State per key:**
```
{
  available: Int
  holders: Map<SessionId, Int>   // session → permits held
}
```

**Log ops:**
- `SEM_INIT(key, permits)` — idempotent; no-op if already exists.
- `SEM_ACQUIRE(key, session, n)` → `Bool`. Non-blocking; session-tied.
- `SEM_RELEASE(key, session, n)` → `ok`. Rejects if would exceed held count.
- `SEM_AVAILABLE(key)` → `Int`
- `SEM_DRAIN(key, session)` → `Int` (permits acquired)

### 3.4 CountDownLatch

**State per key:**
```
{
  count: Int
  initial: Int
}
```

**Log ops:**
- `LATCH_SET(key, count)` — establishes initial count.
- `LATCH_DOWN(key)` → new count. At 0, stays 0.
- `LATCH_GET(key)` → count.
- `LATCH_RESET(key, new)` — only valid when current count is 0.

### 3.5 AtomicReference

**State per key:** `value: ByteArray | null`

**Log ops:**
- `REF_GET(key)` → `ByteArray | null`
- `REF_SET(key, value)`
- `REF_CAS(key, expected, new)` → `Bool` (byte-equality on expected)

---

## 4. Sessions

FencedLock and Semaphore hold resources on behalf of a **session**. Session lifecycle:

- **Creation** — first CP op from a connection auto-creates a session, or client calls `CP.SESSION.CREATE` explicitly. Session ID is returned and must accompany every subsequent CP op from that connection.
- **Heartbeat** — client calls `CP.SESSION.HEARTBEAT sid` every N seconds (default 5s). Missing H heartbeats (default 3 → 15s grace) invalidates the session.
- **Invalidation** — triggers a `SESSION_CLOSED(sid)` Raft log entry. Apply-loop walks all state machines and releases every resource held by that session.
- **Voluntary close** — `CP.SESSION.CLOSE sid` releases immediately without waiting for heartbeat timeout.

Session state lives in a sixth implicit state machine (`SessionRegistry`). Heartbeats are Raft log entries — expensive, but necessary so every replica agrees on which sessions are alive.

---

## 5. TTL and Time in Raft

Wall-clock time **cannot** be read from local clocks — follower clocks may drift, and on leader failover the new leader's clock may be behind the old leader's, which would cause TTLs to "un-expire" or expire out of order.

**Resolution:** timestamps flow through the Raft log.

- The leader stamps each log entry with `ts = max(clock_now, last_committed_ts + 1)`. This guarantees monotonicity across leader changes.
- Followers apply entries in log order, tracking `last_applied_ts`.
- Every state machine's expiry check compares `lease_expiry` against `last_applied_ts` — *not* local clock.
- A background `TTL_TICK` entry is appended every tick-interval (default 100ms) even when there are no user writes, to advance time in idle periods.
- All replicas agree on which keys are expired at any log index — no split-brain on expiry.

---

## 6. Command API

### 6.1 FencedLock (explicit verbs only)

| Command | Response shape |
|---|---|
| `CP.LOCK.TRY cp:lock:K ttl_ms` | `*2 $ok :token` |
| `CP.LOCK.UNLOCK cp:lock:K token` | `:1` / `:0` |
| `CP.LOCK.RENEW cp:lock:K token ttl_ms` | `:1` / `:0` |
| `CP.LOCK.STATE cp:lock:K` | array: `owner, token, ttl_remaining, reentrance` |
| `CP.LOCK.FORCE_UNLOCK cp:lock:K` | `+OK` |

### 6.2 AtomicLong (dual interface)

| Redis on `cp:counter:K` | `CP.*` verb | Reply |
|---|---|---|
| `SET cp:counter:K n` | `CP.LONG.SET K n` | `+OK` |
| `GET cp:counter:K` | `CP.LONG.GET K` | `:long` |
| `INCR / DECR` | `CP.LONG.INCR / DECR K` | `:new` |
| `INCRBY cp:counter:K d` | `CP.LONG.ADD K d` | `:new` |
| — | `CP.LONG.CAS K expected new` | `:1 / :0` |
| — | `CP.LONG.GETADD K d` | `:old` |
| `EXPIRE / PEXPIRE / TTL / PERSIST` | — | standard Redis |

### 6.3 Semaphore (explicit verbs only)

| Command | Reply |
|---|---|
| `CP.SEM.INIT cp:sem:K permits` | `+OK` |
| `CP.SEM.ACQUIRE cp:sem:K n` | `:1 / :0` |
| `CP.SEM.RELEASE cp:sem:K n` | `+OK` |
| `CP.SEM.AVAILABLE cp:sem:K` | `:int` |
| `CP.SEM.DRAIN cp:sem:K` | `:int` |

### 6.4 CountDownLatch (explicit verbs only)

| Command | Reply |
|---|---|
| `CP.LATCH.SET cp:latch:K count` | `+OK` |
| `CP.LATCH.DOWN cp:latch:K` | `:new` |
| `CP.LATCH.GET cp:latch:K` | `:int` |
| `CP.LATCH.RESET cp:latch:K new` | `+OK` / error |

### 6.5 AtomicReference (dual interface)

| Redis on `cp:ref:K` | `CP.*` verb | Reply |
|---|---|---|
| `SET cp:ref:K v` | `CP.REF.SET K v` | `+OK` |
| `GET cp:ref:K` | `CP.REF.GET K` | bulk / nil |
| — | `CP.REF.CAS K expected new` | `:1 / :0` |

### 6.6 Sessions

| Command | Reply |
|---|---|
| `CP.SESSION.CREATE` | `:sid` |
| `CP.SESSION.HEARTBEAT sid` | `+OK` |
| `CP.SESSION.CLOSE sid` | `+OK` |

### 6.7 Introspection

| Command | Reply |
|---|---|
| `CP.INFO` | array: `leader, members, log_size, applied_index, snapshot_index` |
| `CP.MEMBERS` | array of member IDs |

### 6.8 Error Codes (new RESP error prefixes)

| Error | Meaning |
|---|---|
| `-NOTLEADER <hint>` | Request hit a non-leader; client retries at hint |
| `-WRONGTYPE` | Key exists as different primitive (e.g., LOCK on AtomicLong key) |
| `-NOTCP` | Request cannot be routed to CP engine — either a `CP.*` verb with a non-`cp:*` key, or a Redis command outside the compat set used with a `cp:*` key |
| `-NOSESSION` | Session expired or never created |
| `-CAPACITY` | State machine at max-entries cap |
| `-REENTRANCE` | Unlock called with wrong token / not current holder |

---

## 7. Core Constraints

Continues the numbering from `design-spec.md` (which ends at C15).

| ID | Constraint |
|---|---|
| **C16** | **CP namespace isolation.** Every CP key begins with `cp:`. Every `CP.*` verb validates this. Violation → `-NOTCP`. The AP engine never sees a `cp:*` key; the CP engine never sees a non-`cp:*` key. |
| **C17** | **Fencing token monotonicity.** Per lock key, the token returned by successful `LOCK_TRY` is strictly greater than every token previously returned for that key across all time — including across leader changes, snapshots, and restarts. |
| **C18** | **Session-held resource release.** When a session is invalidated (heartbeat timeout or explicit close), every lock owned by it and every permit held by it is released atomically in one log entry. No partial state. |
| **C19** | **Raft log timestamp monotonicity.** Every log entry's embedded timestamp is strictly greater than every previously-committed log entry's timestamp. Enforced at leader append time; holds across leader changes. |
| **C20** | **CP linearizability.** Every committed CP operation appears to execute atomically at some single point between its invocation and its response. Reads through the Raft log (via `LOCK_STATE`, `LONG_GET`, etc.) are linearizable. |
| **C21** | **CP durability on quorum.** A CP operation's success response implies the log entry is durably committed on a majority of CP members. Client-observed success survives any minority crash. |
| **C22** | **No cross-engine state leakage.** A key's existence in the AP engine and a key of the same name existing in the CP engine are independent (the namespaces never collide because of C16, but also: neither engine reads the other's state). |
| **C23** | **TTL determinism.** For any key with a TTL, every CP member computes the same expiration log index. No member sees a key as live while another sees it as expired at the same log position. |

---

## 8. Core Invariants

Continues from `design-spec.md` (which ends at I12).

| ID | Invariant | Assertable by |
|---|---|---|
| **I13** | **Lock mutual exclusion.** At any committed log index, at most one session holds a given lock key (modulo reentrance by the same session). | Concurrent `CP.LOCK.TRY` from N sessions → exactly one returns `ok=true` |
| **I14** | **Fencing token strict monotonicity.** For any lock key, if acquire A returned token `t_A` and later acquire B returned `t_B`, then `t_B > t_A`. | Repeated acquire/release cycles → tokens strictly increasing |
| **I15** | **Session-death release completeness.** After a session is invalidated, zero locks remain owned by it and zero permits are held by it, at the log index of the `SESSION_CLOSED` entry. | Kill client → wait > heartbeat_timeout → `CP.LOCK.STATE` on all its locks shows `owner=null` |
| **I16** | **Minority CP-member failure preserves availability.** Killing ⌊(N-1)/2⌋ CP members does not prevent CP operations from succeeding. | Kill 1 of 3 or 2 of 5 CP members → CP ops still succeed |
| **I17** | **Majority CP-member failure correctly blocks writes.** Killing ⌈N/2⌉ or more CP members causes CP ops to time out (or return `-NOTLEADER` with no valid hint), never to succeed with stale data. | Kill 2 of 3 → `CP.LOCK.TRY` times out; no false success |
| **I18** | **Leader failover preserves held locks.** If the Raft leader crashes, every lock held before the crash is still held (by the same session, same token) after the new leader is elected. | Hold lock → kill leader → elect new leader → `CP.LOCK.STATE` shows same owner/token |
| **I19** | **TTL correctness across leader changes.** A lock with `ttl_remaining = T` before a leader crash still expires at most `T + (election_timeout + commit_latency)` after the crash, with no early expiry. | Hold lock with TTL 30s → kill leader at 10s → new leader → lock expires between 30s and 30s + overhead |
| **I20** | **Snapshot/restore round-trip preserves state.** Restoring a CP member from a Raft snapshot + log suffix yields identical state machine contents to continuous replay from empty. | Take snapshot → mutate → restart from snapshot + log → read state → assert equal |
| **I21** | **CAS atomicity.** `LONG_CAS` / `REF_CAS` either set the value (if expected matches) or do not (if not) — no intermediate states visible to any reader. | Concurrent CAS with same expected → exactly one succeeds |
| **I22** | **Namespace isolation.** `SET cp:foo 1` followed by a Dynamo-engine `GET cp:foo` returns the CP value, never the AP engine's view (the AP engine has no such key). Conversely, `GET foo` never sees a `cp:foo` value. | Write to both namespaces, cross-read, assert correct routing |

---

## 9. Semantics

### 9.1 Write Path (CP)

1. Client sends `CP.LOCK.TRY cp:lock:X 30000` to any node.
2. Node is either CP member or AP-only. If AP-only → forward to current known leader via gRPC.
3. Receiving CP member: if not leader → respond `-NOTLEADER <leader_hint>`. Client retries.
4. Leader: validate session, build log entry `LOCK_TRY(X, session, 30000, ts=T_leader_now)`, append to local log.
5. Leader replicates to CP-member followers. Followers append locally and ack.
6. On majority ack: leader marks entry committed, applies to `FencedLock` state machine, returns result to client.
7. If majority not achievable within timeout → `-NOTLEADER` or timeout error. Client retries.

### 9.2 Read Path (CP)

All CP reads are linearizable — they go through the Raft log (or the leader's read-lease optimization, if MicroRaft provides it):

1. `CP.LOCK.STATE cp:lock:X` → forwarded to leader.
2. Leader executes a no-op log entry (or uses read index / lease optimization) to confirm it is still leader at a committed position.
3. Leader reads state machine, returns result.

(MicroRaft supports linearizable reads via `ReadIndex`. We use that; no hand-rolling.)

### 9.3 Session Lifecycle

1. Client connects, runs any CP op → `SESSION_CREATE` log entry, returns `sid`.
2. Client sends `CP.SESSION.HEARTBEAT sid` every 5s.
3. Leader tracks last-heartbeat timestamp per session. On `TTL_TICK`, it checks every session: if `now - last_heartbeat > session_timeout` (default 15s), append `SESSION_CLOSED(sid)`.
4. Apply-loop of `SESSION_CLOSED`: iterate FencedLock & Semaphore state machines, release all sid-owned resources atomically.

### 9.4 TTL Semantics (CP)

- All TTLs are in wall-clock milliseconds but computed against log-carried timestamps (§5).
- `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST` on `cp:*` keys operate on the owning state machine (AtomicLong / AtomicReference / CountDownLatch / Semaphore).
- For FencedLock, TTL is the lease — `EXPIRE` on a `cp:lock:*` key is rejected; use `CP.LOCK.RENEW` (holder-only).
- A `TTL_TICK` log entry is appended every 100ms when otherwise idle, ensuring expiry happens without user traffic.

### 9.5 Dispatcher Routing Rules

Define the **Redis-compat-for-CP set** as: `SET`, `GET`, `DEL`, `EXISTS`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `SETEX`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `TYPE`.

Routing rules, evaluated in order:

1. Command name starts with `CP.`:
   - If first key argument starts with `cp:` → **CP engine**
   - Else → `-NOTCP` (CP verbs must target the `cp:*` namespace)
2. Else, first key argument starts with `cp:`:
   - If command is in the Redis-compat-for-CP set → **CP engine**
   - Else → `-NOTCP` (the `cp:*` namespace only accepts the compat set; e.g., `LPUSH cp:foo bar` is rejected rather than silently routed to AP)
3. Else → **AP engine**.

This enforces constraint **C16** — the `cp:*` namespace is exclusively owned by the CP engine, and no command leaks across engines based on user mistakes.

---

## 10. Tests That Must Always Pass

Organized by area. Every milestone keeps all prior tests green.

### 10.1 FencedLock Tests

| Test | Asserts |
|---|---|
| `lock_try_acquire_release_roundtrip` | TRY → ok + token; UNLOCK with token → ok; STATE shows unowned |
| `lock_mutual_exclusion` | Two concurrent TRY from different sessions → exactly one ok |
| `lock_fencing_token_monotonic` | 100 acquire/release cycles → tokens strictly increasing |
| `lock_reentrant_same_session` | Same session TRY twice → both ok, same token; need 2 UNLOCKs to release |
| `lock_unlock_wrong_session_rejected` | UNLOCK from non-holder → `-REENTRANCE`, lock unchanged |
| `lock_unlock_wrong_token_rejected` | UNLOCK with stale token → `-REENTRANCE`, lock unchanged |
| `lock_ttl_expires` | TRY with ttl=1000ms → wait 2s → STATE shows unowned |
| `lock_ttl_renew` | TRY ttl=1000 → RENEW ttl=5000 → wait 2s → still held |
| `lock_renew_by_non_holder_rejected` | RENEW from non-holder → error, lease unchanged |
| `lock_force_unlock_overrides` | FORCE_UNLOCK → holder released; next TRY succeeds |

### 10.2 AtomicLong Tests

| Test | Asserts |
|---|---|
| `long_set_get_roundtrip` | SET 42 → GET → 42 |
| `long_incr_decr` | INCR on missing → 1; INCR on 10 → 11; DECR → 10 |
| `long_cas_success` | CAS(0, 5) when value is 0 → ok, value is 5 |
| `long_cas_failure` | CAS(0, 5) when value is 1 → not ok, value is 1 |
| `long_concurrent_incr_linearizable` | N concurrent INCR from N clients → final value is N |
| `long_redis_compat_incr` | `INCR cp:counter:x` works identically to `CP.LONG.INCR x` |
| `long_ttl_expires` | `SET cp:counter:x 5 EX 1` → wait 2s → GET returns nil |

### 10.3 Semaphore Tests

| Test | Asserts |
|---|---|
| `sem_init_acquire_release` | INIT 5 → ACQUIRE 2 → AVAILABLE 3 → RELEASE 2 → AVAILABLE 5 |
| `sem_over_acquire_fails` | INIT 3 → ACQUIRE 5 → ok=false |
| `sem_over_release_rejected` | ACQUIRE 2 → RELEASE 3 → error, state unchanged |
| `sem_session_death_releases` | ACQUIRE 2 → kill session → wait session_timeout → AVAILABLE restored |
| `sem_drain` | INIT 5 → DRAIN → 5; AVAILABLE 0 |
| `sem_concurrent_acquire_exactly_permits_succeed` | INIT 3, 10 concurrent ACQUIRE(1) → exactly 3 succeed |

### 10.4 CountDownLatch Tests

| Test | Asserts |
|---|---|
| `latch_set_down_get` | SET 3 → DOWN → 2 → DOWN → 1 → DOWN → 0 |
| `latch_down_at_zero_stays_zero` | At 0 → DOWN → still 0 |
| `latch_reset_only_at_zero` | Count is 2 → RESET 5 → error; count becomes 0 → RESET 5 → ok |
| `latch_concurrent_down_correct_count` | SET 100, 100 concurrent DOWN → final count 0 |

### 10.5 AtomicReference Tests

| Test | Asserts |
|---|---|
| `ref_set_get_roundtrip` | SET "hello" → GET → "hello" |
| `ref_cas_byte_equality` | CAS matches only on byte-exact equality |
| `ref_concurrent_cas_exactly_one_wins` | N clients CAS with same `expected` → exactly one succeeds |

### 10.6 Session Tests

| Test | Asserts |
|---|---|
| `session_create_heartbeat_close` | CREATE → sid; HEARTBEAT → ok; CLOSE → ok |
| `session_timeout_closes` | CREATE → stop heartbeats → wait > timeout → all its resources released |
| `session_op_without_session_rejected` | `CP.LOCK.TRY` with invalid sid → `-NOSESSION` |

### 10.7 Raft / Cluster Tests

| Test | Asserts |
|---|---|
| `cp_minority_failure_available` | Kill 1 of 3 CP members → CP ops still succeed |
| `cp_majority_failure_unavailable` | Kill 2 of 3 CP members → CP ops time out, never false-succeed |
| `cp_leader_failover_preserves_state` | Hold lock → kill leader → new leader → lock still held, same token |
| `cp_non_leader_forwards` | Send `CP.LOCK.TRY` to AP-only node → succeeds (forwarded to leader) |
| `cp_notleader_hint_on_follower` | Send to CP follower directly → `-NOTLEADER <hint>`, hint points at real leader |
| `cp_snapshot_restore_roundtrip` | Populate state → trigger snapshot → restart member from snapshot → state matches |

### 10.8 Dispatcher / Routing Tests

| Test | Asserts |
|---|---|
| `dispatch_cp_verb_routes_to_cp` | `CP.LONG.INCR cp:x` → CP engine; AP engine never sees it |
| `dispatch_cp_prefix_routes_to_cp` | `INCR cp:x` → CP engine; same result as explicit verb |
| `dispatch_ap_key_routes_to_ap` | `INCR x` (no prefix) → AP engine |
| `dispatch_cp_verb_bad_namespace_rejected` | `CP.LONG.INCR foo` (missing prefix) → `-NOTCP` |
| `dispatch_unsupported_redis_cmd_on_cp_rejected` | `LPUSH cp:foo a` → `-NOTCP` (CP doesn't support LPUSH) |

### 10.9 Invariant Tests

| Test | Asserts invariant |
|---|---|
| `invariant_fencing_token_monotonic_under_chaos` | I14 — across leader changes, kills, snapshots |
| `invariant_mutual_exclusion_under_chaos` | I13 — two clients never both hold the same lock |
| `invariant_session_release_complete` | I15 — no orphaned resources after session death |
| `invariant_linearizable_ops` | I20 — Jepsen-style checker confirms linearizability (simple case; no full Jepsen) |

---

## 11. Reading List

### Consensus & Raft

| Resource | Pages | Relevant to |
|---|---|---|
| **In Search of an Understandable Consensus Algorithm** (Ongaro & Ousterhout, 2014) | 16 | Raft — leader election, log replication, safety, log compaction |
| **Consensus: Bridging Theory and Practice** (Ongaro, Stanford PhD thesis, 2014) — ch. 3–4, 6 | ~50 | The thesis version — membership changes, client interaction, leader leases |
| **MicroRaft documentation and source** | ~500 LOC core | How MicroRaft exposes state machines, handles snapshots, client sessions |
| **Hazelcast CP Subsystem blog series** (2019–2020, by Metin Dumandag) | ~30 pages total | The reference architecture — design decisions for FencedLock, session lifecycle, multi-group sharding |
| **How to do distributed locking** (Martin Kleppmann, 2016) | ~8 page blog post | Why naive `SET NX` is unsafe; fencing tokens as the fix |

### Linearizability & Correctness

| Resource | Pages | Relevant to |
|---|---|---|
| **Linearizability: A Correctness Condition for Concurrent Objects** (Herlihy & Wing, 1990) | ~30 | Formal definition of the guarantee Raft provides |
| **DDIA Ch. 9** (Kleppmann, 2017) — Consistency and Consensus | ~40 | Practical grounding; compares Raft/Paxos/Zab/etc. |

### Session Semantics & Ephemeral State

| Resource | Pages | Relevant to |
|---|---|---|
| **ZooKeeper: Wait-free coordination for Internet-scale systems** (Hunt et al., 2010) | ~14 | The ephemeral-node model that inspired session-held resources |
| **Chubby** (Burrows, 2006) | ~16 | Google's lock service — session leases, fail-over semantics |

---

## 12. Milestones (P5 Sketch)

Detailed sub-phase plan lives in `docs/plans/p5-cp-subsystem.md` (written separately via the writing-plans skill). High-level shape:

| M | Theme | Exit criteria |
|---|---|---|
| **P5A** | MicroRaft integration + single state machine (AtomicLong) | 3-node Raft group forms, `CP.LONG.INCR` works, minority-failure available |
| **P5B** | FencedLock + fencing tokens + lease TTL | `CP.LOCK.TRY/UNLOCK/RENEW` works; tokens monotonic across leader changes |
| **P5C** | Sessions + session-tied resource release | Kill client → locks/permits released after timeout |
| **P5D** | Semaphore + CountDownLatch + AtomicReference | Remaining primitives; all §10 tests pass |
| **P5E** | Command dispatcher + Redis-compat routing + RESP wiring | `redis-cli SET cp:counter:x 5 EX 10` works; `CP.*` verbs work |
| **P5F** | Chaos tests + invariant verification | I13–I22 hold under kill/partition; leader-failover correctness |

---

## 13. Open Questions

| # | Question | Blocking? |
|---|---|---|
| 1 | Should `CP.INFO` include per-state-machine size (locks held, semaphores initialized, etc.) for observability? | No — can add later |
| 2 | Should we add blocking `CP.LATCH.AWAIT key timeout_ms` (server-side wait) in a P5 stretch, or defer to a P6? | Defer — polling is enough for P5 |
| 3 | Do we need a `CP.LOCK.LIST` / `CP.LOCK.SCAN` command for observability? | Defer — can inspect via `CP.INFO` |
| 4 | What is the exact `ReadIndex` / lease-read API surface that MicroRaft exposes? Does it handle leader-stickiness or do we need a read-index round-trip per linearizable read? | Resolve during P5A |
| 5 | How does MicroRaft handle snapshot delivery for newly-joining CP members? (Out of scope for P5, but relevant to future elastic membership.) | Defer |
