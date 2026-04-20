# RaftKV — Phase A Design

**Date:** 2026-04-11
**Project:** RaftKV (new standalone project, sibling to WorkFlow)
**Phase:** A — Correctness
**Target location:** `~/GitHub/Kotlin_Practice/RaftKV/`
**Status:** Design locked, pending implementation plan

---

## 0. Project Context and Decomposition

RaftKV is a learning project to build Raft from scratch in Kotlin and deeply understand log-based replication — the coordinator primitive behind etcd, Consul, CockroachDB, TiDB, and Vitess. It is the follow-up to the WorkFlow engine, chosen to fill a gap: WorkFlow leans on Oracle's MVCC for coordination; RaftKV teaches what databases hide.

The full ambition (dynamic membership, WAL/performance optimizations, Raft-coordinated MySQL cluster) is too large for a single spec and is split into three phases:

| Phase | Theme | Scope |
|---|---|---|
| **A** (this spec) | Correctness | Elections, log replication, persistence, snapshots, linearizable KV, testing ladder, **fixed cluster size** |
| **B** (future) | Production-shaping | Segmented WAL, batched fsync, pipelining, async apply, read-index, **single-server membership changes**, benchmark harness, etcd-raft study |
| **C** (future) | Application pattern | Raft-coordinated MySQL cluster (TiDB/Vitess-style) |

Phase B and Phase C are tracked as future projects. They are **not designed in this spec** and will get their own spec → plan → implementation cycle when the time comes.

---

## 1. Goals, Scope, and Non-Goals

### Goal

Build a Raft-based replicated key-value store in Kotlin that is *provably* correct under adversarial network and crash conditions, through a layered testing strategy culminating in linearizability checking.

### In scope for Phase A

| Feature | Why |
|---|---|
| Leader election with randomized timeouts | Core of Raft |
| Log replication via AppendEntries | Core of Raft |
| Persistence: `currentTerm`, `votedFor`, log entries durably on disk | Crash recovery |
| Snapshot + InstallSnapshot RPC (single-shot, not chunked) | Prevents unbounded log growth |
| Linearizable KV state machine (Get/Put/Delete/CAS) | User-visible guarantee |
| In-process cluster + fake network test harness | Level 2 correctness |
| Fault injection: drop, delay, partition, crash-restart | Actually stresses Raft |
| Linearizability checker on test histories | Level 3 correctness |
| HTTP client API with leader redirect | Real clients, real observability |
| Per-client request ID + dedup table (in state machine) | At-most-once client semantics |

### Fixed decisions

- **Language / runtime:** Kotlin + coroutines
- **Cluster size:** fixed 3 or 5 nodes, configured at startup (no runtime membership changes)
- **Peer transport:** gRPC + protobuf (`grpc-kotlin`)
- **Client transport:** HTTP + JSON (Ktor)
- **Project layout:** standalone Maven multi-module project, no shared build with WorkFlow

### Non-goals (explicit deferments)

All of the following are legitimate Raft/production features but are **out of scope** for Phase A:

- Dynamic membership changes — **Phase B**
- Segmented WAL, batched fsync, pipelining, async apply, read-index — **Phase B**
- Metrics, production deployment, TLS, auth, K8s manifests — **Phase B**
- MySQL integration — **Phase C**
- Real Jepsen test suite (Docker + `iptables` + Clojure)
- TLA+ model checking
- Deterministic simulation testing (TigerBeetle style) — the architecture makes this *possible* in Phase B without rewriting the core
- Performance benchmarking — Phase A cares about "is it right," not "is it fast"
- Learners / non-voting replicas
- Cross-cluster replication

---

## 2. Architecture

### Central principle

> **The Raft state machine is pure. The runtime shell is impure.**

The core is a pure function `(state, event) → (new state, effects)`. It has no threads, no clocks, no sockets, no files. A thin runtime shell wraps it and performs the actual I/O. This is the single most important architectural decision in the project — it is the prerequisite for every level of the testing ladder.

### Three layers

**Layer 1 — Core (`RaftNode`).** Pure Kotlin. Owns `RaftState` (following the Raft paper Figure 2). Exposes exactly one method:

```kotlin
fun step(event: Event, now: Instant): Effects
```

Testable without any runtime at all — feed it events, assert the effects.

**Layer 2 — Runtime shell (`RaftRuntime`).** A single coroutine owns the core and processes events from a `Channel<Event>`. Because only one coroutine ever reaches the core, no locks are needed inside. This is the actor model, analogous to etcd-raft's `Ready`/`Advance` loop but with Kotlin coroutines replacing Go channels.

**Layer 3 — Adapters.** Concrete implementations of the runtime's pluggable interfaces (`Network`, `LogStorage`, `StateStorage`, `StateMachine`, `Clock`). Production adapters use gRPC, files, and real time. Test adapters use a `FakeNetwork`, in-memory storage, and a `VirtualClock`. The core and runtime are identical in both setups — only the adapters swap.

### Data flow

```
                   ┌─────────────────────────────────┐
                   │       RaftNode (pure core)      │
                   │   fun step(event, now): Effects │
                   └────────────▲──────────┬─────────┘
                                │          │
                         Event  │          │  Effects
                                │          ▼
                   ┌────────────┴──────────────────────┐
                   │     RaftRuntime (actor shell)     │
                   │   eventLoop reads Channel<Event>  │
                   └──┬──────┬────────┬───────┬────┬───┘
                      │      │        │       │    │
                      ▼      ▼        ▼       ▼    ▼
                   Network  Log  StateStore  SM  Clock
                  gRPC/fake  file/mem  file/mem  KV  real/virtual
```

### Why one coroutine owns the core

The Raft paper describes state transitions as if they are atomic. Concurrent access to the core from multiple coroutines requires locks everywhere and quickly becomes error-prone. The actor model sidesteps this: one coroutine owns the core, all events go through a channel, all access is serialized. Simpler, faster, more testable.

---

## 3. Core Data Model

This section follows Figure 2 of the Raft paper closely. Deviation is how bugs enter.

### 3.1 Persistent state (must survive restart)

```kotlin
data class PersistentState(
    val currentTerm: Long,             // latest term seen; monotonic
    val votedFor: NodeId?,              // candidateId voted for in currentTerm
    val log: Log                        // 1-indexed (paper convention)
)
```

**Fsync rule.** `currentTerm`, `votedFor`, and any newly appended log entries MUST be durable on disk **before** the node sends a response that acknowledges them. Violating this rule is the classic way to lose elections to split-brain. The runtime shell enforces this via the dispatch order in §4.2.

### 3.2 Volatile state (rebuilt from persistent state)

```kotlin
data class VolatileState(
    val commitIndex: Long,              // highest log index known committed
    val lastApplied: Long               // highest log index applied to state machine
)
```

### 3.3 Leader-only volatile state (reinitialized on election)

```kotlin
data class LeaderState(
    val nextIndex: Map<NodeId, Long>,   // next log index to send to each peer
    val matchIndex: Map<NodeId, Long>   // highest log index known replicated on each peer
)
```

### 3.4 Role state machine

```kotlin
sealed class Role {
    object Follower : Role()
    object Candidate : Role()
    data class Leader(val leaderState: LeaderState) : Role()
}
```

**Transition rules** (enforced inside `step()`):

| From | To | Trigger |
|---|---|---|
| Follower | Candidate | Election timeout without leader contact |
| Candidate | Leader | Majority of votes received |
| Candidate | Follower | Discovered higher term, OR valid AppendEntries from current-term leader |
| Candidate | Candidate | Election timeout → start new election (new term) |
| Leader | Follower | Discovered higher term |
| Any | Follower | Received any RPC with term > currentTerm |

The "any → follower on higher term" rule is enforced at the very top of `step()` as a hard invariant. Forgetting it in one code path is a classic way to cause split-brain.

### 3.5 Log entries

```kotlin
data class LogEntry(
    val term: Long,                     // term when entry was created
    val index: Long,                    // 1-based index
    val command: Command
)

sealed class Command {
    data class KvOp(val op: KvOperation, val requestId: RequestId) : Command()
    object NoOp : Command()
}
```

**The NoOp entry.** On becoming leader, the new leader immediately appends a `NoOp` to its own log. Raft safety requires this: a newly-elected leader cannot commit entries from previous terms directly, only as a side effect of committing an entry from its own term. Without NoOp, a committed-but-not-yet-known entry can hang indefinitely (Figure 8 in the paper).

### 3.6 RPCs

Three RPCs, matching the paper.

**RequestVote** — candidates ask peers for votes.
```kotlin
data class RequestVote(
    val term: Long,
    val candidateId: NodeId,
    val lastLogIndex: Long,
    val lastLogTerm: Long
)
data class RequestVoteResponse(
    val term: Long,
    val voteGranted: Boolean
)
```

**AppendEntries** — leaders replicate entries and heartbeat (with empty `entries`).
```kotlin
data class AppendEntries(
    val term: Long,
    val leaderId: NodeId,
    val prevLogIndex: Long,
    val prevLogTerm: Long,
    val entries: List<LogEntry>,
    val leaderCommit: Long
)
data class AppendEntriesResponse(
    val term: Long,
    val success: Boolean,
    val matchIndex: Long,               // for faster nextIndex updates on success
    val conflictIndex: Long?,           // optimization: speed up log backtracking
    val conflictTerm: Long?
)
```

The `conflictIndex` / `conflictTerm` fields are the paper §5.3 optimization that lets a leader skip back by whole terms on conflict. Without it, re-syncing a stale follower is O(logSize) round-trips. Phase A includes this optimization.

**InstallSnapshot** — used when a follower is so far behind that the leader has compacted the needed entries into a snapshot.
```kotlin
data class InstallSnapshot(
    val term: Long,
    val leaderId: NodeId,
    val lastIncludedIndex: Long,
    val lastIncludedTerm: Long,
    val offset: Long,
    val data: ByteArray,
    val done: Boolean
)
data class InstallSnapshotResponse(
    val term: Long
)
```

Phase A uses **single-shot** InstallSnapshot (the whole snapshot in one RPC). Chunked streaming is a Phase B optimization.

### 3.7 Events and Effects (full list)

```kotlin
sealed class Event {
    data class RpcReceived(val from: NodeId, val rpc: RaftRpc) : Event()
    data class RpcResponse(val from: NodeId, val response: RaftRpcResponse) : Event()
    object ElectionTick : Event()
    object HeartbeatTick : Event()
    data class ClientCommand(val id: RequestId, val command: Command) : Event()
    data class PersistAck(val lastPersistedIndex: Long) : Event()
    data class ApplyAck(val lastAppliedIndex: Long) : Event()
}

data class Effects(
    val sendMessages: List<OutgoingRpc> = emptyList(),
    val persistState: PersistentStateDelta? = null,
    val persistLog: List<LogEntry> = emptyList(),
    val applyToStateMachine: List<LogEntry> = emptyList(),
    val clientResponses: List<ClientResponse> = emptyList(),
    val setElectionTimer: Duration? = null,
    val setHeartbeatTimer: Duration? = null,
    val cancelHeartbeatTimer: Boolean = false,
    val snapshotTrigger: SnapshotTrigger? = null
)
```

### 3.8 Safety invariants

The core must preserve these at all times. They become test assertions.

| Invariant | Meaning |
|---|---|
| **Election Safety** | At most one leader per term |
| **Leader Append-Only** | A leader never overwrites or deletes entries in its own log |
| **Log Matching** | If two logs contain an entry with the same index and term, the logs are identical in all prior entries |
| **Leader Completeness** | If a log entry is committed in term T, it is present in the log of every leader in term ≥ T |
| **State Machine Safety** | If a server has applied a log entry at index i, no other server will ever apply a *different* entry at index i |

---

## 4. Runtime Shell and Adapters

### 4.1 Event loop

```kotlin
class RaftRuntime(
    private val core: RaftNode,
    private val network: Network,
    private val log: LogStorage,
    private val stateStore: StateStorage,
    private val stateMachine: StateMachine,
    private val clock: Clock,
) {
    private val events = Channel<Event>(Channel.UNLIMITED)

    suspend fun run() = coroutineScope {
        launch { pumpNetworkIntoEvents() }
        launch { pumpTimersIntoEvents() }

        for (event in events) {
            val effects = core.step(event, clock.now())
            dispatch(effects)
        }
    }
}
```

One coroutine owns the core. Events arrive from network, timers, and clients — all funneled through a single `Channel<Event>`. No locks.

### 4.2 Fsync ordering — the rule that, if violated, silently breaks safety

The Raft paper Figure 2 requires persistence **before** acknowledging the state change. The runtime's `dispatch()` applies effects in this exact order:

1. `persistState` (write + fsync)
2. `persistLog` (write + fsync)
3. `sendMessages` (network I/O)
4. `applyToStateMachine`
5. `clientResponses`
6. `setElectionTimer` / `setHeartbeatTimer`

Concrete requirements:

| Situation | Order |
|---|---|
| Follower receives AppendEntries with new entries | Persist entries → fsync → send `success=true` |
| Candidate starts election | Persist new term + `votedFor=self` → fsync → send RequestVote |
| Leader appends client command | Persist to own log → fsync → send AppendEntries → only count own match toward quorum **after** fsync |
| Any node grants a vote | Persist `votedFor` → fsync → send `voteGranted=true` |

Tests include a crash-restart harness (see §6.5) that exercises this rule at every dispatch step.

### 4.3 Pluggable interfaces

```kotlin
interface Network {
    suspend fun send(message: OutgoingRpc)
    fun incoming(): Flow<IncomingRpc>
}

interface LogStorage {
    suspend fun append(entries: List<LogEntry>)
    suspend fun read(fromIndex: Long, maxCount: Int): List<LogEntry>
    suspend fun truncateSuffix(fromIndex: Long)
    suspend fun truncatePrefix(throughIndex: Long)
    suspend fun lastIndex(): Long
    suspend fun termAt(index: Long): Long?
}

interface StateStorage {
    suspend fun write(state: PersistentStateDelta)
    suspend fun read(): PersistentStateDelta?
}

interface StateMachine {
    suspend fun apply(command: Command): ApplyResult
    suspend fun snapshot(): ByteArray
    suspend fun restore(snapshot: ByteArray)
}

interface Clock {
    fun now(): Instant
    fun resetElectionTimer(duration: Duration)
    fun resetHeartbeatTimer(duration: Duration)
    fun tickEvents(): Flow<TimerEvent>
}
```

### 4.4 Production adapters

**Network — gRPC.** Protobuf-defined RPCs via `grpc-kotlin`. Unary calls map cleanly to the three Raft RPCs. Coroutine-first.

**LogStorage — file-based.** Append-only binary file with per-entry `[length:u32][crc:u32][protobuf]` framing, plus a fixed-width index file mapping `logIndex → fileOffset`. Phase A uses a **single segment** — no segment rollover or compaction. CRC on every entry catches disk corruption. Truncate is "seek + truncate file" + rewrite index — simple, not fast, correct.

**StateStorage — file-based.** Tiny: just `currentTerm` and `votedFor`. Written atomically: `write tmp → fsync → rename → fsync dir`.

**StateMachine — in-memory KV.** `ConcurrentHashMap<String, ByteArray>` backing Put/Get/Delete/CAS. Snapshots are the whole map serialized with a length-prefixed header + CRC. Snapshots trigger when the log reaches a configurable threshold (default 10,000 entries).

**Clock — real.** `System.nanoTime()` + coroutine-based timers that emit `ElectionTick` / `HeartbeatTick` into the channel.

### 4.5 Test adapters

**FakeNetwork.** Routes messages in-memory between in-process nodes. Supports:

```kotlin
class FakeNetwork {
    fun connect(nodeId: NodeId)
    fun disconnect(nodeId: NodeId)                             // drop all to/from this node
    fun partition(side1: Set<NodeId>, side2: Set<NodeId>)     // split cluster
    fun heal()
    fun setDropProbability(p: Double)
    fun setLatencyRange(min: Duration, max: Duration)
    fun setSeed(seed: Long)                                    // reproducible randomness
    suspend fun deliverOne()                                    // manual delivery
    suspend fun deliverAll()
}
```

Reproducible randomness + manual delivery mode is the foundation of deterministic testing: every fault-injection test logs its seed, and failures can be replayed exactly.

**InMemoryLog / InMemoryStateStore.** Trivial — list-backed.

**VirtualClock.** Priority queue of scheduled timer events. `advance(Duration)` fires timers in order. Together with FakeNetwork, tests complete in milliseconds and are fully deterministic.

### 4.6 Cluster configuration

```yaml
node:
  id: 1
  listen: 0.0.0.0:9091
  data-dir: /var/lib/raftkv/node1

cluster:
  peers:
    - { id: 1, address: node1.local:9091 }
    - { id: 2, address: node2.local:9091 }
    - { id: 3, address: node3.local:9091 }

raft:
  election-timeout-min: 150ms
  election-timeout-max: 300ms
  heartbeat-interval: 50ms
  snapshot-threshold: 10000

client:
  listen: 0.0.0.0:8080
```

Fixed peer list — consistent with Phase A's "no membership changes" scope.

---

## 5. Client API

### 5.1 HTTP REST

Clients talk to any node over HTTP (not gRPC — curl-friendliness matters, and no binary log data is transferred).

```
PUT    /kv/{key}         body: {value}   → 200 {value} | 307 Location: <leader-url>
GET    /kv/{key}                          → 200 {value} | 404 | 307
DELETE /kv/{key}                          → 200          | 307
POST   /kv/{key}/cas     body: {expected, new} → 200 | 409 | 307

GET    /cluster/status                    → { leader, term, commitIndex, nodes: [...] }
```

### 5.2 Leader redirect

Non-leader nodes respond `307 Temporary Redirect` with `Location` pointing to the current leader's client URL. If the node doesn't yet know who the leader is (election in progress), it returns `503` with `Retry-After`.

Rationale for redirect over proxy: simpler, avoids "leader changed mid-proxy" races, keeps the Raft hot path thinner. The cost is one extra round trip per leader change, which is rare.

### 5.3 Request ID and deduplication

Every client command carries `RequestId = (clientId, sequenceNumber)`. The **state machine** (not Raft) tracks per-client last-applied sequence numbers and deduplicates:

```kotlin
class KvStateMachine {
    private val data = HashMap<String, ByteArray>()
    private val lastAppliedSeq = HashMap<ClientId, Long>()
    private val cachedResults = HashMap<RequestId, ApplyResult>()

    fun apply(command: Command.KvOp): ApplyResult {
        val (clientId, seq) = command.requestId
        if (seq <= (lastAppliedSeq[clientId] ?: 0L)) {
            return cachedResults[command.requestId]!!  // already applied
        }
        val result = applyOperation(command.op)
        lastAppliedSeq[clientId] = seq
        cachedResults[command.requestId] = result
        return result
    }
}
```

**Key invariant:** the dedup table is **part of the state machine** — included in snapshots, restored on replay, and replicated by Raft. If it lived outside the state machine, a follower becoming leader would have a different dedup table and could double-apply.

**Cached result retention.** Phase A uses per-client TTL (drop cached results older than N seconds). Phase B can upgrade to client-driven ack.

### 5.4 Linearizable reads

Raft offers several read styles. Phase A uses the simplest: **log-committed reads**. Reads go through the Raft log as a command and are applied in sequence. Slower than read-index but unambiguously linearizable and free of clock assumptions.

| Style | Linearizable? | Cost | Phase |
|---|---|---|---|
| Stale read | No | Free | — |
| **Log-committed read** | **Yes** | **1 AppendEntries round trip** | **A** |
| Read-index | Yes | Less than above | B |
| Lease read | Yes (with clock assumptions) | Free | B |

### 5.5 Client library

A tiny `raftkv-client` module with `RaftKvClient`:
- Caches the current leader; retries on 307.
- Generates monotonic per-client sequence numbers.
- On unknown leader, falls back to a random node with backoff.
- Never reuses sequence numbers across restarts (new client ID per restart — a Phase A simplification).

---

## 6. Testing Strategy

### 6.1 Definition of done

Phase A is "done" when all five gates are green:

| Gate | What it proves |
|---|---|
| **G1** Core unit tests — every `step()` branch | State machine internally correct |
| **G2** In-process cluster tests — all MIT 6.824 Lab 2-equivalent scenarios pass | Raft works under fault injection |
| **G3** Linearizability check — 10,000 random workloads pass under partition + drop | Client-visible history is linearizable |
| **G4** Crash-restart tests — power failure at every dispatch step preserves safety | Persistence and fsync ordering correct |
| **G5** Chaos loop — 1-hour run with random seed, no safety violations | Rare races surface over long horizons |

G1-G4 are CI-gated. G5 is manual.

### 6.2 Level 1 — Core unit tests

Drive `RaftNode.step()` directly. Every branch has at least one test. Every safety invariant from §3.8 has at least one direct assertion. Required tests include:

- Candidate with higher term than follower becomes new leader
- Higher-term AppendEntries causes leader to step down
- NoOp is appended on becoming leader
- commitIndex advances only for entries in current term
- Conflicting entry truncates follower suffix

### 6.3 Level 2 — In-process cluster tests

`TestCluster` hosts N `RaftRuntime` instances sharing a `FakeNetwork` and `VirtualClock`. Required scenarios (ported from MIT 6.824 Lab 2A/2B/2C/2D):

| # | Scenario | Tests |
|---|---|---|
| 1 | Basic leader election | 2A — happy path |
| 2 | Re-election after leader crash | 2A — failover |
| 3 | Concurrent elections with split vote | 2A — randomized timeouts resolve ties |
| 4 | Basic log replication | 2B — AppendEntries happy path |
| 5 | Replication with one dead follower | 2B — majority tolerance |
| 6 | Replication with minority partition | 2B — liveness in majority |
| 7 | Replication with majority partition | 2B — safety (no progress) in minority |
| 8 | Follower catches up after reconnect | 2B — log repair |
| 9 | Leader with stale log cannot win election | 2B — election restriction (safety) |
| 10 | commitIndex does not advance for prior-term entries | 2B — Figure 8 scenario |
| 11 | Persistence across restart | 2C — durable state recovery |
| 12 | Snapshot installation for lagging follower | 2D — InstallSnapshot |
| 13 | Unreliable network: random drops + delays | All — stress test |

Each scenario runs in `VirtualClock` time and completes in milliseconds.

### 6.4 Level 3 — Linearizability check

For each G3 run:

1. Spin up a `TestCluster` with a fault profile (e.g., 30% drop, partitions every 500ms, leader kills every 2s).
2. Spawn N concurrent "clients" doing random Put/Get/CAS operations against random nodes.
3. Log every operation as `(clientId, opId, invocationTime, completionTime, op, result)`.
4. Run history through a Porcupine-port linearizability checker.
5. Assert linearizable; on failure, print the smallest counter-example.

The Porcupine port is ~500 lines of Kotlin in `raftkv-test-harness`.

### 6.5 Level 4 — Crash-restart tests

Target the §4.2 fsync ordering rule.

1. Wrap real `LogStorage`/`StateStorage` in a `CrashableStorage` that records every write.
2. Run a workload for N operations.
3. Choose a random write point to "crash" — truncate everything not yet fsynced.
4. Restart the node from truncated state.
5. Replay the workload.
6. Assert: no committed value is lost.

Run with seeds 1..1000 in CI. Log failing seeds for reproduction.

### 6.6 Level 5 — Chaos loop

Single long-running test combining everything. Runs for 1 hour (configurable). Each iteration: random seed → fresh cluster → random faults → random workload → linearizability check. On failure: log seed + full history. Not CI-gated; run before declaring Phase A done and periodically after refactors.

### 6.7 Not in Phase A

- Real Jepsen — too much infra; in-process linearizability gives ~90% of the value
- TLA+ model checking — maybe Phase B
- Deterministic simulation testing — the pure core architecture *enables* this later without rewriting
- Performance benchmarks — Phase B

---

## 7. Project Layout and Milestones

### 7.1 Repository structure

```
~/GitHub/Kotlin_Practice/RaftKV/
├── pom.xml                           # parent POM
├── README.md
├── docs/
│   ├── design.md                     # this spec (copied in)
│   └── raft-paper-notes.md           # annotated reading notes
│
├── raftkv-core/                      # PURE — zero runtime deps beyond kotlin-stdlib
│   ├── pom.xml
│   └── src/
│       ├── main/kotlin/raftkv/core/
│       │   ├── RaftNode.kt
│       │   ├── RaftState.kt
│       │   ├── Role.kt
│       │   ├── Log.kt
│       │   ├── Rpc.kt
│       │   ├── Event.kt
│       │   └── Effect.kt
│       └── test/kotlin/raftkv/core/
│           └── RaftNodeTest.kt       # Level 1
│
├── raftkv-runtime/                   # IMPURE — event loop + adapters
│   ├── pom.xml                       # deps: core, grpc-kotlin, kotlinx-coroutines
│   └── src/
│       ├── main/kotlin/raftkv/runtime/
│       │   ├── RaftRuntime.kt
│       │   ├── Network.kt
│       │   ├── LogStorage.kt
│       │   ├── StateStorage.kt
│       │   ├── StateMachine.kt
│       │   ├── Clock.kt
│       │   └── proto/raft.proto
│       └── test/kotlin/raftkv/runtime/
│           ├── FakeNetwork.kt
│           ├── VirtualClock.kt
│           ├── InMemoryLog.kt
│           ├── TestCluster.kt
│           └── ClusterTest.kt        # Level 2
│
├── raftkv-server/                    # main(), config, HTTP API
│   ├── pom.xml                       # deps: runtime, ktor
│   └── src/main/kotlin/raftkv/server/
│       ├── Main.kt
│       ├── Config.kt
│       ├── ClientApiServer.kt
│       └── ClusterBootstrap.kt
│
├── raftkv-client/                    # tiny HTTP client library
│   ├── pom.xml
│   └── src/main/kotlin/raftkv/client/
│       └── RaftKvClient.kt
│
└── raftkv-test-harness/              # shared test infra
    ├── pom.xml
    └── src/main/kotlin/raftkv/test/
        ├── Porcupine.kt              # linearizability checker
        ├── FaultInjector.kt
        ├── WorkloadGenerator.kt
        └── ChaosLoop.kt              # G5
```

Module dependency graph:

```
                  ┌─────────────┐
                  │ raftkv-core │  (zero runtime deps)
                  └──────▲──────┘
                         │
                  ┌──────┴──────┐
                  │raftkv-runtime│
                  └──────▲──────┘
                         │
           ┌─────────────┼─────────────┐
           │             │             │
    ┌──────┴────┐ ┌──────┴────┐ ┌──────┴──────┐
    │  server   │ │   client  │ │test-harness │
    └───────────┘ └───────────┘ └─────────────┘
```

`core` has **zero** runtime dependencies beyond `kotlin-stdlib` — compile-time enforcement of the pure-core invariant. Importing gRPC or file I/O in `core` fails the build.

### 7.2 Milestones

| M | Deliverable | Exit criteria |
|---|---|---|
| **M0** | Repo scaffolding — Maven multi-module, CI, README, core type stubs | `mvn package` succeeds; empty `step()` compiles |
| **M1** | Data model — `RaftState`, `Role`, `LogEntry`, RPCs, `Event`, `Effects` | Types compile; serialization tests pass |
| **M2** | Test harness — `FakeNetwork`, `VirtualClock`, `InMemoryLog`, `TestCluster` without Raft logic | Can wire 3 stub nodes, route a message, advance virtual time |
| **M3** | **Leader election** in `step()` | G1 election unit tests; "basic election" + "re-election after crash" + "split vote" L2 scenarios pass |
| **M4** | **Log replication (happy path)** | "basic replication" + "one dead follower" L2 scenarios pass |
| **M5** | **Log repair** — `prevLog*` mismatch, truncate suffix, `conflictIndex`/`conflictTerm` fast-backtrack | "catch up after reconnect" + "stale log cannot win" + Figure 8 scenarios pass |
| **M6** | **Persistence** — `FileLogStorage`, `FileStateStorage`, fsync discipline | G4 crash-restart tests (1000 seeds) pass; "persistence across restart" L2 scenario passes |
| **M7** | **Snapshots** — `KvStateMachine.snapshot()`, log compaction, `InstallSnapshot` | "snapshot installation for lagging follower" L2 scenario passes |
| **M8** | **Client API** — HTTP endpoints, 307 leader redirect, request ID + dedup table | Manual curl demo; dedup replay returns same result |
| **M9** | **Linearizability checker** — Porcupine port | 10,000-op random workload under 30% drop passes (G3 green) |
| **M10** | **Chaos loop** | 1-hour G5 run with zero safety violations |

### 7.3 Reference materials

| Resource | Use |
|---|---|
| Raft paper (Ongaro & Ousterhout, 2014, 16 pages) | Figure 2 is the daily cheat sheet |
| Raft PhD dissertation (Ongaro, 2014) | Ch. 5 (snapshots), Ch. 6 (client interaction) |
| https://raft.github.io/ | Visualization playground |
| etcd-raft source | Reference for `Ready`/`Advance` pattern — **understand, don't copy** |
| MIT 6.824 Lab 2 handouts | Scenario names for L2 tests |
| Porcupine source (`github.com/anishathalye/porcupine`) | Algorithm to port in M9 |

### 7.4 Success signal

Phase A is done when this demo works:

> Start a 5-node cluster. Terminal A hammers Put/Get continuously with a randomized workload, verifying linearizability on the fly. Terminal B kills the leader. Terminal A shows no stalls longer than a second, no errors, no inconsistent reads. Kill a second (non-majority) node — same result. Reconnect one node; it catches up. Trigger a snapshot, kill a node, restart it — it applies the snapshot and catches up. The linearizability check stays green throughout.

That is the moment you have understood log-based replication.

---

## 8. Open Questions

None currently — all design decisions are locked.

Future Phase B/C specs will address:
- Membership change algorithm (single-server vs. joint consensus)
- WAL segmentation, batching, and pipelining details
- Read-index vs. lease-read trade-offs
- MySQL integration model (replicate decisions vs. binlog)
