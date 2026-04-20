# DynaCache P5 — CP Subsystem (Raft-backed Primitives via MicroRaft)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **For this project specifically:** Per `CLAUDE.md` collaboration model, every sub-phase is gated by a **Concept Quiz** (score ≥ 7/10) before any code is written. Do NOT skip the quiz — present the concept from the spec + pre-reading, ask 3–5 "could you design this?" questions, score, then proceed. Quiz prompts are embedded at the start of each sub-phase.

**Goal:** Add a Raft-backed CP subsystem alongside the existing Dynamo AP engine. When this phase is done: a 3-node cluster (CP members = all 3) serves linearizable locks (`CP.LOCK.*`), counters (`CP.LONG.*` + `INCR cp:counter:*`), semaphores, latches, and CAS-on-bytes. Minority failure stays available; majority failure correctly blocks. Fencing tokens are monotonic across leader changes. Sessions release held resources on death.

**Architecture:** New Maven module `dynacache-cp` sits between `dynacache-cluster` and `dynacache-server`. It embeds **MicroRaft** — one Raft group running five state machines (FencedLock, AtomicLong, Semaphore, CountDownLatch, AtomicReference) plus a SessionRegistry. A **CommandDispatcher** at the front of each node routes `CP.*` verbs and `cp:*` keys to the CP engine; everything else goes to the existing AP engine. The two engines share nothing except the dispatcher and gRPC transport. See `docs/design-spec-cp.md` for the full constraint/invariant list (C16–C23, I13–I22).

**Tech Stack:** Adds MicroRaft (`io.microraft:microraft:0.7`, Apache-2.0, embeds Java Raft). Reuses gRPC-Kotlin + Protobuf from the cluster module. No Kotlin-specific Raft wrapper — MicroRaft's Java API is idiomatic enough.

**Plan conventions:**
- `$MVN` = `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn`
- `$ROOT` = `/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/DynaCache`
- Spec references use `§N.M` (→ `docs/design-spec-cp.md`), `C17` = constraint, `I14` = invariant.
- Every sub-phase: quiz gate → tests first → implementation → all prior tests green → commit.

**Pre-reading for P5 (read before starting P5A):**
- **Raft paper** (Ongaro & Ousterhout, 2014) — 16 pages. Focus: §5 leader election, §5.3 log replication, §5.4 safety, §7 log compaction.
- **Raft thesis** (Ongaro, 2014) — ch. 3–4, 6. Deeper on client sessions and linearizable reads (`ReadIndex`).
- **MicroRaft docs + source** (github.com/MicroRaft/MicroRaft) — specifically `StateMachine`, `RaftNode`, `Ops`. ~500 LOC of the core.
- **Hazelcast CP Subsystem blog series** (Metin Dumandag, 2019–2020) — our reference architecture; read FencedLock + session posts.
- **How to do distributed locking** (Kleppmann, 2016) — the fencing-token argument.
- **ZooKeeper paper** (Hunt et al., 2010) — ephemeral-node / session model.
- **DDIA ch. 9** (Kleppmann, 2017) — linearizability, consensus.

---

## Task 0: Create `dynacache-cp` Maven Module

**Files:**
- Create: `$ROOT/dynacache-cp/pom.xml`
- Create: directories `src/main/kotlin/dynacache/cp/`, `src/test/kotlin/dynacache/cp/`, `src/main/proto/`
- Modify: `$ROOT/pom.xml` (add module)
- Modify: `$ROOT/dynacache-server/pom.xml` (replace cluster dep with cp dep)

- [ ] **Step 1: Create module directory structure**

```bash
cd "$ROOT"
mkdir -p dynacache-cp/src/main/kotlin/dynacache/cp
mkdir -p dynacache-cp/src/test/kotlin/dynacache/cp
mkdir -p dynacache-cp/src/main/proto
```

- [ ] **Step 2: Write `dynacache-cp/pom.xml`**

Dependencies: `dynacache-cluster` (which transitively brings `dynacache-engine`, coroutines, gRPC, protobuf), `io.microraft:microraft:0.7`, and JUnit 5 + AssertJ for test scope.

- [ ] **Step 3: Add `<module>dynacache-cp</module>` to parent POM**

Insert between `dynacache-cluster` and `dynacache-server` so build order is: engine → cluster → cp → server.

- [ ] **Step 4: Update `dynacache-server/pom.xml`**

Replace the `dynacache-cluster` dependency with `dynacache-cp` (cp transitively provides cluster).

- [ ] **Step 5: Verify build**

```bash
cd "$ROOT" && $MVN package -q -DskipTests
```

Expected: BUILD SUCCESS, four modules compiled.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(cp): add dynacache-cp Maven module + MicroRaft dependency"
```

---

## Sub-phase P5A: MicroRaft Integration + AtomicLong

**Concept:** MicroRaft is a Java library that implements Raft (leader election, log replication, log compaction, linearizable reads via `ReadIndex`). You embed it as a `RaftNode` per process, plug in:
- A `StateMachine` — `runOperation(commitIndex, op): Object` applies a committed op and returns a result.
- A `RaftModelFactory` + `RaftNodeRuntime` — the I/O layer (how nodes talk to each other, how the log persists).

P5A builds the smallest possible working setup: a single state machine (`AtomicLongStateMachine`) holding `Map<String, Long>`, exposed via `CP.LONG.INCR / GET / CAS`. When this passes, a 3-node Raft group forms, elects a leader, replicates log entries, and survives the loss of one member. This validates the MicroRaft wiring before we pile on more state machines.

**Why AtomicLong first:** No sessions, no TTL, no fencing — just a pure state machine on top of Raft. Isolates the "did I wire MicroRaft correctly?" question from every other moving part.

### Concept Quiz Gate (P5A)

Agent MUST ask and score before starting Task 1. Suggested questions:

1. **Why is replication alone not enough to call a system "linearizable"? What does Raft add beyond N-way replication?** (Targets: understanding of consensus vs. replication — quorum-only is the Dynamo model; Raft adds leader election + log ordering + `ReadIndex`.)
2. **The Raft leader writes an entry to its log, then waits for acks from followers. What exactly must be true before it can return success to the client, and what happens if the leader crashes after committing but before responding?** (Targets: quorum commit rule + client-retry idempotency.)
3. **Why can't a follower serve reads directly from its local state machine without going through Raft?** (Targets: staleness — a partitioned follower's state may be arbitrarily old; linearizability requires confirming current leadership via `ReadIndex` or a no-op commit.)
4. **The AtomicLong state machine applies `INCR` by reading current value, adding 1, writing back. Why is this safe even though multiple followers run this code in parallel?** (Targets: deterministic replay — all replicas apply the same ops in the same order, so they compute identical results.)
5. **We have 3 CP members and the network partitions 1 from 2. Which side can still serve CP writes, and why?** (Targets: majority quorum — the 2-node side forms a majority; the 1-node side cannot elect a leader.)

Score ≥ 7/10 → proceed. Otherwise stop and re-read Raft §5.

### Task 1: MicroRaft Bootstrap — single-node Raft group smoke test

**Files:**
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/RaftRuntime.kt`
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/CpConfig.kt`
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/RaftBootstrapTest.kt`

- [ ] **Step 1: Define `CpConfig` data class**

```kotlin
package dynacache.cp

data class CpConfig(
    val nodeId: String,              // stable ID, e.g. "n1"
    val cpMembers: List<String>,     // all CP member IDs at startup (odd, ≥ 3)
    val sessionTimeoutMs: Long = 15_000,
    val heartbeatMs: Long = 5_000,
    val ttlTickMs: Long = 100,
)
```

- [ ] **Step 2: Write a failing bootstrap test (in-process 3-node Raft group, no gRPC yet)**

```kotlin
package dynacache.cp

import io.microraft.RaftRole
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class RaftBootstrapTest {
    @Test
    fun `3-node Raft group elects exactly one leader`() {
        val cluster = InProcessCpCluster(nodeIds = listOf("n1", "n2", "n3"))
        cluster.start()

        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            val leaders = cluster.nodes.filter { it.raftNode.report.join().result.role == RaftRole.LEADER }
            assertThat(leaders).hasSize(1)
        }

        cluster.shutdown()
    }

    @Test
    fun `leader change on leader crash`() {
        val cluster = InProcessCpCluster(nodeIds = listOf("n1", "n2", "n3"))
        cluster.start()
        val firstLeader = cluster.awaitLeader()
        cluster.kill(firstLeader.nodeId)
        val secondLeader = cluster.awaitLeader(exclude = firstLeader.nodeId)
        assertThat(secondLeader.nodeId).isNotEqualTo(firstLeader.nodeId)
        cluster.shutdown()
    }
}
```

- [ ] **Step 3: Implement `RaftRuntime` wrapping MicroRaft**

Wraps `RaftNode.newBuilder()` construction. Takes a `StateMachine` instance, the config, and a `RaftNodeRuntime` (thread scheduler). Exposes `start()`, `shutdown()`, `replicate(op): CompletableFuture<Result>`, `query(op, QueryPolicy.LINEARIZABLE): CompletableFuture<Result>`.

Study MicroRaft's `LocalRaftGroup` test helper — we imitate it for the in-process test but write our own so we own the shape.

- [ ] **Step 4: Implement `InProcessCpCluster` test harness**

N `RaftRuntime` instances sharing an in-memory `LocalTransport` (direct method calls, no sockets). `awaitLeader(exclude = null)` polls each node's `report()` until one returns `RaftRole.LEADER`. `kill(id)` stops that node's runtime.

- [ ] **Step 5: Run tests — verify pass**

```bash
cd "$ROOT" && $MVN test -pl dynacache-cp -q
```

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(cp): MicroRaft bootstrap — 3-node group elects leader, survives leader crash"
```

### Task 2: `AtomicLongStateMachine` — the first state machine

**Files:**
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/ops/LongOps.kt`
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/sm/AtomicLongStateMachine.kt`
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/sm/AtomicLongStateMachineTest.kt`

Spec refs: §3.2 AtomicLong, §6.2 commands, tests in §10.2.

- [ ] **Step 1: Write failing tests (spec §10.2)**

```kotlin
package dynacache.cp.sm

import dynacache.cp.ops.LongOp
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class AtomicLongStateMachineTest {
    private fun apply(sm: AtomicLongStateMachine, op: LongOp, idx: Long = 1L): Any? =
        sm.runOperation(idx, op)

    @Test
    fun `long_set_get_roundtrip`() {
        val sm = AtomicLongStateMachine()
        apply(sm, LongOp.Set("x", 42))
        assertThat(apply(sm, LongOp.Get("x"))).isEqualTo(42L)
    }

    @Test
    fun `long_incr_decr`() {
        val sm = AtomicLongStateMachine()
        assertThat(apply(sm, LongOp.Incr("x"))).isEqualTo(1L)  // missing → 0 → 1
        apply(sm, LongOp.Set("y", 10))
        assertThat(apply(sm, LongOp.Incr("y"))).isEqualTo(11L)
        assertThat(apply(sm, LongOp.Decr("y"))).isEqualTo(10L)
    }

    @Test
    fun `long_cas_success`() {
        val sm = AtomicLongStateMachine()
        apply(sm, LongOp.Set("x", 0))
        assertThat(apply(sm, LongOp.Cas("x", expected = 0, new = 5))).isEqualTo(true)
        assertThat(apply(sm, LongOp.Get("x"))).isEqualTo(5L)
    }

    @Test
    fun `long_cas_failure`() {
        val sm = AtomicLongStateMachine()
        apply(sm, LongOp.Set("x", 1))
        assertThat(apply(sm, LongOp.Cas("x", expected = 0, new = 5))).isEqualTo(false)
        assertThat(apply(sm, LongOp.Get("x"))).isEqualTo(1L)
    }

    @Test
    fun `long_getadd_returns_old_value`() {
        val sm = AtomicLongStateMachine()
        apply(sm, LongOp.Set("x", 10))
        assertThat(apply(sm, LongOp.GetAdd("x", 5))).isEqualTo(10L)
        assertThat(apply(sm, LongOp.Get("x"))).isEqualTo(15L)
    }

    @Test
    fun `deterministic replay — two SMs fed same ops reach same state`() {
        val ops = listOf(
            LongOp.Set("a", 10),
            LongOp.Incr("a"),
            LongOp.Cas("a", 11, 20),
            LongOp.GetAdd("a", 5),
            LongOp.Decr("a"),
        )
        val sm1 = AtomicLongStateMachine()
        val sm2 = AtomicLongStateMachine()
        ops.forEachIndexed { i, op -> sm1.runOperation((i + 1).toLong(), op) }
        ops.forEachIndexed { i, op -> sm2.runOperation((i + 1).toLong(), op) }
        assertThat(sm1.snapshotState()).isEqualTo(sm2.snapshotState())
    }
}
```

- [ ] **Step 2: Define `LongOp` sealed hierarchy**

```kotlin
package dynacache.cp.ops

import java.io.Serializable

sealed class LongOp : Serializable {
    abstract val key: String
    data class Get(override val key: String) : LongOp()
    data class Set(override val key: String, val value: Long) : LongOp()
    data class Incr(override val key: String) : LongOp()
    data class Decr(override val key: String) : LongOp()
    data class Add(override val key: String, val delta: Long) : LongOp()
    data class Cas(override val key: String, val expected: Long, val new: Long) : LongOp()
    data class GetAdd(override val key: String, val delta: Long) : LongOp()
}
```

`Serializable` matters: MicroRaft serializes ops to the log and over the wire.

- [ ] **Step 3: Implement `AtomicLongStateMachine`**

Implements MicroRaft's `StateMachine` interface. Core structure: `HashMap<String, Long>`. `runOperation(commitIndex, op)` switches on the op type, mutates the map, returns a result value (`Long`, `Boolean`, or `null`).

Also implement `takeSnapshot(commitIndex, snapshotOp)` (emit one op per key as a `Set`) and `installSnapshot(commitIndex, snapshotChunks)` (clear, then replay).

- [ ] **Step 4: Run tests — verify pass**

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(cp): AtomicLongStateMachine — get/set/incr/decr/cas/getadd, deterministic replay"
```

### Task 3: Wire `AtomicLongStateMachine` into the 3-node Raft group — end-to-end

**Files:**
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/CpEngine.kt`
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/CpEngineIncrTest.kt`

- [ ] **Step 1: Write failing end-to-end test**

```kotlin
package dynacache.cp

import dynacache.cp.ops.LongOp
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CpEngineIncrTest {
    @Test
    fun `INCR on cp counter replicates to all 3 members`() {
        val cluster = InProcessCpCluster(listOf("n1", "n2", "n3"))
        cluster.start()
        val leader = cluster.awaitLeader()

        repeat(100) { leader.engine.apply(LongOp.Incr("cp:counter:x")).join() }

        // Read via the leader (linearizable) — should be 100
        assertThat(leader.engine.query(LongOp.Get("cp:counter:x")).join()).isEqualTo(100L)

        // All members' local state machines should agree after applying all committed entries
        cluster.awaitAllApplied()
        for (n in cluster.nodes) {
            assertThat(n.longSm.peek("cp:counter:x")).isEqualTo(100L)
        }
        cluster.shutdown()
    }

    @Test
    fun `minority failure — INCR still succeeds`() {
        val cluster = InProcessCpCluster(listOf("n1", "n2", "n3"))
        cluster.start()
        cluster.awaitLeader()
        cluster.kill("n3")

        val leader = cluster.awaitLeader(exclude = "n3")
        repeat(50) { leader.engine.apply(LongOp.Incr("cp:counter:y")).join() }
        assertThat(leader.engine.query(LongOp.Get("cp:counter:y")).join()).isEqualTo(50L)
        cluster.shutdown()
    }

    @Test
    fun `majority failure — INCR times out or returns NOTLEADER`() {
        val cluster = InProcessCpCluster(listOf("n1", "n2", "n3"))
        cluster.start()
        val firstLeader = cluster.awaitLeader()
        cluster.kill(firstLeader.nodeId)

        // Kill a second member — now only 1 of 3 is alive (minority)
        val survivor = cluster.nodes.first { it.alive && it.nodeId != firstLeader.nodeId }
        val other = cluster.nodes.first { it.alive && it.nodeId != firstLeader.nodeId && it.nodeId != survivor.nodeId }
        cluster.kill(other.nodeId)

        val future = survivor.engine.apply(LongOp.Incr("cp:counter:z"))
        val result = runCatching { future.get(3, java.util.concurrent.TimeUnit.SECONDS) }
        assertThat(result.isFailure).isTrue()  // timeout or NotLeaderException
        cluster.shutdown()
    }
}
```

- [ ] **Step 2: Implement `CpEngine` facade**

```kotlin
package dynacache.cp

import java.util.concurrent.CompletableFuture

class CpEngine(
    private val runtime: RaftRuntime,
) {
    fun apply(op: Any): CompletableFuture<Any?> = runtime.replicate(op)
    fun query(op: Any): CompletableFuture<Any?> = runtime.linearizableQuery(op)
}
```

For P5A it hosts only `AtomicLongStateMachine`. Later sub-phases add more state machines — the dispatcher (inside `runOperation`) will switch on op type.

- [ ] **Step 3: Add a `CompositeStateMachine` that dispatches to sub-state-machines by op type**

```kotlin
package dynacache.cp.sm

class CompositeStateMachine(
    private val longSm: AtomicLongStateMachine,
    // P5B+: lockSm, semSm, latchSm, refSm, sessionRegistry
) : io.microraft.statemachine.StateMachine {
    override fun runOperation(commitIndex: Long, op: Any?): Any? = when (op) {
        is dynacache.cp.ops.LongOp -> longSm.runOperation(commitIndex, op)
        else -> throw IllegalArgumentException("Unknown op type: ${op?.javaClass}")
    }
    // takeSnapshot / installSnapshot delegate to each sub-SM
}
```

This is the extension point for every following sub-phase.

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(cp): CpEngine + CompositeStateMachine — INCR works on 3-node Raft, minority-failure tolerant"
```

### Task 4: gRPC transport for MicroRaft inter-member traffic + client forwarding

**Files:**
- Create: `dynacache-cp/src/main/proto/cp.proto`
- Create: `dynacache-cp/src/main/proto/raft.proto`
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/grpc/RaftGrpcTransport.kt`
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/grpc/CpServiceImpl.kt`
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/grpc/CpGrpcEndToEndTest.kt`

- [ ] **Step 1: Define `raft.proto` — MicroRaft inter-member messages**

Wrap MicroRaft's `AppendEntriesRequest`, `AppendEntriesSuccessResponse`, `AppendEntriesFailureResponse`, `RequestVoteRequest`, `RequestVoteResponse`, `InstallSnapshotRequest`, `TriggerLeaderElectionRequest`. Each wraps the MicroRaft Java type via Java serialization inside a `bytes payload = 1;` field. (MicroRaft internal messages are `Serializable`.)

- [ ] **Step 2: Define `cp.proto` — client-facing CP service**

```protobuf
syntax = "proto3";
package dynacache.cp;

service CpService {
  rpc Apply (ApplyRequest) returns (ApplyResponse);
  rpc Query (ApplyRequest) returns (ApplyResponse);
  rpc GetInfo (InfoRequest) returns (InfoResponse);
  rpc Heartbeat (HeartbeatRequest) returns (HeartbeatResponse);
}

message ApplyRequest  { bytes op_payload = 1; int64 session_id = 2; }
message ApplyResponse {
  oneof result {
    bytes ok_payload = 1;
    NotLeader not_leader = 2;
    string error_code = 3;   // -NOTCP, -WRONGTYPE, -NOSESSION, -REENTRANCE, -CAPACITY
  }
}
message NotLeader { string leader_hint_node_id = 1; string leader_hint_addr = 2; }

message InfoRequest {}
message InfoResponse { string leader = 1; repeated string members = 2; int64 applied_index = 3; int64 snapshot_index = 4; int64 log_size = 5; }

message HeartbeatRequest { int64 session_id = 1; }
message HeartbeatResponse { bool ok = 1; }
```

- [ ] **Step 3: Implement `RaftGrpcTransport`**

Adapter between MicroRaft's `Transport` SPI and gRPC. `send(target, message)` → find the `CpNodeClient` for `target.getId()` → wrap the MicroRaft message as bytes → call gRPC. Incoming messages on the server side → deserialize → call `RaftNode.handle(message)`.

- [ ] **Step 4: Implement `CpServiceImpl` gRPC server**

`apply()` handler: deserialize op payload, check leadership. If follower → return `NotLeader(leader_hint)`. If leader → `raftRuntime.replicate(op)` → await commit → serialize result → return.

`query()` handler: linearizable read via MicroRaft's `QueryPolicy.LINEARIZABLE`.

- [ ] **Step 5: Write end-to-end gRPC test**

Boot 3 processes (in-JVM, but using real gRPC on localhost ports). Client sends `CP.LONG.INCR` to a follower. Follower returns `NotLeader`; client retries against the hint. Second call succeeds.

```kotlin
@Test
fun `follower returns NotLeader hint, retry at hint succeeds`() {
    val cluster = GrpcCpCluster(listOf("n1", "n2", "n3"))
    cluster.start()
    val leader = cluster.awaitLeader()
    val follower = cluster.nodes.first { it.nodeId != leader.nodeId }

    val first = follower.client.apply(LongOp.Incr("cp:counter:x"))
    assertThat(first.isNotLeader).isTrue()
    assertThat(first.leaderHint).isEqualTo(leader.nodeId)

    val second = cluster.nodeById(first.leaderHint).client.apply(LongOp.Incr("cp:counter:x"))
    assertThat(second.value).isEqualTo(1L)
    cluster.shutdown()
}
```

- [ ] **Step 6: Run tests — verify pass**
- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(cp): gRPC transport — MicroRaft inter-member + CpService client endpoint with NotLeader hints"
```

---

## Sub-phase P5B: Log-carried Time + FencedLock + Fencing Tokens

**Concept:** Two hard problems combine here.

**Time in Raft (§5).** Wall-clock time cannot be read from local clocks. Follower clocks may drift; on leader failover, the new leader's clock may be behind the old one, which would cause a TTL to "un-expire." Fix: the leader stamps every log entry with `ts = max(clock_now, last_committed_ts + 1)`, and a background `TTL_TICK` entry is appended every 100 ms to advance time in idle periods. All expiry checks use `last_applied_ts` — never local clock.

**Fencing tokens (Kleppmann 2016).** A lock alone is not safe. Consider: client A acquires, stalls on GC, lock TTL expires, client B acquires, A wakes up and writes to the protected resource. A has the lock "in its head" but lost it at the service. Solution: each successful acquire returns a strictly monotonic token. Protected resources reject writes with stale tokens. DynaCache's `CP.LOCK.TRY` returns `(ok, token)`; `token` is the state machine's per-key counter, which only ever increases — even across crashes, snapshots, and leader changes (invariant I14).

**Why this pair:** FencedLock is the primitive that most justifies the whole CP subsystem. It's also the one where TTL correctness matters most — a lock that un-expires is a mutual-exclusion violation. Getting log-carried time right and fencing-token monotonicity right is the heart of P5.

### Concept Quiz Gate (P5B)

1. **The spec says `ts = max(clock_now, last_committed_ts + 1)`. Why isn't it just `clock_now`? Give a scenario where the `max(...)` is required for correctness.** (Targets: leader failover with clock skew — new leader's clock behind old leader's would make time go backward.)
2. **Why is a `TTL_TICK` log entry necessary? What would break if the system only advanced time on user writes?** (Targets: idle cluster → TTLs never expire → locks never release.)
3. **Explain the "GC pause + TTL expiry" attack on Redis SET NX PX locks. Then explain exactly how a fencing token prevents it.** (Targets: ordering of stale-holder writes at the protected resource; token is monotonic and resource rejects lower-than-seen.)
4. **Why must the fencing token be per-key, not global? And why strictly monotonic rather than just "increasing when acquired"?** (Targets: per-key isolation; strict monotonicity means even reentrance doesn't mint equal tokens across acquire/release pairs.)
5. **A FencedLock state machine is asked: "is this lock expired?" Walk through exactly what value it compares against what. Include what happens during leader failover.** (Targets: compare `lease_expiry` against `last_applied_ts`, never local clock; `last_applied_ts` is recovered from the log on new leader, so comparison is stable.)

Score ≥ 7/10 → proceed.

### Task 5: `LogTimestamp` + `TTL_TICK` entries + `last_applied_ts` tracking

**Files:**
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/time/LogTime.kt`
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/ops/TickOp.kt`
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/time/LogTimeTest.kt`

Spec refs: §5, C19 (timestamp monotonicity), C23 (TTL determinism).

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cp.time

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class LogTimeTest {
    @Test
    fun `leader timestamp is monotonic — never goes backward on clock jumps`() {
        val stamper = LogTimestamper(clock = { 1000L })
        val t1 = stamper.next()
        val t2 = stamper.next()
        assertThat(t2).isGreaterThan(t1)
        // Clock appears to go backward
        val stamper2 = LogTimestamper(clock = { 500L })
        stamper2.observeCommitted(2000L)
        val t3 = stamper2.next()
        assertThat(t3).isGreaterThan(2000L)  // despite clock at 500
    }

    @Test
    fun `observing a higher committed ts bumps our baseline`() {
        val clock = object { var now = 1000L }
        val stamper = LogTimestamper(clock = { clock.now })
        stamper.observeCommitted(5000L)
        val t = stamper.next()
        assertThat(t).isGreaterThanOrEqualTo(5001L)
    }

    @Test
    fun `tick op advances last_applied_ts on idle replica`() {
        val tracker = AppliedTsTracker()
        tracker.onApplied(1L, ts = 1000L)
        tracker.onApplied(2L, ts = 1100L)  // TTL_TICK entry
        assertThat(tracker.lastAppliedTs()).isEqualTo(1100L)
    }
}
```

- [ ] **Step 2: Implement `LogTimestamper`**

```kotlin
package dynacache.cp.time

class LogTimestamper(private val clock: () -> Long) {
    @Volatile private var lastIssued: Long = 0
    fun next(): Long {
        val candidate = maxOf(clock(), lastIssued + 1)
        lastIssued = candidate
        return candidate
    }
    fun observeCommitted(ts: Long) { if (ts > lastIssued) lastIssued = ts }
}
```

One instance per leader. On leader election, `observeCommitted` is called with the `last_applied_ts` recovered from the log, so the new leader never issues a smaller timestamp.

- [ ] **Step 3: Implement `AppliedTsTracker`**

```kotlin
package dynacache.cp.time

class AppliedTsTracker {
    @Volatile private var lastTs: Long = 0
    fun onApplied(index: Long, ts: Long) { if (ts > lastTs) lastTs = ts }
    fun lastAppliedTs(): Long = lastTs
}
```

Every state machine gets a reference to this tracker. `isExpired(leaseExpiry)` = `leaseExpiry <= tracker.lastAppliedTs()`.

- [ ] **Step 4: Define `TickOp` and wire a background `TTL_TICK` append loop**

```kotlin
package dynacache.cp.ops
import java.io.Serializable

/** No-op entry whose sole purpose is to carry a timestamp through the log. */
object TickOp : Serializable { private fun readResolve(): Any = TickOp }
```

A coroutine in `RaftRuntime` (leader only) submits `TickOp` every `CpConfig.ttlTickMs`. Followers apply it just to advance `last_applied_ts`.

- [ ] **Step 5: Run tests — verify pass**
- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(cp): log-carried time — LogTimestamper, AppliedTsTracker, TTL_TICK entry"
```

### Task 6: `FencedLockStateMachine`

**Files:**
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/ops/LockOps.kt`
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/sm/FencedLockStateMachine.kt`
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/sm/FencedLockStateMachineTest.kt`
- Modify: `dynacache-cp/src/main/kotlin/dynacache/cp/sm/CompositeStateMachine.kt` (route `LockOp` to this SM)

Spec refs: §3.1, §6.1, §10.1.

- [ ] **Step 1: Define ops**

```kotlin
package dynacache.cp.ops
import java.io.Serializable

sealed class LockOp : Serializable {
    abstract val key: String
    data class Try(override val key: String, val sessionId: Long, val ttlMs: Long) : LockOp()
    data class Unlock(override val key: String, val sessionId: Long, val token: Long) : LockOp()
    data class Renew(override val key: String, val sessionId: Long, val token: Long, val ttlMs: Long) : LockOp()
    data class State(override val key: String) : LockOp()
    data class ForceUnlock(override val key: String) : LockOp()
}

data class LockState(
    val owner: Long?,          // session id or null
    val token: Long,           // last-issued token for this key (monotonic)
    val leaseExpiry: Long,     // log timestamp
    val reentrance: Int,
)

sealed class LockResult {
    data class Acquired(val token: Long) : LockResult()
    object Rejected : LockResult()
    data class StateView(val state: LockState, val ttlRemainingMs: Long) : LockResult()
}
```

- [ ] **Step 2: Write failing tests (spec §10.1)**

```kotlin
package dynacache.cp.sm

import dynacache.cp.ops.LockOp
import dynacache.cp.ops.LockResult
import dynacache.cp.time.AppliedTsTracker
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class FencedLockStateMachineTest {
    private fun fresh(): Pair<FencedLockStateMachine, AppliedTsTracker> {
        val tracker = AppliedTsTracker()
        return FencedLockStateMachine(tracker) to tracker
    }

    @Test
    fun `lock_try_acquire_release_roundtrip`() {
        val (sm, tr) = fresh()
        tr.onApplied(1, 1_000)
        val r1 = sm.runOperation(1, LockOp.Try("cp:lock:k", sessionId = 1, ttlMs = 10_000))
        assertThat(r1).isInstanceOf(LockResult.Acquired::class.java)
        val token = (r1 as LockResult.Acquired).token

        tr.onApplied(2, 1_100)
        val r2 = sm.runOperation(2, LockOp.Unlock("cp:lock:k", sessionId = 1, token = token))
        assertThat(r2).isEqualTo(true)
    }

    @Test
    fun `lock_mutual_exclusion`() {
        val (sm, tr) = fresh()
        tr.onApplied(1, 1_000)
        val r1 = sm.runOperation(1, LockOp.Try("cp:lock:k", sessionId = 1, ttlMs = 10_000))
        val r2 = sm.runOperation(2, LockOp.Try("cp:lock:k", sessionId = 2, ttlMs = 10_000))
        assertThat(r1).isInstanceOf(LockResult.Acquired::class.java)
        assertThat(r2).isEqualTo(LockResult.Rejected)
    }

    @Test
    fun `lock_fencing_token_monotonic — 100 acquire-release cycles`() {
        val (sm, tr) = fresh()
        var prev = 0L
        for (i in 1..100) {
            tr.onApplied(i * 2L, i * 100L)
            val acquired = sm.runOperation(i * 2L, LockOp.Try("cp:lock:k", sessionId = i.toLong(), ttlMs = 10_000)) as LockResult.Acquired
            assertThat(acquired.token).isGreaterThan(prev)
            prev = acquired.token
            sm.runOperation(i * 2L + 1, LockOp.Unlock("cp:lock:k", sessionId = i.toLong(), token = acquired.token))
        }
    }

    @Test
    fun `lock_reentrant_same_session`() {
        val (sm, tr) = fresh()
        tr.onApplied(1, 1_000)
        val a = sm.runOperation(1, LockOp.Try("cp:lock:k", sessionId = 1, ttlMs = 10_000)) as LockResult.Acquired
        val b = sm.runOperation(2, LockOp.Try("cp:lock:k", sessionId = 1, ttlMs = 10_000)) as LockResult.Acquired
        assertThat(b.token).isEqualTo(a.token)  // same token, reentrance++

        assertThat(sm.runOperation(3, LockOp.Unlock("cp:lock:k", sessionId = 1, token = a.token))).isEqualTo(true)
        val state = sm.runOperation(4, LockOp.State("cp:lock:k")) as LockResult.StateView
        assertThat(state.state.owner).isEqualTo(1L)  // still held — reentrance was 2

        assertThat(sm.runOperation(5, LockOp.Unlock("cp:lock:k", sessionId = 1, token = a.token))).isEqualTo(true)
        val state2 = sm.runOperation(6, LockOp.State("cp:lock:k")) as LockResult.StateView
        assertThat(state2.state.owner).isNull()
    }

    @Test
    fun `lock_unlock_wrong_session_rejected`() {
        val (sm, tr) = fresh()
        tr.onApplied(1, 1_000)
        val a = sm.runOperation(1, LockOp.Try("cp:lock:k", sessionId = 1, ttlMs = 10_000)) as LockResult.Acquired
        val r = sm.runOperation(2, LockOp.Unlock("cp:lock:k", sessionId = 999, token = a.token))
        assertThat(r).isEqualTo(false)  // rejected; higher layer translates to -REENTRANCE
        assertThat((sm.runOperation(3, LockOp.State("cp:lock:k")) as LockResult.StateView).state.owner).isEqualTo(1L)
    }

    @Test
    fun `lock_unlock_wrong_token_rejected`() {
        val (sm, tr) = fresh()
        tr.onApplied(1, 1_000)
        sm.runOperation(1, LockOp.Try("cp:lock:k", sessionId = 1, ttlMs = 10_000))
        val r = sm.runOperation(2, LockOp.Unlock("cp:lock:k", sessionId = 1, token = 99999))
        assertThat(r).isEqualTo(false)
    }

    @Test
    fun `lock_ttl_expires — comparing against last_applied_ts, never wall clock`() {
        val (sm, tr) = fresh()
        tr.onApplied(1, 1_000)
        sm.runOperation(1, LockOp.Try("cp:lock:k", sessionId = 1, ttlMs = 1_000))
        // Advance log time past the lease (via TTL_TICK or user ops)
        tr.onApplied(2, 2_500)
        val next = sm.runOperation(2, LockOp.Try("cp:lock:k", sessionId = 2, ttlMs = 10_000))
        assertThat(next).isInstanceOf(LockResult.Acquired::class.java)  // expired → new acquire wins
        // And the new token must still be strictly greater than the old one
        val old = 1L  // first token
        val newToken = (next as LockResult.Acquired).token
        assertThat(newToken).isGreaterThan(old)
    }

    @Test
    fun `lock_ttl_renew_by_holder`() {
        val (sm, tr) = fresh()
        tr.onApplied(1, 1_000)
        val a = sm.runOperation(1, LockOp.Try("cp:lock:k", sessionId = 1, ttlMs = 1_000)) as LockResult.Acquired
        tr.onApplied(2, 1_500)
        val renew = sm.runOperation(2, LockOp.Renew("cp:lock:k", sessionId = 1, token = a.token, ttlMs = 10_000))
        assertThat(renew).isEqualTo(true)
        tr.onApplied(3, 5_000)  // past original lease but within renewed
        val state = sm.runOperation(3, LockOp.State("cp:lock:k")) as LockResult.StateView
        assertThat(state.state.owner).isEqualTo(1L)
    }

    @Test
    fun `lock_renew_by_non_holder_rejected`() {
        val (sm, tr) = fresh()
        tr.onApplied(1, 1_000)
        val a = sm.runOperation(1, LockOp.Try("cp:lock:k", sessionId = 1, ttlMs = 1_000)) as LockResult.Acquired
        val r = sm.runOperation(2, LockOp.Renew("cp:lock:k", sessionId = 999, token = a.token, ttlMs = 10_000))
        assertThat(r).isEqualTo(false)
    }

    @Test
    fun `lock_force_unlock_overrides`() {
        val (sm, tr) = fresh()
        tr.onApplied(1, 1_000)
        sm.runOperation(1, LockOp.Try("cp:lock:k", sessionId = 1, ttlMs = 10_000))
        sm.runOperation(2, LockOp.ForceUnlock("cp:lock:k"))
        val r = sm.runOperation(3, LockOp.Try("cp:lock:k", sessionId = 2, ttlMs = 10_000))
        assertThat(r).isInstanceOf(LockResult.Acquired::class.java)
    }

    @Test
    fun `fencing token survives snapshot+restore`() {
        val (sm1, tr1) = fresh()
        tr1.onApplied(1, 1_000)
        val a = sm1.runOperation(1, LockOp.Try("cp:lock:k", sessionId = 1, ttlMs = 10_000)) as LockResult.Acquired
        sm1.runOperation(2, LockOp.Unlock("cp:lock:k", sessionId = 1, token = a.token))

        val snapshot = sm1.snapshotState()
        val sm2 = FencedLockStateMachine(tr1)
        sm2.restoreSnapshot(snapshot)

        val next = sm2.runOperation(3, LockOp.Try("cp:lock:k", sessionId = 2, ttlMs = 10_000)) as LockResult.Acquired
        assertThat(next.token).isGreaterThan(a.token)  // I14 — monotonic across snapshot boundary
    }
}
```

- [ ] **Step 3: Implement `FencedLockStateMachine`**

Structure: `HashMap<String, LockState>` plus reference to `AppliedTsTracker`. Key logic:
- `Try(key, sid, ttl)`: if `state == null || isExpired(state.leaseExpiry)` → mint new token = `(state?.token ?: 0) + 1`, set owner = sid, reentrance = 1, leaseExpiry = `now + ttl`. If `state.owner == sid` → reentrance++, return same token. Else → `Rejected`.
- `Unlock(key, sid, token)`: only succeeds if owner == sid AND token == state.token. Decrement reentrance; at 0, clear owner.
- `Renew(key, sid, token, ttl)`: only holder can renew. Extends leaseExpiry. Token stays the same.
- `State(key)`: returns a view; applies expiry cleanup at read time (if `isExpired`, owner = null in the view).
- `ForceUnlock(key)`: clears owner regardless.

**Snapshot format:** serialize the full `HashMap<String, LockState>` via Java serialization inside a single chunk. On restore: replace. The next token for any key is still `state.token + 1` — so monotonicity holds across snapshot boundaries.

- [ ] **Step 4: Wire into `CompositeStateMachine`**

Add a branch `is LockOp -> lockSm.runOperation(...)`.

- [ ] **Step 5: Run tests — verify pass**
- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(cp): FencedLockStateMachine — acquire/unlock/renew/state/force, monotonic tokens, log-time TTL"
```

### Task 7: Leader-failover preserves lock state (I18) + TTL correctness (I19)

**Files:**
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/LockFailoverTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cp

import dynacache.cp.ops.LockOp
import dynacache.cp.ops.LockResult
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class LockFailoverTest {
    @Test
    fun `I18 — leader failover preserves held locks and tokens`() {
        val cluster = InProcessCpCluster(listOf("n1", "n2", "n3"))
        cluster.start()
        val leader = cluster.awaitLeader()

        val acquired = leader.engine.apply(LockOp.Try("cp:lock:x", sessionId = 42, ttlMs = 60_000))
            .get() as LockResult.Acquired
        cluster.awaitAllApplied()

        cluster.kill(leader.nodeId)
        val newLeader = cluster.awaitLeader(exclude = leader.nodeId)

        val state = newLeader.engine.query(LockOp.State("cp:lock:x")).get() as LockResult.StateView
        assertThat(state.state.owner).isEqualTo(42L)
        assertThat(state.state.token).isEqualTo(acquired.token)
        cluster.shutdown()
    }

    @Test
    fun `I14 — fencing token strictly greater across leader change + lease expiry`() {
        val cluster = InProcessCpCluster(listOf("n1", "n2", "n3"))
        cluster.start()
        val leader1 = cluster.awaitLeader()
        val a = leader1.engine.apply(LockOp.Try("cp:lock:x", sessionId = 1, ttlMs = 500)).get() as LockResult.Acquired
        cluster.awaitAllApplied()

        cluster.kill(leader1.nodeId)
        Thread.sleep(1000)  // let lease expire via TTL_TICK on new leader
        val leader2 = cluster.awaitLeader(exclude = leader1.nodeId)
        val b = leader2.engine.apply(LockOp.Try("cp:lock:x", sessionId = 2, ttlMs = 10_000)).get() as LockResult.Acquired
        assertThat(b.token).isGreaterThan(a.token)
        cluster.shutdown()
    }
}
```

- [ ] **Step 2: Run tests — they should pass without new production code**

The monotonic-timestamp + log-carried-time design already guarantees this. If the test fails, debug the `LogTimestamper.observeCommitted(lastAppliedTs)` call on leader election.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "test(cp): leader-failover preserves locks (I18) + fencing token monotonicity across failover (I14)"
```

---

## Sub-phase P5C: Sessions + Session-tied Resource Release

**Concept:** A session ties a client connection to the resources it holds. When a client crashes (or its network partitions away), we can't wait forever — other clients want the lock. But we can't just release immediately — the client might be briefly slow.

Mechanism:
- Every CP op carries a `session_id`. On first use, a `SESSION_CREATE` entry is committed, minting the id.
- Clients send `CP.SESSION.HEARTBEAT sid` every 5 s. Each heartbeat is a Raft log entry (expensive, but necessary so all replicas agree on liveness).
- On every `TTL_TICK`, the leader scans the `SessionRegistry`. Any session with `last_applied_ts - last_heartbeat_ts > session_timeout` (default 15 s = 3 missed heartbeats) triggers a `SESSION_CLOSED(sid)` entry.
- `SESSION_CLOSED` cascades: the apply-loop walks FencedLock + Semaphore state machines, releases every resource held by `sid`, **all in one log entry's apply step** (invariant I15 — atomic cascade).

**Why in the log and not in memory:** every replica must agree on which sessions are alive. Heartbeat-as-log-entry is the only way to make session expiry deterministic across leader failovers. (Cost: O(clients × heartbeat_rate) log entries per second. ZooKeeper pays this cost too. Hazelcast batches heartbeats to amortize.)

### Concept Quiz Gate (P5C)

1. **Why does a session heartbeat have to go through the Raft log? What would break if each leader just tracked heartbeats in its own memory?** (Targets: leader change → new leader has no heartbeat state → cascade of false invalidations, OR session state diverges across replicas.)
2. **The spec says SESSION_CLOSED releases "every resource held by sid atomically in one log entry." Why is atomicity required? Give a scenario where a non-atomic cascade is observable by a client.** (Targets: client observes "some of my locks are released, some still held" — mutual exclusion is broken if another client acquires one but not all.)
3. **A client stalls on GC for 20 seconds (longer than the 15s session timeout). When it resumes and sends CP.LONG.INCR with its old session id, what happens, and why is that safe?** (Targets: -NOSESSION rejection; previously-held resources already released; fencing tokens prevent writes-under-assumed-lock from landing at protected resource.)
4. **Why does Semaphore's holder table map `session → permits held` rather than a flat count? What would break if it was just a count?** (Targets: on session death, we need to know *how many* permits to return; without per-session accounting the cascade is ambiguous.)
5. **Could we skip sessions entirely and just use TTLs on every primitive? What does the session model add that TTL alone doesn't?** (Targets: atomic multi-resource release across primitives; one bad heartbeat clock doesn't silently release an in-use lock; explicit liveness signal.)

Score ≥ 7/10 → proceed.

### Task 8: `SessionRegistry` — create / heartbeat / close

**Files:**
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/ops/SessionOps.kt`
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/sm/SessionRegistry.kt`
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/sm/SessionRegistryTest.kt`
- Modify: `dynacache-cp/src/main/kotlin/dynacache/cp/sm/CompositeStateMachine.kt`

Spec refs: §4, §6.6, §10.6, C18, I15.

- [ ] **Step 1: Define ops**

```kotlin
package dynacache.cp.ops
import java.io.Serializable

sealed class SessionOp : Serializable {
    object Create : SessionOp() { private fun readResolve(): Any = Create }
    data class Heartbeat(val sessionId: Long) : SessionOp()
    data class Close(val sessionId: Long) : SessionOp()
    /** Synthesized by the leader's tick loop, never sent by clients. */
    data class ExpireDueSessions(val asOfTs: Long) : SessionOp()
}

data class SessionInfo(val sessionId: Long, val createdAtTs: Long, val lastHeartbeatTs: Long)
```

- [ ] **Step 2: Write failing tests (spec §10.6)**

```kotlin
package dynacache.cp.sm

import dynacache.cp.ops.SessionOp
import dynacache.cp.time.AppliedTsTracker
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class SessionRegistryTest {
    @Test
    fun `session_create_heartbeat_close`() {
        val tr = AppliedTsTracker()
        val reg = SessionRegistry(tr, sessionTimeoutMs = 15_000)
        tr.onApplied(1, 1_000)
        val sid = reg.runOperation(1, SessionOp.Create) as Long
        assertThat(sid).isGreaterThan(0L)

        tr.onApplied(2, 2_000)
        assertThat(reg.runOperation(2, SessionOp.Heartbeat(sid))).isEqualTo(true)

        tr.onApplied(3, 3_000)
        assertThat(reg.runOperation(3, SessionOp.Close(sid))).isEqualTo(true)
        // Invalid after close
        assertThat(reg.isAlive(sid)).isFalse()
    }

    @Test
    fun `session_timeout — ExpireDueSessions closes sessions past timeout`() {
        val tr = AppliedTsTracker()
        val reg = SessionRegistry(tr, sessionTimeoutMs = 15_000)
        tr.onApplied(1, 1_000)
        val sid = reg.runOperation(1, SessionOp.Create) as Long

        tr.onApplied(2, 20_000)
        @Suppress("UNCHECKED_CAST")
        val expired = reg.runOperation(2, SessionOp.ExpireDueSessions(asOfTs = 20_000)) as List<Long>
        assertThat(expired).containsExactly(sid)
        assertThat(reg.isAlive(sid)).isFalse()
    }

    @Test
    fun `session ids are monotonic across restart`() {
        val tr = AppliedTsTracker()
        val reg = SessionRegistry(tr, sessionTimeoutMs = 15_000)
        tr.onApplied(1, 1_000)
        val a = reg.runOperation(1, SessionOp.Create) as Long
        val b = reg.runOperation(2, SessionOp.Create) as Long

        val snap = reg.snapshotState()
        val reg2 = SessionRegistry(tr, sessionTimeoutMs = 15_000)
        reg2.restoreSnapshot(snap)
        val c = reg2.runOperation(3, SessionOp.Create) as Long
        assertThat(c).isGreaterThan(b)
        assertThat(b).isGreaterThan(a)
    }
}
```

- [ ] **Step 3: Implement `SessionRegistry`**

Core structure: `HashMap<Long, SessionInfo>` + `nextSessionId: Long` counter (persisted in snapshot).
- `Create`: mint `++nextSessionId`, insert, return id.
- `Heartbeat(sid)`: update `lastHeartbeatTs = tr.lastAppliedTs()`. Return `true`/`false` if session exists.
- `Close(sid)`: remove from map. Return `true`/`false`. **Note:** downstream cascade (release locks, permits) is NOT the registry's job — it's the CompositeStateMachine's job, which runs `Close`, then calls `lockSm.onSessionClosed(sid)` and `semSm.onSessionClosed(sid)`.
- `ExpireDueSessions(asOfTs)`: scan, find all `sid` where `asOfTs - lastHeartbeatTs > timeout`, close them, return the list.

Expose `isAlive(sid): Boolean` for the CpService gRPC layer to reject ops with `-NOSESSION`.

- [ ] **Step 4: Extend `CompositeStateMachine` to cascade on SESSION_CLOSED**

```kotlin
fun runOperation(commitIndex: Long, op: Any?): Any? = when (op) {
    is SessionOp.Close -> {
        val ok = sessionReg.runOperation(commitIndex, op) as Boolean
        if (ok) cascadeSessionClose((op).sessionId)
        ok
    }
    is SessionOp.ExpireDueSessions -> {
        @Suppress("UNCHECKED_CAST")
        val closed = sessionReg.runOperation(commitIndex, op) as List<Long>
        closed.forEach { cascadeSessionClose(it) }
        closed
    }
    // ... other ops
}

private fun cascadeSessionClose(sid: Long) {
    lockSm.releaseAllHeldBy(sid)
    semSm.releaseAllHeldBy(sid)
}
```

This is the invariant-I15 atomicity: one log entry, one apply step, all releases happen in-memory before `runOperation` returns.

- [ ] **Step 5: Add `releaseAllHeldBy(sid)` to `FencedLockStateMachine`**

Walk the lock map; for every entry with `owner == sid`, clear owner and reset reentrance to 0. Token stays (still monotonic). This is called *from* `CompositeStateMachine.cascadeSessionClose`, not via a separate log entry.

- [ ] **Step 6: Run tests — verify pass**
- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(cp): SessionRegistry + atomic SESSION_CLOSED cascade (I15)"
```

### Task 9: Leader-side session expiry tick + client heartbeat RPC

**Files:**
- Modify: `dynacache-cp/src/main/kotlin/dynacache/cp/RaftRuntime.kt` (add session-expiry tick)
- Modify: `dynacache-cp/src/main/kotlin/dynacache/cp/grpc/CpServiceImpl.kt` (Heartbeat RPC)
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/SessionExpiryTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cp

import dynacache.cp.ops.LockOp
import dynacache.cp.ops.LockResult
import dynacache.cp.ops.SessionOp
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class SessionExpiryTest {
    @Test
    fun `session-held lock released after heartbeat timeout`() {
        val cluster = InProcessCpCluster(
            listOf("n1", "n2", "n3"),
            sessionTimeoutMs = 2_000,
            ttlTickMs = 100,
        )
        cluster.start()
        val leader = cluster.awaitLeader()

        val sid = leader.engine.apply(SessionOp.Create).get() as Long
        leader.engine.applyForSession(sid, LockOp.Try("cp:lock:x", sid, ttlMs = 60_000)).get()

        // Stop heartbeating (don't send any)
        Thread.sleep(3_000)

        val state = leader.engine.query(LockOp.State("cp:lock:x")).get() as LockResult.StateView
        assertThat(state.state.owner).isNull()
        cluster.shutdown()
    }
}
```

- [ ] **Step 2: Add an expiry tick in `RaftRuntime`**

On the leader only, every `ttlTickMs`, append a `SessionOp.ExpireDueSessions(asOfTs = lastAppliedTs)` entry — but only if there are sessions to potentially expire (skip if registry is empty, to avoid log bloat). Note: `TTL_TICK` and `ExpireDueSessions` are distinct — the former advances time, the latter prunes sessions.

- [ ] **Step 3: Implement `CpService.Heartbeat` RPC**

Leader: append `SessionOp.Heartbeat(sid)` to log → return ok after commit.
Follower: same as Apply — return `NotLeader(hint)`.

- [ ] **Step 4: Add a client-side heartbeat loop helper**

```kotlin
package dynacache.cp.client

class SessionHeartbeater(
    private val client: CpClient,
    private val sessionId: Long,
    private val intervalMs: Long = 5_000,
) { /* coroutine that fires heartbeats, handles NotLeader by re-resolving leader */ }
```

- [ ] **Step 5: Run tests — verify pass**
- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(cp): leader-side session-expiry tick + client heartbeat RPC"
```

---

## Sub-phase P5D: Semaphore + CountDownLatch + AtomicReference

**Concept:** Three primitives share the state-machine pattern we built for AtomicLong and FencedLock, with different coordination semantics:

- **Semaphore** — session-tied like FencedLock (permits are released on session death). But unlike locks, a single key can be partially held by many sessions. Holder table = `Map<sessionId, permitsHeld>`.
- **CountDownLatch** — not session-tied. Monotonically decreasing counter that stays at 0. `RESET` only valid at 0 (to prevent race between late DOWN and RESET).
- **AtomicReference** — like AtomicLong but with byte-array values. CAS uses byte-equality on `expected`.

**Why this cluster:** These are "fill in the remaining primitives" work. Each is small; the concepts (session-tied vs. not, byte-CAS) add one new twist per primitive. Speed-running these validates that our framework (state machines + CompositeStateMachine + sessions + snapshots) actually scales to the full set.

### Concept Quiz Gate (P5D)

1. **Why is Semaphore session-tied but CountDownLatch is not? What would go wrong if CountDownLatch was session-tied?** (Targets: latch semantics — the point of DOWN is to signal "I'm done"; releasing on session death would double-count or fail to count.)
2. **In AtomicReference.CAS, we compare `expected` (bytes) to the stored value. Why is byte-equality the right semantic rather than hash-equality or length-equality?** (Targets: CAS correctness — hash collisions would allow false-positive CAS; length-only is trivially wrong.)
3. **A Semaphore with `available = 5` and `holders = {s1: 2, s2: 3}`. Session s1 dies. What is the state after cascade? Why can't we just do `available += 2`?** (Targets: session cascade must remove the holder entry AND add its permits back — otherwise a double-release via explicit RELEASE still leaves the holder entry stale.)
4. **`CP.LATCH.RESET` is rejected when count > 0. Why not allow it to force-reset at any count? Give a scenario where the restriction matters.** (Targets: race where a worker calls DOWN concurrently with a reset — DOWN would decrement the reset value, producing the wrong new count.)
5. **For snapshots: what exactly does AtomicReference include? Think about byte-array values — are there subtleties?** (Targets: deep-copy on snapshot vs. shared reference; Java serialization handles this, but a naive `Map.copy()` doesn't deep-copy values.)

Score ≥ 7/10 → proceed.

### Task 10: `SemaphoreStateMachine`

**Files:**
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/ops/SemOps.kt`
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/sm/SemaphoreStateMachine.kt`
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/sm/SemaphoreStateMachineTest.kt`
- Modify: `CompositeStateMachine.kt`

Spec refs: §3.3, §6.3, §10.3.

- [ ] **Step 1: Define ops**

```kotlin
package dynacache.cp.ops
import java.io.Serializable

sealed class SemOp : Serializable {
    abstract val key: String
    data class Init(override val key: String, val permits: Int) : SemOp()
    data class Acquire(override val key: String, val sessionId: Long, val n: Int) : SemOp()
    data class Release(override val key: String, val sessionId: Long, val n: Int) : SemOp()
    data class Available(override val key: String) : SemOp()
    data class Drain(override val key: String, val sessionId: Long) : SemOp()
}

data class SemState(val available: Int, val holders: Map<Long, Int>)
```

- [ ] **Step 2: Write failing tests (spec §10.3)**

```kotlin
package dynacache.cp.sm

import dynacache.cp.ops.SemOp
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class SemaphoreStateMachineTest {
    @Test
    fun `sem_init_acquire_release`() {
        val sm = SemaphoreStateMachine()
        sm.runOperation(1, SemOp.Init("cp:sem:k", permits = 5))
        assertThat(sm.runOperation(2, SemOp.Acquire("cp:sem:k", sessionId = 1, n = 2))).isEqualTo(true)
        assertThat(sm.runOperation(3, SemOp.Available("cp:sem:k"))).isEqualTo(3)
        assertThat(sm.runOperation(4, SemOp.Release("cp:sem:k", sessionId = 1, n = 2))).isEqualTo(true)
        assertThat(sm.runOperation(5, SemOp.Available("cp:sem:k"))).isEqualTo(5)
    }

    @Test
    fun `sem_over_acquire_fails`() {
        val sm = SemaphoreStateMachine()
        sm.runOperation(1, SemOp.Init("cp:sem:k", permits = 3))
        assertThat(sm.runOperation(2, SemOp.Acquire("cp:sem:k", sessionId = 1, n = 5))).isEqualTo(false)
        assertThat(sm.runOperation(3, SemOp.Available("cp:sem:k"))).isEqualTo(3)
    }

    @Test
    fun `sem_over_release_rejected`() {
        val sm = SemaphoreStateMachine()
        sm.runOperation(1, SemOp.Init("cp:sem:k", permits = 5))
        sm.runOperation(2, SemOp.Acquire("cp:sem:k", sessionId = 1, n = 2))
        // Release more than held by this session
        assertThat(sm.runOperation(3, SemOp.Release("cp:sem:k", sessionId = 1, n = 3))).isEqualTo(false)
        assertThat(sm.runOperation(4, SemOp.Available("cp:sem:k"))).isEqualTo(3)  // unchanged
    }

    @Test
    fun `sem_drain`() {
        val sm = SemaphoreStateMachine()
        sm.runOperation(1, SemOp.Init("cp:sem:k", permits = 5))
        assertThat(sm.runOperation(2, SemOp.Drain("cp:sem:k", sessionId = 1))).isEqualTo(5)
        assertThat(sm.runOperation(3, SemOp.Available("cp:sem:k"))).isEqualTo(0)
    }

    @Test
    fun `sem_session_death_releases — releaseAllHeldBy restores permits`() {
        val sm = SemaphoreStateMachine()
        sm.runOperation(1, SemOp.Init("cp:sem:k", permits = 5))
        sm.runOperation(2, SemOp.Acquire("cp:sem:k", sessionId = 1, n = 2))
        sm.runOperation(3, SemOp.Acquire("cp:sem:k", sessionId = 2, n = 1))
        sm.releaseAllHeldBy(1L)
        assertThat(sm.runOperation(4, SemOp.Available("cp:sem:k"))).isEqualTo(4)  // 5 - 1 (s2 still holds)
    }

    @Test
    fun `sem_init_idempotent — no-op if already exists`() {
        val sm = SemaphoreStateMachine()
        sm.runOperation(1, SemOp.Init("cp:sem:k", permits = 5))
        sm.runOperation(2, SemOp.Acquire("cp:sem:k", sessionId = 1, n = 2))
        sm.runOperation(3, SemOp.Init("cp:sem:k", permits = 100))  // no-op
        assertThat(sm.runOperation(4, SemOp.Available("cp:sem:k"))).isEqualTo(3)
    }
}
```

- [ ] **Step 3: Implement `SemaphoreStateMachine`**

`HashMap<String, SemState>`. Key operations:
- `Init`: if key absent, insert `SemState(permits, emptyMap())`. Else no-op (spec §3.3: idempotent).
- `Acquire(k, sid, n)`: if `available < n` → `false`. Else `available -= n`, `holders[sid] += n` → `true`.
- `Release(k, sid, n)`: if `holders[sid] < n` → `false` (don't modify). Else `holders[sid] -= n` (remove entry if 0), `available += n` → `true`.
- `Drain(k, sid)`: acquire all available, return count.
- `releaseAllHeldBy(sid)`: for every key where `holders[sid] > 0`, `available += held`, `holders.remove(sid)`.

- [ ] **Step 4: Wire into CompositeStateMachine + cascade**
- [ ] **Step 5: Run tests — verify pass**
- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(cp): SemaphoreStateMachine — session-tied holders, idempotent init, session cascade release"
```

### Task 11: `CountDownLatchStateMachine`

**Files:**
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/ops/LatchOps.kt`
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/sm/CountDownLatchStateMachine.kt`
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/sm/CountDownLatchStateMachineTest.kt`
- Modify: `CompositeStateMachine.kt`

Spec refs: §3.4, §6.4, §10.4.

- [ ] **Step 1: Define ops**

```kotlin
package dynacache.cp.ops
import java.io.Serializable

sealed class LatchOp : Serializable {
    abstract val key: String
    data class Set(override val key: String, val count: Int) : LatchOp()
    data class Down(override val key: String) : LatchOp()
    data class Get(override val key: String) : LatchOp()
    data class Reset(override val key: String, val newCount: Int) : LatchOp()
}
```

- [ ] **Step 2: Write failing tests (spec §10.4)**

```kotlin
package dynacache.cp.sm

import dynacache.cp.ops.LatchOp
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CountDownLatchStateMachineTest {
    @Test
    fun `latch_set_down_get`() {
        val sm = CountDownLatchStateMachine()
        sm.runOperation(1, LatchOp.Set("cp:latch:k", 3))
        assertThat(sm.runOperation(2, LatchOp.Down("cp:latch:k"))).isEqualTo(2)
        assertThat(sm.runOperation(3, LatchOp.Down("cp:latch:k"))).isEqualTo(1)
        assertThat(sm.runOperation(4, LatchOp.Down("cp:latch:k"))).isEqualTo(0)
        assertThat(sm.runOperation(5, LatchOp.Get("cp:latch:k"))).isEqualTo(0)
    }

    @Test
    fun `latch_down_at_zero_stays_zero`() {
        val sm = CountDownLatchStateMachine()
        sm.runOperation(1, LatchOp.Set("cp:latch:k", 1))
        sm.runOperation(2, LatchOp.Down("cp:latch:k"))
        assertThat(sm.runOperation(3, LatchOp.Down("cp:latch:k"))).isEqualTo(0)
        assertThat(sm.runOperation(4, LatchOp.Get("cp:latch:k"))).isEqualTo(0)
    }

    @Test
    fun `latch_reset_only_at_zero`() {
        val sm = CountDownLatchStateMachine()
        sm.runOperation(1, LatchOp.Set("cp:latch:k", 3))
        sm.runOperation(2, LatchOp.Down("cp:latch:k"))
        // count is 2, reset not allowed
        assertThat(sm.runOperation(3, LatchOp.Reset("cp:latch:k", newCount = 5))).isEqualTo(false)
        assertThat(sm.runOperation(4, LatchOp.Get("cp:latch:k"))).isEqualTo(2)

        sm.runOperation(5, LatchOp.Down("cp:latch:k"))
        sm.runOperation(6, LatchOp.Down("cp:latch:k"))
        // count is 0, reset allowed
        assertThat(sm.runOperation(7, LatchOp.Reset("cp:latch:k", newCount = 5))).isEqualTo(true)
        assertThat(sm.runOperation(8, LatchOp.Get("cp:latch:k"))).isEqualTo(5)
    }
}
```

- [ ] **Step 3: Implement `CountDownLatchStateMachine`**

`HashMap<String, Int>` of current counts. `Set(k, c)`: put. `Down(k)`: decrement, floor at 0, return new. `Get(k)`: return current or 0 if absent. `Reset(k, new)`: if current != 0 → `false`; else put `new` → `true`.

Not session-tied. No `releaseAllHeldBy` method needed.

- [ ] **Step 4: Wire into CompositeStateMachine**
- [ ] **Step 5: Run tests — verify pass**
- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(cp): CountDownLatchStateMachine — down, reset-only-at-zero"
```

### Task 12: `AtomicReferenceStateMachine`

**Files:**
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/ops/RefOps.kt`
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/sm/AtomicReferenceStateMachine.kt`
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/sm/AtomicReferenceStateMachineTest.kt`
- Modify: `CompositeStateMachine.kt`

Spec refs: §3.5, §6.5, §10.5.

- [ ] **Step 1: Define ops**

```kotlin
package dynacache.cp.ops
import java.io.Serializable

sealed class RefOp : Serializable {
    abstract val key: String
    data class Get(override val key: String) : RefOp()
    data class Set(override val key: String, val value: ByteArray) : RefOp() {
        override fun equals(other: Any?): Boolean = other is Set && key == other.key && value.contentEquals(other.value)
        override fun hashCode(): Int = 31 * key.hashCode() + value.contentHashCode()
    }
    data class Cas(override val key: String, val expected: ByteArray, val new: ByteArray) : RefOp() {
        override fun equals(other: Any?): Boolean = other is Cas && key == other.key &&
            expected.contentEquals(other.expected) && new.contentEquals(other.new)
        override fun hashCode(): Int = 31 * (31 * key.hashCode() + expected.contentHashCode()) + new.contentHashCode()
    }
}
```

- [ ] **Step 2: Write failing tests (spec §10.5)**

```kotlin
package dynacache.cp.sm

import dynacache.cp.ops.RefOp
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class AtomicReferenceStateMachineTest {
    @Test
    fun `ref_set_get_roundtrip`() {
        val sm = AtomicReferenceStateMachine()
        sm.runOperation(1, RefOp.Set("cp:ref:k", "hello".toByteArray()))
        val r = sm.runOperation(2, RefOp.Get("cp:ref:k")) as ByteArray
        assertThat(String(r)).isEqualTo("hello")
    }

    @Test
    fun `ref_cas_byte_equality`() {
        val sm = AtomicReferenceStateMachine()
        sm.runOperation(1, RefOp.Set("cp:ref:k", "a".toByteArray()))
        assertThat(sm.runOperation(2, RefOp.Cas("cp:ref:k", expected = "a".toByteArray(), new = "b".toByteArray()))).isEqualTo(true)
        assertThat(sm.runOperation(3, RefOp.Cas("cp:ref:k", expected = "a".toByteArray(), new = "c".toByteArray()))).isEqualTo(false)
        val r = sm.runOperation(4, RefOp.Get("cp:ref:k")) as ByteArray
        assertThat(String(r)).isEqualTo("b")
    }

    @Test
    fun `ref_cas_against_null_when_absent_succeeds`() {
        val sm = AtomicReferenceStateMachine()
        // expected is empty bytes representing "not set"
        assertThat(sm.runOperation(1, RefOp.Cas("cp:ref:k", expected = ByteArray(0), new = "init".toByteArray()))).isEqualTo(true)
    }
}
```

- [ ] **Step 3: Implement `AtomicReferenceStateMachine`**

`HashMap<String, ByteArray>`. `Get`: return value or `null`. `Set`: put (defensive copy via `value.copyOf()` to avoid caller mutation). `Cas(k, expected, new)`: compare stored to expected via `contentEquals` (treat absent key as empty byte array for expected-empty match); if equal, put new, return `true`.

- [ ] **Step 4: Wire into CompositeStateMachine**
- [ ] **Step 5: Run tests — verify pass**
- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(cp): AtomicReferenceStateMachine — byte-CAS, defensive copy"
```

---

## Sub-phase P5E: Command Dispatcher + Redis-compat Routing

**Concept:** The dispatcher is the glue between RESP/Netty (P1F) and the two engines. It inspects every command and applies spec §9.5's three routing rules:

1. Command starts with `CP.`: if first key starts with `cp:` → CP engine; else → `-NOTCP`.
2. Else first key starts with `cp:`: if command in Redis-compat-for-CP set (SET, GET, DEL, EXISTS, INCR, DECR, INCRBY, DECRBY, SETEX, EXPIRE, PEXPIRE, TTL, PTTL, PERSIST, TYPE) → CP engine; else → `-NOTCP`.
3. Else → AP engine.

Inside the CP engine, a second-level translation maps Redis-compat commands to CP ops by **key-prefix type**:
- `cp:lock:*` — explicit `CP.LOCK.*` verbs only. Any Redis command hitting this prefix → `-WRONGTYPE` (lock cannot be SET/GET).
- `cp:counter:*` — AtomicLong. `SET → LONG_SET`, `GET → LONG_GET`, `INCR → LONG_INCR`, `INCRBY n → LONG_ADD n`, `DECR → LONG_DECR`, `DECRBY n → LONG_ADD -n`, etc.
- `cp:ref:*` — AtomicReference. `SET → REF_SET`, `GET → REF_GET`. `INCR` on ref → `-WRONGTYPE`.
- `cp:sem:*` / `cp:latch:*` — explicit `CP.*` only; Redis ops rejected `-WRONGTYPE`.

The prefix convention in §3 becomes enforced here.

**Why last:** The dispatcher stitches every prior piece together. It also reveals any inconsistencies between the spec's wire protocol and our state-machine ops. Keeping it last means we already know the state machines work in isolation, so dispatcher bugs isolate cleanly.

### Concept Quiz Gate (P5E)

1. **Walk through `LPUSH cp:foo bar` with the three routing rules. What happens and why?** (Targets: rule 1 no (not CP.), rule 2 matches prefix but LPUSH not in compat set → -NOTCP. C16 enforced.)
2. **Walk through `SET cp:counter:x 5 EX 10`. Which engine, which state machine, which op, and what about the EX argument?** (Targets: CP engine (rule 2 + SET in compat set); AtomicLong SM by prefix; translated to `LongOp.Set + TTL_SET` or combined op; EX maps to log-timestamp-based expiry.)
3. **Walk through `CP.LOCK.TRY foo 30000` (missing `cp:` prefix). Why reject rather than auto-prefix?** (Targets: explicit user intent; auto-prefix would silently succeed and break debugging; spec says `-NOTCP`.)
4. **The dispatcher runs before any engine has a chance to object. Why is the *dispatcher* the enforcement point for C16, not each engine internally?** (Targets: one place to audit; engines remain unaware of each other's namespaces; principle of single authority.)
5. **Suppose a user does `SET cp:lock:x somevalue`. What should happen, and how does the CP engine distinguish a Lock prefix from a Counter prefix?** (Targets: rejected `-WRONGTYPE` because cp:lock:* is FencedLock-only and doesn't accept SET; CP engine parses the second path segment.)

Score ≥ 7/10 → proceed.

### Task 13: `CommandDispatcher` — three routing rules

**Files:**
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/dispatcher/CommandDispatcher.kt`
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/dispatcher/CommandDispatcherTest.kt`

Spec refs: §9.5, C16, §10.8.

- [ ] **Step 1: Write failing tests (spec §10.8)**

```kotlin
package dynacache.cp.dispatcher

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CommandDispatcherTest {
    private val dispatcher = CommandDispatcher(
        redisCompatForCp = setOf("SET","GET","DEL","EXISTS","INCR","DECR","INCRBY","DECRBY","SETEX","EXPIRE","PEXPIRE","TTL","PTTL","PERSIST","TYPE"),
    )

    @Test
    fun `dispatch_cp_verb_routes_to_cp`() {
        assertThat(dispatcher.route(listOf("CP.LONG.INCR", "cp:counter:x"))).isEqualTo(Route.Cp)
    }

    @Test
    fun `dispatch_cp_prefix_routes_to_cp`() {
        assertThat(dispatcher.route(listOf("INCR", "cp:counter:x"))).isEqualTo(Route.Cp)
    }

    @Test
    fun `dispatch_ap_key_routes_to_ap`() {
        assertThat(dispatcher.route(listOf("INCR", "x"))).isEqualTo(Route.Ap)
    }

    @Test
    fun `dispatch_cp_verb_bad_namespace_rejected`() {
        assertThat(dispatcher.route(listOf("CP.LONG.INCR", "foo"))).isEqualTo(Route.Error("-NOTCP"))
    }

    @Test
    fun `dispatch_unsupported_redis_cmd_on_cp_rejected`() {
        assertThat(dispatcher.route(listOf("LPUSH", "cp:foo", "a"))).isEqualTo(Route.Error("-NOTCP"))
    }

    @Test
    fun `dispatch_case_insensitive_command_name`() {
        assertThat(dispatcher.route(listOf("cp.long.incr", "cp:counter:x"))).isEqualTo(Route.Cp)
        assertThat(dispatcher.route(listOf("incr", "cp:counter:x"))).isEqualTo(Route.Cp)
    }
}
```

- [ ] **Step 2: Implement `CommandDispatcher`**

```kotlin
package dynacache.cp.dispatcher

sealed class Route {
    object Cp : Route()
    object Ap : Route()
    data class Error(val code: String) : Route()
}

class CommandDispatcher(private val redisCompatForCp: Set<String>) {
    fun route(tokens: List<String>): Route {
        if (tokens.isEmpty()) return Route.Error("-ERR")
        val cmd = tokens[0].uppercase()
        val firstKey = tokens.getOrNull(1)

        if (cmd.startsWith("CP.")) {
            return if (firstKey?.startsWith("cp:") == true) Route.Cp else Route.Error("-NOTCP")
        }
        if (firstKey?.startsWith("cp:") == true) {
            return if (cmd in redisCompatForCp) Route.Cp else Route.Error("-NOTCP")
        }
        return Route.Ap
    }
}
```

Invariant I22 (namespace isolation) is directly enforced by this function.

- [ ] **Step 3: Run tests — verify pass**
- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(cp): CommandDispatcher — three routing rules, namespace isolation (C16, I22)"
```

### Task 14: CP-side RESP translator — Redis command → CP op

**Files:**
- Create: `dynacache-cp/src/main/kotlin/dynacache/cp/dispatcher/CpCommandTranslator.kt`
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/dispatcher/CpCommandTranslatorTest.kt`

Spec refs: §6.2 (AtomicLong dual interface), §6.5 (AtomicReference dual interface), §9.4 (TTL on cp:*).

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cp.dispatcher

import dynacache.cp.ops.LongOp
import dynacache.cp.ops.RefOp
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CpCommandTranslatorTest {
    private val t = CpCommandTranslator()

    @Test
    fun `INCR cp counter — LongOp Incr`() {
        assertThat(t.translate(listOf("INCR", "cp:counter:x"))).isEqualTo(LongOp.Incr("cp:counter:x"))
    }

    @Test
    fun `SET cp counter — LongOp Set`() {
        assertThat(t.translate(listOf("SET", "cp:counter:x", "42"))).isEqualTo(LongOp.Set("cp:counter:x", 42))
    }

    @Test
    fun `INCRBY cp counter — LongOp Add`() {
        assertThat(t.translate(listOf("INCRBY", "cp:counter:x", "5"))).isEqualTo(LongOp.Add("cp:counter:x", 5))
    }

    @Test
    fun `DECRBY cp counter — LongOp Add negative`() {
        assertThat(t.translate(listOf("DECRBY", "cp:counter:x", "3"))).isEqualTo(LongOp.Add("cp:counter:x", -3))
    }

    @Test
    fun `SET cp ref — RefOp Set with bytes`() {
        val op = t.translate(listOf("SET", "cp:ref:x", "hello"))
        assertThat(op).isInstanceOf(RefOp.Set::class.java)
        assertThat((op as RefOp.Set).value).isEqualTo("hello".toByteArray())
    }

    @Test
    fun `GET cp ref — RefOp Get`() {
        assertThat(t.translate(listOf("GET", "cp:ref:x"))).isEqualTo(RefOp.Get("cp:ref:x"))
    }

    @Test
    fun `INCR on cp ref — WRONGTYPE`() {
        assertThrows<WrongTypeException> { t.translate(listOf("INCR", "cp:ref:x")) }
    }

    @Test
    fun `SET on cp lock — WRONGTYPE (locks are explicit-verb-only)`() {
        assertThrows<WrongTypeException> { t.translate(listOf("SET", "cp:lock:x", "something")) }
    }

    @Test
    fun `unknown prefix — WRONGTYPE`() {
        assertThrows<WrongTypeException> { t.translate(listOf("SET", "cp:xxx:k", "v")) }
    }

    @Test
    fun `explicit CP LONG INCR verb — same op as INCR cp counter`() {
        assertThat(t.translate(listOf("CP.LONG.INCR", "cp:counter:x"))).isEqualTo(LongOp.Incr("cp:counter:x"))
    }
}
```

- [ ] **Step 2: Implement `CpCommandTranslator`**

Parses `cp:<type>:<name>` → enum `CpPrimitiveType { LOCK, COUNTER, SEM, LATCH, REF }`. Builds a dispatch table: `(CpPrimitiveType, CommandName) -> (tokens -> Op)`. Unknown combinations → `WrongTypeException` (mapped to `-WRONGTYPE` at the RESP layer).

For TTL commands (EXPIRE, PEXPIRE, TTL, PTTL, PERSIST) on counter/ref keys, produce a small TTL op hierarchy (extend LongOp and RefOp with `SetTtl(key, expireAtTs)` and `GetTtl(key)` variants — compute `expireAtTs = lastAppliedTs + ms` at translation time on the leader). Lock keys reject TTL commands (spec §9.4 — use `CP.LOCK.RENEW` instead).

- [ ] **Step 3: Run tests — verify pass**
- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(cp): CpCommandTranslator — Redis-compat → CP ops by prefix type + explicit CP.* verbs"
```

### Task 15: Wire dispatcher into Netty RESP handler — end-to-end redis-cli demo

**Files:**
- Modify: `dynacache-server/src/main/kotlin/dynacache/server/RespServerHandler.kt` (add dispatcher)
- Modify: `dynacache-server/src/main/kotlin/dynacache/server/Main.kt` (construct CpEngine, wire to handler)
- Create: `dynacache-server/src/test/kotlin/dynacache/server/CpRedisCliSmokeTest.kt`

- [ ] **Step 1: Write failing smoke test using a real RESP client**

```kotlin
package dynacache.server

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis  // test-scope dep

class CpRedisCliSmokeTest {
    @Test
    fun `redis-cli SET INCR GET on cp counter works`() {
        DynacacheTestServer.start(port = 6400)
        Jedis("localhost", 6400).use { j ->
            j.set("cp:counter:x", "5")
            assertThat(j.incr("cp:counter:x")).isEqualTo(6L)
            assertThat(j.get("cp:counter:x")).isEqualTo("6")
        }
        DynacacheTestServer.stop()
    }

    @Test
    fun `redis-cli LPUSH on cp namespace rejected with NOTCP`() {
        DynacacheTestServer.start(port = 6401)
        Jedis("localhost", 6401).use { j ->
            val ex = org.junit.jupiter.api.Assertions.assertThrows(Exception::class.java) {
                j.lpush("cp:foo", "a")
            }
            assertThat(ex.message).contains("NOTCP")
        }
        DynacacheTestServer.stop()
    }

    @Test
    fun `redis-cli SET on non-cp namespace goes to AP engine`() {
        DynacacheTestServer.start(port = 6402)
        Jedis("localhost", 6402).use { j ->
            j.set("foo", "bar")
            assertThat(j.get("foo")).isEqualTo("bar")
        }
        DynacacheTestServer.stop()
    }
}
```

- [ ] **Step 2: Modify `RespServerHandler` to route via dispatcher**

On each command:
1. Call `dispatcher.route(tokens)`.
2. `Route.Ap` → pass to existing AP engine code path (P1F handler).
3. `Route.Cp` → call `CpCommandTranslator` → submit op to `CpEngine.apply()` (or `query()` for reads) → encode result as RESP.
4. `Route.Error("-NOTCP")` → write RESP error and return.

- [ ] **Step 3: Manual 3-node demo**

Start 3 nodes:
```bash
java -jar dynacache-server.jar --node-id 1 --resp-port 6379 --grpc-port 7379 --cp-members 1:localhost:7379,2:localhost:7380,3:localhost:7381
# ... (similar for nodes 2 and 3)
```

Exercise:
```bash
redis-cli -p 6379 SET cp:counter:hits 0
redis-cli -p 6380 INCR cp:counter:hits    # 1 (forwarded if follower, else local leader)
redis-cli -p 6381 GET cp:counter:hits     # 1 (linearizable)
redis-cli -p 6379 CP.LOCK.TRY cp:lock:job 30000    # ok + token
redis-cli -p 6379 LPUSH cp:foo a          # -NOTCP
redis-cli -p 6379 SET foo bar             # OK (AP engine)
redis-cli -p 6379 GET foo                 # "bar"
```

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(server): wire dispatcher — CP + AP engines on one RESP port, redis-cli cp:* works end-to-end"
```

---

## Sub-phase P5F: Chaos + Invariant Verification

**Concept:** Until now, tests have verified each piece under clean conditions. Real distributed systems misbehave under:

- Leader crash mid-commit
- Network partition (minority island cannot commit)
- Long GC pauses / slow followers
- Snapshot + restore mid-workload
- Concurrent clients racing on the same key

Chaos tests run realistic workloads while injecting faults. Invariant tests are the formal statements we promised: I13 (mutual exclusion), I14 (fencing monotonic), I15 (session-death release), I16/I17 (minority-available / majority-blocks), I18 (failover preserves state), I19 (TTL across failover), I20 (snapshot round-trip), I21 (CAS atomicity), I22 (namespace isolation).

A simple linearizability checker: record every operation as `(invocation_time, response_time, op, result)`. Attempt to find a total order that respects real-time constraints (A's response < B's invocation → A before B in the order) and produces the observed results. If no such order exists, the history is not linearizable. This is the Knossos/Jepsen kernel idea, stripped to its essence. We don't need the full Jepsen — a few hundred ops with N=3 clients finds most bugs.

**Why last:** verifying invariants is how we know the prior sub-phases actually work under stress. Any P5 regression surfaces here first.

### Concept Quiz Gate (P5F)

1. **What's the difference between "all tests pass" and "linearizability is verified"? Give an example of a bug that unit tests miss but a linearizability checker catches.** (Targets: unit tests check fixed orderings; lin checker catches interleavings; e.g., stale read returning an old value during leader change with an uncommitted read.)
2. **A chaos test kills the leader every 5 s while 10 clients INCR a counter. After 60 s, the counter should be exactly `10 × ops_per_client`. Why is this an invariant test, not a performance test?** (Targets: no-lost-updates is a correctness property; violation means the CP subsystem is not linearizable.)
3. **We have 100 fencing tokens recorded from 100 acquire/release cycles across 3 leader changes. How do we verify I14 from this history?** (Targets: sort by timestamp → check strictly increasing; record-based rather than online check.)
4. **Why is "lock held by session A, session A dies, lock released" a harder invariant to test than "lock acquired returns true"? What does the test need to observe?** (Targets: requires wall-clock wait for timeout; requires both the kill signal and the release to be causally observable; requires another session to confirm release by acquiring.)
5. **The checker can't verify total linearizability for large histories (it's NP-hard in the general case). What are we giving up by using a "weak" checker with small N?** (Targets: coverage — we find simple violations but might miss subtle ones at scale; justifies keeping chaos tests small + focused.)

Score ≥ 7/10 → proceed.

### Task 16: Chaos harness

**Files:**
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/chaos/ChaosHarness.kt`

- [ ] **Step 1: Implement `ChaosHarness`**

```kotlin
package dynacache.cp.chaos

import dynacache.cp.InProcessCpCluster
import kotlin.random.Random

class ChaosHarness(
    private val cluster: InProcessCpCluster,
    private val seed: Long,
) {
    private val rng = Random(seed)

    fun killRandomLeaderEvery(intervalMs: Long, durationMs: Long) {
        val end = System.currentTimeMillis() + durationMs
        while (System.currentTimeMillis() < end) {
            Thread.sleep(intervalMs)
            val leader = cluster.currentLeader() ?: continue
            cluster.kill(leader.nodeId)
            cluster.restart(leader.nodeId)  // immediately restart so we keep a 3-node group
        }
    }

    fun partitionMinority(durationMs: Long) {
        val minority = cluster.nodes.take((cluster.nodes.size - 1) / 2).map { it.nodeId }
        cluster.isolate(minority)
        Thread.sleep(durationMs)
        cluster.heal()
    }
}
```

Seeded RNG: every chaos run is reproducible. Log the seed; on failure, rerun with that seed to reproduce.

- [ ] **Step 2: Commit**

```bash
git add -A && git commit -m "test(cp): chaos harness — kill-restart leader, partition minority (seeded for reproducibility)"
```

### Task 17: Invariant tests under chaos (spec §10.9)

**Files:**
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/chaos/InvariantTests.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cp.chaos

import dynacache.cp.InProcessCpCluster
import dynacache.cp.ops.LockOp
import dynacache.cp.ops.LockResult
import dynacache.cp.ops.LongOp
import dynacache.cp.ops.SessionOp
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class InvariantTests {
    @Test
    fun `invariant_fencing_token_monotonic_under_chaos (I14)`() {
        val cluster = InProcessCpCluster(listOf("n1","n2","n3"))
        cluster.start()
        val tokens = ConcurrentLinkedQueue<Long>()
        val stop = AtomicBoolean(false)

        val workers = (1..5).map { c ->
            thread {
                while (!stop.get()) {
                    val leader = cluster.awaitLeader()
                    val sid = leader.engine.apply(SessionOp.Create).get() as Long
                    val r = runCatching {
                        leader.engine.applyForSession(sid, LockOp.Try("cp:lock:k", sid, ttlMs = 500)).get()
                    }.getOrNull()
                    if (r is LockResult.Acquired) {
                        tokens.add(r.token)
                        leader.engine.applyForSession(sid, LockOp.Unlock("cp:lock:k", sid, r.token)).get()
                    }
                }
            }
        }

        val chaos = ChaosHarness(cluster, seed = 12345)
        chaos.killRandomLeaderEvery(intervalMs = 2_000, durationMs = 10_000)
        stop.set(true)
        workers.forEach { it.join() }

        val ordered = tokens.toList()
        // All acquired tokens, in acquisition order, must be strictly increasing.
        // (The test records them as soon as acquired; acquisition order is the observed order.)
        for (i in 1 until ordered.size) assertThat(ordered[i]).isGreaterThan(ordered[i - 1])
        cluster.shutdown()
    }

    @Test
    fun `invariant_mutual_exclusion_under_chaos (I13)`() {
        val cluster = InProcessCpCluster(listOf("n1","n2","n3"))
        cluster.start()
        val holders = ConcurrentLinkedQueue<Pair<Long, Long>>()  // (acquireTs, releaseTs)
        val stop = AtomicBoolean(false)

        val workers = (1..10).map {
            thread {
                while (!stop.get()) {
                    val leader = cluster.awaitLeader()
                    val sid = leader.engine.apply(SessionOp.Create).get() as Long
                    val r = runCatching { leader.engine.applyForSession(sid, LockOp.Try("cp:lock:k", sid, ttlMs = 5_000)).get() }.getOrNull()
                    if (r is LockResult.Acquired) {
                        val acq = System.nanoTime()
                        Thread.sleep(10)
                        val rel = System.nanoTime()
                        holders.add(acq to rel)
                        leader.engine.applyForSession(sid, LockOp.Unlock("cp:lock:k", sid, r.token)).get()
                    }
                }
            }
        }

        ChaosHarness(cluster, 555).killRandomLeaderEvery(3_000, 15_000)
        stop.set(true)
        workers.forEach { it.join() }

        // No two hold intervals overlap
        val sorted = holders.toList().sortedBy { it.first }
        for (i in 1 until sorted.size) assertThat(sorted[i].first).isGreaterThanOrEqualTo(sorted[i - 1].second)
        cluster.shutdown()
    }

    @Test
    fun `invariant_no_lost_updates_on_counter (I20 + linearizability)`() {
        val cluster = InProcessCpCluster(listOf("n1","n2","n3"))
        cluster.start()
        val leader = cluster.awaitLeader()
        leader.engine.apply(LongOp.Set("cp:counter:c", 0)).get()

        val perWorker = 200
        val workers = (1..10).map {
            thread { repeat(perWorker) {
                cluster.awaitLeader().engine.apply(LongOp.Incr("cp:counter:c")).get()
            } }
        }
        ChaosHarness(cluster, 777).killRandomLeaderEvery(1_500, 10_000)
        workers.forEach { it.join() }

        val final = cluster.awaitLeader().engine.query(LongOp.Get("cp:counter:c")).get() as Long
        assertThat(final).isEqualTo((10 * perWorker).toLong())
        cluster.shutdown()
    }

    @Test
    fun `invariant_snapshot_restore_roundtrip (I20)`() {
        val cluster = InProcessCpCluster(listOf("n1","n2","n3"), snapshotAfterEntries = 100)
        cluster.start()
        val leader = cluster.awaitLeader()
        repeat(500) { leader.engine.apply(LongOp.Incr("cp:counter:x")).get() }
        val expected = leader.engine.query(LongOp.Get("cp:counter:x")).get() as Long

        cluster.kill("n3")
        cluster.restart("n3")   // n3 recovers via snapshot + log suffix
        cluster.awaitAllApplied()

        val n3 = cluster.nodeById("n3")
        assertThat(n3.longSm.peek("cp:counter:x")).isEqualTo(expected)
        cluster.shutdown()
    }

    @Test
    fun `invariant_session_release_complete (I15)`() {
        val cluster = InProcessCpCluster(listOf("n1","n2","n3"), sessionTimeoutMs = 2_000)
        cluster.start()
        val leader = cluster.awaitLeader()
        val sid = leader.engine.apply(SessionOp.Create).get() as Long
        leader.engine.applyForSession(sid, LockOp.Try("cp:lock:a", sid, 60_000)).get()
        leader.engine.applyForSession(sid, LockOp.Try("cp:lock:b", sid, 60_000)).get()
        // Don't heartbeat — wait for timeout
        Thread.sleep(3_500)
        val a = leader.engine.query(LockOp.State("cp:lock:a")).get() as LockResult.StateView
        val b = leader.engine.query(LockOp.State("cp:lock:b")).get() as LockResult.StateView
        assertThat(a.state.owner).isNull()
        assertThat(b.state.owner).isNull()
        cluster.shutdown()
    }

    @Test
    fun `invariant_namespace_isolation (I22)`() {
        val cluster = InProcessCpCluster(listOf("n1","n2","n3"), withApEngine = true)
        cluster.start()
        val leader = cluster.awaitLeader()
        // AP engine write to 'foo'
        cluster.apClient.set("foo", "ap-value")
        // CP engine write to 'cp:counter:foo' — different key, different engine
        leader.engine.apply(LongOp.Set("cp:counter:foo", 42)).get()

        assertThat(cluster.apClient.get("foo")).isEqualTo("ap-value")
        assertThat(leader.engine.query(LongOp.Get("cp:counter:foo")).get()).isEqualTo(42L)
        // AP engine has no such key:
        assertThat(cluster.apClient.get("cp:counter:foo")).isNull()
        cluster.shutdown()
    }
}
```

- [ ] **Step 2: Fix any surfaced bugs**

These tests WILL find bugs that unit tests miss. Expected hotspots:
- `LogTimestamper.observeCommitted` not called on leader election → token monotonicity fails.
- `releaseAllHeldBy` not called before returning from `runOperation` → session cascade not atomic.
- Snapshot missing the `nextSessionId` / per-key token counters → monotonicity breaks on restart.

Debug each failure. Every fix gets its own commit.

- [ ] **Step 3: Run tests — verify all pass, re-run with different seeds**

```bash
cd "$ROOT" && $MVN test -pl dynacache-cp -q -Dtest=InvariantTests
# Rerun with a different seed to flush out flakes
```

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "test(cp): invariants I13-I22 verified under chaos (kill-restart, partition, snapshot)"
```

### Task 18: Simple linearizability checker

**Files:**
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/chaos/LinearizabilityChecker.kt`
- Create: `dynacache-cp/src/test/kotlin/dynacache/cp/chaos/LinearizabilityCheckerTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cp.chaos

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class LinearizabilityCheckerTest {
    @Test
    fun `accepts a valid register history`() {
        val history = listOf(
            Event(threadId = 1, kind = Event.Kind.INVOKE, op = "set(1)",   time = 1),
            Event(threadId = 1, kind = Event.Kind.RESPOND, op = "set(1)",   time = 2, result = null),
            Event(threadId = 2, kind = Event.Kind.INVOKE, op = "get",      time = 3),
            Event(threadId = 2, kind = Event.Kind.RESPOND, op = "get",     time = 4, result = 1),
        )
        assertThat(LinearizabilityChecker.checkRegister(history)).isTrue()
    }

    @Test
    fun `rejects an impossible history`() {
        val history = listOf(
            Event(1, Event.Kind.INVOKE,  "set(1)", 1),
            Event(1, Event.Kind.RESPOND, "set(1)", 2),
            Event(2, Event.Kind.INVOKE,  "set(2)", 3),
            Event(2, Event.Kind.RESPOND, "set(2)", 4),
            Event(3, Event.Kind.INVOKE,  "get",    5),
            Event(3, Event.Kind.RESPOND, "get",    6, result = 1),   // impossible — 2 was the latest
        )
        assertThat(LinearizabilityChecker.checkRegister(history)).isFalse()
    }
}
```

- [ ] **Step 2: Implement `LinearizabilityChecker.checkRegister`**

Algorithm (Wing-Gong linearizability for a register):
1. Build the partial order: for each pair (op A, op B), if A's respond-time < B's invoke-time, then A precedes B.
2. Recursively try linearization: at each step, pick a minimal pending op whose result is consistent with the current register state; apply it; recurse. Backtrack on contradiction.

This is exponential in the worst case — keep histories small (≤ 30 ops). It's a correctness oracle, not a scaling tool.

- [ ] **Step 3: Integration test — feed a chaos run's CAS-counter history into the checker**

```kotlin
@Test
fun `chaos CAS counter history is linearizable`() {
    val cluster = InProcessCpCluster(listOf("n1","n2","n3"))
    cluster.start()
    val recorder = HistoryRecorder()
    // ... 5 clients, 20 ops each, mix of GET / INCR / CAS / SET ...
    val history = recorder.events()
    assertThat(LinearizabilityChecker.checkRegister(history)).isTrue()
    cluster.shutdown()
}
```

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "test(cp): linearizability checker — register model, Wing-Gong style, verified on chaos history"
```

---

## P5 Exit Criteria

- [ ] `mvn test` — all tests green (P1–P5)
- [ ] 3-node CP group forms, elects leader, minority-failure tolerant (I16)
- [ ] Majority failure correctly blocks CP writes (I17)
- [ ] All §10 primitive tests pass: FencedLock, AtomicLong, Semaphore, CountDownLatch, AtomicReference
- [ ] Fencing tokens strictly monotonic across leader changes, snapshots, and restarts (I14)
- [ ] Leader failover preserves held locks with same owner + token (I18)
- [ ] TTL correctness across leader changes — no un-expiry, no early expiry (I19, C19, C23)
- [ ] Session death releases all held resources atomically in one log entry (I15, C18)
- [ ] Snapshot + restore round-trip produces identical state (I20)
- [ ] CommandDispatcher routes per spec §9.5 with `-NOTCP` rejections (C16, I22)
- [ ] `redis-cli` end-to-end demo: `SET/GET/INCR` on `cp:counter:*`, `CP.LOCK.*`, AP commands all work on the same port
- [ ] Linearizability checker passes on a chaos workload of at least 100 ops with kill-restart injected

When all green: **P5 is done.** Project is complete — DynaCache is now an AP cache with a CP subsystem, validating the full spec (C1–C23, I1–I22).

---

## Open Items Deferred Out of P5

From spec §13:
- **Q2:** Blocking `CP.LATCH.AWAIT key timeout_ms` — defer to a future P6 stretch.
- **Q3:** `CP.LOCK.LIST / CP.LOCK.SCAN` observability — defer.
- **Q4:** `ReadIndex` vs. lease-read decision — resolved early in Task 3 of P5A; documented inline.
- **Q5:** Snapshot delivery for newly-joining CP members — out of scope; dynamic membership deferred.
