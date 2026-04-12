# DynaCache P2 — Distribution: Ring + Gossip + Replication

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the single-node cache from P1 into a 3-node distributed cluster. When this phase is done, clients connect to any node, keys are routed via consistent hashing, gossip detects failures, writes replicate to N nodes with quorum, and DVVs track causality.

**Architecture:** New `dynacache-cluster` Maven module between engine and server. Cluster module owns partitioning, gossip, replication, and DVVs. Server module adds gRPC transport for inter-node communication. The engine stays pure — no cluster awareness.

**Tech Stack:** Adds gRPC-Kotlin, Protobuf, kotlinx-coroutines to the cluster module.

**Plan conventions:** Same as P1. `$MVN`, `$ROOT` as before.

**Pre-reading for P2:**
- Dynamo paper (DeCandia et al., 2007) — full paper, especially §4 (partitioning), §4.8 (replication)
- Consistent Hashing and Random Trees (Karger et al., 1997)
- SWIM paper (Das et al., 2002)
- Dotted Version Vectors (Preguiça et al., 2012)
- Riak DVV source (`dvvset.erl`) — ~300 LOC

---

## Sub-phase 2A: Consistent Hashing + Request Routing

**Concept:** Partition the key space across nodes using a hash ring with virtual nodes. Every node can compute which node owns any key — no central coordinator. Learn: why virtual nodes solve the load-balancing problem, how preference lists provide replication targets.

### Task 1: Create dynacache-cluster module

**Files:**
- Create: `$ROOT/dynacache-cluster/pom.xml`
- Create: directories for `src/main/kotlin/dynacache/cluster/` and `src/test/kotlin/dynacache/cluster/`
- Modify: `$ROOT/pom.xml` (add module)
- Modify: `$ROOT/dynacache-server/pom.xml` (add cluster dependency)

- [ ] **Step 1: Create module POM with deps on engine + coroutines + grpc**

The cluster module depends on `dynacache-engine`, `kotlinx-coroutines-core`, `grpc-kotlin-stub`, and `protobuf-kotlin`.

- [ ] **Step 2: Add `<module>dynacache-cluster</module>` to parent POM**
- [ ] **Step 3: Add cluster dependency to server POM**
- [ ] **Step 4: Verify build**

```bash
cd "$ROOT" && $MVN package -q -DskipTests
```

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(cluster): add dynacache-cluster Maven module"
```

### Task 2: Hash ring with virtual nodes

**Files:**
- Create: `dynacache-cluster/src/main/kotlin/dynacache/cluster/HashRing.kt`
- Create: `dynacache-cluster/src/test/kotlin/dynacache/cluster/HashRingTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cluster

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class HashRingTest {
    @Test
    fun `ring determinism — same nodes produce same owner for any key`() {
        val ring1 = HashRing(listOf(NodeInfo(1, "node1"), NodeInfo(2, "node2"), NodeInfo(3, "node3")), vnodes = 128)
        val ring2 = HashRing(listOf(NodeInfo(1, "node1"), NodeInfo(2, "node2"), NodeInfo(3, "node3")), vnodes = 128)
        repeat(10_000) { i ->
            val key = "key-$i"
            assertThat(ring1.primaryFor(key)).isEqualTo(ring2.primaryFor(key))
        }
    }

    @Test
    fun `preference list returns N distinct physical nodes`() {
        val ring = HashRing(listOf(NodeInfo(1, "n1"), NodeInfo(2, "n2"), NodeInfo(3, "n3")), vnodes = 128)
        val prefs = ring.preferenceList("somekey", n = 3)
        assertThat(prefs).hasSize(3)
        assertThat(prefs.map { it.id }.toSet()).hasSize(3) // distinct
    }

    @Test
    fun `keys distribute roughly evenly across 3 nodes`() {
        val ring = HashRing(listOf(NodeInfo(1, "n1"), NodeInfo(2, "n2"), NodeInfo(3, "n3")), vnodes = 128)
        val counts = mutableMapOf<Int, Int>()
        repeat(30_000) { i ->
            val owner = ring.primaryFor("key-$i")
            counts[owner.id] = (counts[owner.id] ?: 0) + 1
        }
        // Each node should own roughly 10,000 ± 20%
        for ((_, count) in counts) {
            assertThat(count).isBetween(8_000, 12_000)
        }
    }

    @Test
    fun `adding a node only remaps ~1-N of keys`() {
        val nodes3 = listOf(NodeInfo(1, "n1"), NodeInfo(2, "n2"), NodeInfo(3, "n3"))
        val nodes4 = nodes3 + NodeInfo(4, "n4")
        val ring3 = HashRing(nodes3, vnodes = 128)
        val ring4 = HashRing(nodes4, vnodes = 128)

        var remapped = 0
        repeat(10_000) { i ->
            if (ring3.primaryFor("key-$i") != ring4.primaryFor("key-$i")) remapped++
        }
        // Should remap roughly 1/4 = 25% ± some variance
        assertThat(remapped).isBetween(1_500, 4_000)
    }
}
```

- [ ] **Step 2: Run tests — verify fail**
- [ ] **Step 3: Implement HashRing**

```kotlin
package dynacache.cluster

import java.security.MessageDigest
import java.util.TreeMap

data class NodeInfo(val id: Int, val address: String)

class HashRing(
    private val nodes: List<NodeInfo>,
    private val vnodes: Int = 128,
) {
    private val ring = TreeMap<Long, NodeInfo>()

    init {
        for (node in nodes) {
            for (i in 0 until vnodes) {
                val hash = hash("${node.id}:$i")
                ring[hash] = node
            }
        }
    }

    /** Primary owner: first node clockwise from hash(key). */
    fun primaryFor(key: String): NodeInfo {
        val h = hash(key)
        val entry = ring.ceilingEntry(h) ?: ring.firstEntry()
        return entry.value
    }

    /** N distinct physical nodes walking clockwise from hash(key). */
    fun preferenceList(key: String, n: Int): List<NodeInfo> {
        val h = hash(key)
        val result = mutableListOf<NodeInfo>()
        val seen = mutableSetOf<Int>()

        var it = ring.tailMap(h, true).entries.iterator()
        while (result.size < n) {
            if (!it.hasNext()) it = ring.entries.iterator() // wrap around
            val node = it.next().value
            if (seen.add(node.id)) result.add(node)
        }
        return result
    }

    private fun hash(input: String): Long {
        val md = MessageDigest.getInstance("SHA-256")
        val digest = md.digest(input.toByteArray())
        return ((digest[0].toLong() and 0xFF) shl 56) or
               ((digest[1].toLong() and 0xFF) shl 48) or
               ((digest[2].toLong() and 0xFF) shl 40) or
               ((digest[3].toLong() and 0xFF) shl 32) or
               ((digest[4].toLong() and 0xFF) shl 24) or
               ((digest[5].toLong() and 0xFF) shl 16) or
               ((digest[6].toLong() and 0xFF) shl 8) or
               (digest[7].toLong() and 0xFF)
    }
}
```

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(cluster): consistent hash ring — SHA-256, vnodes, preference list"
```

### Task 3: Request router — local execute or forward via gRPC

**Files:**
- Create: `dynacache-cluster/src/main/kotlin/dynacache/cluster/RequestRouter.kt`
- Create: `dynacache-cluster/src/main/proto/cluster.proto`
- Create: `dynacache-cluster/src/test/kotlin/dynacache/cluster/RequestRouterTest.kt`

- [ ] **Step 1: Define cluster.proto**

```protobuf
syntax = "proto3";
package dynacache.cluster;

service ClusterService {
    rpc Forward(ForwardRequest) returns (ForwardResponse);
    rpc Replicate(ReplicateRequest) returns (ReplicateResponse);
    rpc Gossip(GossipMessage) returns (GossipAck);
}

message ForwardRequest {
    repeated string tokens = 1;  // raw RESP command tokens
}

message ForwardResponse {
    bytes resp_payload = 1;  // RESP-encoded response bytes
}

message ReplicateRequest {
    string key = 1;
    bytes value = 2;
    DataType type = 3;
    DvvProto dvv = 4;
    int64 expires_at_ms = 5;
}

message ReplicateResponse {
    bool success = 1;
}

message GossipMessage {
    int32 sender_id = 1;
    repeated MemberState members = 2;
}

message GossipAck {
    repeated MemberState members = 1;
}

message MemberState {
    int32 node_id = 1;
    string address = 2;
    MemberStatus status = 3;
    int64 heartbeat = 4;
}

enum MemberStatus {
    ALIVE = 0;
    SUSPECT = 1;
    DEAD = 2;
}

enum DataType {
    STRING = 0;
    HASH = 1;
    LIST = 2;
    ZSET = 3;
}

message DvvProto {
    int32 dot_node_id = 1;
    int64 dot_counter = 2;
    map<int32, int64> causal_context = 3;
}
```

- [ ] **Step 2: Implement RequestRouter**

The router takes a command, hashes the key, checks if this node is the coordinator. If local: execute on the local engine. If remote: forward via gRPC to the coordinator. For multi-key commands (MGET, MSET, DEL), split by coordinator and aggregate results.

- [ ] **Step 3: Write tests with mock gRPC (in-process)**

Test that keys route to the correct node, forwarding works, and multi-key commands split correctly.

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(cluster): request router — local execute or gRPC forward by hash ring"
```

---

## Sub-phase 2B: SWIM Gossip — Membership + Failure Detection

**Concept:** Nodes discover who is alive without a central authority. SWIM uses random probing: each round, a node pings one random peer. If no ack, it asks K other peers to relay (ping-req). If still no ack, the target is suspected. After T rounds as suspect, it's declared dead. Learn: why random probing scales logarithmically, how piggybacked gossip propagates membership state.

### Task 4: SWIM gossip protocol

**Files:**
- Create: `dynacache-cluster/src/main/kotlin/dynacache/cluster/SwimGossip.kt`
- Create: `dynacache-cluster/src/main/kotlin/dynacache/cluster/MembershipTable.kt`
- Create: `dynacache-cluster/src/test/kotlin/dynacache/cluster/SwimGossipTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cluster

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class SwimGossipTest {
    @Test
    fun `all nodes discover each other after startup`() {
        val cluster = InProcessCluster(nodeCount = 3)
        cluster.runGossipRounds(10)
        for (node in cluster.nodes) {
            assertThat(node.gossip.aliveMembers()).hasSize(3)
        }
    }

    @Test
    fun `dead node detected within O(log N) rounds`() {
        val cluster = InProcessCluster(nodeCount = 5)
        cluster.runGossipRounds(10) // all discover each other
        cluster.kill(nodeId = 3)

        var rounds = 0
        while (cluster.nodes.filter { it.alive }.any { it.gossip.statusOf(3) != MemberStatus.DEAD }) {
            cluster.runGossipRounds(1)
            rounds++
            if (rounds > 20) break
        }
        assertThat(rounds).isLessThanOrEqualTo(15) // O(log 5) ≈ 3, with suspect timeout ~4 rounds
    }

    @Test
    fun `recovered node detected alive again`() {
        val cluster = InProcessCluster(nodeCount = 3)
        cluster.runGossipRounds(10)
        cluster.kill(nodeId = 2)
        cluster.runGossipRounds(15) // node 2 declared dead

        cluster.revive(nodeId = 2)
        cluster.runGossipRounds(10)
        for (node in cluster.nodes.filter { it.alive }) {
            assertThat(node.gossip.statusOf(2)).isEqualTo(MemberStatus.ALIVE)
        }
    }

    @Test
    fun `gossip convergence — membership change propagates to all`() {
        val cluster = InProcessCluster(nodeCount = 5)
        cluster.runGossipRounds(20)
        // Kill node 4, verify all survivors eventually agree
        cluster.kill(nodeId = 4)
        cluster.runGossipRounds(20)
        val survivors = cluster.nodes.filter { it.alive }
        for (node in survivors) {
            assertThat(node.gossip.statusOf(4)).isEqualTo(MemberStatus.DEAD)
        }
    }
}
```

- [ ] **Step 2: Implement MembershipTable**

Tracks `Map<NodeId, MemberState>` with status (ALIVE/SUSPECT/DEAD) and a monotonically increasing heartbeat counter.

- [ ] **Step 3: Implement SwimGossip**

One gossip round:
1. Increment own heartbeat
2. Pick a random peer from membership table (alive or suspect)
3. Send `Ping(senderState)` — includes full membership table (piggybacked)
4. If ack received: merge remote membership into local (highest heartbeat wins)
5. If no ack within RTT: pick K random peers, send `PingReq(target)` through them
6. If still no ack: mark target SUSPECT, start suspect timer (T rounds)
7. After T rounds as SUSPECT without refutation: mark DEAD

- [ ] **Step 4: Create InProcessCluster test harness**

A test utility that runs N gossip instances with an in-memory network (no real sockets). `runGossipRounds(n)` calls each node's gossip tick n times. Messages are delivered synchronously. `kill(nodeId)` stops the node from sending/receiving. `revive(nodeId)` re-enables it.

- [ ] **Step 5: Run tests — verify pass**
- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(cluster): SWIM gossip — ping/ping-req/suspect/dead with piggybacked membership"
```

---

## Sub-phase 2C: Dotted Version Vectors

**Concept:** Track causality of writes without the unbounded growth of classic vector clocks. DVVs assign a (node_id, counter) dot to each write and carry a causal context that represents what the writer has seen. Two DVVs can be compared: one dominates, or they're concurrent. Learn: why per-server tracking bounds DVV size, how the dot+context model works.

### Task 5: DVV implementation

**Files:**
- Create: `dynacache-cluster/src/main/kotlin/dynacache/cluster/Dvv.kt`
- Create: `dynacache-cluster/src/test/kotlin/dynacache/cluster/DvvTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cluster

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class DvvTest {
    @Test
    fun `sequential writes — B dominates A`() {
        val clock = DvvClock(nodeId = 1)
        val a = clock.increment(Dvv.ZERO)
        val b = clock.increment(a)
        assertThat(b.dominates(a)).isTrue()
        assertThat(a.dominates(b)).isFalse()
    }

    @Test
    fun `concurrent writes on different nodes — neither dominates`() {
        val clock1 = DvvClock(nodeId = 1)
        val clock2 = DvvClock(nodeId = 2)
        val a = clock1.increment(Dvv.ZERO)
        val b = clock2.increment(Dvv.ZERO)
        assertThat(a.dominates(b)).isFalse()
        assertThat(b.dominates(a)).isFalse()
        assertThat(a.isConcurrentWith(b)).isTrue()
    }

    @Test
    fun `merge of concurrent DVVs descends from both`() {
        val clock1 = DvvClock(nodeId = 1)
        val clock2 = DvvClock(nodeId = 2)
        val a = clock1.increment(Dvv.ZERO)
        val b = clock2.increment(Dvv.ZERO)
        val merged = Dvv.merge(a, b)
        // A write after the merge should dominate both a and b
        val c = clock1.increment(merged)
        assertThat(c.dominates(a)).isTrue()
        assertThat(c.dominates(b)).isTrue()
    }

    @Test
    fun `DVV size bounded by cluster size, not client count`() {
        val clocks = (1..3).map { DvvClock(nodeId = it) }
        var current = Dvv.ZERO
        // 10,000 writes through 3 nodes
        repeat(10_000) { i ->
            val clock = clocks[i % 3]
            current = clock.increment(current)
        }
        assertThat(current.causalContext.size).isLessThanOrEqualTo(3)
    }

    @Test
    fun `counter never reused after simulated restart`() {
        val clock = DvvClock(nodeId = 1)
        val a = clock.increment(Dvv.ZERO)
        val b = clock.increment(a)
        val counterBefore = b.dot.counter

        // Simulate restart: new clock starts from persisted counter
        val clock2 = DvvClock(nodeId = 1, startCounter = counterBefore)
        val c = clock2.increment(b)
        assertThat(c.dot.counter).isGreaterThan(counterBefore)
    }
}
```

- [ ] **Step 2: Run tests — verify fail**
- [ ] **Step 3: Implement Dvv and DvvClock**

```kotlin
package dynacache.cluster

data class Dot(val nodeId: Int, val counter: Long)

data class Dvv(
    val dot: Dot,
    val causalContext: Map<Int, Long>,  // nodeId → highest counter seen
) {
    /** This DVV dominates other if it has seen everything other has seen. */
    fun dominates(other: Dvv): Boolean {
        // Our context must include other's dot
        val ourCounterForOtherNode = causalContext[other.dot.nodeId] ?: 0L
        if (ourCounterForOtherNode < other.dot.counter) return false
        // Our context must include all of other's context
        for ((nodeId, counter) in other.causalContext) {
            if ((causalContext[nodeId] ?: 0L) < counter) return false
        }
        return true
    }

    fun isConcurrentWith(other: Dvv): Boolean = !dominates(other) && !other.dominates(this)

    companion object {
        val ZERO = Dvv(Dot(0, 0), emptyMap())

        fun merge(a: Dvv, b: Dvv): Dvv {
            val mergedContext = HashMap<Int, Long>()
            // Merge both causal contexts: take max counter per node
            for ((nodeId, counter) in a.causalContext) {
                mergedContext[nodeId] = maxOf(counter, mergedContext[nodeId] ?: 0L)
            }
            for ((nodeId, counter) in b.causalContext) {
                mergedContext[nodeId] = maxOf(counter, mergedContext[nodeId] ?: 0L)
            }
            // Include both dots
            mergedContext[a.dot.nodeId] = maxOf(a.dot.counter, mergedContext[a.dot.nodeId] ?: 0L)
            mergedContext[b.dot.nodeId] = maxOf(b.dot.counter, mergedContext[b.dot.nodeId] ?: 0L)
            // The merged DVV has a synthetic dot (will be overwritten on next write)
            return Dvv(Dot(0, 0), mergedContext)
        }
    }
}

class DvvClock(private val nodeId: Int, startCounter: Long = 0L) {
    private var counter = startCounter

    fun increment(context: Dvv): Dvv {
        counter++
        val newContext = HashMap(context.causalContext)
        // Include the previous dot in the context
        if (context.dot.nodeId != 0 || context.dot.counter != 0L) {
            newContext[context.dot.nodeId] = maxOf(
                context.dot.counter,
                newContext[context.dot.nodeId] ?: 0L
            )
        }
        return Dvv(Dot(nodeId, counter), newContext)
    }

    fun currentCounter(): Long = counter
}
```

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(cluster): Dotted Version Vectors — increment, dominance, merge, bounded size"
```

---

## Sub-phase 2D: Replication + Quorum R/W

**Concept:** The coordinator writes locally, then replicates to N-1 peers. Write returns after W acks. Read queries R replicas, compares DVVs, returns the newest. This is the Dynamo quorum model — just counting, not consensus. Learn: why R+W>N gives "strong-ish" consistency, why async replication means stale reads are possible, how DVVs detect staleness.

### Task 6: Replication manager

**Files:**
- Create: `dynacache-cluster/src/main/kotlin/dynacache/cluster/ReplicationManager.kt`
- Create: `dynacache-cluster/src/test/kotlin/dynacache/cluster/ReplicationTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cluster

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ReplicationTest {
    @Test
    fun `write replicates to N nodes, returns after W acks`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(5)
        cluster.write("foo", "bar".toByteArray())
        // Value should be on at least W=2 nodes
        val nodesWithKey = cluster.nodes.count { it.engine.hasKey("foo") }
        assertThat(nodesWithKey).isGreaterThanOrEqualTo(2)
    }

    @Test
    fun `read from R replicas returns most recent by DVV`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(5)
        cluster.write("k", "v1".toByteArray())
        cluster.write("k", "v2".toByteArray())
        val result = cluster.read("k")
        assertThat(result).isEqualTo("v2".toByteArray())
    }

    @Test
    fun `minority failure — reads and writes still succeed`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(5)
        cluster.write("k", "v".toByteArray())
        cluster.kill(cluster.nodes[2].id)
        // W=2 achievable with 2 live nodes
        cluster.write("k2", "v2".toByteArray())
        assertThat(cluster.read("k2")).isEqualTo("v2".toByteArray())
    }

    @Test
    fun `majority failure — writes fail`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(5)
        cluster.kill(cluster.nodes[1].id)
        cluster.kill(cluster.nodes[2].id)
        // W=2 not achievable with 1 live node
        val result = cluster.tryWrite("k", "v".toByteArray())
        assertThat(result.isError).isTrue()
    }
}
```

- [ ] **Step 2: Implement ReplicationManager**

The write path:
1. Compute preference list for the key
2. Write locally if this node is in the list
3. Send `ReplicateRequest` via gRPC to other nodes in the list
4. Collect acks with a timeout
5. If W acks (including local): return success
6. Else: return error

The read path:
1. Send read requests to R nodes in the preference list
2. Collect responses (value + DVV)
3. Return the value with the dominating DVV
4. (Read repair handled in P3)

- [ ] **Step 3: Enhance InProcessCluster to support replication**

The `InProcessCluster` test harness routes `ReplicateRequest` calls between in-process nodes using direct method calls (no real gRPC needed for unit tests).

- [ ] **Step 4: Run tests — verify pass**
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(cluster): replication manager — async N-way replication with W/R quorum"
```

### Task 7: Wire cluster into the Netty server — multi-node demo

**Files:**
- Modify: `dynacache-server/src/main/kotlin/dynacache/server/Main.kt`
- Modify: `dynacache-server/src/main/kotlin/dynacache/server/RespServerHandler.kt`
- Create: `dynacache-server/src/main/kotlin/dynacache/server/ClusterGrpcServer.kt`

- [ ] **Step 1: Implement ClusterGrpcServer**

A gRPC server that exposes the `ClusterService` (Forward, Replicate, Gossip). Runs on a separate port from RESP.

- [ ] **Step 2: Modify Main to accept cluster config**

Config: `--node-id`, `--resp-port`, `--grpc-port`, `--peers` (comma-separated `id:host:port`). On startup: create `HashRing`, `SwimGossip`, `ReplicationManager`, `RequestRouter`. Wire the RESP handler through the router instead of directly to the engine.

- [ ] **Step 3: Manual 3-node demo**

Start 3 nodes in separate terminals:
```bash
java -jar dynacache-server.jar --node-id 1 --resp-port 6379 --grpc-port 7379 --peers 1:localhost:7379,2:localhost:7380,3:localhost:7381
java -jar dynacache-server.jar --node-id 2 --resp-port 6380 --grpc-port 7380 --peers ...
java -jar dynacache-server.jar --node-id 3 --resp-port 6381 --grpc-port 7381 --peers ...
```

```bash
redis-cli -p 6379 SET foo bar
redis-cli -p 6380 GET foo   # returns "bar" (forwarded to coordinator or read from replica)
redis-cli -p 6381 GET foo   # same
```

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(server): multi-node cluster — RESP + gRPC, 3-node demo works"
```

---

## P2 Exit Criteria

- [ ] `mvn test` — all tests green (including P1 tests)
- [ ] `ring_determinism` — 3 nodes compute identical preference lists
- [ ] `write_read_quorum` — W=2, R=2, N=3 works
- [ ] `minority_failure_available` — kill 1 of 3, reads/writes still work
- [ ] `majority_failure_unavailable` — kill 2 of 3, writes fail
- [ ] `gossip_detects_failure` — dead node detected within O(log N) rounds
- [ ] `gossip_detects_recovery` — recovered node detected alive
- [ ] DVV tests — dominance, concurrent detection, bounded size
- [ ] 3-node `redis-cli` demo — SET on one node, GET on another

When all green: **P2 is done.** Move to P3 (Fault Tolerance).
