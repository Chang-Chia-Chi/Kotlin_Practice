# DynaCache P3 — Fault Tolerance: Handoff + Repair + Convergence

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the cluster survive and recover from partitions, node failures, and data divergence. When this phase is done: partition the cluster, write to both sides, heal, and all replicas converge to a merged state via DVV conflict resolution, read repair, and Merkle-tree anti-entropy.

**Architecture:** Builds on P2's cluster module. Adds sloppy quorum, hinted handoff, read repair, Merkle trees, anti-entropy sync, and type-specific merge rules. No new modules — all additions go into `dynacache-cluster`.

**Plan conventions:** Same as P1/P2.

**Pre-reading for P3:**
- Dynamo paper §4.5 (sloppy quorum), §4.6 (hinted handoff), §4.7 (anti-entropy)
- Merkle Hash Trees — any modern summary
- CRDT survey (Shapiro et al., 2011) — skim §2-3 for merge semantics background
- Riak source: `riak_kv_vnode.erl` (read repair), `riak_core_hashtree.erl` (Merkle)

---

## Sub-phase 3A: Sloppy Quorum + Hinted Handoff

**Concept:** When a node in the preference list is unreachable, don't fail the write — send it to the next healthy node on the ring as a "hint." The hint is stored temporarily and forwarded when the target recovers. This is how Dynamo stays available during partial failures. Learn: the difference between strict and sloppy quorum, why hints must be exact replays (not summaries).

### Task 1: Hinted handoff store

**Files:**
- Create: `dynacache-cluster/src/main/kotlin/dynacache/cluster/HintStore.kt`
- Create: `dynacache-cluster/src/test/kotlin/dynacache/cluster/HintStoreTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cluster

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class HintStoreTest {
    @Test
    fun `store and retrieve hints for a target node`() {
        val store = HintStore()
        val hint = Hint(
            targetNodeId = 2,
            key = "foo",
            value = "bar".toByteArray(),
            type = dynacache.engine.DataType.STRING,
            dvv = Dvv(Dot(1, 1), mapOf(1 to 1L)),
            expiresAtMs = -1L,
        )
        store.add(hint)
        val hints = store.hintsFor(targetNodeId = 2)
        assertThat(hints).hasSize(1)
        assertThat(hints[0].key).isEqualTo("foo")
    }

    @Test
    fun `drain removes hints after retrieval`() {
        val store = HintStore()
        store.add(Hint(2, "k1", "v1".toByteArray(), dynacache.engine.DataType.STRING, Dvv.ZERO, -1L))
        store.add(Hint(2, "k2", "v2".toByteArray(), dynacache.engine.DataType.STRING, Dvv.ZERO, -1L))
        val drained = store.drain(targetNodeId = 2)
        assertThat(drained).hasSize(2)
        assertThat(store.hintsFor(2)).isEmpty()
    }

    @Test
    fun `hints for different targets are independent`() {
        val store = HintStore()
        store.add(Hint(2, "k1", "v".toByteArray(), dynacache.engine.DataType.STRING, Dvv.ZERO, -1L))
        store.add(Hint(3, "k2", "v".toByteArray(), dynacache.engine.DataType.STRING, Dvv.ZERO, -1L))
        assertThat(store.hintsFor(2)).hasSize(1)
        assertThat(store.hintsFor(3)).hasSize(1)
    }
}
```

- [ ] **Step 2: Implement HintStore**

```kotlin
package dynacache.cluster

data class Hint(
    val targetNodeId: Int,
    val key: String,
    val value: ByteArray,
    val type: dynacache.engine.DataType,
    val dvv: Dvv,
    val expiresAtMs: Long,
)

class HintStore {
    private val hints = HashMap<Int, MutableList<Hint>>()  // targetNodeId → hints

    fun add(hint: Hint) {
        hints.getOrPut(hint.targetNodeId) { mutableListOf() }.add(hint)
    }

    fun hintsFor(targetNodeId: Int): List<Hint> = hints[targetNodeId] ?: emptyList()

    fun drain(targetNodeId: Int): List<Hint> {
        return hints.remove(targetNodeId) ?: emptyList()
    }

    fun hasHintsFor(targetNodeId: Int): Boolean = hints[targetNodeId]?.isNotEmpty() == true
}
```

- [ ] **Step 3: Run tests — verify pass**
- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(cluster): hint store — buffer writes for unreachable nodes"
```

### Task 2: Sloppy quorum in ReplicationManager

**Files:**
- Modify: `dynacache-cluster/src/main/kotlin/dynacache/cluster/ReplicationManager.kt`
- Create: `dynacache-cluster/src/test/kotlin/dynacache/cluster/SloppyQuorumTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cluster

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class SloppyQuorumTest {
    @Test
    fun `write succeeds during minority failure via sloppy quorum`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(5)
        cluster.kill(cluster.nodes[2].id)  // one node down

        // Write should still succeed — sloppy quorum uses next healthy node
        val result = cluster.tryWrite("k", "v".toByteArray())
        assertThat(result.isSuccess).isTrue()
    }

    @Test
    fun `hints replayed when target recovers`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(5)
        val killedId = cluster.nodes[2].id
        cluster.kill(killedId)

        cluster.write("k", "v".toByteArray())

        // Revive node, run handoff
        cluster.revive(killedId)
        cluster.runGossipRounds(5)
        cluster.runHintHandoff()

        // The revived node should now have the key
        assertThat(cluster.nodes[2].engine.hasKey("k")).isTrue()
    }

    @Test
    fun `hint fidelity — replayed write matches original DVV and value`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(5)
        val killedId = cluster.nodes[2].id
        cluster.kill(killedId)

        cluster.write("k", "test-value".toByteArray())
        // Capture DVV from a live replica
        val originalDvv = cluster.getDvv(cluster.nodes[0].id, "k")

        cluster.revive(killedId)
        cluster.runGossipRounds(5)
        cluster.runHintHandoff()

        // DVV on revived node should match
        val replayedDvv = cluster.getDvv(killedId, "k")
        assertThat(replayedDvv).isEqualTo(originalDvv)
    }
}
```

- [ ] **Step 2: Modify ReplicationManager write path**

When a node in the preference list is unreachable:
1. Walk further along the ring to find the next healthy node
2. Send the write as a hint (flagged with `targetNodeId`)
3. The receiving node stores it in HintStore
4. Count hint acks toward W

When gossip detects a node recovered:
1. Check HintStore for hints targeting that node
2. Send all hints as ReplicateRequest to the recovered node
3. Delete hints after successful delivery

- [ ] **Step 3: Run tests — verify pass**
- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(cluster): sloppy quorum + hinted handoff — writes survive minority failure"
```

---

## Sub-phase 3B: Read Repair

**Concept:** On every read, if the R responses disagree (different DVVs), push the winning version to the stale replicas. This is lazy convergence — divergence is detected and fixed on the read path. Learn: why read repair is "free" (you're already reading from R replicas), why it's not sufficient alone (keys that are never read never converge — that's what anti-entropy is for).

### Task 3: Read repair in ReplicationManager

**Files:**
- Modify: `dynacache-cluster/src/main/kotlin/dynacache/cluster/ReplicationManager.kt`
- Create: `dynacache-cluster/src/test/kotlin/dynacache/cluster/ReadRepairTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cluster

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ReadRepairTest {
    @Test
    fun `stale replica repaired on read`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(5)
        cluster.write("k", "v1".toByteArray())

        // Manually make node 2's copy stale (simulate missed replication)
        cluster.forceStale(nodeId = cluster.nodes[2].id, key = "k", value = "old".toByteArray())

        // Read triggers read repair
        val result = cluster.read("k")
        assertThat(result).isEqualTo("v1".toByteArray())

        // After read, the stale replica should be updated
        cluster.drainAsyncRepairs()
        val node2Value = cluster.directRead(cluster.nodes[2].id, "k")
        assertThat(node2Value).isEqualTo("v1".toByteArray())
    }

    @Test
    fun `read repair is async — does not block read response`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(5)
        cluster.write("k", "v".toByteArray())
        cluster.forceStale(nodeId = cluster.nodes[2].id, key = "k", value = "old".toByteArray())

        // Read should return immediately, not wait for repair to complete
        val start = System.nanoTime()
        cluster.read("k")
        val elapsed = System.nanoTime() - start
        // Should be fast — not blocked by repair network call
        assertThat(elapsed).isLessThan(100_000_000L) // 100ms
    }
}
```

- [ ] **Step 2: Modify read path in ReplicationManager**

After collecting R responses and selecting the winner:
1. Compare DVVs from all R responses
2. Identify stale replicas (where the DVV doesn't match the winner)
3. Fire-and-forget: send the winning value+DVV to each stale replica via gRPC Replicate
4. Return the winning value to the client immediately (don't wait for repair acks)

- [ ] **Step 3: Run tests — verify pass**
- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(cluster): read repair — stale replicas fixed on read path (async)"
```

---

## Sub-phase 3C: Merkle Tree Anti-Entropy

**Concept:** Read repair only fixes keys that are read. For keys that sit unread, we need a background process that proactively finds and fixes divergence. Merkle trees are the data structure: hash each key-value pair, build a tree, compare roots between replicas. If roots differ, walk down to find the divergent subtrees. Exchange only the differing keys. Learn: how Merkle trees make O(n) comparison into O(log n), how to structure per-vnode range trees.

### Task 4: Merkle tree for key ranges

**Files:**
- Create: `dynacache-cluster/src/main/kotlin/dynacache/cluster/MerkleTree.kt`
- Create: `dynacache-cluster/src/test/kotlin/dynacache/cluster/MerkleTreeTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cluster

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class MerkleTreeTest {
    @Test
    fun `identical data produces identical root hash`() {
        val data = mapOf("k1" to "v1".toByteArray(), "k2" to "v2".toByteArray())
        val tree1 = MerkleTree.build(data)
        val tree2 = MerkleTree.build(data)
        assertThat(tree1.rootHash()).isEqualTo(tree2.rootHash())
    }

    @Test
    fun `different data produces different root hash`() {
        val tree1 = MerkleTree.build(mapOf("k1" to "v1".toByteArray()))
        val tree2 = MerkleTree.build(mapOf("k1" to "v2".toByteArray()))
        assertThat(tree1.rootHash()).isNotEqualTo(tree2.rootHash())
    }

    @Test
    fun `diff identifies divergent keys`() {
        val data1 = mapOf("k1" to "v1".toByteArray(), "k2" to "same".toByteArray(), "k3" to "v3".toByteArray())
        val data2 = mapOf("k1" to "v1-changed".toByteArray(), "k2" to "same".toByteArray(), "k3" to "v3".toByteArray())
        val tree1 = MerkleTree.build(data1)
        val tree2 = MerkleTree.build(data2)
        val divergent = MerkleTree.diff(tree1, tree2)
        assertThat(divergent).containsExactly("k1")
    }

    @Test
    fun `diff handles missing keys`() {
        val data1 = mapOf("k1" to "v1".toByteArray(), "k2" to "v2".toByteArray())
        val data2 = mapOf("k1" to "v1".toByteArray())
        val tree1 = MerkleTree.build(data1)
        val tree2 = MerkleTree.build(data2)
        val divergent = MerkleTree.diff(tree1, tree2)
        assertThat(divergent).contains("k2")
    }
}
```

- [ ] **Step 2: Implement MerkleTree**

A binary hash tree. Leaf nodes hash individual key-value pairs. Interior nodes hash the concatenation of their children's hashes. Build from a sorted list of key-hash pairs. `diff()` walks both trees in parallel, descending only into subtrees with different hashes.

- [ ] **Step 3: Run tests — verify pass**
- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(cluster): Merkle tree — build, rootHash, diff for anti-entropy"
```

### Task 5: Anti-entropy sync process

**Files:**
- Create: `dynacache-cluster/src/main/kotlin/dynacache/cluster/AntiEntropySync.kt`
- Create: `dynacache-cluster/src/test/kotlin/dynacache/cluster/AntiEntropySyncTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cluster

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class AntiEntropySyncTest {
    @Test
    fun `divergent replica converges after anti-entropy cycle`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 3, r = 2)
        cluster.runGossipRounds(5)
        cluster.write("k", "correct".toByteArray())

        // Corrupt one replica silently
        cluster.corruptKey(nodeId = cluster.nodes[2].id, key = "k", value = "wrong".toByteArray())

        // Run anti-entropy
        cluster.runAntiEntropy()

        // Corrupted replica should be fixed
        val fixed = cluster.directRead(cluster.nodes[2].id, "k")
        assertThat(fixed).isEqualTo("correct".toByteArray())
    }

    @Test
    fun `missing key on one replica is synced`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 3, r = 2)
        cluster.runGossipRounds(5)
        cluster.write("k", "v".toByteArray())
        cluster.deleteKeyLocally(cluster.nodes[1].id, "k")  // simulate data loss

        cluster.runAntiEntropy()

        assertThat(cluster.directRead(cluster.nodes[1].id, "k")).isEqualTo("v".toByteArray())
    }
}
```

- [ ] **Step 2: Implement AntiEntropySync**

Periodic background process (one coroutine per node):
1. For each vnode range this node owns: build a Merkle tree
2. Pick a replica peer for that range
3. Exchange Merkle roots. If equal: done. If different: exchange tree hashes level by level to identify divergent keys.
4. For divergent keys: exchange values + DVVs. Apply merge rules (DVV dominance or type-specific merge).
5. Repeat every `antiEntropyIntervalSeconds` (configurable, default 60).

- [ ] **Step 3: Run tests — verify pass**
- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(cluster): anti-entropy sync — Merkle tree comparison + key-level repair"
```

---

## Sub-phase 3D: Conflict Resolution + Convergence

**Concept:** When a network partition heals, both sides may have written to the same key independently. DVVs detect this as concurrent writes (neither dominates). Type-specific merge rules resolve the conflict without data loss. Learn: why merge must be commutative/associative/idempotent (CRDT properties), why LWW is simple but lossy, and how per-field merging preserves more information.

### Task 6: Type-specific merge rules

**Files:**
- Create: `dynacache-cluster/src/main/kotlin/dynacache/cluster/ConflictResolver.kt`
- Create: `dynacache-cluster/src/test/kotlin/dynacache/cluster/ConflictResolverTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cluster

import dynacache.engine.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ConflictResolverTest {
    private val resolver = ConflictResolver()

    @Test
    fun `String — LWW by DVV, tiebreak by highest node ID`() {
        val dvv1 = Dvv(Dot(1, 5), mapOf(1 to 5L))
        val dvv2 = Dvv(Dot(2, 3), mapOf(2 to 3L))
        // Concurrent: neither dominates
        val result = resolver.merge(
            DataType.STRING, "v1".toByteArray(), dvv1,
            DataType.STRING, "v2".toByteArray(), dvv2,
        )
        // Tiebreak: highest node-ID (2) wins
        assertThat(String(result.value as ByteArray)).isEqualTo("v2")
    }

    @Test
    fun `Hash — field-level merge, per-field LWW`() {
        val hash1 = hashMapOf("a" to "1".toByteArray(), "b" to "2".toByteArray())
        val hash2 = hashMapOf("b" to "3".toByteArray(), "c" to "4".toByteArray())
        val dvv1 = Dvv(Dot(1, 1), mapOf(1 to 1L))
        val dvv2 = Dvv(Dot(2, 1), mapOf(2 to 1L))

        val result = resolver.merge(
            DataType.HASH, hash1, dvv1,
            DataType.HASH, hash2, dvv2,
        )
        @Suppress("UNCHECKED_CAST")
        val merged = result.value as HashMap<String, ByteArray>
        assertThat(merged).containsKeys("a", "b", "c")  // union of fields
        assertThat(String(merged["b"]!!)).isEqualTo("3")  // node 2 wins tiebreak for "b"
    }

    @Test
    fun `Sorted Set — element-level merge, max score on conflict`() {
        val zset1 = SortedSetValue().apply { add(1.0, "alice"); add(5.0, "bob") }
        val zset2 = SortedSetValue().apply { add(3.0, "alice"); add(10.0, "charlie") }
        val dvv1 = Dvv(Dot(1, 1), mapOf(1 to 1L))
        val dvv2 = Dvv(Dot(2, 1), mapOf(2 to 1L))

        val result = resolver.merge(
            DataType.ZSET, zset1, dvv1,
            DataType.ZSET, zset2, dvv2,
        )
        val merged = result.value as SortedSetValue
        assertThat(merged.score("alice")).isEqualTo(3.0)   // max score
        assertThat(merged.score("bob")).isEqualTo(5.0)     // only in zset1
        assertThat(merged.score("charlie")).isEqualTo(10.0) // only in zset2
    }
}
```

- [ ] **Step 2: Implement ConflictResolver**

A `merge(type1, value1, dvv1, type2, value2, dvv2)` method that:
1. If dvv1 dominates dvv2: return (value1, dvv1)
2. If dvv2 dominates dvv1: return (value2, dvv2)
3. Concurrent — apply type-specific merge:
   - **String**: LWW, tiebreak by highest node-ID in the dot
   - **Hash**: union of all fields. For fields present in both, LWW by dot node-ID
   - **List**: concatenate both lists (union of appends). This is a best-effort merge — lists don't have clean CRDT semantics
   - **Sorted Set**: union of members. For members in both, take max score
4. Return merged value + merged DVV (via `Dvv.merge()`)

- [ ] **Step 3: Run tests — verify pass**
- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(cluster): conflict resolver — type-specific merge rules for concurrent DVVs"
```

### Task 7: Full convergence integration test

**Files:**
- Create: `dynacache-cluster/src/test/kotlin/dynacache/cluster/ConvergenceTest.kt`

- [ ] **Step 1: Write the convergence test**

```kotlin
package dynacache.cluster

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ConvergenceTest {
    @Test
    fun `convergence after partition — concurrent writes merge correctly`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(5)

        // Partition: node 1 vs nodes 2+3
        cluster.partition(side1 = setOf(1), side2 = setOf(2, 3))

        // Write to both sides of the partition
        cluster.writeVia(nodeId = 1, key = "k", value = "from-side1".toByteArray())
        cluster.writeVia(nodeId = 2, key = "k", value = "from-side2".toByteArray())

        // Heal partition
        cluster.healPartition()
        cluster.runGossipRounds(10)
        cluster.runAntiEntropy()
        cluster.drainAsyncRepairs()

        // All replicas should agree
        val values = cluster.nodes.map { cluster.directRead(it.id, "k") }
        assertThat(values.distinct()).hasSize(1)  // all the same
    }

    @Test
    fun `convergence checker — all keys agree after chaos`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(5)

        // Write 100 keys
        repeat(100) { i -> cluster.write("k$i", "v$i".toByteArray()) }

        // Chaos: partition, writes, heal, repeat
        cluster.partition(setOf(1, 2), setOf(3))
        repeat(50) { i -> cluster.writeVia(1, "k$i", "updated-$i".toByteArray()) }
        cluster.healPartition()
        cluster.runGossipRounds(10)
        cluster.runHintHandoff()
        cluster.runAntiEntropy()
        cluster.drainAsyncRepairs()

        // Convergence check: every key has same value on all replicas
        repeat(100) { i ->
            val key = "k$i"
            val values = cluster.nodes.map { cluster.directRead(it.id, key) }
            val nonNull = values.filterNotNull()
            if (nonNull.isNotEmpty()) {
                assertThat(nonNull.distinct()).hasSize(1)
            }
        }
    }
}
```

- [ ] **Step 2: Run tests — verify pass**
- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat(cluster): convergence tests — partition + heal + anti-entropy → all replicas agree"
```

---

## P3 Exit Criteria

- [ ] `mvn test` — all tests green (including P1 + P2)
- [ ] `hinted_handoff_replays` — partitioned node recovers its data
- [ ] `read_repair_fixes_stale` — stale replica corrected on read
- [ ] `anti_entropy_heals_divergence` — corrupted replica fixed by Merkle sync
- [ ] `convergence_after_partition` — concurrent writes on both sides merge correctly
- [ ] Conflict resolution: String LWW, Hash field-merge, ZSet element-merge all tested
- [ ] Convergence checker passes after chaos sequence

When all green: **P3 is done.** Move to P4 (Persistence + Snapshots).
