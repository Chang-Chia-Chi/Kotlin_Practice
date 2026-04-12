# DynaCache P4 — Persistence + Snapshots

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add persistence so nodes survive restarts, and Chandy-Lamport distributed snapshots for cluster-wide consistent state capture. When this phase is done: kill all nodes, restart from RDB snapshots, data intact. Trigger a Chandy-Lamport snapshot while traffic is running, restore later, cluster state is consistent.

**Architecture:** RDB serialization goes in `dynacache-engine` (pure — serialize data structures to/from bytes). Chandy-Lamport coordination goes in `dynacache-cluster`. Server module wires up periodic save and snapshot trigger endpoints.

**Plan conventions:** Same as P1/P2/P3.

**Pre-reading for P4:**
- Chandy-Lamport paper (1985) — 10 pages, the full algorithm
- Mattern (1989) — Virtual Time, formalizes consistent cuts
- Redis source: `rdb.c` — `rdbSave` / `rdbLoad` cycle

---

## Sub-phase 4A: RDB Serialization Format

**Concept:** Serialize all data structures (String, Hash, List, Sorted Set) plus metadata (DVV, TTL, type tag) into a compact binary format. Deserialize on startup. The format is append-only entries with CRC integrity checking. Learn: binary serialization design, CRC for corruption detection, how to snapshot without stopping the world.

### Task 1: RDB serializer and deserializer

**Files:**
- Create: `dynacache-engine/src/main/kotlin/dynacache/engine/RdbSerializer.kt`
- Create: `dynacache-engine/src/test/kotlin/dynacache/engine/RdbSerializerTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.engine

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

class RdbSerializerTest {
    @Test
    fun `roundtrip String entry`() {
        val entry = RdbEntry(
            key = "foo",
            type = DataType.STRING,
            value = "bar".toByteArray(),
            expiresAtMs = 1000L,
            dvvBytes = byteArrayOf(),  // serialized DVV
        )
        val buf = ByteArrayOutputStream()
        RdbSerializer.writeEntry(buf, entry)
        val parsed = RdbSerializer.readEntry(ByteArrayInputStream(buf.toByteArray()))
        assertThat(parsed!!.key).isEqualTo("foo")
        assertThat(parsed.value).isEqualTo("bar".toByteArray())
        assertThat(parsed.expiresAtMs).isEqualTo(1000L)
    }

    @Test
    fun `roundtrip Hash entry`() {
        val hash = mapOf("f1" to "v1".toByteArray(), "f2" to "v2".toByteArray())
        val serialized = RdbSerializer.serializeHash(hash)
        val deserialized = RdbSerializer.deserializeHash(serialized)
        assertThat(deserialized).containsKeys("f1", "f2")
        assertThat(String(deserialized["f1"]!!)).isEqualTo("v1")
    }

    @Test
    fun `roundtrip Sorted Set entry`() {
        val zset = listOf(1.5 to "alice", 3.0 to "bob")
        val serialized = RdbSerializer.serializeSortedSet(zset)
        val deserialized = RdbSerializer.deserializeSortedSet(serialized)
        assertThat(deserialized).containsExactly(1.5 to "alice", 3.0 to "bob")
    }

    @Test
    fun `roundtrip List entry`() {
        val list = listOf("a", "b", "c").map { it.toByteArray() }
        val serialized = RdbSerializer.serializeList(list)
        val deserialized = RdbSerializer.deserializeList(serialized)
        assertThat(deserialized.map { String(it) }).containsExactly("a", "b", "c")
    }

    @Test
    fun `full snapshot roundtrip with all types`() {
        val entries = listOf(
            RdbEntry("str", DataType.STRING, "val".toByteArray(), -1L, byteArrayOf()),
            RdbEntry("hash", DataType.HASH, RdbSerializer.serializeHash(mapOf("f" to "v".toByteArray())), -1L, byteArrayOf()),
            RdbEntry("list", DataType.LIST, RdbSerializer.serializeList(listOf("a".toByteArray())), -1L, byteArrayOf()),
            RdbEntry("zset", DataType.ZSET, RdbSerializer.serializeSortedSet(listOf(1.0 to "m")), -1L, byteArrayOf()),
        )
        val buf = ByteArrayOutputStream()
        RdbSerializer.writeSnapshot(buf, entries)
        val restored = RdbSerializer.readSnapshot(ByteArrayInputStream(buf.toByteArray()))
        assertThat(restored).hasSize(4)
        assertThat(restored.map { it.key }).containsExactlyInAnyOrder("str", "hash", "list", "zset")
    }

    @Test
    fun `CRC detects corruption`() {
        val buf = ByteArrayOutputStream()
        RdbSerializer.writeSnapshot(buf, listOf(
            RdbEntry("k", DataType.STRING, "v".toByteArray(), -1L, byteArrayOf())
        ))
        val bytes = buf.toByteArray()
        // Corrupt one byte
        bytes[bytes.size / 2] = (bytes[bytes.size / 2].toInt() xor 0xFF).toByte()
        val result = runCatching {
            RdbSerializer.readSnapshot(ByteArrayInputStream(bytes))
        }
        assertThat(result.isFailure).isTrue()
    }

    @Test
    fun `expired keys excluded from snapshot`() {
        val entries = listOf(
            RdbEntry("live", DataType.STRING, "v".toByteArray(), -1L, byteArrayOf()),
            RdbEntry("expired", DataType.STRING, "v".toByteArray(), 500L, byteArrayOf()),
        )
        val filtered = entries.filter { it.expiresAtMs == -1L || it.expiresAtMs > 1000L }
        assertThat(filtered).hasSize(1)
        assertThat(filtered[0].key).isEqualTo("live")
    }
}
```

- [ ] **Step 2: Implement RdbSerializer**

Binary format per entry:
```
[key_len:u32][key_bytes][type:u8][dvv_len:u32][dvv_bytes][expires_at:i64][value_len:u32][value_bytes][crc32:u32]
```

Snapshot format:
```
[magic:u32 = 0xDC_CA_01][entry_count:u32][entry]*[file_crc32:u32]
```

CRC32 on each entry for per-entry corruption detection. CRC32 on the whole file as a final check.

Type-specific value serialization:
- STRING: raw bytes
- HASH: `[field_count:u32][field_len:u32][field_bytes][value_len:u32][value_bytes]*`
- LIST: `[element_count:u32][element_len:u32][element_bytes]*`
- ZSET: `[member_count:u32][score:f64][member_len:u32][member_bytes]*`

- [ ] **Step 3: Run tests — verify pass**
- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(engine): RDB serializer — binary format with CRC32 for all data types"
```

### Task 2: Snapshot engine — save and restore

**Files:**
- Create: `dynacache-engine/src/main/kotlin/dynacache/engine/SnapshotEngine.kt`
- Create: `dynacache-engine/src/test/kotlin/dynacache/engine/SnapshotEngineTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.engine

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class SnapshotEngineTest {
    @TempDir lateinit var tmpDir: Path

    @Test
    fun `save and restore roundtrip — all keys intact`() {
        val engine = DataEngine()
        engine.execute(Command.Set("s", "hello".toByteArray()))
        engine.execute(Command.HSet("h", mapOf("f" to "v".toByteArray())))
        engine.execute(Command.RPush("l", listOf("a".toByteArray(), "b".toByteArray())))
        engine.execute(Command.ZAdd("z", listOf(1.0 to "alice", 2.0 to "bob")))
        engine.execute(Command.Set("expiring", "gone".toByteArray(), exSeconds = 60))

        val snapshotFile = tmpDir.resolve("dump.rdb")
        SnapshotEngine.save(engine, snapshotFile)

        val restored = DataEngine()
        SnapshotEngine.restore(restored, snapshotFile)

        assertThat((restored.execute(Command.Get("s")) as Response.BulkString).value)
            .isEqualTo("hello".toByteArray())
        assertThat(restored.execute(Command.HLen("h"))).isEqualTo(Response.IntegerReply(1))
        assertThat(restored.execute(Command.LLen("l"))).isEqualTo(Response.IntegerReply(2))
        assertThat(restored.execute(Command.ZCard("z"))).isEqualTo(Response.IntegerReply(2))
    }

    @Test
    fun `snapshot excludes expired keys`() {
        var now = 1000L
        val engine = DataEngine(clock = { now })
        engine.execute(Command.Set("live", "v".toByteArray()))
        engine.execute(Command.Set("dead", "v".toByteArray(), exSeconds = 1))
        now = 3000L  // "dead" is expired

        val snapshotFile = tmpDir.resolve("dump.rdb")
        SnapshotEngine.save(engine, snapshotFile)

        val restored = DataEngine()
        SnapshotEngine.restore(restored, snapshotFile)
        assertThat(restored.execute(Command.DbSize())).isEqualTo(Response.IntegerReply(1))
    }

    @Test
    fun `snapshot during concurrent writes is consistent`() {
        val engine = DataEngine()
        // Pre-populate
        repeat(1000) { i -> engine.execute(Command.Set("k$i", "v$i".toByteArray())) }

        // Take snapshot (point-in-time)
        val snapshotFile = tmpDir.resolve("dump.rdb")
        val snapshot = SnapshotEngine.capturePointInTime(engine)

        // Continue writing after capture
        repeat(1000) { i -> engine.execute(Command.Set("k$i", "UPDATED".toByteArray())) }

        // Save the captured snapshot (not the current state)
        SnapshotEngine.saveFromCapture(snapshot, snapshotFile)

        val restored = DataEngine()
        SnapshotEngine.restore(restored, snapshotFile)

        // Restored values should be the pre-update values
        val sample = restored.execute(Command.Get("k0")) as Response.BulkString
        assertThat(String(sample.value!!)).isEqualTo("v0")
    }
}
```

- [ ] **Step 2: Implement SnapshotEngine**

`save(engine, path)`: iterate all entries in the engine, filter expired, serialize via `RdbSerializer`, write to file atomically (write to tmp → rename).

`restore(engine, path)`: read file, deserialize, bulk-insert into engine.

`capturePointInTime(engine)`: capture a consistent snapshot of all data structures. For the single-threaded engine, this is just a deep copy of the store map (since commands are not interleaved during capture). Returns a list of `RdbEntry`.

- [ ] **Step 3: Run tests — verify pass**
- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(engine): snapshot engine — save/restore/point-in-time capture"
```

### Task 3: Wire periodic RDB save into server

**Files:**
- Modify: `dynacache-server/src/main/kotlin/dynacache/server/Main.kt`

- [ ] **Step 1: Add periodic save coroutine**

On server startup, launch a coroutine that calls `SnapshotEngine.save()` every `rdbIntervalSeconds` (from config, default 300). Also save on graceful shutdown.

On server startup, if an RDB file exists at the data dir, restore from it before accepting connections.

- [ ] **Step 2: Manual test**

```bash
# Start server, write some keys
redis-cli SET foo bar
# Kill server (Ctrl+C — triggers graceful shutdown save)
# Restart server
redis-cli GET foo  # should return "bar"
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat(server): periodic RDB save + restore on startup"
```

---

## Sub-phase 4B: Chandy-Lamport Distributed Snapshots

**Concept:** Capture a globally consistent snapshot across all nodes while traffic is running. The Chandy-Lamport algorithm uses **markers** sent on all communication channels. When a node receives a marker, it records its local state and begins recording in-flight messages. The result is a consistent cut — if event A caused event B and B is in the snapshot, then A is too. Learn: what "consistent cut" means, why FIFO channel ordering is required, how markers propagate.

### Task 4: Chandy-Lamport coordinator

**Files:**
- Create: `dynacache-cluster/src/main/kotlin/dynacache/cluster/ChandyLamport.kt`
- Create: `dynacache-cluster/src/test/kotlin/dynacache/cluster/ChandyLamportTest.kt`

- [ ] **Step 1: Write failing tests**

```kotlin
package dynacache.cluster

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ChandyLamportTest {
    @Test
    fun `snapshot captures consistent state during active traffic`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(5)
        repeat(100) { i -> cluster.write("k$i", "v$i".toByteArray()) }

        // Start traffic in background
        val trafficHandle = cluster.startBackgroundTraffic(keysPerSecond = 100)

        // Take Chandy-Lamport snapshot
        val snapshot = cluster.takeChandyLamportSnapshot()

        trafficHandle.stop()

        // Snapshot should be non-empty
        assertThat(snapshot.nodeStates).hasSize(3)
        assertThat(snapshot.nodeStates.values.sumOf { it.entryCount }).isGreaterThan(0)
    }

    @Test
    fun `consistent cut property — causal ordering preserved`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(5)

        // Write A, then B (A causally before B)
        cluster.write("A", "valueA".toByteArray())
        cluster.write("B", "valueB".toByteArray())

        val snapshot = cluster.takeChandyLamportSnapshot()

        // If B is in the snapshot, A must also be in the snapshot
        val hasB = snapshot.containsKey("B")
        val hasA = snapshot.containsKey("A")
        if (hasB) assertThat(hasA).isTrue()
    }

    @Test
    fun `restore from Chandy-Lamport snapshot`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(5)
        repeat(50) { i -> cluster.write("k$i", "v$i".toByteArray()) }

        val snapshot = cluster.takeChandyLamportSnapshot()

        // Continue writing (post-snapshot)
        repeat(50) { i -> cluster.write("k$i", "UPDATED".toByteArray()) }

        // Restore from snapshot
        cluster.restoreFromSnapshot(snapshot)

        // Values should be pre-update
        val val0 = cluster.read("k0")
        assertThat(val0).isEqualTo("v0".toByteArray())
    }

    @Test
    fun `timeout aborts snapshot cleanly`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(5)
        cluster.kill(cluster.nodes[2].id)  // one node unreachable

        val result = cluster.tryTakeChandyLamportSnapshot(timeoutMs = 1000)
        assertThat(result.isAborted).isTrue()
        // No corruption — cluster continues operating normally
        cluster.write("k", "v".toByteArray())
        assertThat(cluster.read("k")).isEqualTo("v".toByteArray())
    }
}
```

- [ ] **Step 2: Implement ChandyLamport**

The algorithm, implemented as a coroutine-driven protocol:

**SnapshotCoordinator (initiator):**
1. Generate a unique snapshot ID
2. Record own local state (via `SnapshotEngine.capturePointInTime()`)
3. Send `Marker(snapshotId)` on all outgoing gRPC channels
4. Start recording incoming messages on all channels
5. Wait for all nodes to report their state + channel recordings
6. Assemble the global snapshot: `Map<NodeId, LocalState>` + `Map<ChannelId, List<Message>>`

**SnapshotParticipant (each node):**
1. On receiving first `Marker(snapshotId)`:
   - Record own local state
   - Send `Marker(snapshotId)` on all outgoing channels
   - Start recording incoming messages on all channels except the one the marker arrived on
2. On receiving `Marker(snapshotId)` from channel C (subsequent):
   - Stop recording on channel C
3. When markers received on all channels:
   - Send `SnapshotDone(nodeId, localState, channelRecordings)` to coordinator

**Timeout:** If not all nodes respond within the configured timeout, abort the snapshot. No state change, no corruption.

Add gRPC messages to `cluster.proto`:
```protobuf
message SnapshotMarker {
    string snapshot_id = 1;
    int32 sender_id = 2;
}

message SnapshotState {
    string snapshot_id = 1;
    int32 node_id = 2;
    bytes local_state = 3;          // RDB-serialized local entries
    repeated ChannelLog channels = 4;
}

message ChannelLog {
    int32 from_node_id = 1;
    repeated bytes messages = 2;    // recorded in-flight messages
}
```

- [ ] **Step 3: Run tests — verify pass**
- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(cluster): Chandy-Lamport distributed snapshots — marker protocol + consistent cut"
```

---

## Sub-phase 4C: Final Integration + Demo

**Concept:** Wire everything together. Periodic RDB saves, on-demand Chandy-Lamport via HTTP endpoint, restore on startup. Run the full demo from the spec's success signal.

### Task 5: Snapshot HTTP endpoint + periodic save

**Files:**
- Modify: server `Main.kt`
- Add HTTP endpoint for triggering Chandy-Lamport snapshot

- [ ] **Step 1: Add `/admin/snapshot` endpoint**

```
POST /admin/snapshot → triggers Chandy-Lamport, returns snapshot ID
GET  /admin/snapshot/{id} → returns snapshot status (in-progress, done, aborted)
POST /admin/restore/{id} → restores cluster from snapshot
```

Wire via a simple HTTP handler in Netty (no REST framework — just pattern-match the path).

- [ ] **Step 2: Commit**

```bash
git add -A && git commit -m "feat(server): snapshot admin endpoints — trigger/status/restore"
```

### Task 6: Full integration test — the success signal

**Files:**
- Create: `dynacache-cluster/src/test/kotlin/dynacache/cluster/FullIntegrationTest.kt`

- [ ] **Step 1: Write the full integration test**

```kotlin
package dynacache.cluster

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Tag

@Tag("integration")
class FullIntegrationTest {
    @Test
    fun `full success signal demo`() {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        cluster.runGossipRounds(10)

        // 1. Basic operations
        cluster.write("foo", "bar".toByteArray())
        assertThat(cluster.read("foo")).isEqualTo("bar".toByteArray())

        // 2. Sorted set
        cluster.executeVia(1, listOf("ZADD", "leaderboard", "100", "alice", "200", "bob"))
        val zrange = cluster.executeVia(2, listOf("ZRANGE", "leaderboard", "0", "-1", "WITHSCORES"))
        assertThat(zrange).isNotNull()

        // 3. Kill minority — still works
        cluster.kill(3)
        cluster.runGossipRounds(10)
        cluster.write("k2", "v2".toByteArray())
        assertThat(cluster.read("k2")).isEqualTo("v2".toByteArray())

        // 4. Restart killed node — handoff replays
        cluster.revive(3)
        cluster.runGossipRounds(10)
        cluster.runHintHandoff()
        assertThat(cluster.directRead(3, "k2")).isEqualTo("v2".toByteArray())

        // 5. Concurrent writes during partition — converge after heal
        cluster.partition(setOf(1), setOf(2, 3))
        cluster.writeVia(1, "conflict", "side1".toByteArray())
        cluster.writeVia(2, "conflict", "side2".toByteArray())
        cluster.healPartition()
        cluster.runGossipRounds(10)
        cluster.runAntiEntropy()
        cluster.drainAsyncRepairs()
        val values = cluster.nodes.mapNotNull { cluster.directRead(it.id, "conflict") }
        assertThat(values.distinct()).hasSize(1) // all agree

        // 6. Chandy-Lamport snapshot during traffic
        cluster.startBackgroundTraffic(keysPerSecond = 50).use {
            val snapshot = cluster.takeChandyLamportSnapshot()
            assertThat(snapshot.nodeStates).hasSize(3)
            assertThat(snapshot.isComplete).isTrue()
        }

        // 7. RDB save + restore
        cluster.saveRdb()
        val preSaveValue = cluster.read("foo")
        cluster.restartAllNodes()
        assertThat(cluster.read("foo")).isEqualTo(preSaveValue)
    }
}
```

- [ ] **Step 2: Run full test suite**

```bash
cd "$ROOT" && $MVN test -q
```

Expected: ALL PASS.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: P4 complete — RDB persistence + Chandy-Lamport + full integration test"
```

---

## P4 Exit Criteria

- [ ] `mvn test` — all tests green (P1 + P2 + P3 + P4)
- [ ] `rdb_save_restore_roundtrip` — all types survive save/restore
- [ ] `rdb_excludes_expired` — expired keys not in snapshot
- [ ] `rdb_concurrent_writes` — snapshot is point-in-time consistent
- [ ] `chandy_lamport_consistent_cut` — causal ordering preserved
- [ ] `chandy_lamport_restorable` — cluster restored from snapshot correctly
- [ ] `chandy_lamport_timeout_aborts` — clean abort when node unreachable
- [ ] Full integration test passes — the spec's success signal demo

When all green: **DynaCache is done.**

---

## Project Complete Checklist

- [ ] P1: Single-node `redis-cli` works for all commands
- [ ] P2: 3-node cluster routes, replicates, survives minority failure
- [ ] P3: Partitions heal, conflicts merge, replicas converge
- [ ] P4: RDB persistence + Chandy-Lamport distributed snapshots
- [ ] All spec tests (§6) pass
- [ ] All spec invariants (§4) verified
- [ ] All spec constraints (§3) respected
- [ ] Success signal demo works end-to-end
