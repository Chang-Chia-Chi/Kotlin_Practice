package dynacache.cluster

import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.ReplicateAck
import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import dynacache.engine.persist.CommandCodec
import dynacache.engine.persist.FsyncPolicy
import dynacache.engine.persist.SnapshotEngine
import dynacache.engine.persist.WalReader
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * Replication and quorum at the `CommandEngine` seam through the test kit's cluster (spec 5.1
 * steps 4 to 6 and 8, spec 5.2 steps 1 to 4, C4): a write through one node is read through
 * another, W and R are counted in distinct nodes, and a quorum that cannot form is an error
 * rather than a hang.
 */
class ReplicationTest {

    private val key = Key("orders:4711")

    @Test
    fun quorum_config_rejects_r_plus_w_not_above_n() {
        assertThrows(IllegalArgumentException::class.java) { ReplicationConfig(n = 3, w = 1, r = 2) }
        assertThrows(IllegalArgumentException::class.java) { ReplicationConfig(n = 3, w = 0, r = 3) }
        assertThrows(IllegalArgumentException::class.java) { ReplicationConfig(n = 3, w = 4, r = 3) }
        assertEquals(ReplicationConfig(3, 2, 2), ReplicationConfig(n = 3, w = 2, r = 2))
    }

    @Test
    fun write_read_quorum() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope)
        val (writer, reader) = cluster.nodes
        val coordinator = cluster.ring.preferenceList(key, 3).first()

        assertEquals(Reply.Simple("OK"), cluster.writeVia(writer, key, "v1".toByteArray()))
        assertEquals(Reply.Bulk("v1".toByteArray()), cluster.readVia(reader, key))

        // Every replica holds the value under the one version the coordinator created.
        cluster.drainMessages()
        assertEquals(List(3) { Reply.Bulk("v1".toByteArray()) }, cluster.readAllReplicas(key).values.toList())
        val version = cluster.replication(coordinator).version(key)!!
        assertEquals(Dot(coordinator, 1), version.dot)
        cluster.nodes.forEach { assertEquals(version, cluster.replication(it).version(key), "version on $it") }

        // A TTL crosses as an absolute instant (spec 5.4): every replica answers the same PTTL.
        val expiring = Key("session:9")
        val set = Command.Set(expiring, "s".toByteArray(), ttl = Duration.ofSeconds(10))
        assertEquals(Reply.Simple("OK"), cluster.settle(cluster.router(writer).submit(set)))
        cluster.drainMessages()
        for (node in cluster.ring.preferenceList(expiring, 3)) {
            val ttl = cluster.engine(node).submit(Command.Ttl(expiring, Command.Ttl.Precision.MILLIS)).await()
            assertEquals(Reply.Integer(10_000), ttl, "PTTL on $node")
        }
        cluster.close()
    }

    /**
     * One of three down, with gossip having noticed. Sloppy quorum is T25, so the coordinator
     * itself must be up: the victim is the key's last replica.
     */
    @Test
    fun minority_failure_available() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope)
        val victim = cluster.ring.preferenceList(key, 3).last()
        cluster.network.kill(victim)
        cluster.membership.set(victim, MemberState.DEAD)
        val (writer, reader) = cluster.nodes - victim

        assertEquals(Reply.Simple("OK"), cluster.writeVia(writer, key, "v1".toByteArray()))
        assertEquals(Reply.Bulk("v1".toByteArray()), cluster.readVia(reader, key))
        assertEquals(Reply.Bulk(null), cluster.engine(victim).submit(Command.Get(key)).await())
        cluster.close()
    }

    /**
     * Two of three down and gossip has not noticed yet, so the coordinator asks them and hears
     * nothing: the deadline runs in virtual time and the answer is the quorum error, not a hang.
     */
    @Test
    fun majority_failure_unavailable() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope)
        val (coordinator, first, second) = cluster.ring.preferenceList(key, 3)
        cluster.network.kill(first)
        cluster.network.kill(second)

        assertEquals(
            Reply.Error("ERR", "quorum not reached: write needs 2 of 3 nodes, 1 answered within 1s"),
            cluster.writeVia(coordinator, key, "v1".toByteArray()),
        )
        assertEquals(
            Reply.Error("ERR", "quorum not reached: read needs 2 of 3 nodes, 1 answered within 1s"),
            cluster.readVia(coordinator, key),
        )
        cluster.close()
    }

    /**
     * W = 3 on three nodes needs two acks besides the coordinator's own. Only the coordinator
     * runs replication here; its two replicas are bare endpoints the test answers from, so the
     * same replica acking twice is exactly what arrives.
     */
    @Test
    fun C4_write_needs_w_distinct_acks() = runTest {
        val nodes = listOf(NodeId("node-1"), NodeId("node-2"), NodeId("node-3"))
        val ring = Ring.of(nodes.toSet())
        val (coordinator, first, second) = ring.preferenceList(key, 3)
        val network = InMemoryTransport()
        val replicas = listOf(first, second).associateWith { network.endpoint(it) }
        val engine = ApEngine(partitionCount = 1, clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC))
        val replication = Replication(
            self = coordinator,
            ring = ring,
            config = ReplicationConfig(n = 3, w = 3, r = 1),
            engine = engine,
            store = VersionedStore(engine, DotCounter.of(coordinator, emptyList())),
            transport = network.endpoint(coordinator),
            membership = ScriptedMembership(nodes),
            clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC),
            scope = backgroundScope,
        )
        backgroundScope.launch { for (envelope in network.endpoint(coordinator).inbound) replication.receive(envelope) }
        suspend fun ack(from: NodeId, id: Long) {
            val envelope = Envelope.newBuilder().setFrom(from.name).setTo(coordinator.name)
                .setReplicateAck(ReplicateAck.newBuilder().setId(id)).build()
            replicas.getValue(from).send(coordinator, envelope)
        }
        suspend fun replicateId(): Long {
            repeat(10) {
                // Waits out the partition thread the write runs on, as the kit's drain does, so
                // the write's completion is queued before the yield that runs its next step.
                engine.submit(Command.DbSize).get()
                yield()
                network.drain()
                replicas.getValue(first).inbound.tryReceive().getOrNull()?.let { return it.replicate.id }
            }
            error("no Replicate reached $first")
        }

        val sameNodeTwice = replication.submit(Command.Set(key, "v1".toByteArray()))
        val request = replicateId()
        ack(first, request)
        ack(first, request)
        network.drain()
        yield()
        assertFalse(sameNodeTwice.isDone, "two acks from one node counted as two")
        assertEquals(
            Reply.Error("ERR", "quorum not reached: write needs 3 of 3 nodes, 2 answered within 1s"),
            sameNodeTwice.await(),
        )

        val twoNodes = replication.submit(Command.Set(key, "v2".toByteArray()))
        val next = replicateId()
        ack(first, next)
        ack(second, next)
        network.drain()
        yield()
        assertEquals(Reply.Simple("OK"), twoNodes.await())
        engine.close()
    }

    /**
     * The coordinator holds an older version than its replicas, then a newer one than they do;
     * each read answers with the value under the dominating version, whichever node held it.
     */
    @Test
    fun C4_read_returns_highest_dvv() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope)
        val (coordinator, first, second) = cluster.ring.preferenceList(key, 3)
        val counter = DotCounter.of(coordinator, emptyList())
        val older = Dvv(counter.next(), emptyMap())
        val newer = older.bump(counter)
        val newest = newer.bump(counter)

        cluster.seed(coordinator, key, "stale".toByteArray(), older)
        cluster.seed(first, key, "fresh".toByteArray(), newer)
        cluster.seed(second, key, "fresh".toByteArray(), newer)
        assertEquals(Reply.Bulk("fresh".toByteArray()), cluster.readVia(first, key))

        cluster.seed(coordinator, key, "fresher".toByteArray(), newest)
        assertEquals(Reply.Bulk("fresher".toByteArray()), cluster.readVia(second, key))

        assertEquals(2, cluster.replication(coordinator).divergentReads.get())
        cluster.close()
    }

    /**
     * One key per coordinator, in argument order, written through one node and read, deleted
     * and counted through others: each part went to its own coordinator (ADR 0002 across
     * nodes), or the other contacts would hold nothing.
     */
    @Test
    fun fanned_command_splits_by_coordinator() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope)
        val (first, second, third) = cluster.nodes
        val keys = cluster.nodes.map { node ->
            (1..100).map { Key("k$it") }.first { cluster.ring.preferenceList(it, 3).first() == node }
        }
        val pairs = keys.mapIndexed { index, key -> key to "v$index".toByteArray() }

        assertEquals(Reply.Simple("OK"), cluster.settle(cluster.router(first).submit(Command.MSet(pairs))))
        assertEquals(
            Reply.Array(pairs.map { Reply.Bulk(it.second) } + Reply.Bulk(null)),
            cluster.settle(cluster.router(second).submit(Command.MGet(keys + Key("missing")))),
        )
        assertEquals(Reply.Integer(3), cluster.settle(cluster.router(third).submit(Command.ExistsKeys(keys))))
        assertEquals(Reply.Integer(3), cluster.settle(cluster.router(third).submit(Command.DelKeys(keys))))
        assertEquals(Reply.Integer(0), cluster.settle(cluster.router(first).submit(Command.ExistsKeys(keys))))
        cluster.close()
    }

    /**
     * I2 across a coordinator restart (T51). Before the restart the coordinator wrote the key
     * twice, so its replicas hold the second value under its dot `(coord, 2)`. The restart empties
     * the coordinator's version table; if its counter also started over, the third write would be
     * stamped `(coord, 1)` or `(coord, 2)`, a dot the replicas already hold, so they would keep
     * their value and still ack -- the acknowledged write would live on the coordinator alone.
     * With the counter resumed above every dot it ever handed out, the third write is new to
     * every replica: a quorum read through a replica answers it, and read repair leaves it in place.
     */
    @Test
    fun I2_acknowledged_write_survives_coordinator_restart() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope)
        val (coordinator, first, second) = cluster.ring.preferenceList(key, 3)
        assertEquals(Reply.Simple("OK"), cluster.writeVia(coordinator, key, "v1".toByteArray()))
        assertEquals(Reply.Simple("OK"), cluster.writeVia(coordinator, key, "v2".toByteArray()))
        cluster.drainMessages()
        assertEquals(Dot(coordinator, 2), cluster.replication(first).version(key)!!.dot)

        cluster.restart(coordinator)

        assertEquals(Reply.Simple("OK"), cluster.writeVia(coordinator, key, "v3".toByteArray()))
        assertTrue(cluster.replication(coordinator).version(key)!!.dot.counter > 2, "the post-restart dot is new to every replica")
        assertEquals(Reply.Bulk("v3".toByteArray()), cluster.readVia(first, key))
        cluster.drainMessages()
        val v3 = Reply.Bulk("v3".toByteArray())
        assertEquals(mapOf(coordinator to v3, first to v3, second to v3), cluster.readAllReplicas(key), "after read repair")
        assertEquals(v3, cluster.readVia(second, key))
        cluster.assertConverged()
        cluster.close()
    }

    /**
     * ADR 0003 (T65): a `Replicate` carries the entry the coordinator logged, byte for byte. A
     * conditional `SET` with a TTL is logged with the condition decided away and the deadline
     * settled as an instant, and those bytes are what ship, so a replica re-decides neither.
     * A replica's own log holds that entry as the engine redoes one: the `SET` and then the
     * `EXPIRE` the entry decodes to, one entry each, under the coordinator's deadline.
     */
    @Test
    fun replica_applies_exactly_the_logged_entry(@TempDir dir: Path) = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 3, r = 1, scope = backgroundScope)
        val (coordinator, first, second) = cluster.ring.preferenceList(key, 3)
        val clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC)
        for (node in cluster.nodes) {
            SnapshotEngine(cluster.engine(node), Files.createDirectories(dir.resolve(node.name)), clock, fsync = FsyncPolicy.NEVER).restore()
        }
        fun logOf(node: NodeId) = WalReader(dir.resolve(node.name).resolve("wal.0")).readAll().entries.map { it.op to it.payload }

        val set = Command.Set(key, "v".toByteArray(), Command.Set.Condition.NX, Duration.ofSeconds(10))
        assertEquals(Reply.Simple("OK"), cluster.submitVia(coordinator, set))

        val logged = logOf(coordinator).single()
        assertEntry(CommandCodec.encode(Command.Set(key, "v".toByteArray(), ttl = Duration.ofSeconds(10)), Instant.EPOCH), logged)
        val shipped = cluster.network.sent.filter { it.hasReplicate() }.map { it.replicate.command.toByteArray() }
        assertEquals(2, shipped.size, "one Replicate per replica")
        shipped.forEach { assertArrayEquals(byteArrayOf(logged.first) + logged.second, it, "the shipped bytes are the logged entry") }

        val redone = CommandCodec.decode(logged.first, logged.second).map { CommandCodec.encode(it, Instant.EPOCH) }
        assertEquals(2, redone.size, "a SET with a deadline redoes as SET then EXPIRE")
        for (replica in listOf(first, second)) {
            val held = logOf(replica)
            assertEquals(redone.size, held.size, "entries logged on $replica")
            redone.zip(held).forEach { (expected, actual) -> assertEntry(expected, actual) }
        }
        cluster.close()
    }

    private fun assertEntry(expected: Pair<Byte, ByteArray>, actual: Pair<Byte, ByteArray>) {
        assertEquals(expected.first, actual.first, "op code")
        assertArrayEquals(expected.second, actual.second, "body")
    }
}
