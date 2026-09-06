package dynacache.cluster

import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.ReplicateAck
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

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
        val replication = Replication(
            self = coordinator,
            ring = ring,
            config = ReplicationConfig(n = 3, w = 3, r = 1),
            engine = RecordingEngine(),
            transport = network.endpoint(coordinator),
            membership = ScriptedMembership(nodes),
            counter = DotCounter.of(coordinator, emptyList()),
            clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC),
            tokens = TokenCodec::tokens,
            parse = TokenCodec::command,
            scope = backgroundScope,
        )
        backgroundScope.launch { for (envelope in network.endpoint(coordinator).inbound) replication.receive(envelope) }
        suspend fun ack(from: NodeId, id: Long) {
            val envelope = Envelope.newBuilder().setFrom(from.name).setTo(coordinator.name)
                .setReplicateAck(ReplicateAck.newBuilder().setId(id)).build()
            replicas.getValue(from).send(coordinator, envelope)
        }
        suspend fun replicateId(): Long {
            yield()
            network.drain()
            return replicas.getValue(first).inbound.receive().replicate.id
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
}
