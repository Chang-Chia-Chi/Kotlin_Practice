package dynacache.cluster

import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.ReadReply
import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import dynacache.engine.install
import dynacache.engine.view
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Read repair at the `CommandEngine` seam through the test kit's cluster (spec 5.2 step 5): a
 * quorum read that finds a replica behind the winning version pushes that version to it after
 * the client has its reply, and leaves a concurrent sibling alone.
 */
class ReadRepairTest {

    private val key = Key("orders:4711")

    /** R = N so every replica answers the read and the stale one is certain to be among them. */
    private fun cluster(scope: CoroutineScope) =
        InProcessCluster(nodeCount = 3, n = 3, w = 1, r = 3, scope = scope)

    @Test
    fun read_repair_fixes_stale() = runTest {
        val cluster = cluster(backgroundScope)
        val (coordinator, first, second) = cluster.ring.preferenceList(key, 3)
        val counter = DotCounter.of(coordinator, emptyList())
        val older = Dvv(counter.next(), emptyMap())
        val newer = older.bump(counter)
        cluster.seed(coordinator, key, "fresh".toByteArray(), newer)
        cluster.seed(first, key, "stale".toByteArray(), older)
        cluster.seed(second, key, "fresh".toByteArray(), newer)

        assertEquals(Reply.Bulk("fresh".toByteArray()), cluster.readVia(second, key))
        cluster.drainMessages()

        assertEquals(List(3) { Reply.Bulk("fresh".toByteArray()) }, cluster.readAllReplicas(key).values.toList())
        cluster.nodes.forEach { assertEquals(newer, cluster.replication(it).version(key), "version on $it") }
        assertEquals(1, cluster.replication(coordinator).divergentReads.get())
        assertEquals(1, cluster.replication(coordinator).repairsSent.get())
        cluster.close()
    }

    /**
     * One replica holds a version concurrent with the coordinator's (a dot of its own, seen by
     * nobody). The read answers the tiebreak winner and repairs nothing: neither side dominates,
     * so both versions stay where they are for the merge (spec 5.3, T29) to reconcile.
     */
    @Test
    fun read_repair_skips_concurrent_siblings() = runTest {
        val cluster = cluster(backgroundScope)
        val (coordinator, first, second) = cluster.ring.preferenceList(key, 3)
        val mine = Dvv(DotCounter.of(coordinator, emptyList()).next(), emptyMap())
        val sibling = Dvv(DotCounter.of(first, emptyList()).next(), emptyMap())
        cluster.seed(coordinator, key, "mine".toByteArray(), mine)
        cluster.seed(first, key, "sibling".toByteArray(), sibling)
        cluster.seed(second, key, "mine".toByteArray(), mine)
        val expected = if (lastWriter.compare(mine, sibling) > 0) "mine" else "sibling"

        assertEquals(Reply.Bulk(expected.toByteArray()), cluster.readVia(second, key))
        cluster.drainMessages()

        assertEquals(Reply.Bulk("sibling".toByteArray()), cluster.engine(first).submit(Command.Get(key)).await())
        assertEquals(sibling, cluster.replication(first).version(key))
        assertEquals(mine, cluster.replication(coordinator).version(key))
        assertEquals(mine, cluster.replication(second).version(key))
        assertEquals(1, cluster.replication(coordinator).divergentReads.get())
        assertEquals(0, cluster.replication(coordinator).repairsSent.get())
        cluster.close()
    }

    /**
     * The winner is a replica, not the coordinator, and holds a hash: the coordinator has only
     * the reply to `HGET`, so it asks the winner to push, and the whole hash arrives on the two
     * nodes behind it, fields and version alike.
     */
    @Test
    fun read_repair_repairs_a_hash() = runTest {
        val cluster = cluster(backgroundScope)
        val (coordinator, winner, second) = cluster.ring.preferenceList(key, 3)
        val counter = DotCounter.of(coordinator, emptyList())
        val older = Dvv(counter.next(), emptyMap())
        val newer = older.bump(counter)
        val stale = Command.HSet(key, listOf("f1".toByteArray() to "old".toByteArray()))
        val fresh = Command.HSet(key, listOf("f1".toByteArray() to "new".toByteArray(), "f2".toByteArray() to "added".toByteArray()))
        cluster.seed(coordinator, stale, older)
        cluster.seed(winner, fresh, newer)
        cluster.seed(second, stale, older)

        assertEquals(Reply.Bulk("new".toByteArray()), cluster.settle(cluster.router(second).submit(Command.HGet(key, "f1".toByteArray()))))
        cluster.drainMessages()

        cluster.nodes.forEach { node ->
            assertEquals(mapOf("f1" to "new", "f2" to "added"), fields(cluster.engine(node).submit(Command.HGetAll(key)).await()), "hash on $node")
            assertEquals(newer, cluster.replication(node).version(key), "version on $node")
        }
        assertEquals(2, cluster.replication(coordinator).repairsSent.get())
        cluster.close()
    }

    /**
     * Only the coordinator runs replication; its one replica (N = 2) is a bare endpoint that
     * answers the read holding nothing and never acknowledges anything after it. The client's reply is
     * done at virtual time zero, before the repair has even left, so the repair waited for
     * nothing and the reply waited for no repair.
     */
    @Test
    fun read_repair_does_not_delay_reply() = runTest {
        val nodes = listOf(NodeId("node-1"), NodeId("node-2"), NodeId("node-3"))
        val ring = Ring.of(nodes.toSet())
        val (coordinator, first) = ring.preferenceList(key, 2)
        val network = InMemoryTransport()
        val replica = network.endpoint(first)
        val engine = ApEngine(partitionCount = 2, clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC))
        val replication = Replication(
            self = coordinator,
            ring = ring,
            config = ReplicationConfig(n = 2, w = 1, r = 2),
            engine = engine,
            transport = network.endpoint(coordinator),
            membership = ScriptedMembership(nodes),
            counter = DotCounter.of(coordinator, emptyList()),
            clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC),
            tokens = TokenCodec::tokens,
            parse = TokenCodec::command,
            view = { key -> engine.view(listOf(key)).thenApply { it.firstOrNull() } },
            install = engine::install,
            scope = backgroundScope,
        )
        backgroundScope.launch { for (envelope in network.endpoint(coordinator).inbound) replication.receive(envelope) }
        suspend fun arrived(body: Envelope.BodyCase): Envelope {
            repeat(10) {
                network.drain()
                yield()
                replica.inbound.tryReceive().getOrNull()?.let { if (it.bodyCase == body) return it }
            }
            error("no $body reached $first")
        }
        assertEquals(Reply.Simple("OK"), replication.submit(Command.Set(key, "v1".toByteArray())).await())

        val read = replication.submit(Command.Get(key))
        val request = arrived(Envelope.BodyCase.READ).read
        val empty = ReadReply.newBuilder().setId(request.id).setReply(ReplyWire.encode(Reply.Bulk(null)))
        replica.send(coordinator, Envelope.newBuilder().setFrom(first.name).setTo(coordinator.name).setReadReply(empty).build())
        network.drain()
        yield()

        assertTrue(read.isDone, "the reply waited for something after the read quorum")
        assertEquals(Reply.Bulk("v1".toByteArray()), read.await())
        assertEquals(0, currentTime)
        val repair = arrived(Envelope.BodyCase.REPLICATE_VALUE).replicateValue
        assertEquals(replication.version(key), Dvv.decode(repair.dvv.toByteArray()))
        assertEquals(1, replication.repairsSent.get())
        engine.close()
    }

    /** An `HGETALL` reply as fields, since a rebuilt hash table need not iterate in the original's order. */
    private fun fields(reply: Reply): Map<String, String> =
        (reply as Reply.Array).items.map { String((it as Reply.Bulk).bytes!!) }.chunked(2).associate { (field, value) -> field to value }
}
