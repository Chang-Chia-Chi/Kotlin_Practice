package dynacache.cluster

import com.google.protobuf.ByteString
import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Forward
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * The router at its `CommandEngine` seam (spec 5.1 steps 1 to 3, 5.2 step 1): a command whose
 * coordinator is this node runs here, any other command crosses to its coordinator and comes
 * back as the same reply.
 */
class RouterTest {

    private val nodes = setOf(NodeId("node-1"), NodeId("node-2"), NodeId("node-3"))
    private val ring = Ring.of(nodes)
    private val key = Key("orders:4711")

    @Test
    fun router_executes_locally_when_coordinator() = runTest {
        val coordinator = ring.preferenceList(key, N).first()
        val engine = RecordingEngine(Reply.Simple("OK"))
        val network = InMemoryTransport()
        val router = Router(
            self = coordinator,
            ring = ring,
            n = N,
            local = engine,
            transport = network.endpoint(coordinator),
            tokens = TokenCodec::tokens,
            parse = TokenCodec::command,
            scope = backgroundScope,
        )

        val reply = router.submit(Command.Set(key, "v1".toByteArray())).await()

        assertEquals(Reply.Simple("OK"), reply)
        assertEquals(1, engine.submitted.size)
    }

    /** N = 1 so the contact is no replica of the key: what lands on its engine got there by not forwarding. */
    @Test
    fun router_forwards_to_coordinator() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 1, w = 1, r = 1, scope = backgroundScope)
        val coordinator = cluster.ring.preferenceList(key, N).first()
        val contact = cluster.nodes.first { it != coordinator }

        assertEquals(Reply.Simple("OK"), cluster.writeVia(contact, key, "v1".toByteArray()))

        assertEquals(Reply.Bulk("v1".toByteArray()), cluster.engine(coordinator).submit(Command.Get(key)).await())
        assertEquals(Reply.Bulk(null), cluster.engine(contact).submit(Command.Get(key)).await())
        cluster.close()
    }

    /**
     * A forward changes nothing about the answer. The same commands run twice on two identical
     * clusters, once through a contact that must forward and once on the coordinator itself,
     * and every reply shape the crossing has to carry -- a status, a bulk, a nil bulk, an
     * integer, an array and an error -- comes back the same either way.
     */
    @Test
    fun router_forwarded_reply_identical_to_local() = runTest {
        val forwarding = InProcessCluster(nodeCount = 3, n = N, w = 2, r = 2, scope = backgroundScope)
        val direct = InProcessCluster(nodeCount = 3, n = N, w = 2, r = 2, scope = backgroundScope)
        val coordinator = forwarding.ring.preferenceList(key, N).first()
        val contact = forwarding.nodes.first { it != coordinator }

        for (command in replyShapes()) {
            assertEquals(
                direct.settle(direct.router(coordinator).submit(command)),
                forwarding.settle(forwarding.router(contact).submit(command)),
                "reply for ${TokenCodec.tokens(command).joinToString(" ") { it.decodeToString() }}",
            )
        }

        forwarding.close()
        direct.close()
    }

    /**
     * A coordinator the contact cannot reach: the future is not left open, it answers with the
     * error the deadline names. No sleeping -- the deadline runs on `runTest`'s virtual clock.
     */
    @Test
    fun router_forward_timeout_is_an_error() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = N, w = 2, r = 2, scope = backgroundScope)
        val coordinator = cluster.ring.preferenceList(key, N).first()
        val contact = cluster.nodes.first { it != coordinator }
        cluster.network.networkPartition(listOf(setOf(contact), (cluster.nodes - contact).toSet()))

        val reply = cluster.writeVia(contact, key, "v1".toByteArray())

        assertEquals(Reply.Error("ERR", "forward timeout after 2s waiting for $coordinator"), reply)
        assertEquals(Reply.Bulk(null), cluster.engine(coordinator).submit(Command.Get(key)).await())
        cluster.close()
    }

    /**
     * Tokens the coordinator cannot read answer the contact with an error, and its demux
     * survives to answer the next forward: a throw there would take the node's gossip with it.
     */
    @Test
    fun router_unreadable_forward_is_an_error_and_the_node_lives() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = N, w = 2, r = 2, scope = backgroundScope)
        val coordinator = cluster.ring.preferenceList(key, N).first()
        val contact = cluster.nodes.first { it != coordinator }
        // An endpoint with no router of its own, so the answer stays readable in this test.
        val onlooker = NodeId("onlooker")
        val garbage = Envelope.newBuilder().setFrom(onlooker.name).setTo(coordinator.name)
            .setForward(Forward.newBuilder().setId(1).addToken(ByteString.copyFromUtf8("NOSUCH")))
            .build()

        cluster.network.endpoint(onlooker).send(coordinator, garbage)
        cluster.drainMessages()

        val answered = cluster.network.endpoint(onlooker).inbound.tryReceive().getOrNull()
        assertEquals("ERR", (ReplyWire.decode(answered!!.forwardReply.reply) as Reply.Error).kind)
        assertEquals(Reply.Simple("OK"), cluster.writeVia(contact, key, "v1".toByteArray()))
        cluster.close()
    }

    /** One command per reply shape a `ForwardReply` has to carry, in the order they run. */
    private fun replyShapes(): List<Command> = listOf(
        Command.Get(key),
        Command.Set(key, "v1".toByteArray()),
        Command.Get(key),
        Command.IncrBy(key, 1),
        Command.Del(key),
        Command.HSet(key, listOf("f".toByteArray() to "v".toByteArray())),
        Command.HGetAll(key),
    )

    private companion object {
        const val N = 3
    }
}
