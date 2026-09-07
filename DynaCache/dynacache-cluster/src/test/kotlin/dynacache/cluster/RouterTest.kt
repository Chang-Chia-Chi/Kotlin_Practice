package dynacache.cluster

import com.google.protobuf.ByteString
import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Forward
import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.CommandEngine
import dynacache.engine.Key
import dynacache.engine.Reply
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.CompletableFuture
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * The router at its `CommandEngine` seam (spec 5.1 steps 1 to 3, 5.2 step 1): a command whose
 * coordinator is this node runs here, any other command crosses to its coordinator and comes
 * back as the same reply.
 */
class RouterTest {

    private val nodes = listOf(NodeId("node-1"), NodeId("node-2"), NodeId("node-3"))
    private val ring = Ring.of(nodes.toSet())
    private val key = Key("orders:4711")
    private val str = Key("s")
    private val counter = Key("n")
    private val hash = Key("h")
    private val list = Key("l")
    private val zset = Key("z")
    // One hash tag, so both keys have one coordinator and the multi-key samples compare a run
    // that forwards every part with a run that forwards none (the engine's own tag rule, T17).
    private val one = Key("{multi}:a")
    private val two = Key("{multi}:b")

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
                "reply for $command",
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
     * Bytes the coordinator cannot read answer the contact with an error, and its demux survives
     * to answer the next forward: a throw there would take the node's gossip with it. No peer of
     * this build can write such a forward now that the codec is both ends of it (T64); a
     * corrupted envelope still can, and this is the op code that no command has.
     */
    @Test
    fun router_unreadable_forward_is_an_error_and_the_node_lives() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = N, w = 2, r = 2, scope = backgroundScope)
        val coordinator = cluster.ring.preferenceList(key, N).first()
        val contact = cluster.nodes.first { it != coordinator }
        // An endpoint with no router of its own, so the answer stays readable in this test.
        val onlooker = NodeId("onlooker")
        val garbage = Envelope.newBuilder().setFrom(onlooker.name).setTo(coordinator.name)
            .setForward(Forward.newBuilder().setId(1).setCommand(ByteString.copyFrom(byteArrayOf(99))))
            .build()

        cluster.network.endpoint(onlooker).send(coordinator, garbage)
        cluster.drainMessages()

        val answered = cluster.network.endpoint(onlooker).inbound.tryReceive().getOrNull()
        assertEquals("ERR", (ReplyWire.decode(answered!!.forwardReply.reply) as Reply.Error).kind)
        assertEquals(Reply.Simple("OK"), cluster.writeVia(contact, key, "v1".toByteArray()))
        cluster.close()
    }

    /** A local engine that answers nothing until [answer], and records what reached it. */
    private class HoldingEngine : CommandEngine {

        val submitted = mutableListOf<Command>()

        private val held = mutableListOf<CompletableFuture<Reply>>()

        override fun submit(command: Command): CompletableFuture<Reply> {
            submitted += command
            return CompletableFuture<Reply>().also { held += it }
        }

        /** Answers each held part with its own key, so the joined reply shows the order it kept. */
        fun answer() = held.forEachIndexed { at, future ->
            future.complete(Reply.Bulk((submitted[at] as Command.Keyed).key.bytes))
        }

        override fun close() = Unit
    }

    /**
     * T79 at the router's seam: the parts of a multi-key command go out together, not one after
     * the previous one answered. Both keys share a hash tag, so this node coordinates both and
     * nothing forwards; the engine holds every future it hands back, so two submitted parts can
     * only mean the router did not wait for the first.
     */
    @Test
    fun router_split_submits_every_part_before_any_answers() = runTest {
        val coordinator = ring.preferenceList(one, N).first()
        val engine = HoldingEngine()
        val router = Router(
            self = coordinator,
            ring = ring,
            n = N,
            local = engine,
            transport = InMemoryTransport().endpoint(coordinator),
            scope = backgroundScope,
        )

        val reply = router.submit(Command.MGet(listOf(one, two)))

        assertEquals(listOf(one, two), engine.submitted.map { (it as Command.Keyed).key }, "both parts are in")
        assertFalse(reply.isDone, "neither part has answered yet")
        engine.answer()
        assertEquals(Reply.Array(listOf(Reply.Bulk(one.bytes), Reply.Bulk(two.bytes))), reply.await())
    }

    /**
     * Spec 5.1 steps 1 to 3 over the whole command hierarchy: every variant a router forwards,
     * run once through the key's coordinator and once, on an identical set of nodes, through a
     * node that has to forward, answers the same either way. The samples run in order and build
     * state up, so the comparison is over real replies and not a column of nils.
     *
     * The routers sit straight on engines here, with no [Replication] between: what this asserts
     * is the forward's own round trip, with nothing under it.
     */
    @Test
    fun forward_round_trips_every_keyed_variant() = runTest {
        val forwarding = Routers(backgroundScope)
        val direct = Routers(backgroundScope)

        crossing().forEach(::classify)
        for (command in crossing()) {
            val coordinator = ring.preferenceList(firstKey(command), N).first()
            val contact = nodes.first { it != coordinator }
            // The hash tag has to hold, or the direct run of a fanned sample forwards parts too
            // and the comparison stops being forwarded against local.
            if (command is Command.Fanned) {
                assertTrue(
                    command.keys.all { ring.preferenceList(it, N).first() == coordinator },
                    "$command spans more than one coordinator",
                )
            }
            val here = direct.settle(direct.router(coordinator).submit(command))
            // No sample is a type error or a bad argument, so an error here is the crossing's
            // own: it is what an unreadable forward would look like on both sides at once.
            assertFalse(here is Reply.Error, "$command answered $here without leaving this node")
            assertEquals(
                here,
                forwarding.settle(forwarding.router(contact).submit(command)),
                "reply for $command forwarded from $contact to $coordinator",
            )
        }

        forwarding.close()
        direct.close()
    }

    /**
     * One router per node over one engine per node, on one network: the forward seam with
     * nothing under it. The kit's [InProcessCluster] is the same shape with replication in
     * between, which a forward does not need and which cannot yet carry every variant.
     */
    private inner class Routers(scope: CoroutineScope) {
        private val network = InMemoryTransport()
        private val engines = nodes.associateWith { ApEngine(8, Clock.fixed(Instant.EPOCH, ZoneOffset.UTC)) }
        private val routers = nodes.associateWith { node ->
            Router(
                self = node,
                ring = ring,
                n = N,
                local = engines.getValue(node),
                transport = network.endpoint(node),
                scope = scope,
            )
        }

        init {
            for (node in nodes) {
                val loop = InboundLoop(network.endpoint(node).inbound, forwards = routers.getValue(node)::receive)
                scope.launch { loop.run() }
            }
        }

        fun router(node: NodeId): Router = routers.getValue(node)

        /** Drives the network until [answer] is there: a forward needs one round out and one back. */
        suspend fun settle(answer: CompletableFuture<Reply>): Reply {
            repeat(SETTLE_ROUNDS) {
                if (answer.isDone) return answer.await()
                network.drain()
                yield()
                // Waits out the partition thread each engine ran on, as the kit's drain does.
                engines.values.forEach { it.submit(Command.DbSize).get() }
                yield()
            }
            return answer.await()
        }

        fun close() {
            engines.values.forEach(ApEngine::close)
            nodes.forEach { network.endpoint(it).close() }
        }
    }

    /**
     * One command of every variant that crosses, in an order that leaves each one something to
     * answer about: the string, hash, list and sorted-set keys are built before they are read.
     * [classify] is what makes the list total; this is what it runs on.
     */
    private fun crossing(): List<Command> = listOf(
        // String, with the conditional `SET` and the TTL a forward has to carry.
        Command.Set(str, "v1".toByteArray(), Command.Set.Condition.NX, Duration.ofSeconds(60)),
        Command.Get(str),
        Command.Exists(str),
        Command.Type(str),
        Command.Ttl(str, Command.Ttl.Precision.SECONDS),
        Command.StrLen(str),
        Command.Append(str, "tail".toByteArray()),
        Command.Persist(str),
        Command.Expire(str, Instant.EPOCH.plusSeconds(120)),
        Command.IncrBy(counter, 5),

        // Hash.
        Command.HSet(hash, listOf("f".toByteArray() to "v".toByteArray(), "g".toByteArray() to "w".toByteArray())),
        Command.HGet(hash, "f".toByteArray()),
        Command.HGetAll(hash),
        Command.HMGet(hash, listOf("f".toByteArray(), "g".toByteArray())),
        Command.HExists(hash, "f".toByteArray()),
        Command.HKeys(hash),
        Command.HVals(hash),
        Command.HLen(hash),
        Command.HScan(hash, 0, "f*".toByteArray(), 10),
        Command.HMSet(hash, listOf("f".toByteArray() to "v2".toByteArray())),
        Command.HDel(hash, listOf("g".toByteArray())),

        // List.
        Command.Push(list, listOf("a".toByteArray(), "b".toByteArray()), Command.End.TAIL),
        Command.LRange(list, 0, -1),
        Command.LLen(list),
        Command.LIndex(list, 0),
        Command.LSet(list, 0, "c".toByteArray()),
        Command.LRem(list, 1, "c".toByteArray()),
        Command.Pop(list, Command.End.HEAD),

        // Sorted set.
        Command.ZAdd(zset, listOf("1".toByteArray() to "m".toByteArray(), "2".toByteArray() to "p".toByteArray()), changed = true),
        Command.ZScore(zset, "m".toByteArray()),
        Command.ZCard(zset),
        Command.ZRange(zset, 0, -1, withScores = true, reverse = false),
        Command.ZRank(zset, "m".toByteArray(), reverse = false),
        Command.ZRangeByScore(zset, "-inf".toByteArray(), "+inf".toByteArray(), withScores = false, offset = 0, count = -1),
        Command.ZScan(zset, 0, null, 10),
        Command.ZIncrBy(zset, "1.5".toByteArray(), "m".toByteArray()),
        Command.ZRem(zset, listOf("p".toByteArray())),
        Command.Del(str),

        // Fanned: the multi-key write and read, and the two multi-key keyspace commands. The
        // router splits these before it routes, so what crosses is each single-key part.
        Command.MSet(listOf(one to "1".toByteArray(), two to "2".toByteArray())),
        Command.MGet(listOf(one, two)),
        Command.ExistsKeys(listOf(one, two)),
        Command.DelKeys(listOf(one, two)),
    )

    /**
     * Says whether [command] crosses the cluster seam. The `when` is exhaustive over [Command],
     * so a variant added to the engine stops the build here and its author has to answer the
     * question; [crossing] is the list of the ones that do, and the rest never reach a forward
     * because the router runs them on the node the client reached (T19, C16).
     */
    private fun classify(command: Command) = when (command) {
        is Command.Keyed, is Command.Fanned -> Unit
        is Command.EveryPartition, is Command.Cp, Command.Ping, Command.CommandTable, is Command.Scan -> Unit
    }

    /** The key whose coordinator a forward of [command] is aimed at. */
    private fun firstKey(command: Command): Key = when (command) {
        is Command.Keyed -> command.key
        is Command.Fanned -> command.keys.first()
        else -> error("$command has no key")
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
        const val SETTLE_ROUNDS = 100
    }
}
