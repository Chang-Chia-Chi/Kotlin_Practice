package dynacache.cluster

import dynacache.cluster.proto.Envelope
import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.CompletableFuture
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield

/**
 * The test kit's cluster: [nodeCount] nodes named `node-1..N`, one shared immutable [Ring]
 * (a pure function of the node set, T17), and per node one [ApEngine], one endpoint on one
 * [InMemoryTransport] and one [Router] whose inbound loop runs on [scope]. [n], [w] and [r]
 * are the quorum settings later tickets read.
 *
 * `writeVia` and `readVia` go through the contact node's router, so a key the contact does not
 * coordinate crosses the network (T19); replication arrives with T22 and this shape stays.
 */
class InProcessCluster(
    nodeCount: Int,
    val n: Int,
    val w: Int,
    val r: Int,
    scope: CoroutineScope,
    clock: Clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC),
    partitionsPerNode: Int = 8,
) {
    val nodes: List<NodeId> = List(nodeCount) { NodeId("node-${it + 1}") }
    val ring: Ring = Ring.of(nodes.toSet())
    val network = InMemoryTransport()
    private val engines = nodes.associateWith { ApEngine(partitionsPerNode, clock) }
    private val transports = nodes.associateWith { network.endpoint(it) }
    private val gossiped = nodes.associateWith { mutableListOf<Envelope>() }
    private val routers = nodes.associateWith { node ->
        Router(
            self = node,
            ring = ring,
            n = n,
            local = engines.getValue(node),
            transport = transports.getValue(node),
            tokens = TokenCodec::tokens,
            parse = TokenCodec::command,
            scope = scope,
            gossip = { gossiped.getValue(node).add(it) },
        )
    }

    init {
        routers.values.forEach { router -> scope.launch { router.run() } }
    }

    fun engine(node: NodeId): ApEngine = engines.getValue(node)
    fun transport(node: NodeId): Transport = transports.getValue(node)
    fun router(node: NodeId): Router = routers.getValue(node)

    /** What the demux on [node] handed to gossip: the envelopes SWIM would have answered. */
    fun gossipOn(node: NodeId): List<Envelope> = gossiped.getValue(node)

    /** Delivers everything in flight on the network, then lets each node's demux read it. */
    suspend fun drainMessages() {
        network.drain()
        yield()
    }

    suspend fun writeVia(node: NodeId, key: Key, value: ByteArray): Reply =
        settle(router(node).submit(Command.Set(key, value)))

    suspend fun readVia(node: NodeId, key: Key): Reply =
        settle(router(node).submit(Command.Get(key)))

    /** What each of the key's [n] preference-list nodes holds, by node: local reads, no hop. */
    suspend fun readAllReplicas(key: Key): Map<NodeId, Reply> =
        ring.preferenceList(key, n).associateWith { engine(it).submit(Command.Get(key)).await() }

    fun close() {
        engines.values.forEach { it.close() }
        transports.values.forEach { it.close() }
    }

    /**
     * Drives the network until [answer] is there. Nothing moves on an [InMemoryTransport] until
     * a drain, and a forward needs two: the request, then the reply. The engine completes its
     * futures on a partition thread, so the round count is a bound rather than an exact number;
     * past it the caller simply awaits, which is what lets a forward's deadline fire in virtual
     * time when no reply is ever coming.
     */
    suspend fun settle(answer: CompletableFuture<Reply>): Reply {
        repeat(SETTLE_ROUNDS) {
            if (answer.isDone) return answer.await()
            drainMessages()
            yield()
        }
        return answer.await()
    }

    private companion object {
        const val SETTLE_ROUNDS = 100
    }
}
