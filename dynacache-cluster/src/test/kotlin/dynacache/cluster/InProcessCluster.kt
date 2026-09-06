package dynacache.cluster

import com.google.protobuf.ByteString
import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Replicate
import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import java.nio.file.Path
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.CompletableFuture
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield

/**
 * The test kit's cluster: [nodeCount] nodes named `node-1..N`, one shared immutable [Ring]
 * (a pure function of the node set, T17), one scripted [membership] every node reads, and per
 * node one [ApEngine], one endpoint on one [InMemoryTransport], one [Replication] wrapping the
 * engine, one [DistributedSnapshot] and one [Router] wrapping that, whose inbound loop and hint
 * handoff run on [scope]. [config] is the quorum: [n] replicas, [w] acks per write, [r] answers
 * per read. [snapshotDir] is the one directory every node's snapshot part lands in (T36).
 *
 * `writeVia` and `readVia` go through the contact node's router, so a key the contact does not
 * coordinate crosses the network (T19) and every write reaches its replicas (T22).
 */
class InProcessCluster(
    nodeCount: Int,
    n: Int,
    w: Int,
    r: Int,
    scope: CoroutineScope,
    clock: Clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC),
    partitionsPerNode: Int = 8,
    snapshotDir: Path = Path.of("target", "snapshots"),
) {
    val nodes: List<NodeId> = List(nodeCount) { NodeId("node-${it + 1}") }
    val ring: Ring = Ring.of(nodes.toSet())
    val config = ReplicationConfig(n, w, r)
    val n: Int get() = config.n
    val network = InMemoryTransport()
    val membership = ScriptedMembership(nodes)
    private val engines = nodes.associateWith { ApEngine(partitionsPerNode, clock) }
    private val transports = nodes.associateWith { network.endpoint(it) }
    private val gossiped = nodes.associateWith { mutableListOf<Envelope>() }
    private val replications = nodes.associateWith { node ->
        Replication(
            self = node,
            ring = ring,
            config = config,
            engine = engines.getValue(node),
            transport = transports.getValue(node),
            membership = membership,
            counter = DotCounter.of(node, emptyList()),
            clock = clock,
            tokens = TokenCodec::tokens,
            parse = TokenCodec::command,
            scope = scope,
        )
    }
    private val snapshots: Map<NodeId, DistributedSnapshot> = nodes.associateWith { node ->
        DistributedSnapshot(
            node, nodes - node, engines.getValue(node), transports.getValue(node), snapshotDir, clock,
            demux = { routers.getValue(node).receive(it) },
            scope = scope,
        )
    }
    private val routers: Map<NodeId, Router> = nodes.associateWith { node ->
        Router(
            self = node,
            ring = ring,
            n = n,
            local = replications.getValue(node),
            transport = transports.getValue(node),
            tokens = TokenCodec::tokens,
            parse = TokenCodec::command,
            scope = scope,
            others = { if (!replications.getValue(node).receive(it)) gossiped.getValue(node).add(it) },
            snapshots = snapshots.getValue(node)::receive,
        )
    }

    init {
        routers.values.forEach { router -> scope.launch { router.run() } }
        replications.values.forEach { replication -> scope.launch { replication.runHandoff() } }
    }

    fun engine(node: NodeId): ApEngine = engines.getValue(node)
    fun transport(node: NodeId): Transport = transports.getValue(node)
    fun replication(node: NodeId): Replication = replications.getValue(node)
    fun router(node: NodeId): Router = routers.getValue(node)
    fun snapshot(node: NodeId): DistributedSnapshot = snapshots.getValue(node)

    /** What the demux on [node] handed to gossip: the envelopes SWIM would have answered. */
    fun gossipOn(node: NodeId): List<Envelope> = gossiped.getValue(node)

    /**
     * Delivers everything in flight on the network, lets each node's demux read it, waits for
     * every engine to finish what the demux handed it (so a partition thread's hop never races
     * the caller), and lets what those engines completed run; again until nothing is in flight,
     * since a reply a node sends on its own coroutine is not in flight until that coroutine ran.
     */
    suspend fun drainMessages() {
        do {
            network.drain()
            yield()
            engines.values.forEach { it.submit(Command.DbSize).get() }
            yield()
        } while (network.inFlight)
    }

    /** Drives the network until no node holds a hint, or gives up after the settle bound. */
    suspend fun drainHints() {
        repeat(SETTLE_ROUNDS) {
            if (replications.values.all { it.hintCount == 0 }) return
            drainMessages()
            yield()
        }
    }

    suspend fun writeVia(node: NodeId, key: Key, value: ByteArray): Reply =
        settle(router(node).submit(Command.Set(key, value)))

    suspend fun readVia(node: NodeId, key: Key): Reply =
        settle(router(node).submit(Command.Get(key)))

    /** What each of the key's [n] preference-list nodes holds, by node: local reads, no hop. */
    suspend fun readAllReplicas(key: Key): Map<NodeId, Reply> =
        ring.preferenceList(key, n).associateWith { engine(it).submit(Command.Get(key)).await() }

    /**
     * Puts [value] under [key] on [node] exactly as a replica receives a write: a `Replicate`
     * from an endpoint no node owns, so [node] applies it and holds [dvv] for the key. This is
     * how a test makes replicas disagree.
     */
    suspend fun seed(node: NodeId, key: Key, value: ByteArray, dvv: Dvv) {
        val seeder = network.endpoint(SEEDER)
        val body = Replicate.newBuilder().setId(0)
            .addAllToken(TokenCodec.tokens(Command.Set(key, value)).map(ByteString::copyFrom))
            .setDvv(ByteString.copyFrom(dvv.encode()))
        seeder.send(node, Envelope.newBuilder().setFrom(SEEDER.name).setTo(node.name).setReplicate(body).build())
        repeat(SETTLE_ROUNDS) {
            drainMessages()
            if (seeder.inbound.tryReceive().isSuccess) return
        }
        error("$node never acknowledged the seed of $key")
    }

    fun close() {
        engines.values.forEach { it.close() }
        transports.values.forEach { it.close() }
    }

    /**
     * Drives the network until [answer] is there. Nothing moves on an [InMemoryTransport] until
     * a drain, and a forward needs two: the request, then the reply; a quorum needs two more.
     * The round count is a bound: past it the caller simply awaits, which is what lets a
     * deadline fire in virtual time when no reply is ever coming.
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
        val SEEDER = NodeId("seeder")
    }
}
