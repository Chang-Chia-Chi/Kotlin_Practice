package dynacache.cluster

import com.google.protobuf.ByteString
import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Replicate
import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import dynacache.engine.Value
import dynacache.engine.view
import dynacache.engine.install
import dynacache.engine.persist.CommandCodec
import dynacache.engine.persist.DotCeilingStore
import java.nio.file.Path
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.CompletableFuture
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Assertions.assertEquals

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
    private val scope: CoroutineScope,
    private val clock: Clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC),
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
    private val ceilings = nodes.associateWith { DotCeilingStore.inMemory() }
    private val replications = HashMap<NodeId, Replication>()
    private val antiEntropies = HashMap<NodeId, AntiEntropy>()
    private val routers = HashMap<NodeId, Router>()
    private val loops = HashMap<NodeId, List<Job>>()
    private val snapshots: Map<NodeId, DistributedSnapshot> = nodes.associateWith { node ->
        DistributedSnapshot(
            node, nodes - node, engines.getValue(node), transports.getValue(node), snapshotDir, clock,
            demux = { routers.getValue(node).receive(it) },
            scope = scope,
        )
    }
    init {
        nodes.forEach(::start)
    }

    /** Builds [node]'s replication layer over its engine and transport and starts its loops. */
    private fun start(node: NodeId) {
        val engine = engines.getValue(node)
        val transport = transports.getValue(node)
        val counter = DotCounter.of(node, emptyList(), ceilings.getValue(node))
        val replication = Replication(
            self = node,
            ring = ring,
            config = config,
            engine = engine,
            transport = transport,
            membership = membership,
            counter = counter,
            clock = clock,
            view = { key -> engine.view(listOf(key)).thenApply { it.firstOrNull() } },
            install = engine::install,
            scope = scope,
        )
        val antiEntropy = AntiEntropy(
            self = node,
            ring = ring,
            n = n,
            engine = engine,
            replication = replication,
            transport = transport,
            membership = membership,
            counter = counter,
        )
        val router = Router(
            self = node,
            ring = ring,
            n = n,
            local = replication,
            transport = transport,
            scope = scope,
            others = { if (!replication.receive(it) && !antiEntropy.receive(it)) gossiped.getValue(node).add(it) },
            snapshots = snapshots.getValue(node)::receive,
        )
        replications[node] = replication
        antiEntropies[node] = antiEntropy
        routers[node] = router
        loops[node] = listOf(scope.launch { router.run() }, scope.launch { replication.runHandoff() })
    }

    /**
     * Restarts [node]'s replication layer over the same engine, as a process restart does while
     * the engine restores from disk: an empty version table, a counter rebuilt from what the node
     * persisted, and fresh loops on the same transport (T51). The replicas keep their memory.
     */
    fun restart(node: NodeId) {
        loops.getValue(node).forEach(Job::cancel)
        start(node)
    }

    fun engine(node: NodeId): ApEngine = engines.getValue(node)
    fun transport(node: NodeId): Transport = transports.getValue(node)
    fun replication(node: NodeId): Replication = replications.getValue(node)
    fun router(node: NodeId): Router = routers.getValue(node)
    fun antiEntropy(node: NodeId): AntiEntropy = antiEntropies.getValue(node)

    /** One anti-entropy step on [node], the network driven until the step has its answers. */
    suspend fun antiEntropyStep(node: NodeId) {
        val step = scope.launch { antiEntropy(node).tick() }
        repeat(SETTLE_ROUNDS) {
            if (step.isCompleted) return
            drainMessages()
            yield()
        }
        step.join()
    }

    /** One full anti-entropy cycle on [node]: every range it replicates, once. */
    suspend fun antiEntropyCycle(node: NodeId) = repeat(antiEntropy(node).ranges.size) { antiEntropyStep(node) }
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

    /** Every key a command or a seed went to through this kit: what [assertConverged] checks. */
    val written = LinkedHashSet<Key>()

    suspend fun writeVia(node: NodeId, key: Key, value: ByteArray): Reply = submitVia(node, Command.Set(key, value))

    suspend fun readVia(node: NodeId, key: Key): Reply = submitVia(node, Command.Get(key))

    /** [command] through [node]'s router, as a client would send it, driven until answered. */
    suspend fun submitVia(node: NodeId, command: Command.Keyed): Reply {
        written += command.key
        return settle(router(node).submit(command))
    }

    /**
     * [command] coordinated by [node] itself, whether or not the ring says so: what spec 5.1
     * step 7's failover would do when the coordinator is unreachable, which the router does not
     * do yet (progress T22 deviation 7). This is how a test writes on both sides of a partition.
     */
    suspend fun submitOn(node: NodeId, command: Command.Keyed): Reply {
        written += command.key
        return settle(replication(node).submit(command))
    }

    /** What each of the key's [n] preference-list nodes holds, by node: local reads, no hop. */
    suspend fun readAllReplicas(key: Key): Map<NodeId, Reply> =
        ring.preferenceList(key, n).associateWith { engine(it).submit(Command.Get(key)).await() }

    /**
     * Puts [value] under [key] on [node] exactly as a replica receives a write: a `Replicate`
     * from an endpoint no node owns, so [node] applies it and holds [dvv] for the key. This is
     * how a test makes replicas disagree.
     */
    suspend fun seed(node: NodeId, key: Key, value: ByteArray, dvv: Dvv) = seed(node, Command.Set(key, value), dvv)

    /** [seed] with any write, so a replica can be made to hold a hash or a list under [dvv]. */
    suspend fun seed(node: NodeId, write: Command.Keyed, dvv: Dvv) {
        written += write.key
        val seeder = network.endpoint(SEEDER)
        val body = Replicate.newBuilder().setId(0)
            .setCommand(ByteString.copyFrom(CommandCodec.frame(write, clock.instant())))
            .setDvv(ByteString.copyFrom(dvv.encode()))
        seeder.send(node, Envelope.newBuilder().setFrom(SEEDER.name).setTo(node.name).setReplicate(body).build())
        repeat(SETTLE_ROUNDS) {
            drainMessages()
            if (seeder.inbound.tryReceive().isSuccess) return
        }
        error("$node never acknowledged the seed of ${write.key}")
    }

    /**
     * I1 (spec 4): heals every network partition, brings every node back alive, drains messages
     * and hints, runs one full anti-entropy cycle on every node, drains again, and asserts that
     * every replica of every key in [written] holds the same value, deadline and version. The first
     * divergent key fails with both sides. A key absent on a replica compares as absent alone:
     * anti-entropy carries no tombstones (progress T28 deviation 3), so the version a delete
     * leaves behind is never synced to a replica that holds none, and no client can see it.
     */
    suspend fun assertConverged() {
        network.heal()
        nodes.forEach(network::restart)
        membership.members.values.filter { it.state != MemberState.ALIVE }
            .forEach { membership.set(it.node, MemberState.ALIVE, it.incarnation + 1) }
        drainMessages()
        drainHints()
        for (node in nodes) {
            val sync = antiEntropy(node)
            val target = sync.rangesCompared.get() + sync.ranges.size
            var steps = 0
            while (sync.rangesCompared.get() < target) {
                check(steps++ < 2 * sync.ranges.size) { "$node's anti-entropy cycle did not complete: a peer stayed silent" }
                antiEntropyStep(node)
            }
        }
        drainMessages()
        for (key in written) {
            val held = ring.preferenceList(key, n).associateWith { node ->
                val stored = engine(node).view(listOf(key)).await().firstOrNull()
                Triple(stored?.let { canon(it.value) }, stored?.expiresAt, stored?.let { replication(node).version(key) })
            }
            val (reference, expected) = held.entries.first()
            for ((node, actual) in held) assertEquals(expected, actual, "$key on $node differs from $reference")
        }
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

/** A value as plain data, so two values compare by content: what a client would read back. */
fun canon(value: Value): Any = when (value) {
    is Value.Str -> value.bytes.toList()
    is Value.Hash -> value.fields.entries().associate { (name, bytes) -> name to bytes.toList() }
    is Value.List -> value.items.map { it.toList() }
    is Value.ZSet -> value.order.forward().map { it.member.toList() to it.score }.toList() to
        value.scores.entries().associate { (member, score) -> member to score }
}
