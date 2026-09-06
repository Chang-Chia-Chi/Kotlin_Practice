package dynacache.server

import dynacache.cluster.AntiEntropy
import dynacache.cluster.DotCounter
import dynacache.cluster.GrpcTransport
import dynacache.cluster.HostPort
import dynacache.cluster.NodeId
import dynacache.cluster.Replication
import dynacache.cluster.ReplicationConfig
import dynacache.cluster.Ring
import dynacache.cluster.Router
import dynacache.cluster.Swim
import dynacache.cluster.Transport
import dynacache.cluster.proto.Envelope
import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.CommandEngine
import dynacache.engine.Key
import dynacache.engine.PartitionContext
import dynacache.engine.Reply
import dynacache.engine.install
import dynacache.engine.view
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch
import java.time.Clock
import java.util.concurrent.CompletableFuture
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * One cluster node: everything a client can reach on one machine, assembled. The RESP socket in
 * front, the gRPC transport behind, and between them the stack plan 2.3 draws --
 * `Router(Replication(ApEngine))` -- with SWIM reading the same transport through the router's
 * demux (T19). The node presents the `CommandEngine` shape to its own RESP pipeline, so the
 * handler submits to a cluster exactly as it submits to a single engine.
 *
 * [addresses] is read at send time, not at construction (T23), so three nodes on ephemeral gRPC
 * ports can be built first and told each other's ports afterwards; [nodes] is the ring, which is
 * known up front because a node id is not an address. Dynamic membership is not built (plan 2.4),
 * so both are fixed for the life of the node.
 */
class ClusterNode(
    val self: NodeId,
    nodes: Set<NodeId>,
    addresses: Map<NodeId, HostPort>,
    respPort: Int = 6379,
    grpcPort: Int = 7379,
    private val config: ReplicationConfig = ReplicationConfig(n = 3, w = 2, r = 2),
    partitionCount: Int = 16,
    clock: Clock = Clock.systemUTC(),
    gossipPeriod: Duration = 1.seconds,
) : CommandEngine, AutoCloseable {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val ring = Ring.of(nodes)
    private val parser = CommandParser(clock)

    private val engine = ApEngine(partitionCount, clock)
    private val wire = GrpcTransport(self, addresses, grpcPort)
    private val counter = DotCounter.of(self, emptyList())

    private val swim = Swim(
        self = self,
        peers = nodes - self,
        // Deaf on purpose: the router's demux is this node's one reader of the transport and
        // hands gossip here through `deliver`, so SWIM's own `tryReceive` must find nothing
        // rather than race the demux for a forwarded command (T19 deviation 5).
        transport = NodeTransport(wire, reads = false),
        random = Random.Default,
        period = gossipPeriod,
    )

    private val replication = Replication(
        self = self,
        ring = ring,
        config = config,
        engine = engine,
        transport = NodeTransport(wire, reads = false),
        membership = swim,
        counter = counter,
        clock = clock,
        tokens = ::commandToTokens,
        parse = ::parse,
        view = { key -> engine.view(listOf(key)).thenApply { it.firstOrNull() } },
        install = engine::install,
        scope = scope,
    )

    private val antiEntropy = AntiEntropy(
        self = self,
        ring = ring,
        n = config.n,
        engine = engine,
        replication = replication,
        transport = NodeTransport(wire, reads = false),
        membership = swim,
        counter = counter,
    )

    private val router = Router(
        self = self,
        ring = ring,
        n = config.n,
        local = replication,
        transport = NodeTransport(wire, reads = true),
        tokens = ::commandToTokens,
        parse = ::parse,
        scope = scope,
        others = { if (!replication.receive(it) && !antiEntropy.receive(it)) swim.deliver(it) },
    )

    private val server = DynaCacheServer(respPort, engine, ap = this, clock = clock)

    /** The gRPC port this node listens on; the only way to learn an ephemeral one. */
    val grpcPort: Int get() = wire.boundPort

    /** The RESP port this node listens on. Reads only after [start]. */
    val respPort: Int get() = server.boundPort

    /** Opens the RESP socket and starts the node's four background loops. */
    fun start() {
        server.start()
        scope.launch { router.run() }
        scope.launch { swim.run() }
        scope.launch { replication.runHandoff() }
        scope.launch { antiEntropy.run() }
    }

    /**
     * `INFO` is the node's whole observability (plan 2.4), so the cluster's state is a section of
     * it. Every other command goes to the router unchanged: the router decides whether this node
     * coordinates the key, and nothing below learns that a client asked.
     */
    override fun submit(command: Command): CompletableFuture<Reply> =
        if (command == Command.Info) router.submit(command).thenApply(::withClusterSection) else router.submit(command)

    /**
     * A batch runs only where its keys are coordinated (T19 deviation 4). It is not replicated
     * either (T22 deviation 5): the writes land on this node's engine and reach no replica.
     */
    override fun <R> atomically(keys: List<Key>, block: (PartitionContext) -> R): CompletableFuture<R> =
        router.atomically(keys, block)

    override fun close() {
        server.close()
        scope.cancel()
        wire.close()
        engine.close()
    }

    /**
     * The `# Cluster` section, appended to whatever the engine's `INFO` already said. The
     * membership table's key set is fixed at construction, so reading it from the RESP thread
     * while the gossip coroutine updates a row cannot restructure the map underneath.
     */
    private fun withClusterSection(reply: Reply): Reply {
        val body = (reply as? Reply.Bulk)?.bytes ?: return reply
        val view = swim.members.values.sortedBy { it.node }
        val lines = listOf(
            "# Cluster",
            "cluster_enabled:1",
            "cluster_my_id:$self",
            "cluster_known_nodes:${view.size}",
            "cluster_quorum:n=${config.n},w=${config.w},r=${config.r}",
            "cluster_hints_pending:${replication.hintCount}",
            "cluster_ranges_compared:${antiEntropy.rangesCompared}",
            "cluster_keys_synced:${antiEntropy.keysSynced}",
        ) + view.map { "member_${it.node}:${it.state.name.lowercase()},${it.incarnation}" } + ""
        return Reply.Bulk(body + lines.joinToString(CRLF).toByteArray(Charsets.ISO_8859_1))
    }

    /** What a peer forwarded, read exactly as this node's own clients are read. */
    private fun parse(tokens: List<ByteArray>): Command = when (val parsed = parser.parse(tokens)) {
        is Parsed.Ok -> parsed.command
        is Parsed.Failed -> throw IllegalArgumentException(parsed.error.message)
    }

    private companion object {
        const val CRLF = "\r\n"
    }
}

/**
 * This node's view of its own transport. Two things the collaborators above it need and the
 * gRPC adapter does not promise:
 *
 * A send to a node that is gone is a **dropped envelope, not a throw**. gRPC reports a down peer
 * out of `send` (T23) and nothing above it has an error path for that: gossip's tick would die
 * with the exception and stop detecting the very failure it just saw, and a quorum's fan-out
 * would fail the write instead of waiting for the replicas that are up. The in-memory adapter
 * every cluster test was written against simply drops, and this makes gRPC agree.
 *
 * [reads] is false for every copy but the router's: one channel needs one reader (T19).
 */
private class NodeTransport(private val wire: Transport, private val reads: Boolean) : Transport {

    private val deaf = Channel<Envelope>()

    override val inbound: ReceiveChannel<Envelope> get() = if (reads) wire.inbound else deaf

    override suspend fun send(to: NodeId, envelope: Envelope) {
        try {
            wire.send(to, envelope)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (unreachable: Exception) {
            // The peer is down or going down. Gossip will notice; a quorum waits for the rest.
        }
    }

    /** The wire is the node's, not this view's: [ClusterNode.close] closes it once. */
    override fun close() {
        deaf.close()
    }
}

/**
 * `--peers=id=host:port,...` and friends: one node of a cluster rather than a single node.
 * `--node` names this node, `--grpc` its cluster port, `--quorum=n/w/r` the replication factor,
 * and the RESP port, partition count and persistence are the positional arguments [main]
 * already takes.
 */
internal fun clusterMain(flags: Map<String, String>, respPort: Int, partitionCount: Int) {
    val addresses = flags.getValue("peers").split(",").associate { peer ->
        val (id, address) = peer.split("=", limit = 2)
        val (host, port) = address.split(":", limit = 2)
        NodeId(id) to HostPort(host, port.toInt())
    }
    val self = NodeId(flags.getValue("node"))
    require(self in addresses) { "--peers must name every node including $self" }
    val (n, w, r) = (flags["quorum"] ?: "3/2/2").split("/").map(String::toInt)
    val node = ClusterNode(
        self = self,
        nodes = addresses.keys,
        addresses = addresses,
        respPort = respPort,
        grpcPort = flags["grpc"]?.toInt() ?: addresses.getValue(self).port,
        config = ReplicationConfig(n, w, r),
        partitionCount = partitionCount,
    )
    Runtime.getRuntime().addShutdownHook(Thread(node::close))
    node.start()
    println("DynaCache $self listening on ${node.respPort}, cluster on ${node.grpcPort}, N=$n W=$w R=$r")
}
