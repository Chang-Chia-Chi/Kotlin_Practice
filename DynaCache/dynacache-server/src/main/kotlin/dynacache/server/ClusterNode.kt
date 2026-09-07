package dynacache.server

import dynacache.cluster.AntiEntropy
import dynacache.cluster.DistributedSnapshot
import dynacache.cluster.DotCounter
import dynacache.cluster.GrpcTransport
import dynacache.cluster.HostPort
import dynacache.cluster.InboundLoop
import dynacache.cluster.NodeId
import dynacache.cluster.Replication
import dynacache.cluster.ReplicationConfig
import dynacache.cluster.Ring
import dynacache.cluster.Router
import dynacache.cluster.VersionedStore
import dynacache.cluster.Swim
import dynacache.cp.CP_DIR
import dynacache.cp.cpAddressBook
import dynacache.cp.cpNode
import dynacache.engine.ApEngine
import dynacache.engine.BatchEngine
import dynacache.engine.Command
import dynacache.engine.CommandEngine
import dynacache.engine.EvictionPolicy
import dynacache.engine.Key
import dynacache.engine.PartitionContext
import dynacache.engine.Reply
import dynacache.engine.persist.DotCeilingStore
import dynacache.engine.persist.FileSnapshotParts
import dynacache.engine.persist.FsyncPolicy
import dynacache.engine.persist.SnapshotEngine
import io.microraft.RaftConfig
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.nio.file.Path
import java.time.Clock
import java.util.concurrent.CompletableFuture
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * One cluster node: everything a client can reach on one machine, assembled. The RESP socket in
 * front, the gRPC transport behind, and between them the stack plan 2.3 draws --
 * `Router(Replication(ApEngine))` -- with one [InboundLoop] over the whole of it: one transport,
 * one reader, and the handler order in one place (T68). The node presents the `CommandEngine`
 * shape to its own RESP pipeline, so the handler submits to a cluster exactly as it submits to a
 * single engine.
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
    private val clock: Clock = Clock.systemUTC(),
    gossipPeriod: Duration = 1.seconds,
    /** Where this node's own RDB and log live (spec 2.8); null is a node that persists nothing. */
    dataDir: Path? = null,
    fsync: FsyncPolicy = FsyncPolicy.EVERY_SECOND,
    /** The root of the snapshot sets this node takes part in; null is a node that takes none. */
    snapshotDir: Path? = null,
    /** This node's memory threshold and the policy it sheds keys by (spec 2.7); a node's own. */
    maxMemoryBytes: Long? = null,
    policy: EvictionPolicy = EvictionPolicy.LRU,
    /**
     * The CP group (CP spec 2.2), empty on a node with no CP subsystem at all. This node holds the
     * replicated log when [cpMembers] names it and forwards to whoever leads when it does not, so
     * an AP-only node is the same constructor with a group that leaves it out.
     */
    cpMembers: List<NodeId> = emptyList(),
    /** Where the CP members listen, read at send time exactly as [addresses] is. */
    cpAddresses: Map<NodeId, HostPort> = emptyMap(),
    cpPort: Int = 0,
    /**
     * MicroRaft's timings, defaulted to its own. A calibration knob rather than a constant: a
     * failover takes as long as a heartbeat timeout, and how long that should be is an operator's
     * call about a real network, not something this assembly can know (T45).
     */
    cpRaft: RaftConfig = RaftConfig.DEFAULT_RAFT_CONFIG,
) : CommandEngine, BatchEngine, AutoCloseable {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val ring = Ring.of(nodes)

    private val engine = ApEngine(partitionCount, clock, maxMemoryBytes = maxMemoryBytes, policy = policy)
    private val wire = GrpcTransport(self, addresses, grpcPort)
    /**
     * This node's dots resume above the ceiling it last reserved at `<dataDir>/dots` (T51), so a
     * restart never re-stamps a write with a dot its replicas already hold. A node with no data
     * directory forgets its ceiling as it forgets its keys. The counter's other floor is the
     * version table, which [start] rebuilds from this node's snapshot and log before the port
     * opens and which raises the counter key by key on the way in (T67), so nothing is scanned
     * here: the table is empty until then.
     */
    private val counter = DotCounter.of(
        self, emptyList(), dataDir?.let { DotCeilingStore.inFile(it.resolve(DOT_CEILING_FILE)) } ?: DotCeilingStore.inMemory(),
    )

    private val swim = Swim(
        self = self,
        peers = nodes - self,
        transport = wire,
        random = Random.Default,
        period = gossipPeriod,
    )

    /** The node's (value, version) pairs over its engine (T66): what replication and anti-entropy read and install. */
    private val store = VersionedStore(engine, counter)

    private val replication = Replication(
        self = self,
        ring = ring,
        config = config,
        engine = engine,
        store = store,
        transport = wire,
        membership = swim,
        clock = clock,
        scope = scope,
    )

    private val antiEntropy = AntiEntropy(
        self = self,
        ring = ring,
        n = config.n,
        store = store,
        transport = wire,
        membership = swim,
    )

    private val router = Router(
        self = self,
        ring = ring,
        n = config.n,
        local = replication,
        transport = wire,
        scope = scope,
    )

    /**
     * This node's part of a **snapshot set**: the inbound loop consults it ahead of every other
     * handler, so a marker is consumed and an in-flight envelope recorded before the envelope is
     * handled (T36). Null on a node with no [snapshotDir], and then the hook always says no.
     */
    private val distributed: DistributedSnapshot? = snapshotDir?.let {
        DistributedSnapshot(
            self = self,
            peers = nodes - self,
            transport = wire,
            parts = FileSnapshotParts(it, self.name, engine, clock),
            demux = { envelope -> inbound.deliver(envelope) },
            scope = scope,
        )
    }

    /** This node's one reader of [wire], and the one place its handler order lives (T68). */
    private val inbound = InboundLoop(
        inbound = wire.inbound,
        snapshots = { distributed?.receive(it) ?: false },
        forwards = router::receive,
        replication = replication::receive,
        antiEntropy = antiEntropy::receive,
        gossip = swim::deliver,
    )

    /**
     * Local persistence, exactly as single-node `main` has it: the last snapshot and the log
     * after it restored before the port opens, a snapshot on the tick's interval and one more at
     * shutdown. A cluster node needs its own because a restart is what makes a node warm again;
     * the cluster's own repair paths only cover what a peer still holds.
     */
    private val snapshots = dataDir?.let { SnapshotEngine(engine, it, clock, fsync = fsync) }

    /**
     * This node's CP subsystem, or null when it was given no group. The two engines are siblings
     * here: the same three nodes hold the AP ring and the replicated log, and the dispatcher in
     * front of them is what decides which one a command was for (CP spec 2.1, 9.5).
     */
    private val cp = cpMembers
        .takeIf { it.isNotEmpty() }
        ?.let { cpNode(self, it, cpAddresses, cpPort, dataDir?.resolve(CP_DIR), clock, cpRaft) }

    private val server = DynaCacheServer(respPort, engine, cp = cp?.engine, ap = this, batch = this, clock = clock, fsync = fsync) {
        engine.tick()
        engine.wal?.tick()
        // The leader's TTL tick (CP spec 5): log time moves on, and a session past its timeout is
        // closed with everything it held. A follower's tick does nothing.
        cp?.runtime?.tick()
        snapshots?.maybeSave(clock.instant())
    }

    /** The gRPC port this node listens on; the only way to learn an ephemeral one. */
    val grpcPort: Int get() = wire.boundPort

    /** The CP gRPC port this member listens on; 0 on a node that runs no Raft member. */
    val cpPort: Int get() = cp?.cpPort ?: 0

    /** The RESP port this node listens on. Reads only after [start]. */
    val respPort: Int get() = server.boundPort

    /** Restores what this node persisted, opens the RESP socket and starts its four loops. */
    fun start() {
        snapshots?.restore()
        // The Raft member joins its group before the port opens, so the first client to arrive
        // finds an engine that is already electing rather than one that has not begun.
        cp?.runtime?.start()
        server.start()
        scope.launch { inbound.run() }
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
     * A batch runs only where its keys are coordinated (T19 deviation 4): it is one partition's
     * uninterrupted run (C12), and a forward would have to carry the caller's block, which is
     * code. A batch whose keys this node does not coordinate fails the future rather than
     * answering, since the signature's `R` is the caller's own type and has no error shape.
     *
     * It is not replicated either (T22 deviation 5): the writes land on this node's engine and
     * reach no replica, which is why it goes to [engine] and not through [replication].
     */
    override fun <R> atomically(keys: List<Key>, block: (PartitionContext) -> R): CompletableFuture<R> {
        val elsewhere = keys.map { it to ring.preferenceList(it, config.n).first() }.firstOrNull { it.second != self }
        if (elsewhere != null) {
            return CompletableFuture.failedFuture(
                IllegalStateException("${elsewhere.first} is coordinated by ${elsewhere.second}, not $self")
            )
        }
        return engine.atomically(keys, block)
    }

    /**
     * Starts snapshot [id] from this node (spec 2.8 step 1). An operator's control rather than a
     * client's verb: nothing a Redis client can ask for takes a distributed cut, so it is a
     * method here and not a command at the handler. Completion is [snapshotComplete].
     */
    fun snapshot(id: String) {
        val part = checkNotNull(distributed) { "$self was given no snapshot directory" }
        scope.launch { part.initiate(id) }
    }

    /** Whether every incoming channel of [id] has closed here; the set is done when all nodes say so. */
    fun snapshotComplete(id: String): Boolean = distributed?.complete(id) == true

    /**
     * Loads this node's part of snapshot [id] and replays what its channels recorded (I12). A
     * startup operation on a fresh node: it neither flushes the engine nor resets the version
     * table (T36 deviation 6), so it runs before this node's first client, not beside one.
     *
     * The part has to be here and complete. An id this node has no complete part of fails with
     * an `IllegalArgumentException` and changes nothing, rather than restoring the empty state a
     * missing part looks like on disk: an operator's typo does not empty a node.
     */
    fun restoreSnapshot(id: String) = runBlocking {
        checkNotNull(distributed) { "$self was given no snapshot directory" }.restoreFrom(id)
    }

    override fun close() {
        server.close()
        scope.cancel()
        cp?.close()
        wire.close()
        // The shutdown save (spec 2.8), while the engine is still open and nothing submits.
        snapshots?.let { runCatching(it::close) }
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
            // A snapshot set this node's storage refused (T80). The node kept serving, so this
            // count is the only place the operator learns its disk would not take a part.
            "cluster_snapshots_abandoned:${distributed?.abandoned ?: 0}",
        ) + listOfNotNull(distributed?.lastAbandoned?.let { "cluster_snapshot_last_failure:$it" }) +
            view.map { "member_${it.node}:${it.state.name.lowercase()},${it.incarnation}" } + ""
        return Reply.Bulk(body + lines.joinToString(CRLF).toByteArray(Charsets.ISO_8859_1))
    }

    private companion object {
        const val CRLF = "\r\n"
        const val DOT_CEILING_FILE = "dots"
    }
}

/**
 * `--peers=id=host:port,...` and friends: one node of a cluster rather than a single node.
 * `--node` names this node, `--grpc` its cluster port, `--quorum=n/w/r` the replication factor,
 * and the RESP port, partition count and persistence are the positional arguments [main]
 * already takes -- [dataDir] and [fsync] mean here exactly what they mean on a single node, and
 * the snapshot sets this node takes part in live under it, since a marker may arrive from any
 * peer and a node with nowhere to put its part cannot answer one.
 *
 * [cpGroup] is `main`'s last positional argument, `id@host:port,...`: the CP members of CP spec
 * 2.2, or nothing at all for a cluster that runs the AP engine alone. Which entry this node is
 * `--node` already says, so the single node's `cp-self` argument has nothing to add here.
 */
internal fun clusterMain(
    flags: Map<String, String>,
    respPort: Int,
    partitionCount: Int,
    dataDir: Path? = null,
    fsync: FsyncPolicy = FsyncPolicy.EVERY_SECOND,
    cpGroup: String? = null,
) {
    val addresses = flags.getValue("peers").split(",").associate { peer ->
        val (id, address) = peer.split("=", limit = 2)
        val (host, port) = address.split(":", limit = 2)
        NodeId(id) to HostPort(host, port.toInt())
    }
    val self = NodeId(flags.getValue("node"))
    require(self in addresses) { "--peers must name every node including $self" }
    val (n, w, r) = (flags["quorum"] ?: "3/2/2").split("/").map(String::toInt)
    val cp = cpAddressBook(cpGroup)
    val node = ClusterNode(
        self = self,
        nodes = addresses.keys,
        addresses = addresses,
        respPort = respPort,
        grpcPort = flags["grpc"]?.toInt() ?: addresses.getValue(self).port,
        config = ReplicationConfig(n, w, r),
        partitionCount = partitionCount,
        dataDir = dataDir,
        fsync = fsync,
        snapshotDir = dataDir?.resolve("snapshots"),
        cpMembers = cp.keys.toList(),
        cpAddresses = cp,
        cpPort = cp[self]?.port ?: 0,
    )
    Runtime.getRuntime().addShutdownHook(Thread(node::close))
    node.start()
    println("DynaCache $self listening on ${node.respPort}, cluster on ${node.grpcPort}, N=$n W=$w R=$r")
    if (cp.isNotEmpty()) {
        val here = if (self in cp) "a member on ${node.cpPort}" else "forwarding"
        println("CP group ${cp.keys.joinToString()}, $self is $here")
    }
}
