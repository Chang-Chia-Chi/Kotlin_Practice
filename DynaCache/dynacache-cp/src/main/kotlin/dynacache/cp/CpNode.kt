package dynacache.cp

import dynacache.cluster.HostPort
import dynacache.cluster.NodeId
import dynacache.engine.CommandEngine
import io.microraft.RaftConfig
import java.nio.file.Path
import java.time.Clock

/**
 * This node's part in the CP subsystem: the [engine] a connection submits CP work to, and the
 * [runtime] when this node holds the replicated log itself rather than forwarding to it.
 *
 * This and [cpNode] used to sit in the server file, which then changed for a Raft reason as well
 * as for the socket, the handler and `main` (T83). The store, the runtime, the engine and the
 * gRPC presence are all this module's, so the order they go together in is too. Nothing here
 * knows about Netty or the command line: a composition root passes a group, an address book, a
 * port and a directory, and gets back a node to start, read a port from and close.
 */
class CpNode(
    val engine: CommandEngine,
    val runtime: RaftRuntime?,
    private val grpc: CpGrpcServer?,
) : AutoCloseable {

    /** The port this member's `CpService` and `RaftService` listen on; 0 on an AP-only node. */
    val cpPort: Int get() = grpc?.boundPort ?: 0

    /** The gRPC presence goes first, then the engine, which is what closes the Raft node. */
    override fun close() {
        grpc?.close()
        engine.close()
    }
}

/**
 * This node's CP subsystem, built in the one order a member has to be built in: the store (so it
 * reads back what it remembered), then the runtime on top of it, then the engine, then the gRPC
 * presence the other members reach it through.
 *
 * [self] outside [members] is an AP-only node: it holds no Raft node and forwards every CP command
 * to whoever leads (CP spec 2.2, 2.4). [addresses] is read at send time, so members on ephemeral
 * ports can be built first and told each other's ports afterwards. [storeDir] is null for a member
 * that keeps its log in memory, which is a member that cannot come back from a restart.
 */
fun cpNode(
    self: NodeId,
    members: List<NodeId>,
    addresses: Map<NodeId, HostPort>,
    port: Int,
    storeDir: Path?,
    clock: Clock,
    raft: RaftConfig = RaftConfig.DEFAULT_RAFT_CONFIG,
): CpNode {
    if (self !in members) return CpNode(ForwardingCpEngine(members, addresses), null, null)
    val store = storeDir?.let { FileRaftStore(it) } ?: InMemoryRaftStore()
    val config = CpConfig(self, members, raft = raft, clock = clock)
    val runtime = RaftRuntime(config, GrpcRaftTransport(self, addresses), store)
    val engine = CpEngine(runtime)
    return CpNode(engine, runtime, CpGrpcServer(runtime, engine, port))
}

/**
 * `id@host:port,...` as the address book every CP member is given, empty when nothing was named.
 * How a CP group is written down is this module's, not the composition root's: the entries are in
 * the same order on every node, since CP membership is fixed at startup (CP spec 2.2).
 */
fun cpAddressBook(members: String?): Map<NodeId, HostPort> =
    members?.split(",")?.filter(String::isNotBlank)?.associate { entry ->
        val (id, address) = entry.split('@', limit = 2)
        NodeId(id) to HostPort(address.substringBeforeLast(':'), address.substringAfterLast(':').toInt())
    }.orEmpty()

/** Where a CP member's own Raft store lives, under whatever data directory the node was given. */
const val CP_DIR = "cp"
