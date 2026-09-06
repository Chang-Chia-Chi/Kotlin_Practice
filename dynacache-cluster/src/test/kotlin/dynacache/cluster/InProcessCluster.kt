package dynacache.cluster

import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

/**
 * The test kit's cluster: [nodeCount] nodes named `node-1..N`, one shared immutable [Ring]
 * (a pure function of the node set, T17), and per node one [ApEngine] plus one endpoint on
 * one [InMemoryTransport]. [n], [w] and [r] are the quorum settings later tickets read.
 *
 * `writeVia` and `readVia` run on the named node's local engine; the coordinator hop arrives
 * with T19 and replication with T22, and this shape stays.
 */
class InProcessCluster(
    nodeCount: Int,
    val n: Int,
    val w: Int,
    val r: Int,
    clock: Clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC),
    partitionsPerNode: Int = 8,
) {
    val nodes: List<NodeId> = List(nodeCount) { NodeId("node-${it + 1}") }
    val ring: Ring = Ring.of(nodes.toSet())
    val network = InMemoryTransport()
    private val engines = nodes.associateWith { ApEngine(partitionsPerNode, clock) }
    private val transports = nodes.associateWith { network.endpoint(it) }

    fun engine(node: NodeId): ApEngine = engines.getValue(node)
    fun transport(node: NodeId): Transport = transports.getValue(node)

    /** Delivers everything in flight on the network. */
    suspend fun drainMessages() = network.drain()

    suspend fun writeVia(node: NodeId, key: Key, value: ByteArray): Reply =
        engine(node).submit(Command.Set(key, value)).await()

    suspend fun readVia(node: NodeId, key: Key): Reply =
        engine(node).submit(Command.Get(key)).await()

    /** What each of the key's [n] preference-list nodes holds, by node. */
    suspend fun readAllReplicas(key: Key): Map<NodeId, Reply> =
        ring.preferenceList(key, n).associateWith { readVia(it, key) }

    fun close() {
        engines.values.forEach { it.close() }
        transports.values.forEach { it.close() }
    }
}
