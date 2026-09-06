package dynacache.cluster

import dynacache.cluster.proto.ClusterServiceGrpcKt
import dynacache.cluster.proto.Delivered
import dynacache.cluster.proto.Envelope
import io.grpc.Grpc
import io.grpc.InsecureChannelCredentials
import io.grpc.InsecureServerCredentials
import io.grpc.ManagedChannel
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel

/** Where a node's gRPC server listens. */
data class HostPort(val host: String, val port: Int)

/**
 * The gRPC adapter of the [Transport] seam (spec 2.3, cluster-facing). No codec: the generated
 * [Envelope] is the wire message, carried by one unary `Deliver` call per send.
 *
 * Hosts a server on [port]; pass 0 for an ephemeral one and read [boundPort] for what it got.
 * A [ManagedChannel] per peer opens on the first send to that peer. [peers] is read at send
 * time, not at construction, so a caller holding a mutable map may fill in addresses once every
 * node has bound - which is how two nodes on ephemeral ports learn each other.
 *
 * [send] returns when the peer has accepted the envelope. So a peer that is down is a send
 * error rather than a hang, and sequential sends to one peer arrive in the order they were sent.
 */
class GrpcTransport(
    private val self: NodeId,
    private val peers: Map<NodeId, HostPort>,
    port: Int,
) : Transport, AutoCloseable {

    private val received = Channel<Envelope>(Channel.UNLIMITED)
    private val channels = ConcurrentHashMap<NodeId, ManagedChannel>()

    private val server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
        .addService(Inbox())
        .build()
        .start()

    /** The port the server listens on; the only way to learn an ephemeral one. */
    val boundPort: Int get() = server.port

    override val inbound: ReceiveChannel<Envelope> get() = received

    /** This node's receive side: what a peer's `Deliver` call lands in. */
    private inner class Inbox : ClusterServiceGrpcKt.ClusterServiceCoroutineImplBase() {
        override suspend fun deliver(request: Envelope): Delivered {
            received.send(request)
            return Delivered.getDefaultInstance()
        }
    }

    override suspend fun send(to: NodeId, envelope: Envelope) {
        val channel = channels.computeIfAbsent(to) {
            val address = requireNotNull(peers[to]) { "$self has no address for $to" }
            Grpc.newChannelBuilder("${address.host}:${address.port}", InsecureChannelCredentials.create())
                .build()
        }
        ClusterServiceGrpcKt.ClusterServiceCoroutineStub(channel).deliver(envelope)
    }

    override fun close() {
        server.shutdownNow().awaitTermination(SHUTDOWN_SECONDS, TimeUnit.SECONDS)
        channels.values.forEach { it.shutdownNow() }
        channels.clear()
        received.close()
    }

    private companion object {
        const val SHUTDOWN_SECONDS = 5L
    }
}
