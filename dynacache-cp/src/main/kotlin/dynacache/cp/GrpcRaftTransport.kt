package dynacache.cp

import dynacache.cluster.HostPort
import dynacache.cluster.NodeId
import dynacache.cp.proto.Accepted
import dynacache.cp.proto.RaftServiceGrpc
import io.grpc.Grpc
import io.grpc.InsecureChannelCredentials
import io.grpc.ManagedChannel
import io.grpc.stub.StreamObserver
import io.microraft.RaftEndpoint
import io.microraft.model.message.RaftMessage
import io.microraft.transport.Transport
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * The gRPC adapter of MicroRaft's [Transport] seam: one `RaftService.Handle` call per message, to
 * the [CpGrpcServer] of the target member.
 *
 * [addresses] is the address book that turns a [CpEndpoint] into a host and port. It is read at
 * send time, not at construction, so members binding ephemeral ports can fill each other in after
 * the fact - the same arrangement the cluster's own gRPC transport uses (T23).
 *
 * Sending never blocks the caller and never fails it. MicroRaft calls [send] from the node's own
 * thread and expects a message to be best-effort: a peer that is down or slow loses the message,
 * and the algorithm's next heartbeat or election round sends it again.
 */
class GrpcRaftTransport(
    private val self: NodeId,
    private val addresses: Map<NodeId, HostPort>,
) : Transport, AutoCloseable {

    private val channels = ConcurrentHashMap<NodeId, ManagedChannel>()
    private val stubs = ConcurrentHashMap<NodeId, RaftServiceGrpc.RaftServiceStub>()

    override fun send(target: RaftEndpoint, message: RaftMessage) {
        val to = (target as CpEndpoint).nodeId
        val peer = stubs[to] ?: addresses[to]?.let { address -> stubs.computeIfAbsent(to) { stubTo(to, address) } }
        peer?.handle(CpWire.encode(message), DROP_THE_ANSWER)
    }

    /** True when this node knows where [endpoint] listens; whether it answers is Raft's business. */
    override fun isReachable(endpoint: RaftEndpoint): Boolean = (endpoint as CpEndpoint).nodeId in addresses

    override fun close() {
        channels.values.forEach { it.shutdownNow().awaitTermination(SHUTDOWN_SECONDS, TimeUnit.SECONDS) }
        channels.clear()
        stubs.clear()
    }

    private fun stubTo(to: NodeId, address: HostPort): RaftServiceGrpc.RaftServiceStub {
        require(to != self) { "$self would send a Raft message to itself" }
        val channel = channels.computeIfAbsent(to) {
            Grpc.newChannelBuilder("${address.host}:${address.port}", InsecureChannelCredentials.create()).build()
        }
        return RaftServiceGrpc.newStub(channel)
    }

    private companion object {
        const val SHUTDOWN_SECONDS = 5L

        /** A Raft message has no reply and no error handling: losing one is a normal event. */
        val DROP_THE_ANSWER = object : StreamObserver<Accepted> {
            override fun onNext(value: Accepted) = Unit
            override fun onError(t: Throwable) = Unit
            override fun onCompleted() = Unit
        }
    }
}
