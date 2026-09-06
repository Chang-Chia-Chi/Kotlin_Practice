package dynacache.cluster

import dynacache.cluster.proto.Envelope
import kotlinx.coroutines.channels.ReceiveChannel

/**
 * The seam through which nodes exchange cluster messages (plan 2.3, CONTEXT.md "transport").
 * The messages are the generated protobuf [Envelope]s themselves, so no codec sits between
 * the in-memory adapter and the gRPC one (T23).
 *
 * One endpoint per node. [send] hands an envelope to the transport and returns; delivery is
 * the adapter's business. Envelopes from one sender to one receiver arrive in the order they
 * were sent; nothing is promised across pairs. [inbound] is this node's receive side.
 */
interface Transport {
    suspend fun send(to: NodeId, envelope: Envelope)
    val inbound: ReceiveChannel<Envelope>
    fun close()
}
