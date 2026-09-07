package dynacache.cluster

import dynacache.cluster.proto.Envelope
import kotlinx.coroutines.channels.ReceiveChannel

/**
 * The send half of the transport seam (plan 2.3, CONTEXT.md "transport"): what a module that
 * only talks to peers needs, and nothing more. The messages are the generated protobuf
 * [Envelope]s themselves, so no codec sits between the in-memory adapter and the gRPC one (T23).
 *
 * One promise, and every adapter owes it: **a peer that cannot be reached is a dropped envelope,
 * never a throw**. Nothing above this seam has an error path for a peer that is gone -- gossip's
 * tick would die with the exception and stop detecting the very failure it just saw, and a
 * quorum's fan-out would fail the write instead of waiting for the replicas that are up. What
 * the peer does with a delivered envelope is its business; [send] hands it over and returns.
 *
 * Envelopes from one sender to one receiver arrive in the order they were sent; nothing is
 * promised across pairs.
 */
interface Outbound {
    suspend fun send(to: NodeId, envelope: Envelope)
}

/**
 * A node's whole endpoint: [Outbound] plus this node's receive side. One endpoint per node and
 * one reader of [inbound] -- the node's [InboundLoop], which owns the handler order (T68).
 */
interface Transport : Outbound {
    val inbound: ReceiveChannel<Envelope>
    fun close()
}
