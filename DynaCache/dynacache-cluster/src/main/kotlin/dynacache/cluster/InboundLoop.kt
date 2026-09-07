package dynacache.cluster

import dynacache.cluster.proto.Envelope
import kotlinx.coroutines.channels.ReceiveChannel

/**
 * A node's one reader of its [Transport.inbound], and the one place its handler order lives
 * (T68). One channel needs one reader: two would race for a forwarded command, so everything
 * a node receives arrives here and is offered to the handlers in this order.
 *
 * 1. [snapshots] -- `DistributedSnapshot.receive`, ahead of everything: a Chandy-Lamport marker
 *    is consumed and an in-flight envelope recorded before the envelope is handled (C10, T36).
 * 2. [forwards] -- `Router.receive`: a `Forward` this node coordinates and the `ForwardReply`
 *    an earlier forward is waiting on (spec 5.1).
 * 3. [replication] -- `Replication.receive`: the writes, reads, acks and repairs of a quorum.
 * 4. [antiEntropy] -- `AntiEntropy.receive`: the Merkle and key-sync exchange.
 * 5. [gossip] -- `Swim.deliver`, the last handler and the only total one: every envelope carries
 *    the piggybacked membership table, so whatever no one else claimed still merges (I8).
 *
 * Each handler but the last answers whether the envelope was its own; the first that says yes
 * ends it. The default of every handler is deaf, so a node assembled without one -- a node with
 * no snapshot directory, a router test with nothing under the forward seam -- passes what it
 * does not handle down the chain unchanged.
 */
class InboundLoop(
    private val inbound: ReceiveChannel<Envelope>,
    private val snapshots: suspend (Envelope) -> Boolean = { false },
    private val forwards: suspend (Envelope) -> Boolean = { false },
    private val replication: suspend (Envelope) -> Boolean = { false },
    private val antiEntropy: suspend (Envelope) -> Boolean = { false },
    private val gossip: suspend (Envelope) -> Unit = {},
) {

    /** The production driver: everything this node receives, until the transport closes. */
    suspend fun run() {
        for (envelope in inbound) deliver(envelope)
    }

    /** Everything already waiting and no more: what a test that steps a node by rounds needs. */
    suspend fun drain() {
        while (true) deliver(inbound.tryReceive().getOrNull() ?: return)
    }

    /** One inbound envelope, offered to the handlers in order until one claims it. */
    suspend fun deliver(envelope: Envelope) {
        if (snapshots(envelope)) return
        if (forwards(envelope)) return
        if (replication(envelope)) return
        if (antiEntropy(envelope)) return
        gossip(envelope)
    }
}
