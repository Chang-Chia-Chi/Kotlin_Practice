package dynacache.cluster

import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Forward
import dynacache.cluster.proto.Marker
import dynacache.cluster.proto.MerkleRoot
import dynacache.cluster.proto.Ping
import dynacache.cluster.proto.Replicate
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * A node's one inbound loop (T68): the handler order lives here and nowhere else, and every
 * handler but the last says whether the envelope was its own (spec 2.8 C10, spec 5.1).
 */
class InboundLoopTest {

    @Test
    fun inbound_order_is_snapshots_forwards_replication_antientropy_gossip() = runTest {
        val channel = Channel<Envelope>(Channel.UNLIMITED)
        val seen = mutableListOf<String>()
        val loop = InboundLoop(
            inbound = channel,
            snapshots = { seen += "snapshots"; it.hasMarker() },
            forwards = { seen += "forwards"; it.hasForward() },
            replication = { seen += "replication"; it.hasReplicate() },
            antiEntropy = { seen += "antiEntropy"; it.hasMerkleRoot() },
            gossip = { seen += "gossip" },
        )

        // All five arrive together, each for a different handler, in one batch on one channel.
        listOf(marker, forward, replicate, merkleRoot, ping).forEach { channel.trySend(it) }
        channel.close()
        loop.run()

        assertEquals(
            listOf(
                "snapshots",
                "snapshots", "forwards",
                "snapshots", "forwards", "replication",
                "snapshots", "forwards", "replication", "antiEntropy",
                "snapshots", "forwards", "replication", "antiEntropy", "gossip",
            ),
            seen,
        )
    }

    /** A marker stops at the snapshot hook: it is consumed, never handled again (C10). */
    @Test
    fun a_handler_that_claims_an_envelope_ends_it() = runTest {
        val channel = Channel<Envelope>(Channel.UNLIMITED)
        val gossiped = mutableListOf<Envelope>()
        val loop = InboundLoop(channel, snapshots = { it.hasMarker() }, gossip = { gossiped += it })

        loop.deliver(marker)
        loop.deliver(ping)

        assertEquals(listOf(ping), gossiped)
    }

    private val marker = envelope { it.setMarker(Marker.newBuilder().setSnapshotId("s1")) }
    private val forward = envelope { it.setForward(Forward.newBuilder().setId(1)) }
    private val replicate = envelope { it.setReplicate(Replicate.newBuilder().setId(1)) }
    private val merkleRoot = envelope { it.setMerkleRoot(MerkleRoot.newBuilder().setId(1)) }
    private val ping = envelope { it.setPing(Ping.newBuilder().setSeq(1)) }

    private fun envelope(body: (Envelope.Builder) -> Envelope.Builder): Envelope =
        body(Envelope.newBuilder().setFrom("node-2").setTo("node-1")).build()
}
