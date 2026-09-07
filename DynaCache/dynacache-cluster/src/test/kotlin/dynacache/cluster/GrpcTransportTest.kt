package dynacache.cluster

import dynacache.cluster.proto.Ack
import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Forward
import dynacache.cluster.proto.ForwardReply
import dynacache.cluster.proto.KeySync
import dynacache.cluster.proto.KeySyncReply
import dynacache.cluster.proto.Leaf
import dynacache.cluster.proto.Marker
import dynacache.cluster.proto.MerkleRoot
import dynacache.cluster.proto.MerkleRootReply
import dynacache.cluster.proto.Ping
import dynacache.cluster.proto.PingReq
import dynacache.cluster.proto.Read
import dynacache.cluster.proto.ReadReply
import dynacache.cluster.proto.Repair
import dynacache.cluster.proto.Replicate
import dynacache.cluster.proto.ReplicateAck
import dynacache.cluster.proto.ReplyMsg
import dynacache.cluster.proto.Version
import com.google.protobuf.ByteString
import java.net.ServerSocket
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * The gRPC adapter of the [Transport] seam over real localhost sockets (spec 2.3 cluster-facing).
 * Real time, so every wait carries a deadline and `runBlocking` rather than `runTest`, whose
 * virtual clock would fire those deadlines while a socket was still in flight.
 */
class GrpcTransportTest {

    private val alpha = NodeId("alpha")
    private val bravo = NodeId("bravo")

    @Test
    fun grpc_transport_roundtrip_every_message_type() = runBlocking {
        val peers = mutableMapOf<NodeId, HostPort>()
        GrpcTransport(alpha, peers, port = 0).use { a ->
            GrpcTransport(bravo, peers, port = 0).use { b ->
                peers[alpha] = HostPort(LOCALHOST, a.boundPort)
                peers[bravo] = HostPort(LOCALHOST, b.boundPort)

                for (case in messageTypes()) {
                    val there = envelopeOf(case, from = alpha, to = bravo)
                    val back = envelopeOf(case, from = bravo, to = alpha)

                    a.send(bravo, there)
                    b.send(alpha, back)

                    assertEquals(there, withTimeout(DEADLINE) { b.inbound.receive() })
                    assertEquals(back, withTimeout(DEADLINE) { a.inbound.receive() })
                }
            }
        }
    }

    /**
     * The [Outbound] seam's one promise, on both of its adapters (T68). Nothing above the seam
     * has an error path for a peer that is gone: gossip's tick would die with the exception and
     * stop detecting the very failure it just saw, and a quorum's fan-out would fail the write
     * instead of waiting for the replicas that are up.
     */
    @Test
    fun unreachable_peer_is_a_drop_on_both_adapters() = runBlocking {
        GrpcTransport(alpha, mapOf(bravo to HostPort(LOCALHOST, portNothingListensOn())), port = 0).use { grpc ->
            withTimeout(DEADLINE) { grpc.send(bravo, envelopeOf(Envelope.BodyCase.PING, alpha, bravo)) }
            assertTrue(grpc.inbound.tryReceive().isFailure, "a send to a dead peer was answered")
        }

        val network = InMemoryTransport()
        val memory = network.endpoint(alpha)
        network.endpoint(bravo)
        network.kill(bravo)
        memory.send(bravo, envelopeOf(Envelope.BodyCase.PING, alpha, bravo))
        network.drain()
        assertTrue(memory.inbound.tryReceive().isFailure, "a send to a dead peer was answered")
    }

    /** A port the OS just handed out and took back, so nothing is behind it. */
    private fun portNothingListensOn(): Int = ServerSocket(0).use { it.localPort }

    private fun messageTypes(): List<Envelope.BodyCase> =
        Envelope.BodyCase.values().filter { it != Envelope.BodyCase.BODY_NOT_SET }

    /**
     * One envelope per `oneof body` case in `cluster.proto`. A `when` expression must be
     * exhaustive, so a case added by a later ticket stops this file compiling until it is given
     * an envelope of its own - which is what keeps "every message type" true as the oneof grows.
     */
    private fun envelopeOf(case: Envelope.BodyCase, from: NodeId, to: NodeId): Envelope {
        val envelope = Envelope.newBuilder().setFrom(from.name).setTo(to.name)
        val withBody: Envelope.Builder = when (case) {
            Envelope.BodyCase.PING -> envelope.setPing(Ping.newBuilder().setSeq(7))
            Envelope.BodyCase.ACK -> envelope.setAck(Ack.newBuilder().setSeq(7))
            Envelope.BodyCase.PING_REQ -> envelope.setPingReq(PingReq.newBuilder().setSeq(7).setTarget("charlie"))
            Envelope.BodyCase.FORWARD -> envelope.setForward(
                // `GET k` as the engine codec writes it: op code 18, then the key length-prefixed.
                Forward.newBuilder().setId(7)
                    .setCommand(ByteString.copyFrom(byteArrayOf(18, 0, 0, 0, 1, 'k'.code.toByte())))
            )
            Envelope.BodyCase.FORWARD_REPLY -> envelope.setForwardReply(
                ForwardReply.newBuilder().setId(7).setReply(ReplyMsg.newBuilder().setSimple("OK"))
            )
            Envelope.BodyCase.REPLICATE -> envelope.setReplicate(
                Replicate.newBuilder().setId(7).setCommand(ByteString.copyFromUtf8("command"))
                    .setDvv(ByteString.copyFromUtf8("dvv")).setExpiresAtMillis(9).setHintFor("charlie")
            )
            Envelope.BodyCase.REPLICATE_ACK -> envelope.setReplicateAck(ReplicateAck.newBuilder().setId(7))
            Envelope.BodyCase.READ -> envelope.setRead(
                Read.newBuilder().setId(7).setCommand(ByteString.copyFromUtf8("command"))
            )
            Envelope.BodyCase.READ_REPLY -> envelope.setReadReply(
                ReadReply.newBuilder().setId(7).setReply(ReplyMsg.newBuilder().setSimple("OK")).setDvv(ByteString.copyFromUtf8("dvv"))
            )
            Envelope.BodyCase.MARKER -> envelope.setMarker(Marker.newBuilder().setSnapshotId("s7"))
            Envelope.BodyCase.MERKLE_ROOT -> envelope.setMerkleRoot(
                MerkleRoot.newBuilder().setId(7).setVnode(3).setRoot(ByteString.copyFromUtf8("root"))
            )
            Envelope.BodyCase.MERKLE_ROOT_REPLY -> envelope.setMerkleRootReply(
                MerkleRootReply.newBuilder().setId(7).setRoot(ByteString.copyFromUtf8("root")).addLeaf(
                    Leaf.newBuilder().setKey(ByteString.copyFromUtf8("k")).setValueHash(ByteString.copyFromUtf8("h")).setDvv(ByteString.copyFromUtf8("dvv"))
                )
            )
            Envelope.BodyCase.KEY_SYNC -> envelope.setKeySync(
                KeySync.newBuilder().setId(7).addKey(ByteString.copyFromUtf8("k")).addVersion(version())
            )
            Envelope.BodyCase.KEY_SYNC_REPLY -> envelope.setKeySyncReply(KeySyncReply.newBuilder().setId(7).addVersion(version()))
            Envelope.BodyCase.REPLICATE_VALUE -> envelope.setReplicateValue(version())
            Envelope.BodyCase.REPAIR -> envelope.setRepair(
                Repair.newBuilder().setKey(ByteString.copyFromUtf8("k")).addTarget("charlie")
            )
            Envelope.BodyCase.BODY_NOT_SET -> throw AssertionError("BODY_NOT_SET is not a message type")
        }
        return withBody.build()
    }

    private fun version(): Version.Builder = Version.newBuilder()
        .setKey(ByteString.copyFromUtf8("k")).setValue(ByteString.copyFromUtf8("v"))
        .setDvv(ByteString.copyFromUtf8("dvv")).setExpiresAtMillis(9)

    private companion object {
        const val LOCALHOST = "localhost"
        val DEADLINE = 10.seconds
    }
}
