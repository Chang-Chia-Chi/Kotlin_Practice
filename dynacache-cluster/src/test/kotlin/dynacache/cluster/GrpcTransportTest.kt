package dynacache.cluster

import dynacache.cluster.proto.Ack
import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Forward
import dynacache.cluster.proto.ForwardReply
import dynacache.cluster.proto.Ping
import dynacache.cluster.proto.PingReq
import dynacache.cluster.proto.Read
import dynacache.cluster.proto.ReadReply
import dynacache.cluster.proto.Replicate
import dynacache.cluster.proto.ReplicateAck
import dynacache.cluster.proto.ReplyMsg
import com.google.protobuf.ByteString
import io.grpc.StatusException
import java.net.ServerSocket
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
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

    @Test
    fun grpc_peer_down_is_a_send_error() {
        val peers = mapOf(bravo to HostPort(LOCALHOST, portNothingListensOn()))
        GrpcTransport(alpha, peers, port = 0).use { a ->
            assertThrows(StatusException::class.java) {
                runBlocking {
                    withTimeout(DEADLINE) { a.send(bravo, envelopeOf(Envelope.BodyCase.PING, alpha, bravo)) }
                }
            }
        }
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
                Forward.newBuilder().setId(7).addAllToken(listOf("GET", "k").map(ByteString::copyFromUtf8))
            )
            Envelope.BodyCase.FORWARD_REPLY -> envelope.setForwardReply(
                ForwardReply.newBuilder().setId(7).setReply(ReplyMsg.newBuilder().setSimple("OK"))
            )
            Envelope.BodyCase.REPLICATE -> envelope.setReplicate(
                Replicate.newBuilder().setId(7).addAllToken(listOf("SET", "k", "v").map(ByteString::copyFromUtf8))
                    .setDvv(ByteString.copyFromUtf8("dvv")).setExpiresAtMillis(9)
            )
            Envelope.BodyCase.REPLICATE_ACK -> envelope.setReplicateAck(ReplicateAck.newBuilder().setId(7))
            Envelope.BodyCase.READ -> envelope.setRead(
                Read.newBuilder().setId(7).addAllToken(listOf("GET", "k").map(ByteString::copyFromUtf8))
            )
            Envelope.BodyCase.READ_REPLY -> envelope.setReadReply(
                ReadReply.newBuilder().setId(7).setReply(ReplyMsg.newBuilder().setSimple("OK")).setDvv(ByteString.copyFromUtf8("dvv"))
            )
            Envelope.BodyCase.BODY_NOT_SET -> throw AssertionError("BODY_NOT_SET is not a message type")
        }
        return withBody.build()
    }

    private companion object {
        const val LOCALHOST = "localhost"
        val DEADLINE = 10.seconds
    }
}
