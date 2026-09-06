package dynacache.cp

import com.google.protobuf.ByteString
import dynacache.cp.proto.Accepted
import dynacache.cp.proto.CpInfo
import dynacache.cp.proto.CpRequest
import dynacache.cp.proto.CpResponse
import dynacache.cp.proto.CpServiceGrpcKt
import dynacache.cp.proto.HeartbeatRequest
import dynacache.cp.proto.HeartbeatResponse
import dynacache.cp.proto.InfoRequest
import dynacache.cp.proto.RaftEnvelope
import dynacache.cp.proto.RaftServiceGrpcKt
import dynacache.engine.Command
import dynacache.engine.Reply
import io.grpc.Grpc
import io.grpc.InsecureServerCredentials
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.future.await

/**
 * One CP member's gRPC presence: `RaftService`, where the other members' MicroRaft traffic lands,
 * and `CpService`, where a client or an AP-only node's forwarded command lands (CP spec 2.4).
 *
 * `Apply` submits to this member's own [CpEngine], so a follower answers `-NOTLEADER <hint>` and
 * does not forward: CP spec 9.1 step 3 leaves the retry to the client, which keeps one command
 * from touring the group.
 *
 * Pass 0 for [port] to take an ephemeral one and read [boundPort] for what it got.
 */
class CpGrpcServer(
    private val runtime: RaftRuntime,
    private val engine: CpEngine,
    port: Int = 0,
) : AutoCloseable {

    private val server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
        .addService(RaftInbox())
        .addService(CpCalls())
        .build()
        .start()

    /** The port the server listens on; the only way to learn an ephemeral one. */
    val boundPort: Int get() = server.port

    /** The `CP.INFO` data this member can see (CP spec 6.7); empty leader when it knows of none. */
    suspend fun info(): CpInfo = CpWire.info(runtime.node.getReport().await().result)

    override fun close() {
        server.shutdownNow().awaitTermination(SHUTDOWN_SECONDS, TimeUnit.SECONDS)
    }

    private inner class RaftInbox : RaftServiceGrpcKt.RaftServiceCoroutineImplBase() {
        override suspend fun handle(request: RaftEnvelope): Accepted {
            runtime.node.handle(CpWire.decode(request))
            return Accepted.getDefaultInstance()
        }
    }

    private inner class CpCalls : CpServiceGrpcKt.CpServiceCoroutineImplBase() {

        override suspend fun apply(request: CpRequest): CpResponse {
            val reply = engine.submit(CpWire.decodeCommand(request.command.toByteArray())).await()
            return CpResponse.newBuilder().setReply(ByteString.copyFrom(CpWire.encode(reply))).build()
        }

        override suspend fun getInfo(request: InfoRequest): CpInfo = info()

        /** `ok` only when this member's engine applied the heartbeat: a dead session or a follower says no. */
        override suspend fun heartbeat(request: HeartbeatRequest): HeartbeatResponse {
            val session = request.sessionId.toLongOrNull()
            val reply = session?.let { engine.submit(Command.Cp.SessionHeartbeat(it)).await() }
            return HeartbeatResponse.newBuilder().setOk(reply is Reply.Simple).build()
        }
    }

    private companion object {
        const val SHUTDOWN_SECONDS = 5L
    }
}
