package dynacache.cp

import com.google.protobuf.ByteString
import dynacache.cluster.HostPort
import dynacache.cluster.NodeId
import dynacache.cp.proto.CpRequest
import dynacache.cp.proto.CpServiceGrpc
import dynacache.cp.proto.HeartbeatRequest
import dynacache.engine.Command
import dynacache.engine.Reply
import io.grpc.Grpc
import io.grpc.InsecureChannelCredentials
import io.grpc.ManagedChannel
import io.microraft.RaftConfig
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

/**
 * A three-member CP group on localhost, each member a real gRPC server on an ephemeral port, plus
 * one AP-only node that holds no Raft node and forwards. The address book is filled in as the
 * servers bind and read at send time, which is how members on ephemeral ports find each other.
 */
class GrpcCpKit(size: Int = 3) : AutoCloseable {

    /** Fast elections and a short heartbeat timeout, so a failover test finishes in seconds. */
    private val raft: RaftConfig = RaftConfig.newBuilder()
        .setLeaderElectionTimeoutMillis(200)
        .setLeaderHeartbeatPeriodSecs(1)
        .setLeaderHeartbeatTimeoutSecs(2)
        .build()

    val members: List<NodeId> = (1..size).map { NodeId("cp$it") }

    private val addresses = ConcurrentHashMap<NodeId, HostPort>()
    private val runtimes = ConcurrentHashMap<NodeId, RaftRuntime>()
    private val transports = ConcurrentHashMap<NodeId, GrpcRaftTransport>()
    private val servers = ConcurrentHashMap<NodeId, CpGrpcServer>()
    private val killed = ConcurrentHashMap.newKeySet<NodeId>()

    private val clients = ConcurrentHashMap<NodeId, ManagedChannel>()

    /** The AP-only node: no Raft node of its own, so every CP command is forwarded (CP spec 2.2). */
    val apOnly = ForwardingCpEngine(members, addresses)

    init {
        members.forEach(::spawn)
        runtimes.values.forEach { it.start() }
    }

    fun live(): List<NodeId> = members.filterNot(killed::contains)

    fun leader(): RaftRuntime {
        val candidates = live().map { runtimes.getValue(it) }
        CompletableFuture.anyOf(*candidates.map { it.elected }.toTypedArray())
            .get(ELECTION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        return candidates.firstOrNull { it.isLeader } ?: error("a member reported leadership but none holds it")
    }

    /**
     * Waits until [member] has learned who the leader is, which it does on the leader's first
     * heartbeat. Bounded by the election deadline; nothing here sleeps for a fixed time.
     */
    fun awaitLeaderKnown(member: NodeId): NodeId = runBlocking {
        withTimeout(ELECTION_TIMEOUT_MILLIS) {
            var leader = runtimes.getValue(member).node.term.leaderEndpoint
            while (leader == null) {
                delay(POLL_MILLIS)
                leader = runtimes.getValue(member).node.term.leaderEndpoint
            }
            NodeId(leader.id.toString())
        }
    }

    /** Sends [command] straight at [member]'s `CpService`, as a client that picked its own node. */
    fun applyDirect(member: NodeId, command: Command.Cp): Reply {
        val request = CpRequest.newBuilder().setCommand(ByteString.copyFrom(CpWire.encode(command))).build()
        return CpWire.decodeReply(stub(member).apply(request).reply.toByteArray())
    }

    /** The `Heartbeat` call of `CpService` at [member], as a client keeping its session alive. */
    fun heartbeat(member: NodeId, session: String): Boolean =
        stub(member).heartbeat(HeartbeatRequest.newBuilder().setSessionId(session).build()).ok

    private fun stub(member: NodeId): CpServiceGrpc.CpServiceBlockingStub {
        val channel = clients.computeIfAbsent(member) {
            val address = addresses.getValue(member)
            Grpc.newChannelBuilder("${address.host}:${address.port}", InsecureChannelCredentials.create()).build()
        }
        return CpServiceGrpc.newBlockingStub(channel).withDeadlineAfter(REPLY_TIMEOUT_SECS, TimeUnit.SECONDS)
    }

    fun killMember(member: NodeId) {
        killed.add(member)
        servers.remove(member)?.close()
        runtimes.remove(member)?.close()
        transports.remove(member)?.close()
    }

    override fun close() {
        apOnly.close()
        clients.values.forEach { it.shutdownNow() }
        clients.clear()
        servers.values.forEach { it.close() }
        runtimes.values.forEach { it.close() }
        transports.values.forEach { it.close() }
    }

    private fun spawn(member: NodeId) {
        val transport = GrpcRaftTransport(member, addresses)
        val runtime = RaftRuntime(CpConfig(member, members, raft = raft), transport)
        val server = CpGrpcServer(runtime, CpEngine(runtime))
        transports[member] = transport
        runtimes[member] = runtime
        servers[member] = server
        addresses[member] = HostPort("localhost", server.boundPort)
    }

    private companion object {
        const val ELECTION_TIMEOUT_MILLIS = 20_000L
        const val REPLY_TIMEOUT_SECS = 20L
        const val POLL_MILLIS = 10L
    }
}
