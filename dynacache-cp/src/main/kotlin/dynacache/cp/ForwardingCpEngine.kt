package dynacache.cp

import com.google.protobuf.ByteString
import dynacache.cluster.HostPort
import dynacache.cluster.NodeId
import dynacache.cp.proto.CpRequest
import dynacache.cp.proto.CpServiceGrpcKt
import dynacache.cp.proto.InfoRequest
import dynacache.engine.Command
import dynacache.engine.CommandEngine
import dynacache.engine.Key
import dynacache.engine.PartitionContext
import dynacache.engine.Reply
import io.grpc.Grpc
import io.grpc.InsecureChannelCredentials
import io.grpc.ManagedChannel
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.future
import kotlinx.coroutines.withTimeout

/**
 * An AP-only node's CP engine (CP spec 2.2): it holds no Raft node, so every CP command is
 * forwarded over `CpService.Apply` to the member it believes is the leader.
 *
 * The believed leader is learned from `GetInfo` and remembered, so the common case is one call.
 * It is forgotten again the moment the answer is `-NOTLEADER` or the member does not answer at
 * all, and the next round asks the members afresh - which is how a client of this node rides out
 * a leader failover without knowing one happened.
 */
class ForwardingCpEngine(
    private val cpMembers: List<NodeId>,
    private val addresses: Map<NodeId, HostPort>,
    private val deadline: Duration = Duration.ofSeconds(20),
) : CommandEngine {

    private val channels = ConcurrentHashMap<NodeId, ManagedChannel>()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    @Volatile
    private var believedLeader: NodeId? = null

    override fun submit(command: Command): CompletableFuture<Reply> {
        // The same edge as CpEngine's (C16): this node forwards CP work on cp: keys, nothing else.
        if (command !is Command.Cp) {
            return CompletableFuture.completedFuture(Reply.Error("NOTCP", "$command is not a CP command"))
        }
        if (!command.key.isCp()) {
            return CompletableFuture.completedFuture(Reply.Error("NOTCP", "${command.key} is not a cp: key"))
        }
        // CP spec 6.7 is a report, not a log entry: CP.INFO asks the leader for one and
        // CP.MEMBERS is answered from the fixed membership this node was configured with.
        if (command is Command.Cp.Info) return info()
        if (command is Command.Cp.Members) {
            return CompletableFuture.completedFuture(
                Reply.Array(cpMembers.map { Reply.Bulk(it.name.toByteArray()) }),
            )
        }
        val request = CpRequest.newBuilder().setCommand(ByteString.copyFrom(CpWire.encode(command))).build()
        return scope.future { withTimeout(deadline.toMillis()) { forward(request) } }
    }

    /** `CP.INFO` (CP spec 6.7) as the leader reports it. */
    fun info(): CompletableFuture<Reply> = scope.future {
        withTimeout(deadline.toMillis()) {
            val member = believedLeader ?: discoverLeader() ?: cpMembers.first()
            CpWire.infoReply(stub(member).getInfo(InfoRequest.getDefaultInstance()))
        }
    }

    override fun <R> atomically(keys: List<Key>, block: (PartitionContext) -> R): CompletableFuture<R> =
        throw NotImplementedError("CP has no batches: the Raft log already serializes every entry")

    override fun close() {
        scope.cancel()
        channels.values.forEach { it.shutdownNow() }
        channels.clear()
    }

    private suspend fun forward(request: CpRequest): Reply {
        while (true) {
            val leader = believedLeader ?: discoverLeader()
            val reply = leader?.let { attempt(it, request) }
            if (reply != null && !reply.isNotLeader()) return reply

            // Silence, or a member that has stopped being the leader: forget it and ask the group
            // again. A failover takes an election to resolve, so this is the loop that rides it
            // out; the deadline in submit is what ends it.
            // ponytail: a retry after silence is at-least-once, so a command that committed just
            // as the connection dropped can apply twice. Deduplicating it needs a per-session
            // request id, and sessions arrive in T41 (CP spec 4).
            believedLeader = null
            delay(RETRY_MILLIS)
        }
    }

    private suspend fun attempt(leader: NodeId, request: CpRequest): Reply? =
        runCatching { stub(leader).apply(request) }
            .getOrNull()
            ?.let { CpWire.decodeReply(it.reply.toByteArray()) }

    /** Asks the CP members in turn and takes the first answer that names a leader. */
    private suspend fun discoverLeader(): NodeId? {
        for (member in cpMembers) {
            val leader = runCatching { stub(member).getInfo(InfoRequest.getDefaultInstance()) }
                .getOrNull()?.leader?.takeIf(String::isNotEmpty) ?: continue
            return NodeId(leader).also { believedLeader = it }
        }
        return null
    }

    private fun Reply.isNotLeader(): Boolean = this is Reply.Error && kind == "NOTLEADER"

    private fun stub(member: NodeId): CpServiceGrpcKt.CpServiceCoroutineStub {
        val channel = channels.computeIfAbsent(member) {
            val address = requireNotNull(addresses[member]) { "no address for CP member $member" }
            Grpc.newChannelBuilder("${address.host}:${address.port}", InsecureChannelCredentials.create()).build()
        }
        return CpServiceGrpcKt.CpServiceCoroutineStub(channel)
    }

    private fun Key.isCp(): Boolean = toString().startsWith("cp:")

    private companion object {
        /** How long to wait before asking the members again; an election is measured in seconds. */
        const val RETRY_MILLIS = 50L
    }
}
