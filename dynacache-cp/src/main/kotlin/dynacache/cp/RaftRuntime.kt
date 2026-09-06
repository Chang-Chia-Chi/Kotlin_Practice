package dynacache.cp

import io.microraft.RaftNode
import io.microraft.RaftRole
import io.microraft.persistence.NopRaftStore
import io.microraft.transport.Transport
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

/**
 * One CP member's MicroRaft node: the replicated log, the state machine that applies it, and the
 * transport it reaches the other members through. The transport is the seam (plan 2.3) — tests
 * pass an in-memory one, T43 passes the gRPC one — and this class knows nothing about either.
 */
class RaftRuntime(
    val config: CpConfig,
    transport: Transport,
    val stateMachine: AtomicLongStateMachine = AtomicLongStateMachine(),
) : AutoCloseable {

    init {
        require(config.isCpMember) { "${config.nodeId} is not a CP member of ${config.cpMembers}" }
    }

    val endpoint = CpEndpoint(config.nodeId)

    /** Completes the first time this member wins an election. */
    private val leadership = CompletableFuture<RaftRuntime>()

    val node: RaftNode = RaftNode.newBuilder()
        .setGroupId(config.groupId)
        .setLocalEndpoint(endpoint)
        .setInitialGroupMembers(config.endpoints)
        .setConfig(config.raft)
        .setTransport(transport)
        .setStateMachine(stateMachine)
        // ponytail: no persistence, so a restarted member replays from the leader instead of from
        // its own disk. Snapshot and restore are T45 (I20); a RaftStore lands with them.
        .setStore(NopRaftStore())
        .setRaftNodeReportListener { report ->
            if (report.role == RaftRole.LEADER) leadership.complete(this)
        }
        .build()

    /** True when this member is the one that may replicate; every other member answers NOTLEADER. */
    val isLeader: Boolean get() = endpoint == node.term.leaderEndpoint

    fun start(): RaftRuntime = apply { node.start().join() }

    /** Blocks until this member has won an election, or the configured timeout passes. */
    fun awaitLeadership(): RaftRuntime =
        leadership.get(config.leaderElectionTimeout.toMillis(), TimeUnit.MILLISECONDS)

    /** The future that [awaitLeadership] waits on, for a caller racing several members. */
    val elected: CompletableFuture<RaftRuntime> get() = leadership

    override fun close() {
        runCatching { node.terminate().join() }
    }
}
