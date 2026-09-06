package dynacache.cp

import dynacache.cluster.NodeId
import io.microraft.RaftConfig
import io.microraft.RaftEndpoint
import java.time.Clock
import java.time.Duration

/**
 * A cluster node's address inside the Raft group. MicroRaft compares endpoints by equality, so
 * this is a data class over the cluster's own [NodeId] and the gRPC transport (T43) can map it
 * back to a peer address.
 */
data class CpEndpoint(val nodeId: NodeId) : RaftEndpoint {
    override fun getId(): Any = nodeId.name
}

/**
 * What a node needs to take part in the CP subsystem: who it is, and which nodes hold the
 * replicated log. CP membership is fixed at startup (CP spec 2.2); a node whose [nodeId] is not
 * in [cpMembers] is AP-only and forwards CP work to the leader (T43).
 *
 * The member count must be odd and at least three, so a majority is always well defined.
 */
data class CpConfig(
    val nodeId: NodeId,
    val cpMembers: List<NodeId>,
    val groupId: String = "dynacache-cp",
    val raft: RaftConfig = RaftConfig.DEFAULT_RAFT_CONFIG,
    /** How long a caller waits for a leader to be elected before giving up. */
    val leaderElectionTimeout: Duration = Duration.ofSeconds(10),
    /** This member's wall clock; only a leader reads it, and only to stamp log entries (CP spec 5). */
    val clock: Clock = Clock.systemUTC(),
    /** How long the log may go without an entry before the leader's [RaftRuntime.tick] appends one. */
    val tickInterval: Duration = Duration.ofMillis(100),
) {
    init {
        require(cpMembers.size >= 3) { "a CP group needs at least 3 members, got ${cpMembers.size}" }
        require(cpMembers.size % 2 == 1) { "a CP group needs an odd member count, got ${cpMembers.size}" }
        require(cpMembers.toSet().size == cpMembers.size) { "duplicate CP member in $cpMembers" }
    }

    /** True when this node holds the replicated log rather than forwarding to it. */
    val isCpMember: Boolean get() = nodeId in cpMembers

    val endpoints: List<CpEndpoint> get() = cpMembers.map(::CpEndpoint)
}
