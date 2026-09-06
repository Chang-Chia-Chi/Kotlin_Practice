package dynacache.cluster

import kotlinx.coroutines.flow.Flow

enum class MemberState { ALIVE, SUSPECT, DEAD }

/** One row of the gossip's table: what this node currently believes about [node]. */
data class Member(val node: NodeId, val state: MemberState, val incarnation: Long)

/**
 * The gossip's current view of the cluster (plan 2.3, CONTEXT.md "membership"): every node
 * with its state and incarnation, and the changes as they happen. Replication and hinted
 * handoff read it; only SWIM writes it.
 */
interface Membership {
    val members: Map<NodeId, Member>
    val changes: Flow<Member>

    val alive: Set<NodeId> get() = nodesIn(MemberState.ALIVE)
    val suspect: Set<NodeId> get() = nodesIn(MemberState.SUSPECT)
    val dead: Set<NodeId> get() = nodesIn(MemberState.DEAD)

    private fun nodesIn(state: MemberState): Set<NodeId> =
        members.values.filter { it.state == state }.mapTo(LinkedHashSet()) { it.node }
}
