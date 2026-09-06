package dynacache.cluster

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow

/**
 * The test kit's adapter of the [Membership] seam: every node starts alive and the test
 * scripts the rest with [set]. Replication (T22) and hinted handoff (T25) read it.
 */
class ScriptedMembership(nodes: Collection<NodeId>) : Membership {
    override val members = LinkedHashMap<NodeId, Member>()
    private val flow = MutableSharedFlow<Member>(extraBufferCapacity = 1024)
    override val changes: Flow<Member> get() = flow

    init { nodes.forEach { members[it] = Member(it, MemberState.ALIVE, 0) } }

    fun set(node: NodeId, state: MemberState, incarnation: Long = 0) {
        val member = Member(node, state, incarnation)
        members[node] = member
        flow.tryEmit(member)
    }
}
