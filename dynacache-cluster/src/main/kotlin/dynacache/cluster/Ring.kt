package dynacache.cluster

import dynacache.engine.Key
import java.security.MessageDigest

/** A physical node in the cluster, named once and compared by that name. */
@JvmInline
value class NodeId(val name: String) : Comparable<NodeId> {
    override fun compareTo(other: NodeId): Int = name.compareTo(other.name)
    override fun toString(): String = name
}

/**
 * The consistent hash ring (spec 2.4): every node owns [vnodesPerNode] vnodes whose positions
 * are SHA-256 of `"<node>#<vnode index>"`, and a key sits at SHA-256 of its
 * [Key.hashedBytes] - the same hash-tag rule the engine uses, so tagged keys land together.
 *
 * The ring is a pure function of the node set and the vnode count (I5): two rings built from
 * the same inputs anywhere in the cluster answer every question identically.
 *
 * It decides placement only. Which partition executor runs a command is the engine's own
 * layer and the ring never touches it (ADR 0001, CONTEXT.md).
 */
class Ring private constructor(
    /** The physical nodes, sorted by name. */
    val nodes: List<NodeId>,
    val vnodesPerNode: Int,
    /** Every vnode, ordered clockwise; their ranges tile the ring. */
    val vnodes: List<Vnode>,
) {

    /** Where [key] falls on the ring: 63 bits of SHA-256 over its [Key.hashedBytes]. */
    fun positionOf(key: Key): Long = positionOf(key.hashedBytes)

    /** The vnode whose range holds the key: the unit Merkle trees and anti-entropy compare. */
    fun vnodeOf(key: Key): Vnode = vnodes[successorOf(positionOf(key))]

    /**
     * The [n] distinct nodes met walking clockwise from the key's position; the first is the
     * coordinator. Fails when [n] asks for more nodes than the ring has.
     */
    fun preferenceList(key: Key, n: Int): List<NodeId> {
        require(n in 1..nodes.size) { "a preference list of $n needs 1..${nodes.size} nodes" }
        val chosen = ArrayList<NodeId>(n)
        var at = successorOf(positionOf(key))
        repeat(vnodes.size) {
            val owner = vnodes[at].owner
            if (owner !in chosen) {
                chosen.add(owner)
                if (chosen.size == n) return chosen
            }
            at = (at + 1) % vnodes.size
        }
        error("unreachable: $n distinct nodes exist among ${vnodes.size} vnodes")
    }

    /** The index of the first vnode at or clockwise of [position]; wraps to 0 past the end. */
    private fun successorOf(position: Long): Int {
        var low = 0
        var high = vnodes.size
        while (low < high) {
            val mid = (low + high) ushr 1
            if (vnodes[mid].position < position) low = mid + 1 else high = mid
        }
        return if (low == vnodes.size) 0 else low
    }

    companion object {
        /** Spec 2.4's floor: at least this many vnodes per physical node. */
        const val VNODES_PER_NODE: Int = 128

        fun of(nodes: Set<NodeId>, vnodesPerNode: Int = VNODES_PER_NODE): Ring {
            require(nodes.isNotEmpty()) { "a ring needs at least one node" }
            require(vnodesPerNode >= VNODES_PER_NODE) {
                "spec 2.4 asks for at least $VNODES_PER_NODE vnodes per node, not $vnodesPerNode"
            }
            val sorted = nodes.sorted()
            val placed = sorted.flatMap { node ->
                (0 until vnodesPerNode).map { index ->
                    Vnode(node, index, positionOf("$node#$index".toByteArray()), 0L)
                }
            }
            // Ties are broken by owner then index so colliding positions still order the same
            // way on every node (I5).
            val clockwise = placed.sortedWith(
                compareBy({ it.position }, { it.owner }, { it.index })
            )
            // Each vnode's range opens where the one before it closed; the first one wraps.
            val ring = clockwise.mapIndexed { i, vnode ->
                val predecessor = if (i == 0) clockwise.last() else clockwise[i - 1]
                vnode.copy(rangeStart = predecessor.position)
            }
            return Ring(sorted, vnodesPerNode, ring)
        }

        private fun positionOf(bytes: ByteArray): Long {
            val digest = MessageDigest.getInstance("SHA-256").digest(bytes)
            var position = 0L
            for (i in 0 until 8) position = (position shl 8) or (digest[i].toLong() and 0xff)
            return position and Long.MAX_VALUE
        }
    }
}

/**
 * One node's slice of the ring: [owner]'s [index]th vnode sits at [position] and holds every key
 * in `(rangeStart, position]`, wrapping at the top of the ring for the first vnode.
 */
data class Vnode(
    val owner: NodeId,
    val index: Int,
    val position: Long,
    val rangeStart: Long,
) {
    fun holds(position: Long): Boolean =
        if (rangeStart < this.position) position > rangeStart && position <= this.position
        else position > rangeStart || position <= this.position
}
