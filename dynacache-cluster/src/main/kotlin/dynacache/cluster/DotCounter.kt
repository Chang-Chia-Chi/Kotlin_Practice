package dynacache.cluster

import java.util.concurrent.atomic.AtomicLong

/**
 * The one source of [Dot]s a node hands out: strictly increasing, never reused (C2). Built
 * from a scan of the node's local data so a restart resumes above every own counter that
 * ever reached disk; where that scan comes from is P4's persistence, not this class.
 */
class DotCounter private constructor(val node: NodeId, private val last: AtomicLong) {

    fun next(): Dot = Dot(node, last.incrementAndGet())

    companion object {
        fun of(node: NodeId, localData: Iterable<Dvv>): DotCounter {
            val highest = localData.maxOfOrNull { dvv ->
                maxOf(if (dvv.dot.node == node) dvv.dot.counter else 0L, dvv.context[node] ?: 0L)
            } ?: 0L
            return DotCounter(node, AtomicLong(highest))
        }
    }
}
