package dynacache.cluster

import dynacache.engine.persist.DotCeilingStore
import java.util.concurrent.atomic.AtomicLong

/**
 * The one source of [Dot]s a node hands out: strictly increasing, never reused (C2), restart
 * included. Two floors on start: the ceiling last reserved in [ceilings], and the highest own
 * counter in every version the node holds again -- the versions restored from its snapshot and
 * its log, handed here one at a time by [saw] as the table is rebuilt (T67), so nothing that
 * reached disk is reused and the counter resumes exactly where it left off rather than a
 * block above it. From
 * there the counter works in blocks of [block] dots: crossing the reserved ceiling persists the
 * next one before the crossing dot is handed out (T51), so a write pays for the disk once per
 * block and a restart begins above every dot the node ever gave a write.
 */
class DotCounter private constructor(
    val node: NodeId,
    private val ceilings: DotCeilingStore,
    private val block: Long,
) {

    @Volatile private var ceiling = ceilings.load()
    private val last = AtomicLong(ceiling)

    /**
     * A version this node holds: the counter resumes above every dot of its own inside it, so a
     * table rebuilt from disk is a floor exactly as the persisted ceiling is (T67, C2). Only
     * ever raises, and is called while the node is recovering, before the first [next].
     */
    fun saw(dvv: Dvv) {
        val own = maxOf(if (dvv.dot.node == node) dvv.dot.counter else 0L, dvv.context[node] ?: 0L)
        last.updateAndGet { maxOf(it, own) }
    }

    fun next(): Dot {
        val counter = last.incrementAndGet()
        if (counter > ceiling) reserveFor(counter)
        return Dot(node, counter)
    }

    /**
     * Every dot past the ceiling waits here until a ceiling above it is on disk; the one that
     * arrives first does the writing, the rest re-check and go. A reservation that fails leaves
     * the ceiling where it was, so the dot is never handed out and the next caller tries again.
     */
    @Synchronized
    private fun reserveFor(counter: Long) {
        if (counter <= ceiling) return
        val reserved = (counter / block + 1) * block
        ceilings.reserve(reserved)
        ceiling = reserved
    }

    companion object {
        /** One fsync per this many writes; a crash wastes at most this many counters. */
        const val BLOCK = 1000L

        /** [localData] is what the node already holds; a node whose table is rebuilt later uses [saw]. */
        fun of(
            node: NodeId,
            localData: Iterable<Dvv>,
            ceilings: DotCeilingStore = DotCeilingStore.inMemory(),
            block: Long = BLOCK,
        ): DotCounter {
            require(block > 0) { "block must be positive, was $block" }
            return DotCounter(node, ceilings, block).also { counter -> localData.forEach(counter::saw) }
        }
    }
}
