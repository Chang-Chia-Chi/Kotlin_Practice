package dynacache.cluster

import dynacache.engine.persist.DotCeilingStore
import java.util.concurrent.atomic.AtomicLong

/**
 * The one source of [Dot]s a node hands out: strictly increasing, never reused (C2), restart
 * included. Two floors on start: the highest own counter in a scan of the node's local data
 * (so nothing that reached disk is reused) and the ceiling last reserved in [ceilings]. From
 * there the counter works in blocks of [block] dots: crossing the reserved ceiling persists the
 * next one before the crossing dot is handed out (T51), so a write pays for the disk once per
 * block and a restart begins above every dot the node ever gave a write.
 */
class DotCounter private constructor(
    val node: NodeId,
    private val ceilings: DotCeilingStore,
    private val block: Long,
    scanned: Long,
) {

    @Volatile private var ceiling = ceilings.load()
    private val last = AtomicLong(maxOf(scanned, ceiling))

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

        fun of(
            node: NodeId,
            localData: Iterable<Dvv>,
            ceilings: DotCeilingStore = DotCeilingStore.inMemory(),
            block: Long = BLOCK,
        ): DotCounter {
            require(block > 0) { "block must be positive, was $block" }
            val highest = localData.maxOfOrNull { dvv ->
                maxOf(if (dvv.dot.node == node) dvv.dot.counter else 0L, dvv.context[node] ?: 0L)
            } ?: 0L
            return DotCounter(node, ceilings, block, highest)
        }
    }
}
