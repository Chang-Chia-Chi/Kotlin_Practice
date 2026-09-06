package dynacache.cluster

import java.io.ByteArrayOutputStream

/** The event that created one version: the [counter]th write coordinated by [node]. */
data class Dot(val node: NodeId, val counter: Long)

/**
 * A dotted version vector (spec 2.5, Preguica et al. 2012): the [dot] that created this
 * version plus the [context] it was built on, `node -> highest counter seen`. Together they
 * stand for a set of dots: the dot itself and, per context entry, every counter up to it.
 * Ordering is set inclusion over those dots, so a version whose dot fills a gap its context
 * never saw stays concurrent with a version that saw the gap.
 */
data class Dvv(val dot: Dot, val context: Map<NodeId, Long>) {

    /** This version saw everything [other] stands for, and something more (spec 5.3). */
    fun dominates(other: Dvv): Boolean = covers(other) && !other.covers(this)

    /** Neither saw the other: spec 5.3 hands the pair to the type's merge rule (T29). */
    fun isConcurrent(other: Dvv): Boolean = !covers(other) && !other.covers(this)

    /** The next write built on this version: a fresh dot from [counter] over everything this saw. */
    fun bump(counter: DotCounter): Dvv = Dvv(counter.next(), context.covering(dot))

    /**
     * A new version descending from both (spec 5.3): a fresh dot from [counter] over a
     * context that covers both dots and both contexts (max per node).
     */
    fun merge(other: Dvv, counter: DotCounter): Dvv =
        Dvv(counter.next(), (context maxWith other.context).covering(dot, other.dot))

    /**
     * Wire form, hand-rolled so the engine's stdlib-only RDB codec can share it (T31 may lift it
     * there): `dot.node dot.counter entries (node counter)*`, names as length-prefixed UTF-8,
     * numbers as unsigned LEB128 varints, entries sorted by node so equal DVVs encode alike.
     */
    fun encode(): ByteArray {
        val out = ByteArrayOutputStream()
        fun varint(value: Long) {
            var rest = value
            while (rest and 0x7fL.inv() != 0L) {
                out.write(((rest and 0x7f) or 0x80).toInt())
                rest = rest ushr 7
            }
            out.write(rest.toInt())
        }
        fun node(node: NodeId) = node.name.toByteArray().let { varint(it.size.toLong()); out.write(it) }
        node(dot.node)
        varint(dot.counter)
        varint(context.size.toLong())
        for ((node, counter) in context.toSortedMap()) {
            node(node)
            varint(counter)
        }
        return out.toByteArray()
    }

    private fun covers(other: Dvv): Boolean =
        covers(other.dot) && other.context.all { (node, upTo) -> coversUpTo(node, upTo) }

    private fun covers(dot: Dot): Boolean = dot == this.dot || dot.counter <= seen(dot.node)

    /** Every dot of [node] from 1 to [upTo]: seen in the context, or all but the last, which is ours. */
    private fun coversUpTo(node: NodeId, upTo: Long): Boolean =
        upTo <= seen(node) || (dot == Dot(node, upTo) && upTo - 1 <= seen(node))

    private fun seen(node: NodeId): Long = context[node] ?: 0L

    private infix fun Map<NodeId, Long>.maxWith(other: Map<NodeId, Long>): Map<NodeId, Long> =
        (keys + other.keys).associateWith { maxOf(this[it] ?: 0L, other[it] ?: 0L) }

    private fun Map<NodeId, Long>.covering(vararg dots: Dot): Map<NodeId, Long> =
        this maxWith dots.associate { it.node to it.counter }

    companion object {
        /** The inverse of [encode]; fails on a truncated, overlong or trailing-bytes input. */
        fun decode(bytes: ByteArray): Dvv {
            var at = 0
            fun varint(): Long {
                var value = 0L
                var shift = 0
                while (true) {
                    require(at < bytes.size && shift < 64) { "truncated DVV at byte $at" }
                    val byte = bytes[at++].toLong()
                    value = value or ((byte and 0x7f) shl shift)
                    if (byte and 0x80 == 0L) return value
                    shift += 7
                }
            }
            fun node(): NodeId {
                val length = varint().toInt()
                require(length in 0..bytes.size - at) { "truncated DVV at byte $at" }
                return NodeId(String(bytes, at, length, Charsets.UTF_8)).also { at += length }
            }
            val dot = Dot(node(), varint())
            val context = LinkedHashMap<NodeId, Long>()
            repeat(varint().toInt()) { context[node()] = varint() }
            require(at == bytes.size) { "${bytes.size - at} trailing bytes after DVV" }
            return Dvv(dot, context)
        }
    }
}
