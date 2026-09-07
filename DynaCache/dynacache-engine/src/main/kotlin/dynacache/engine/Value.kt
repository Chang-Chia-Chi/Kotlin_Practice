package dynacache.engine

import dynacache.engine.ds.HashTable
import dynacache.engine.ds.SkipList

/**
 * What a key holds. A key is one kind and never another, and a command declares the [Kind]
 * it needs so a wrong-type command is refused before it can touch the entry (C13).
 */
sealed class Value(val kind: Kind) {

    /** The word `TYPE` reports, and what a command names when it needs a kind. */
    enum class Kind(val text: String) {
        STRING("string"),
        HASH("hash"),
        LIST("list"),
        ZSET("zset"),
    }

    class Str(val bytes: ByteArray) : Value(Kind.STRING)

    /**
     * Field names are held as ISO-8859-1 text: that charset maps every byte to one character and
     * back, so a binary-safe field name survives and the table hashes text.
     */
    class Hash : Value(Kind.HASH) {
        val fields = ElementTable<ByteArray> { it.size.toLong() }
    }

    /**
     * An ordered sequence. Kotlin's own [ArrayDeque] is a circular buffer under [ElementList], so
     * both ends push and pop in O(1) (spec 2.1) and `LINDEX`, `LSET` and `LRANGE` still index in
     * O(1).
     */
    class List(elements: Collection<ByteArray> = emptyList()) : Value(Kind.LIST) {
        val items = ElementList(elements)
    }

    /**
     * The dual index of spec 2.1: [scores] answers "what does this member score" in O(1) and is
     * where member uniqueness lives, [order] answers every question about position in O(log n).
     * The two are one value and are written together; nothing may update one without the other,
     * which is why [writeScore] and [removeMember] are the only ways in and out.
     * Member names are held as ISO-8859-1 text for the same reason [Hash] holds field names so.
     */
    class ZSet(val order: SkipList) : Value(Kind.ZSET) {
        /** A score is a `Double`, so every member costs the same beside its own name. */
        val scores = ElementTable<Double> { Long.SIZE_BYTES.toLong() }

        /**
         * Writes one (member, score) into both indexes at once, answering whether the member was
         * new. The score map holds the member's one score, so an existing member is a move in the
         * list rather than a second entry; that pairing is what makes the dual index a single
         * value. Every writer of a sorted set goes through here -- the command path and a restore
         * from a snapshot alike -- so the two indexes cannot drift apart.
         */
        fun writeScore(score: Double, member: ByteArray): Boolean {
            val previous = scores.put(fieldName(member), score)
            if (previous == null) {
                order.insert(score, member)
                return true
            }
            if (previous != score) order.updateScore(previous, member, score)
            return false
        }

        /**
         * Drops [member] from both indexes at once, answering whether it was there. The score map
         * says whether the member was there and the list is then told the same thing, so one index
         * cannot silently disagree with the other about what was removed.
         */
        fun removeMember(member: ByteArray): Boolean {
            val score = scores.remove(fieldName(member)) ?: return false
            order.remove(score, member)
            return true
        }
    }

    /**
     * What this value costs, near enough for spec 2.7 to decide when to evict: the payload's own
     * bytes plus [ELEMENT_BYTES] per element of an aggregate, standing for the node, the pointers
     * and the object header a JVM spends on holding one element. A score is a `Double`, so it
     * counts as eight. Field and member names are ISO-8859-1, one character to the byte.
     *
     * Approximate by design and by name, as Redis's own `used_memory` is: an exact count would
     * mean walking the JVM's object graph.
     *
     * Read, never counted: every aggregate keeps the running total its own mutations book, so
     * [PartitionStore]'s recharge after a command is O(1) whatever the value holds. A list command
     * costs the same on a hundred thousand elements as on one.
     */
    fun approximateBytes(): Long = when (this) {
        is Str -> bytes.size.toLong()
        is Hash -> fields.bytes
        is List -> items.bytes
        is ZSet -> scores.bytes
    }

    internal companion object {
        /** What one element of an aggregate costs beyond its own bytes. */
        const val ELEMENT_BYTES = 16L
    }
}

/**
 * The elements of a [Value.List] and what they cost. Every mutation books its own byte delta, so
 * [bytes] is read rather than counted and the charge after a list command is O(1) in the list's
 * length (spec 2.7). Kotlin's own [ArrayDeque] is underneath, so both ends still push and pop in
 * O(1) and every index still reads in O(1).
 */
class ElementList(elements: Collection<ByteArray> = emptyList()) : AbstractMutableList<ByteArray>() {

    private val items = ArrayDeque(elements)

    /** What these elements cost: their own bytes plus [Value.ELEMENT_BYTES] each. */
    var bytes: Long = items.sumOf { it.size + Value.ELEMENT_BYTES }
        private set

    /**
     * How many elements have been read out of this list, over its whole life. A caller measures
     * one operation by the difference across it, which is how the constant-charge test proves the
     * recharge reads no element at all; that is why nothing resets it. After [SkipList.comparisons].
     */
    var visits: Long = 0L
        private set

    override val size: Int get() = items.size

    override fun get(index: Int): ByteArray {
        visits++
        return items[index]
    }

    override fun set(index: Int, element: ByteArray): ByteArray {
        val replaced = items.set(index, element)
        bytes += element.size - replaced.size
        return replaced
    }

    override fun add(index: Int, element: ByteArray) {
        items.add(index, element)
        bytes += element.size + Value.ELEMENT_BYTES
    }

    override fun removeAt(index: Int): ByteArray {
        val removed = items.removeAt(index)
        bytes -= removed.size + Value.ELEMENT_BYTES
        return removed
    }

    fun addFirst(element: ByteArray) = add(0, element)

    fun addLast(element: ByteArray) = add(size, element)

    fun removeFirst(): ByteArray = removeAt(0)

    fun removeLast(): ByteArray = removeAt(size - 1)

    fun removeLastOrNull(): ByteArray? = if (isEmpty()) null else removeLast()
}

/**
 * The named elements of an aggregate -- a [Value.Hash]'s fields, a [Value.ZSet]'s scored members
 * -- and what they cost. Every put and remove books its own byte delta, so [bytes] is read rather
 * than counted and the charge after a command is O(1) in the number of names (spec 2.7). Names are
 * ISO-8859-1 text, one character to the byte; [valueBytes] is what one value costs beside its name.
 */
class ElementTable<V>(private val valueBytes: (V) -> Long) {

    private val table = HashTable<String, V>()

    /** What these elements cost: their names, their values and [Value.ELEMENT_BYTES] each. */
    var bytes: Long = 0L
        private set

    /** How many elements have been read out of this table; see [ElementList.visits]. */
    var visits: Long = 0L
        private set

    val size: Int get() = table.size

    fun get(name: String): V? {
        visits++
        return table.get(name)
    }

    /** Stores [value] under [name]; answers the value it replaced, null when the name was new. */
    fun put(name: String, value: V): V? {
        val previous = table.put(name, value)
        bytes += if (previous == null) name.length + valueBytes(value) + Value.ELEMENT_BYTES
        else valueBytes(value) - valueBytes(previous)
        return previous
    }

    /** Drops [name]; answers the value that was under it, null when there was none. */
    fun remove(name: String): V? = table.remove(name)?.also {
        bytes -= name.length + valueBytes(it) + Value.ELEMENT_BYTES
    }

    /** Every element, in no defined order. Do not mutate the table while iterating. */
    fun entries(): Sequence<Map.Entry<String, V>> = table.entries().onEach { visits++ }

    /** This table's share of an `HSCAN` or a `ZSCAN`; the cursor to continue from comes back. */
    fun scan(cursor: Long, count: Int, keep: (String, V) -> Boolean, emit: (String, V) -> Unit): Long =
        walk(table, cursor, count, keep, emit)
}

/** The error Redis answers when an argument that should be a score is not one. */
internal val NOT_A_FLOAT = Reply.Error("ERR", "value is not a valid float")

/** The error Redis answers when a `ZRANGEBYSCORE` bound is not one. */
internal val NOT_A_RANGE = Reply.Error("ERR", "min or max is not a float")

/** One end of a score range: where it sits and whether the entry sitting exactly there is in. */
internal class ScoreBound(val score: Double, val inclusive: Boolean)

/**
 * A `ZRANGEBYSCORE` bound, or null when it is not one. Redis writes an exclusive bound with a
 * leading `(`; everything after it is an ordinary score, infinities included.
 */
internal fun parseBound(bytes: ByteArray): ScoreBound? {
    val exclusive = bytes.firstOrNull() == '('.code.toByte()
    val score = parseScore(if (exclusive) bytes.copyOfRange(1, bytes.size) else bytes) ?: return null
    return ScoreBound(score, !exclusive)
}

/**
 * A score argument as a number, or null when it is not one. Redis's `strtod` accepts the
 * infinities by name and nothing else non-numeric; the character guard is what rejects Kotlin's
 * own extras (`1.0f`, `NaN`, `0x1p3`, padding) that `toDoubleOrNull` would otherwise take. NaN
 * is never a score: it sorts nowhere, so it is refused here rather than reaching the skip list.
 */
internal fun parseScore(bytes: ByteArray): Double? {
    val text = bytes.toString(Charsets.ISO_8859_1)
    return when (text.lowercase()) {
        "inf", "+inf", "infinity", "+infinity" -> Double.POSITIVE_INFINITY
        "-inf", "-infinity" -> Double.NEGATIVE_INFINITY
        else -> if (text.any { it !in NUMERIC }) null else text.toDoubleOrNull()?.takeIf { !it.isNaN() }
    }
}

/**
 * A score as Redis writes it in a reply: a whole number carries no decimal point, and the
 * infinities are words.
 *
 * ponytail: past 2^53 the shortest round-trip form is Kotlin's `1.0E17`, where Redis's `%.17Lg`
 * writes `1e+17`. Only a client parsing the exponent form notices; a formatter of its own is the
 * repair if the RESP acceptance of T16 ever asks for one.
 */
internal fun scoreText(score: Double): String = when {
    score == Double.POSITIVE_INFINITY -> "inf"
    score == Double.NEGATIVE_INFINITY -> "-inf"
    score == Math.rint(score) && Math.abs(score) < 1e17 -> score.toLong().toString()
    else -> score.toString()
}

private const val NUMERIC = "0123456789.eE+-"

/** A field name as the store keys it. */
internal fun fieldName(field: ByteArray): String = field.toString(Charsets.ISO_8859_1)

/** The bytes of a field name the store keyed by [fieldName]. */
fun fieldBytes(name: String): ByteArray = name.toByteArray(Charsets.ISO_8859_1)
