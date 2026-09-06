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
    class Hash(val fields: HashTable<String, ByteArray> = HashTable()) : Value(Kind.HASH)

    /**
     * An ordered sequence. Kotlin's own [ArrayDeque] is a circular buffer, so both ends push and
     * pop in O(1) (spec 2.1) and `LINDEX`, `LSET` and `LRANGE` still index in O(1).
     */
    class List(val items: ArrayDeque<ByteArray> = ArrayDeque()) : Value(Kind.LIST)

    /**
     * The dual index of spec 2.1: [scores] answers "what does this member score" in O(1) and is
     * where member uniqueness lives, [order] answers every question about position in O(log n).
     * The two are one value and are written together; nothing may update one without the other.
     * Member names are held as ISO-8859-1 text for the same reason [Hash] holds field names so.
     */
    class ZSet(val order: SkipList) : Value(Kind.ZSET) {
        val scores = HashTable<String, Double>()

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
     * ponytail: O(elements), so the cost of measuring a big aggregate is the aggregate's size.
     * [Partition] calls it once per command on the one key that command touched, which is the
     * same order as the command's own work for a String and more than it for one field of a big
     * hash. Per-element deltas threaded through every mutation site would make it O(1) and cost
     * a running total in every structure.
     */
    fun approximateBytes(): Long = when (this) {
        is Str -> bytes.size.toLong()
        is Hash -> fields.entries().sumOf { it.key.length + it.value.size + ELEMENT_BYTES }
        is List -> items.sumOf { it.size + ELEMENT_BYTES }
        is ZSet -> scores.entries().sumOf { it.key.length + Long.SIZE_BYTES + ELEMENT_BYTES }
    }

    internal companion object {
        /** What one element of an aggregate costs beyond its own bytes. */
        const val ELEMENT_BYTES = 16L
    }
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
