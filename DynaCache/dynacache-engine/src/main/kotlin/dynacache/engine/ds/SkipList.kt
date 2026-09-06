package dynacache.engine.ds

import kotlin.random.Random

/**
 * One element of a sorted set: a score and the member bytes scored by it. Two entries are the
 * same entry when their scores and their member bytes are the same.
 *
 * [member] is the list's own array, not a copy, exactly as [dynacache.engine.Key] holds its
 * bytes: a caller that mutates it moves an entry without the list noticing.
 */
class Entry(val score: Double, val member: ByteArray) {

    override fun equals(other: Any?): Boolean =
        this === other ||
            (other is Entry && score == other.score && member.contentEquals(other.member))

    // `score == other.score` is IEEE equality, where -0.0 and 0.0 are one score, so the hash has
    // to agree: adding 0.0 turns -0.0 into 0.0 and leaves every other score alone.
    override fun hashCode(): Int = 31 * (score + 0.0).hashCode() + member.contentHashCode()

    override fun toString(): String = "${member.toString(Charsets.ISO_8859_1)}=$score"
}

/**
 * The ordered index behind Sorted Set: a probabilistic skip list (Pugh, 1990) keyed by
 * (score, member), ascending by score with the member bytes as an unsigned lexicographic
 * tiebreak (I3).
 *
 * Not thread-safe by design: one partition executor owns it and runs one command at a time
 * (C1), so no data structure in the engine locks.
 *
 * [levels] is the only randomness; injecting it makes a run reproducible.
 */
class SkipList(private val levels: Random) {

    /** The usual way to build one: a seed rather than a generator. */
    constructor(seed: Long) : this(Random(seed))

    /** How many entries the list holds. */
    var size: Int = 0
        private set

    /**
     * How many (score, member) comparisons the list has made walking search paths, over its whole
     * life. A caller measures one batch of searches by the difference across it; that is what the
     * log-n property test reads, and it is why nothing resets this.
     */
    var comparisons: Long = 0L
        private set

    private val head = Node(0.0, EMPTY, MAX_LEVEL)

    /** The highest level any node currently reaches; searches start here, not at [MAX_LEVEL]. */
    private var level = 1

    /** The last entry, where a reverse traversal starts. */
    private var tail: Node? = null

    /**
     * Adds (score, member). Returns false and changes nothing when that exact entry is already
     * present; a member at a different score is a different entry (the sorted set's own
     * member uniqueness lives in the score map beside this list, not here).
     */
    fun insert(score: Double, member: ByteArray): Boolean {
        val path = pathTo(score, member)
        val update = path.update
        if (update[0]!!.next[0]?.holds(score, member) == true) return false

        val newLevel = randomLevel()
        if (newLevel > level) {
            for (i in level until newLevel) {
                update[i] = head
                path.crossed[i] = 0
                head.span[i] = size
            }
            level = newLevel
        }
        val node = Node(score, member, newLevel)
        for (i in 0 until newLevel) {
            node.next[i] = update[i]!!.next[i]
            update[i]!!.next[i] = node
            // The predecessor's old reach, minus the part of it now covered below the new node.
            val skipped = path.crossed[0] - path.crossed[i]
            node.span[i] = update[i]!!.span[i] - skipped
            update[i]!!.span[i] = skipped + 1
        }
        for (i in newLevel until level) update[i]!!.span[i]++
        node.backward = if (update[0] === head) null else update[0]
        val after = node.next[0]
        if (after == null) tail = node else after.backward = node
        size++
        return true
    }

    /** Removes (score, member). Returns false and changes nothing when it is not there. */
    fun remove(score: Double, member: ByteArray): Boolean {
        val update = pathTo(score, member).update
        val doomed = update[0]!!.next[0]
        if (doomed == null || !doomed.holds(score, member)) return false

        for (i in 0 until level) {
            if (update[i]!!.next[i] === doomed) {
                update[i]!!.span[i] += doomed.span[i] - 1
                update[i]!!.next[i] = doomed.next[i]
            } else {
                update[i]!!.span[i]--
            }
        }
        val after = doomed.next[0]
        if (after == null) tail = doomed.backward else after.backward = doomed.backward
        while (level > 1 && head.next[level - 1] == null) level--
        size--
        return true
    }

    /** The 0-based position of (score, member) in ascending order, or -1 when it is absent. */
    fun rank(score: Double, member: ByteArray): Int {
        val path = pathTo(score, member)
        val found = path.update[0]!!.next[0]?.holds(score, member) == true
        return if (found) path.crossed[0] else -1
    }

    /**
     * Moves [member] from [score] to [newScore], keeping the order. Returns false and changes
     * nothing when (score, member) is not there. This is a remove and an insert: relinking the
     * node in place, as Redis does when the move does not cross a neighbour, saves one walk of
     * the list and no complexity, so it stays unwritten until a profile asks for it.
     */
    fun updateScore(score: Double, member: ByteArray, newScore: Double): Boolean {
        if (!remove(score, member)) return false
        insert(newScore, member)
        return true
    }

    /**
     * Every entry whose score falls between [min] and [max], in ascending order. Bounds are
     * inclusive unless said otherwise, and the infinities are legal bounds; an empty list comes
     * back when the range is empty or inverted.
     */
    fun rangeByScore(
        min: Double,
        max: Double,
        minInclusive: Boolean = true,
        maxInclusive: Boolean = true,
    ): List<Entry> {
        var x = head
        for (i in level - 1 downTo 0) {
            while (x.next[i]?.let { it.score < min || (!minInclusive && it.score == min) } == true) {
                x = x.next[i]!!
            }
        }
        val found = ArrayList<Entry>()
        var node = x.next[0]
        while (node != null && (node.score < max || (maxInclusive && node.score == max))) {
            found.add(Entry(node.score, node.member))
            node = node.next[0]
        }
        return found
    }

    /**
     * Every entry whose position falls between [start] and [stop], both 0-based and inclusive.
     * Bounds outside the list are clamped, so `rangeByRank(0, size - 1)` is the whole list and an
     * inverted or out-of-range window is empty. Negative Redis indices are the caller's business.
     */
    fun rangeByRank(start: Int, stop: Int): List<Entry> {
        val from = start.coerceAtLeast(0)
        val to = stop.coerceAtMost(size - 1)
        if (from > to) return emptyList()

        var node = nodeAtRank(from)
        val found = ArrayList<Entry>(to - from + 1)
        var rank = from
        while (node != null && rank <= to) {
            found.add(Entry(node.score, node.member))
            node = node.next[0]
            rank++
        }
        return found
    }

    /** Every entry in ascending order. */
    fun forward(): Sequence<Entry> =
        generateSequence(head.next[0]) { it.next[0] }.map { Entry(it.score, it.member) }

    /** Every entry in descending order, walked through the level-0 back pointers. */
    fun backward(): Sequence<Entry> =
        generateSequence(tail) { it.backward }.map { Entry(it.score, it.member) }

    /**
     * The search path down to (score, member): at every level the rightmost node before it, and
     * how many entries lie before that node. Index 0 holds the immediate predecessor, so
     * `crossed[0]` is the rank the target has or would have. Insert and remove relink along the
     * same path, which is why one walk serves all three.
     */
    private fun pathTo(score: Double, member: ByteArray): Path {
        val path = Path()
        var x = head
        var crossed = 0
        for (i in level - 1 downTo 0) {
            while (x.next[i]?.let { precedes(it, score, member) } == true) {
                crossed += x.span[i]
                x = x.next[i]!!
            }
            path.update[i] = x
            path.crossed[i] = crossed
        }
        return path
    }

    /** The node at 0-based position [rank], reached by counting spans rather than steps. */
    private fun nodeAtRank(rank: Int): Node? {
        val target = rank + 1
        var x = head
        var traversed = 0
        for (i in level - 1 downTo 0) {
            while (x.next[i] != null && traversed + x.span[i] <= target) {
                traversed += x.span[i]
                x = x.next[i]!!
            }
        }
        return if (traversed == target) x else null
    }

    /** True when [node] sorts strictly before (score, member). The one place a search compares. */
    private fun precedes(node: Node, score: Double, member: ByteArray): Boolean {
        comparisons++
        return node.score < score || (node.score == score && compareMembers(node.member, member) < 0)
    }

    private fun Node.holds(score: Double, member: ByteArray): Boolean =
        this.score == score && this.member.contentEquals(member)

    private fun randomLevel(): Int {
        var l = 1
        while (l < MAX_LEVEL && levels.nextDouble() < BRANCH) l++
        return l
    }

    private class Node(val score: Double, val member: ByteArray, height: Int) {
        val next = arrayOfNulls<Node>(height)

        /** How many entries `next[i]` steps over; 1 when it points at the very next entry. */
        val span = IntArray(height)

        /** The previous entry at level 0, which is what makes a reverse traversal O(1) a step. */
        var backward: Node? = null
    }

    private class Path {
        val update = arrayOfNulls<Node>(MAX_LEVEL)
        val crossed = IntArray(MAX_LEVEL)
    }

    private companion object {
        const val MAX_LEVEL = 32
        const val BRANCH = 0.25
        val EMPTY = ByteArray(0)

        /** Redis compares member bytes with memcmp, so bytes are unsigned here too. */
        fun compareMembers(a: ByteArray, b: ByteArray): Int {
            val shared = minOf(a.size, b.size)
            for (i in 0 until shared) {
                val d = (a[i].toInt() and 0xFF) - (b[i].toInt() and 0xFF)
                if (d != 0) return d
            }
            return a.size - b.size
        }
    }
}
