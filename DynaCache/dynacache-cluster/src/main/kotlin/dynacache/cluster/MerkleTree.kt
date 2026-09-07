package dynacache.cluster

import dynacache.engine.Key
import java.security.MessageDigest
import java.util.Arrays

/** One key's contribution to a Merkle tree: its [valueHash] and the [dvv] that version carries. */
class MerkleLeaf(val key: Key, val valueHash: ByteArray, val dvv: Dvv)

/**
 * The Merkle tree of one vnode range (spec 2.4): the range's `(key, value hash, DVV)` triples in
 * key byte order under a fixed [fanout], SHA-256 at every level.
 *
 * The tree is a pure function of the triples (C6): [of] sorts what it is given, so two nodes
 * holding the same range produce the same [root] whatever order their scans yielded. It hashes
 * what it is handed and nothing else - the caller decides what a value hash is, and no value is
 * stored.
 *
 * Hash arrays are held, not copied: a caller must not mutate [root] or a leaf's [valueHash].
 */
class MerkleTree private constructor(
    /** The range's leaves, ascending by unsigned key bytes. */
    private val leaves: List<MerkleLeaf>,
    /** The width of every node: two trees compare, and descend, only at one fan-out. */
    val fanout: Int,
    /** `levels[0]` is the leaf hashes, each level above it the [fanout]-way chunking of the one below. */
    private val levels: List<List<ByteArray>>,
) {

    /** The hash the anti-entropy exchange of T28 compares first. */
    val root: ByteArray get() = levels.last().single()

    /** How many leaves the range holds: what [heightOf] turns back into this tree's [height]. */
    val size: Int get() = leaves.size

    /** The number of levels; `height - 1` is the root's level and 0 is the leaves. */
    val height: Int get() = levels.size

    /**
     * The keys this tree and [other] disagree about, grouped into the leaf spans the descent
     * stopped at. Subtrees whose hashes already agree are never opened, so a range that matches
     * costs one comparison however many keys it holds.
     *
     * A key is divergent when only one side holds it, or the two sides hold different value
     * hashes or different DVVs. The result is exact: a suspect span is checked key by key
     * against [other], so a range is reported only when something in it really differs.
     */
    fun diff(other: MerkleTree): List<DivergentRange> {
        val suspect = suspectLeaves(other)
        val ranges = ArrayList<DivergentRange>()
        var from = 0
        while (from < suspect.size) {
            var to = from
            while (to + 1 < suspect.size && suspect[to + 1] == suspect[to] + 1) to++
            divergenceIn(other, suspect.subList(from, to + 1))?.let { ranges.add(it) }
            from = to + 1
        }
        return ranges
    }

    /**
     * The leaf positions a descent against [other] suspects, ascending: the descent starts at
     * the root and, at each level, opens only the nodes whose hashes disagreed. This is the
     * whole comparison, and T84's exchange runs the same three steps a level at a time over the
     * wire ([hashesAt], [differing], [childrenOf]) so a subtree that matches never crosses.
     */
    fun suspectLeaves(other: MerkleTree): List<Int> {
        require(fanout == other.fanout) {
            "Merkle trees compare only at one fan-out: $fanout against ${other.fanout}"
        }
        if (root.contentEquals(other.root)) return emptyList()
        // Trees of different height have no level to compare; checking every leaf still gives
        // the exact answer below, it only spends the descent's saving.
        if (height != other.height) return (0 until maxOf(size, other.size)).toList()
        var level = height - 1
        var positions = listOf(0)
        while (true) {
            positions = differing(level, positions, other.hashesAt(level, positions))
            if (level == 0 || positions.isEmpty()) return positions
            positions = childrenOf(positions)
            level--
        }
    }

    /** This tree's hashes at [level] for [positions], in order, null where it has no such node. */
    fun hashesAt(level: Int, positions: List<Int>): List<ByteArray?> =
        positions.map { levels.getOrNull(level)?.getOrNull(it) }

    /**
     * Which of [positions] at [level] this tree disagrees about, [theirs] being the other side's
     * hashes for the same positions in the same order. A position neither side has a node at
     * agrees; a position only one side has does not.
     */
    fun differing(level: Int, positions: List<Int>, theirs: List<ByteArray?>): List<Int> {
        require(positions.size == theirs.size) {
            "a level answers one hash per position asked: ${theirs.size} for ${positions.size}"
        }
        val mine = hashesAt(level, positions)
        return positions.filterIndexed { at, _ -> !sameHash(mine[at], theirs[at]) }
    }

    /** The positions at the level below that [positions] cover: [fanout] children each, in order. */
    fun childrenOf(positions: List<Int>): List<Int> =
        positions.flatMap { it * fanout until (it + 1) * fanout }

    /** The leaves this tree holds among [positions], in position order. */
    fun leavesAt(positions: List<Int>): List<MerkleLeaf> = positions.mapNotNull { leaves.getOrNull(it) }

    /**
     * The keys this tree and [theirs] hold differently, ascending: [theirs] is the other side's
     * leaves at the same [positions] a descent suspected. A key only one side holds diverges, and
     * so does one whose leaf hash differs, which is what [diff] decides key by key at the bottom
     * of a local descent. Outside the suspect positions the two sides hold identical leaves, so
     * these positions hold every key that can differ.
     */
    fun divergentKeys(positions: List<Int>, theirs: List<MerkleLeaf>): List<Key> {
        val here = leavesAt(positions).associateBy { it.key }
        val there = theirs.associateBy { it.key }
        return (here.keys + there.keys)
            .sortedWith { a, b -> compareKeys(a, b) }
            .filter { !sameHash(here[it]?.let(::leafHash), there[it]?.let(::leafHash)) }
    }

    /** The keys of one suspect span, and which of them [other] really disagrees about. */
    private fun divergenceIn(other: MerkleTree, span: List<Int>): DivergentRange? {
        val keys = span
            .flatMap { listOfNotNull(leaves.getOrNull(it)?.key, other.leaves.getOrNull(it)?.key) }
            .distinct()
            .sortedWith { a, b -> compareKeys(a, b) }
        val divergent = keys.filter { key ->
            val mine = hashOf(key)
            val theirs = other.hashOf(key)
            mine == null || theirs == null || !mine.contentEquals(theirs)
        }
        return if (divergent.isEmpty()) null else DivergentRange(keys.first(), keys.last(), divergent)
    }

    /** This range's leaf hash for [key], or null when the range does not hold it. */
    private fun hashOf(key: Key): ByteArray? =
        leaves.binarySearch { compareKeys(it.key, key) }.let { if (it >= 0) levels[0][it] else null }

    companion object {
        /** Spec 2.4's default width: 16 children per node. */
        const val FANOUT: Int = 16

        fun of(leaves: Iterable<MerkleLeaf>, fanout: Int = FANOUT): MerkleTree {
            require(fanout >= 2) { "a Merkle tree needs a fan-out of at least 2, not $fanout" }
            val sorted = leaves.sortedWith { a, b -> compareKeys(a.key, b.key) }
            for (i in 1 until sorted.size) {
                require(compareKeys(sorted[i - 1].key, sorted[i].key) != 0) {
                    "a vnode range holds ${sorted[i].key} once, not twice"
                }
            }
            val levels = ArrayList<List<ByteArray>>()
            levels.add(sorted.map { leafHash(it) })
            // An empty range still has a root - the node hash of no children - so a replica that
            // holds nothing and one that holds nothing agree without a descent.
            if (levels[0].isEmpty()) levels.add(listOf(nodeHash(emptyList())))
            while (levels.last().size != 1) {
                levels.add(levels.last().chunked(fanout) { nodeHash(it) })
            }
            return MerkleTree(sorted, fanout, levels)
        }

        /**
         * The [height] a tree of [leafCount] leaves has under [fanout], without building it:
         * how a descent over the wire learns from a peer's leaf count alone whether the two
         * trees line up level by level (T84).
         */
        fun heightOf(leafCount: Int, fanout: Int = FANOUT): Int {
            require(fanout >= 2) { "a Merkle tree needs a fan-out of at least 2, not $fanout" }
            if (leafCount <= 0) return 2
            var width = leafCount
            var height = 1
            while (width != 1) {
                width = (width + fanout - 1) / fanout
                height++
            }
            return height
        }

        /** Two hashes agree when both are absent or both are the same bytes. */
        private fun sameHash(a: ByteArray?, b: ByteArray?): Boolean =
            if (a == null || b == null) a == null && b == null else a.contentEquals(b)

        /** Key order is the ring's order: unsigned bytes, so it matches how keys are exchanged. */
        private fun compareKeys(a: Key, b: Key): Int = Arrays.compareUnsigned(a.bytes, b.bytes)

        // The level tags keep a leaf hash from ever being read as a node hash (second preimage).
        private const val LEAF_TAG: Byte = 0
        private const val NODE_TAG: Byte = 1

        private fun leafHash(leaf: MerkleLeaf): ByteArray {
            val digest = MessageDigest.getInstance("SHA-256")
            digest.update(LEAF_TAG)
            // Length-prefixed so no two different triples can ever hash the same bytes.
            for (part in listOf(leaf.key.bytes, leaf.valueHash, leaf.dvv.encode())) {
                digest.update(lengthOf(part))
                digest.update(part)
            }
            return digest.digest()
        }

        private fun nodeHash(children: List<ByteArray>): ByteArray {
            val digest = MessageDigest.getInstance("SHA-256")
            digest.update(NODE_TAG)
            for (child in children) digest.update(child)
            return digest.digest()
        }

        private fun lengthOf(part: ByteArray): ByteArray {
            val size = part.size
            return byteArrayOf(
                (size ushr 24).toByte(), (size ushr 16).toByte(), (size ushr 8).toByte(), size.toByte()
            )
        }
    }
}

/**
 * One span of leaves whose subtree hashes disagreed, running from [from] to [to] in key order,
 * and the [keys] inside it that really differ. T28 exchanges exactly these keys.
 */
data class DivergentRange(val from: Key, val to: Key, val keys: List<Key>)
