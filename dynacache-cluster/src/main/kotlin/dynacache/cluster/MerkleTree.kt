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
    private val fanout: Int,
    /** `levels[0]` is the leaf hashes, each level above it the [fanout]-way chunking of the one below. */
    private val levels: List<List<ByteArray>>,
) {

    /** The hash the anti-entropy exchange of T28 compares first. */
    val root: ByteArray get() = levels.last().single()

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
        require(fanout == other.fanout) {
            "Merkle trees compare only at one fan-out: $fanout against ${other.fanout}"
        }
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

    /** The leaf indices under a subtree pair that disagreed, ascending. */
    private fun suspectLeaves(other: MerkleTree): List<Int> {
        if (root.contentEquals(other.root)) return emptyList()
        // Trees of different height have no level to compare; checking every leaf still gives
        // the exact answer below, it only spends the descent's saving.
        if (levels.size != other.levels.size) {
            return (0 until maxOf(leaves.size, other.leaves.size)).toList()
        }
        val suspect = ArrayList<Int>()
        descend(other, levels.size - 1, 0, suspect)
        return suspect
    }

    private fun descend(other: MerkleTree, level: Int, index: Int, suspect: MutableList<Int>) {
        val mine = levels[level].getOrNull(index)
        val theirs = other.levels[level].getOrNull(index)
        if (mine == null && theirs == null) return
        if (mine != null && theirs != null && mine.contentEquals(theirs)) return
        if (level == 0) suspect.add(index)
        else for (child in index * fanout until (index + 1) * fanout) descend(other, level - 1, child, suspect)
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
