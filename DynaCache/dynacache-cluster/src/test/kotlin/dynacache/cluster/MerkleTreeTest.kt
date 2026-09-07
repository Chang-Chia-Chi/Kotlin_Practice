package dynacache.cluster

import dynacache.engine.Key
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import kotlin.random.Random

/**
 * The Merkle tree of one vnode range (spec 2.4, C6). It hashes the triples it is given and
 * never looks at a value: wiring it into anti-entropy is T28.
 */
class MerkleTreeTest {

    private val alpha = NodeId("alpha")

    @Test
    fun C6_identical_data_identical_root() {
        val leaves = (0 until 40).map { leaf("key$it") }

        val inOrder = MerkleTree.of(leaves)
        val shuffled = MerkleTree.of(leaves.shuffled(Random(20260906)))

        assertArrayEquals(inOrder.root, shuffled.root)
    }

    @Test
    fun merkle_empty_range_has_stable_root() {
        val empty = MerkleTree.of(emptyList())

        assertArrayEquals(empty.root, MerkleTree.of(emptyList()).root)
        assertFalse(empty.root.contentEquals(MerkleTree.of(listOf(leaf("key0"))).root))
    }

    @Test
    fun merkle_one_changed_key_changes_root() {
        val leaves = (0 until 40).map { leaf("key$it") }
        val changed = leaves.toMutableList().apply { this[17] = leaf("key17", value = "rewritten") }

        assertFalse(MerkleTree.of(leaves).root.contentEquals(MerkleTree.of(changed).root))
    }

    /** Two replicas can hold the same bytes under different clocks; C6 must see that as divergence. */
    @Test
    fun merkle_dvv_change_alone_changes_root() {
        val leaves = (0 until 40).map { leaf("key$it") }
        val reclocked = leaves.toMutableList().apply { this[17] = leaf("key17", counter = 9L) }

        assertArrayEquals(leaves[17].valueHash, reclocked[17].valueHash)
        assertFalse(MerkleTree.of(leaves).root.contentEquals(MerkleTree.of(reclocked).root))
    }

    @Test
    fun merkle_diff_names_only_divergent_ranges() {
        val leaves = range(40)
        val mine = MerkleTree.of(leaves)
        val theirs = MerkleTree.of(leaves.replacing(17, leaf("key17", value = "rewritten")))

        assertEquals(emptyList<DivergentRange>(), mine.diff(mine))
        assertEquals(
            listOf(DivergentRange(Key("key17"), Key("key17"), listOf(Key("key17")))),
            mine.diff(theirs),
        )
        assertEquals(mine.diff(theirs), theirs.diff(mine))
    }

    @Test
    fun merkle_diff_detects_missing_key_on_one_side() {
        val leaves = range(40)
        val mine = MerkleTree.of(leaves)

        // Dropping a key shifts every leaf after it, so the descent suspects the whole tail; only
        // the key that is really absent survives the per-key check.
        val withoutMiddle = MerkleTree.of(leaves.without(17))
        assertEquals(
            listOf(DivergentRange(Key("key17"), Key("key39"), listOf(Key("key17")))),
            mine.diff(withoutMiddle),
        )
        assertEquals(mine.diff(withoutMiddle), withoutMiddle.diff(mine))

        val withoutLast = MerkleTree.of(leaves.without(39))
        assertEquals(
            listOf(DivergentRange(Key("key39"), Key("key39"), listOf(Key("key39")))),
            mine.diff(withoutLast),
        )

        // 17 leaves are one level deeper than 16, so the trees do not line up and every leaf is
        // suspect; the answer is still exactly the one key the shorter range lacks.
        val deeper = MerkleTree.of(range(17))
        assertEquals(
            listOf(DivergentRange(Key("key00"), Key("key16"), listOf(Key("key16")))),
            deeper.diff(MerkleTree.of(range(16))),
        )
    }

    /**
     * The pieces T84's exchange descends with decide what [MerkleTree.diff] decides locally:
     * the positions a descent suspects, and then the keys those positions really differ about.
     * The wire path only ever sees the other side's leaves at the suspect positions, which is
     * why this holds: outside them the two sides hold identical leaves.
     */
    @Test
    fun merkle_descent_pieces_decide_what_diff_decides() {
        val leaves = wide(300)
        val mine = MerkleTree.of(leaves)
        for (changed in listOf(listOf(0), listOf(17), listOf(299), listOf(3, 200))) {
            val theirs = MerkleTree.of(changed.fold(leaves) { so, at -> so.replacing(at, wideLeaf(at, "rewritten")) })
            val suspect = mine.suspectLeaves(theirs)

            assertEquals(
                mine.diff(theirs).flatMap { it.keys },
                mine.divergentKeys(suspect, theirs.leavesAt(suspect)),
                "the descent and the local diff name the same keys for $changed",
            )
        }
    }

    /** A peer's leaf count is all the sender needs to know whether the two trees line up. */
    @Test
    fun merkle_height_follows_from_the_leaf_count() {
        for (count in listOf(0, 1, 2, 15, 16, 17, 255, 256, 257, 300)) {
            assertEquals(MerkleTree.of(wide(count)).height, MerkleTree.heightOf(count), "$count leaves")
        }
    }

    private fun wide(size: Int) = (0 until size).map { wideLeaf(it) }

    private fun wideLeaf(at: Int, value: String? = null) =
        "key%03d".format(at).let { leaf(it, value = value ?: "value-of-$it") }

    private fun List<MerkleLeaf>.without(at: Int) = toMutableList().apply { removeAt(at) }

    /** Keys are zero-padded so their byte order is their list order. */
    private fun range(size: Int) = (0 until size).map { leaf("key%02d".format(it)) }

    private fun List<MerkleLeaf>.replacing(at: Int, leaf: MerkleLeaf) =
        toMutableList().apply { this[at] = leaf }

    private fun leaf(name: String, value: String = "value-of-$name", counter: Long = 1L) =
        MerkleLeaf(Key(name), value.toByteArray(), Dvv(Dot(alpha, counter), emptyMap()))
}
