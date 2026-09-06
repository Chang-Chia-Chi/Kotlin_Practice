package dynacache.engine.ds

/**
 * An open hash table with chaining and incremental rehash, after Redis's `dict.c`: the key map
 * behind every partition store and every Hash. Bucket arrays are powers of two. Once the load
 * factor crosses 1 (or falls under 1/8) a second array is allotted and every following
 * operation migrates exactly one bucket into it (`_dictRehashStep`), so no operation ever pays
 * for the whole table; a lookup meanwhile reads both arrays and an insert lands in the new one.
 *
 * Not thread-safe by design: one partition executor owns it and runs one command at a time
 * (C1), so no data structure in the engine locks.
 */
class HashTable<K, V> {

    private class Node<K, V>(override val key: K, override var value: V, val hash: Int, var next: Node<K, V>?) :
        Map.Entry<K, V>

    private var main = arrayOfNulls<Node<K, V>>(MIN_BUCKETS)

    /** The array being migrated into, present only while rehashing. */
    private var spare: Array<Node<K, V>?>? = null

    /** How many keys the table holds. */
    var size: Int = 0
        private set

    /**
     * The next bucket of the old array to migrate, or [NOT_REHASHING]. Public so a test can
     * prove the one-bucket-per-operation rule from outside.
     */
    var rehashProgress: Int = NOT_REHASHING
        private set

    fun get(key: K): V? {
        rehashStep()
        return find(key)?.value
    }

    /** Stores [value] under [key]; answers the value it replaced, null when the key was new. */
    fun put(key: K, value: V): V? {
        rehashStep()
        find(key)?.let { node ->
            val previous = node.value
            node.value = value
            return previous
        }
        val hash = spread(key)
        val into = spare ?: main
        val at = hash and (into.size - 1)
        into[at] = Node(key, value, hash, into[at])
        size++
        if (spare == null && size > main.size) startRehash(main.size * 2)
        return null
    }

    /** Drops [key]; answers the value that was under it, null when there was none. */
    fun remove(key: K): V? {
        rehashStep()
        val hash = spread(key)
        for (table in tables()) {
            val at = hash and (table.size - 1)
            var previous: Node<K, V>? = null
            var node = table[at]
            while (node != null) {
                if (node.hash == hash && node.key == key) {
                    if (previous == null) table[at] = node.next else previous.next = node.next
                    size--
                    if (spare == null && main.size > MIN_BUCKETS && size < main.size / 8) startRehash(main.size / 2)
                    return node.value
                }
                previous = node
                node = node.next
            }
        }
        return null
    }

    fun clear() {
        main = arrayOfNulls(MIN_BUCKETS)
        spare = null
        rehashProgress = NOT_REHASHING
        size = 0
    }

    /** Every entry, in no defined order. Do not mutate the table while iterating. */
    fun entries(): Sequence<Map.Entry<K, V>> = sequence {
        for (table in tables()) for (head in table) {
            var node = head
            while (node != null) {
                yield(node)
                node = node.next
            }
        }
    }

    /**
     * Visits the bucket at [cursor], and answers the cursor to pass back next; 0 ends the walk.
     * Redis's `dictScan`: reverse binary iteration, with the cursor's bits counted from the top
     * so that growing or shrinking the table mid-walk never skips a bucket. Every key present
     * for the whole walk is visited at least once; a shrink may repeat one (C15). Stateless: the
     * table keeps nothing between calls, and the walk neither mutates nor steps the rehash.
     */
    fun scan(cursor: Long, visit: (K, V) -> Unit): Long {
        val other = spare ?: return visitBucket(main, cursor, visit).let { next(cursor, main.size - 1L) }
        val (small, large) = if (main.size < other.size) main to other else other to main
        val smallMask = small.size - 1L
        val largeMask = large.size - 1L
        visitBucket(small, cursor, visit)
        var v = cursor
        do {
            visitBucket(large, v, visit)
            v = next(v, largeMask)
        } while (v and (largeMask xor smallMask) != 0L)
        return v
    }

    /** One key drawn by bucket sampling: a random bucket, then a random node of its chain. */
    fun randomKey(random: java.util.Random): K? {
        if (size == 0) return null
        while (true) {
            val table = spare?.takeIf { random.nextInt(main.size + it.size) >= main.size } ?: main
            var node = table[random.nextInt(table.size)] ?: continue
            var length = 0
            var walk: Node<K, V>? = node
            while (walk != null) {
                length++
                walk = walk.next
            }
            repeat(random.nextInt(length)) { node = node.next!! }
            return node.key
        }
    }

    private inline fun visitBucket(table: Array<Node<K, V>?>, cursor: Long, visit: (K, V) -> Unit) {
        var node = table[(cursor and (table.size - 1L)).toInt()]
        while (node != null) {
            visit(node.key, node.value)
            node = node.next
        }
    }

    /** The cursor after [v]: the masked bits incremented as if written most-significant first. */
    private fun next(v: Long, mask: Long): Long =
        java.lang.Long.reverse(java.lang.Long.reverse(v or mask.inv()) + 1)

    private fun tables(): List<Array<Node<K, V>?>> = listOfNotNull(main, spare)

    private fun startRehash(buckets: Int) {
        spare = arrayOfNulls(buckets)
        rehashProgress = 0
    }

    /** Moves one bucket of the old array into the new one; the last one swaps the arrays. */
    private fun rehashStep() {
        val into = spare ?: return
        var node = main[rehashProgress]
        main[rehashProgress] = null
        while (node != null) {
            val next = node.next
            val at = node.hash and (into.size - 1)
            node.next = into[at]
            into[at] = node
            node = next
        }
        if (++rehashProgress == main.size) {
            main = into
            spare = null
            rehashProgress = NOT_REHASHING
        }
    }

    private fun find(key: K): Node<K, V>? {
        val hash = spread(key)
        for (table in tables()) {
            var node = table[hash and (table.size - 1)]
            while (node != null && (node.hash != hash || node.key != key)) node = node.next
            if (node != null) return node
        }
        return null
    }

    /** The key's hash with its high bits folded down, so a power-of-two mask sees all of them. */
    private fun spread(key: K): Int = key.hashCode().let { it xor (it ushr 16) }

    companion object {
        const val NOT_REHASHING = -1
        private const val MIN_BUCKETS = 4
    }
}
