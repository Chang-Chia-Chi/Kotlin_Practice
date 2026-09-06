package dynacache.engine.ds

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class HashTableTest {

    @Test
    fun hashtable_put_get_remove() {
        val table = HashTable<String, Int>()
        assertEquals(0, table.size)
        assertNull(table.get("a"))
        assertNull(table.put("a", 1), "a new key has no previous value")
        assertEquals(1, table.put("a", 2), "an overwrite answers the value it replaced")
        assertNull(table.put("b", 3))
        assertEquals(2, table.get("a"))
        assertEquals(3, table.get("b"))
        assertEquals(2, table.size)
        assertEquals(2, table.remove("a"))
        assertNull(table.remove("a"), "removing twice removes nothing")
        assertNull(table.get("a"))
        assertEquals(1, table.size)
        assertEquals(listOf("b" to 3), table.entries().map { it.key to it.value }.toList())
        table.clear()
        assertEquals(0, table.size)
        assertNull(table.get("b"))
        assertTrue(table.entries().none())
    }

    /**
     * The growth rule has one bucket migrated per operation, so a table that crosses the load
     * threshold is still rehashing several operations later, and every key is reachable meanwhile.
     */
    @Test
    fun incremental_rehash_no_block() {
        val table = HashTable<Int, Int>()
        var key = 0
        while (table.rehashProgress == HashTable.NOT_REHASHING) table.put(key, key++)
        assertEquals(0, table.rehashProgress, "growth starts with nothing migrated yet")
        var steps = 0
        while (table.rehashProgress != HashTable.NOT_REHASHING) {
            assertEquals(steps, table.rehashProgress, "one bucket per operation, no more")
            assertEquals(steps % key, table.get(steps % key), "every key is reachable mid-rehash; the read is the step")
            steps++
        }
        assertTrue(steps > 1, "the rehash spread over several operations rather than one: $steps")
        for (k in 0 until key) assertEquals(k, table.get(k), "key $k survived the swap")
    }

    /** Under a seeded storm, the rehash progress never advances by more than one bucket per operation. */
    @Test
    fun no_single_operation_migrates_more_than_one_bucket() {
        val table = HashTable<Int, Int>()
        val random = java.util.Random(5)
        val shadow = HashMap<Int, Int>()
        repeat(20_000) {
            val before = table.rehashProgress
            val k = random.nextInt(2_000)
            if (random.nextInt(3) == 0) assertEquals(shadow.remove(k), table.remove(k)) else assertEquals(shadow.put(k, it), table.put(k, it))
            val after = table.rehashProgress
            val oneBucketAtMost = after == HashTable.NOT_REHASHING || after == before + 1 || (before == HashTable.NOT_REHASHING && after == 0)
            assertTrue(oneBucketAtMost, "op $it moved the rehash from $before to $after")
            assertEquals(shadow.size, table.size)
        }
        for ((k, v) in shadow) assertEquals(v, table.get(k))
    }

    /** Every key the table holds, gathered by one full scan; duplicates are kept so a test can see them. */
    private fun fullScan(table: HashTable<Int, Int>, between: (Int) -> Unit = {}): List<Int> {
        val seen = ArrayList<Int>()
        var cursor = 0L
        var calls = 0
        do {
            cursor = table.scan(cursor) { key, _ -> seen += key }
            between(calls++)
        } while (cursor != 0L)
        return seen
    }

    @Test
    fun scan_during_rehash_no_miss() {
        val table = HashTable<Int, Int>()
        val present = (0 until 100).toList()
        present.forEach { table.put(it, it) }
        while (table.rehashProgress != HashTable.NOT_REHASHING) table.get(0)
        var grown = false
        var extra = 1_000
        val seen = fullScan(table) { call ->
            // Each call inserts fresh keys: growth triggers mid-scan and rehashes one bucket per op.
            repeat(10) { table.put(extra, extra++) }
            if (table.rehashProgress != HashTable.NOT_REHASHING) grown = true
        }
        assertTrue(grown, "the scan crossed a rehash")
        assertTrue(seen.containsAll(present), "missed: ${present - seen.toSet()}")
    }

    @Test
    fun scan_may_duplicate() {
        val table = HashTable<Int, Int>()
        (0 until 1_024).forEach { table.put(it, it) }
        while (table.rehashProgress != HashTable.NOT_REHASHING) table.get(0)
        val survivors = (0 until 1_024 step 64).toSet()
        val seen = fullScan(table) { call ->
            // The first call deletes almost everything: the table shrinks mid-scan, and a shrink
            // is what makes reverse binary iteration revisit a bucket.
            if (call == 0) (0 until 1_024).filter { it !in survivors }.forEach { table.remove(it) }
        }
        assertTrue(seen.containsAll(survivors), "missed: ${survivors - seen.toSet()}")
        assertTrue(seen.size > seen.toSet().size, "a shrink mid-scan repeats keys, which a client tolerates")
        assertTrue(survivors.containsAll(seen.filter { it in survivors }), "every repeat is a real key")
    }
}
