package dynacache.engine

import dynacache.engine.ds.SkipList
import dynacache.engine.testkit.MutableClock
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.time.Duration
import java.time.Instant
import java.util.Random

/**
 * The store on its own: accounting, expiry and eviction without a command in sight. What the
 * interpreter does around one keyed command is two calls -- [settle] below -- so these tests
 * drive the store the way [Partition.execute] drives it and nothing else.
 */
class PartitionStoreTest {

    private val clock = MutableClock(Instant.parse("2026-09-06T00:00:00Z"))

    /** The clock moves before every command, as a real one always has, and answers its instant. */
    private fun now(): Instant {
        clock.now += Duration.ofMillis(1)
        return clock.now
    }

    /** Key number [i], always the same width, so every seeded entry costs the same. */
    private fun key(i: Int) = Key("k%03d".format(i))

    /** Key number [i]'s own value: 100 bytes like every other, and unlike every other. */
    private fun value(i: Int) = "value-%03d".format(i).padEnd(100, '.').toByteArray()

    /**
     * What one seeded entry costs, read back off a store with no threshold rather than copied
     * from the formula, which would only ever agree with itself. The tests then size their
     * budgets in entries.
     */
    private val entryBytes: Long = uncapped().let {
        it.put(key(0), clock.now, Value.Str(value(0)))
        it.usedBytes
    }

    private fun uncapped() = PartitionStore(Random(1), TICK_MILLIS, Long.MAX_VALUE, EvictionPolicy.LRU)

    /** A store whose share holds exactly [entries] seeded entries. */
    private fun capped(entries: Int, policy: EvictionPolicy = EvictionPolicy.LRU) =
        PartitionStore(Random(20260907), TICK_MILLIS, entries * entryBytes, policy)

    /**
     * What [Partition.execute] does after every keyed command: the key it touched is billed for
     * what it costs now, and then the store settles back under its share while keeping that key.
     */
    private fun PartitionStore.settle(key: Key, at: Instant) {
        account(key)
        evictUntil(at, key)
    }

    /** One `SET` of key number [i]. */
    private fun PartitionStore.write(i: Int, ttl: Duration? = null) {
        val at = now()
        put(key(i), at, Value.Str(value(i)), ttl?.let(at::plus))
        settle(key(i), at)
    }

    /** One `GET` of key number [i]: true when it was there. */
    private fun PartitionStore.read(i: Int): Boolean {
        val at = now()
        val hit = get(key(i), at) != null
        settle(key(i), at)
        return hit
    }

    /**
     * A write that cannot fit under the share at all. Before this ticket the eviction step it
     * triggered had no reason to spare it: it emptied the store and then took the new key too,
     * having replied as though it had been stored.
     */
    @ParameterizedTest
    @EnumSource(EvictionPolicy::class)
    fun eviction_never_evicts_the_key_being_written(policy: EvictionPolicy) {
        val store = capped(3, policy)
        repeat(3) { store.write(it) }
        val big = Key("big")
        val at = now()
        store.put(big, at, Value.Str(ByteArray((4 * entryBytes).toInt())))
        store.settle(big, at)

        assertNotNull(store.peek(big, at), "the key the write was for survived its own eviction step")
        assertEquals(1, store.size, "and every other key paid for it")
        repeat(3) { assertFalse(store.read(it), "key $it went") }
    }

    /** The hash under [key], replacing whatever other kind was there; the driver is kind-agnostic. */
    private fun PartitionStore.hashAt(key: Key, at: Instant): Value.Hash =
        get(key, at)?.value as? Value.Hash ?: put(key, at, Value.Hash()).value as Value.Hash

    private fun PartitionStore.listAt(key: Key, at: Instant): Value.List =
        get(key, at)?.value as? Value.List ?: put(key, at, Value.List()).value as Value.List

    private fun PartitionStore.zsetAt(key: Key, at: Instant, seed: Long): Value.ZSet =
        get(key, at)?.value as? Value.ZSet ?: put(key, at, Value.ZSet(SkipList(seed))).value as Value.ZSet

    /** What the entries actually cost, counted from scratch the way [PartitionStore.put] bills them. */
    private fun PartitionStore.countedBytes(): Long =
        entries().sumOf { it.key.bytes.size + PartitionStore.ENTRY_BYTES + it.value.value.approximateBytes() }

    /**
     * I6's neighbour and the reason this store exists: the running total is the sum of what the
     * entries hold, after every step of a seeded sequence of writes, in-place growth and shrinkage
     * over all four kinds, deletions, deadlines, wheel ticks and evictions. A total kept by
     * arithmetic drifts the moment one path forgets to book its side; recounting after each step
     * is what makes that drift a failing assertion rather than a wrong `INFO`.
     */
    @Test
    fun store_used_bytes_equals_sum_of_entries_after_any_sequence() {
        val draw = Random(20260907)
        // Small enough that writes cross it often, so eviction is part of the sequence.
        val store = capped(12)
        var evicted = 0
        repeat(STEPS) { step ->
            val key = key(draw.nextInt(30))
            val at = now()
            when (draw.nextInt(11)) {
                0 -> store.put(key, at, Value.Str(ByteArray(draw.nextInt(200))))
                1 -> store.put(key, at, Value.Str(ByteArray(draw.nextInt(200))), at.plusMillis(draw.nextInt(50) + 1L))
                2 -> store.hashAt(key, at).fields.put("f${draw.nextInt(8)}", ByteArray(draw.nextInt(40)))
                3 -> store.hashAt(key, at).fields.remove("f${draw.nextInt(8)}")
                4 -> store.listAt(key, at).items.addLast(ByteArray(draw.nextInt(40)))
                5 -> store.listAt(key, at).items.removeLastOrNull()
                6 -> store.zsetAt(key, at, draw.nextLong())
                    .writeScore(draw.nextDouble(), "m${draw.nextInt(8)}".toByteArray())
                7 -> store.forget(key)
                8 -> store.expireAt(key, at, if (draw.nextBoolean()) at.plusMillis(20) else null)
                9 -> store.get(key, at)
                10 -> store.tick(at)
            }
            store.account(key)
            val before = store.size
            store.evictUntil(at, key)
            evicted += before - store.size
            assertEquals(store.countedBytes(), store.usedBytes, "step $step drifted, on key $key")
        }
        assertTrue(evicted > 0, "the sequence never reached the threshold, so eviction went untested")
    }

    /**
     * Spec 2.7's admission rule. Key 0 is read until the sketch rates it far above anything else,
     * so when the window overflows and offers it to the main space it is taken; every one-hit key
     * after it loses the same comparison and never gets in. Under sampling LRU key 0 would have
     * gone early -- it is read once and then never again while thirty writes go past it -- so the
     * survival is the policy's doing and not the clock's.
     */
    @Test
    fun tinylfu_admits_frequent() {
        val store = capped(6, EvictionPolicy.W_TINYLFU)
        store.write(0)
        repeat(10) { assertTrue(store.read(0), "key 0 is read until the sketch rates it high") }
        // Key 1 is read exactly as often as any of the churn keys that follow it: once.
        store.write(1)
        repeat(30) { store.write(it + 2) }

        assertTrue(store.read(0), "the frequent key was admitted to the main space and survived the churn")
        assertFalse(store.read(1), "the one-hit key was not")
        assertEquals(6, store.size, "the threshold still holds six entries")
    }

    /** A Zipf key: five characters wide whatever its number, so every entry costs the same. */
    private fun zipfKey(i: Int) = Key("z%04d".format(i))

    /**
     * A seeded Zipf trace over [keys] keys: key `i` is drawn with probability proportional to
     * `1/(i+1)`, the skew a cache is for. The cumulative weights are built once and the draw is a
     * binary search into them, so the trace costs nothing next to replaying it.
     */
    private fun zipfTrace(accesses: Int, keys: Int, seed: Long): IntArray {
        val cumulative = DoubleArray(keys)
        var total = 0.0
        for (i in 0 until keys) {
            total += 1.0 / (i + 1)
            cumulative[i] = total
        }
        val draw = Random(seed)
        return IntArray(accesses) {
            val at = java.util.Arrays.binarySearch(cumulative, draw.nextDouble() * total)
            (if (at >= 0) at else -at - 1).coerceIn(0, keys - 1)
        }
    }

    /**
     * Replays [trace] against a store of [policy] holding about two hundred entries, reading each
     * key and writing it back on a miss, and answers with how many reads hit. The clock moves on
     * every access, hit or miss: recency is what LRU has, and a trace that stood still would take
     * it away from the policy this comparison is meant to beat.
     */
    private fun hitsUnder(policy: EvictionPolicy, trace: IntArray): Int {
        val store = capped(200, policy)
        val value = ByteArray(100) { '.'.code.toByte() }
        var hits = 0
        for (i in trace) {
            val at = now()
            if (store.get(zipfKey(i), at) != null) hits++ else store.put(zipfKey(i), at, Value.Str(value))
            store.settle(zipfKey(i), at)
        }
        return hits
    }

    /**
     * The reason W-TinyLFU is in the spec at all: on a skewed trace it keeps the keys that are
     * asked for often, where sampling LRU keeps the keys that were asked for last. Both policies
     * see the identical trace and the identical budget, so the difference is the policy.
     */
    @Test
    fun tinylfu_hit_ratio_beats_lru_on_zipf() {
        val trace = zipfTrace(accesses = 20_000, keys = 2_000, seed = 20260906)
        val lru = hitsUnder(EvictionPolicy.LRU, trace)
        val tiny = hitsUnder(EvictionPolicy.W_TINYLFU, trace)
        assertTrue(
            tiny > lru,
            "over ${trace.size} accesses to 2000 keys with room for about 200: " +
                "W-TinyLFU hit $tiny times, LRU hit $lru times",
        )
    }

    private companion object {
        const val TICK_MILLIS = 1000L
        const val STEPS = 2_000
    }
}
