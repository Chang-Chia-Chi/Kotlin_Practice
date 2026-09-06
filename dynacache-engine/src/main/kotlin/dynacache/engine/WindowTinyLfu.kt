package dynacache.engine

import dynacache.engine.ds.CountMinSketch

/**
 * Caffeine's W-TinyLFU (spec 2.7) as one object: the whole of "which key goes" for a partition
 * that was built with [EvictionPolicy.W_TINYLFU]. The partition tells it what happened -- a key
 * was touched, resized or forgotten -- and asks it for a [victim]; nothing else in the partition
 * knows the policy, exactly as nothing else knew the sampling loop it replaces.
 *
 * The keyspace is three LRU lists. A key the policy has not seen enters the **window**, one
 * percent of the partition's share, which is there to absorb a burst of one-hit keys without ever
 * letting them touch what the cache is actually holding. When the window is over its share its
 * least recently used key is a **candidate** for the **main space**, and the sketch decides: the
 * candidate is admitted only if it has been seen more often than the main space's own victim, and
 * the loser of that comparison is what gets evicted. The main space is segmented -- a candidate
 * lands on **probation**, a hit while on probation earns it **protection**, and a protected space
 * over its eighty percent demotes its coldest key back to probation -- so a key has to be asked
 * for twice before a burst of newcomers can no longer reach it.
 *
 * Recency inside a list is the list's own order and not the clock: a [LinkedHashSet] re-inserted
 * on every touch is exact LRU in O(1), where the partition's sampling loop was an approximation.
 * Frequency is the sketch, which is why [CountMinSketch.halve] has to run: without aging, the key
 * that was hot an hour ago outranks the key that is hot now forever.
 *
 * Not thread-safe, and does not need to be: it belongs to one partition and is only ever touched
 * from that partition's own thread.
 */
internal class WindowTinyLfu(
    maxBytes: Long,
    seed: Long,
    /** What the entry under a key currently costs, from the partition's own accounting. */
    private val sizeOf: (Key) -> Long,
) {

    private val sketch = CountMinSketch(width = SKETCH_WIDTH, seed = seed)

    /** The admission window, least recently used first. */
    private val window = LinkedHashSet<Key>()

    /** The main space's first segment: admitted, but not yet asked for a second time. */
    private val probation = LinkedHashSet<Key>()

    /** The main space's second segment: asked for again while on probation. */
    private val protectedKeys = LinkedHashSet<Key>()

    private var windowBytes = 0L
    private var protectedBytes = 0L

    /** Spec 2.7's one percent. At least one byte, so a threshold under a hundred bytes still has one. */
    private val windowMax = maxOf(1L, maxBytes / 100)

    /** Caffeine's split of the main space: four fifths of it is protected from newcomers. */
    private val protectedMax = (maxBytes - windowMax) / 100 * 80

    /** Accesses recorded since the sketch was last aged; see [age]. */
    private var sampled = 0L

    /**
     * One access to a live [key], recorded once per command. Raises the key's frequency and moves
     * it to the most recently used end of whichever list holds it -- promoting it out of probation
     * on the way, which is what "asked for twice" buys. A key the policy has not seen enters the
     * window and is charged for there.
     */
    fun touch(key: Key) {
        sketch.increment(key)
        age()
        if (window.remove(key)) {
            window.add(key)
        } else if (protectedKeys.remove(key)) {
            protectedKeys.add(key)
        } else if (probation.remove(key)) {
            protect(key)
        } else {
            window.add(key)
            windowBytes += sizeOf(key)
        }
    }

    /** The entry under [key] grew or shrank in place by [delta] bytes, wherever it lives. */
    fun resized(key: Key, delta: Long) {
        if (delta == 0L) return
        if (key in window) windowBytes += delta else if (key in protectedKeys) protectedBytes += delta
    }

    /** [key] has left the store, taking [bytes] with it. */
    fun forgotten(key: Key, bytes: Long) {
        if (window.remove(key)) windowBytes -= bytes
        else if (protectedKeys.remove(key)) protectedBytes -= bytes
        else probation.remove(key)
    }

    /** The store was emptied wholesale (`FLUSHDB`). The sketch keeps what it learned; the lists do not. */
    fun clear() {
        window.clear()
        probation.clear()
        protectedKeys.clear()
        windowBytes = 0
        protectedBytes = 0
    }

    /**
     * The next key to evict, or null when the policy holds none. Admission happens here because
     * this is the only moment it can: a candidate leaves the window exactly when something has to
     * be given up, so the comparison that admits it is the same comparison that names the victim.
     */
    fun victim(): Key? {
        if (windowBytes > windowMax && window.size > 1) {
            val candidate = window.first()
            val victim = probation.firstOrNull()
            if (victim != null) {
                // TinyLFU's admission filter: the candidate has to have been seen more often than
                // the key it would displace. A tie goes to the incumbent, which is what stops a
                // stream of never-repeated keys from washing the main space out one key at a time.
                if (sketch.estimate(candidate) <= sketch.estimate(victim)) return candidate
                admit(candidate)
                return victim
            }
            // Nothing on probation to compare against, so the candidate is admitted unopposed and
            // the eviction falls to the window's next coldest key.
            admit(candidate)
            window.firstOrNull()?.let { return it }
        }
        // Under its share, or the window has run out: the coldest of the main space goes, and the
        // window only when the main space is empty.
        return probation.firstOrNull() ?: protectedKeys.firstOrNull() ?: window.firstOrNull()
    }

    /** Moves the window's candidate into the main space, on probation. */
    private fun admit(candidate: Key) {
        window.remove(candidate)
        windowBytes -= sizeOf(candidate)
        probation.add(candidate)
    }

    /**
     * A hit on probation earns protection. The protected segment is bounded, so taking one key in
     * may put its coldest keys back on probation -- demotion, not eviction: a demoted key is still
     * in the cache, it has just lost its head start over the next candidate.
     */
    private fun protect(key: Key) {
        protectedKeys.add(key)
        protectedBytes += sizeOf(key)
        while (protectedBytes > protectedMax && protectedKeys.size > 1) {
            val demoted = protectedKeys.first()
            protectedKeys.remove(demoted)
            protectedBytes -= sizeOf(demoted)
            probation.add(demoted)
        }
    }

    /**
     * Halves every counter once the policy has recorded [SAMPLES_PER_KEY] accesses per key it
     * holds -- Caffeine's sample size, which scales the aging period with the cache rather than
     * fixing it: a big cache needs a long memory and a small one needs a short one. [MIN_SAMPLE]
     * is the floor, so a nearly empty policy does not age its sketch away on every access.
     */
    private fun age() {
        sampled++
        val sample = maxOf(MIN_SAMPLE, SAMPLES_PER_KEY * (window.size + probation.size + protectedKeys.size))
        if (sampled >= sample) {
            sketch.halve()
            sampled = 0
        }
    }

    private companion object {
        /** Counters per sketch row. Sixteen kilobytes a partition at [CountMinSketch]'s default depth. */
        const val SKETCH_WIDTH = 4096

        /** Caffeine's `10 x maximumSize`, expressed against the keys the policy actually holds. */
        const val SAMPLES_PER_KEY = 10L

        /** The floor under the aging period, so an empty policy does not halve its sketch constantly. */
        const val MIN_SAMPLE = 100L
    }
}
