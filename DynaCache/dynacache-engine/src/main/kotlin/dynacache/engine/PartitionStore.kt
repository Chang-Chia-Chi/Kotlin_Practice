package dynacache.engine

import dynacache.engine.ds.HashTable
import dynacache.engine.ds.TimerWheel
import java.time.Instant
import java.util.Random

/**
 * The keys one partition holds, and everything that decides which of them are still there: the
 * entries themselves, their deadlines and the wheel that fires them, the running byte total, and
 * the eviction policy (spec 2.7, 5.4, 5.5, I6). Kind-agnostic: it knows a [Value] costs bytes and
 * nothing about what any kind of value means.
 *
 * The interpreter above it names keys and values and reads replies out of them; it never counts a
 * byte and never chooses a victim. Six operations are the whole of what it asks for -- [get],
 * [put], [forget], [expireAt], [account] and [evictUntil] -- and the rest of this surface is the
 * keyspace-wide walk that `KEYS`, `SCAN`, `INFO` and the replication views need.
 *
 * Not thread-safe, and does not need to be: one store belongs to one partition executor and is
 * only ever touched from that thread (C1 by construction, ADR 0001).
 */
internal class PartitionStore(
    private val random: Random,
    private val tickMillis: Long,
    /**
     * This partition's even share of the node's memory threshold. `Long.MAX_VALUE` is a node with
     * no threshold: a share nothing can cross is a partition that never evicts, with no second
     * branch to say so.
     */
    private val maxBytes: Long,
    /** Which key this store gives up when it is over [maxBytes]; see [coldest]. */
    policy: EvictionPolicy,
) {

    /** What one key holds: its value, its deadline, and the two numbers the policy reads. */
    class Entry(val value: Value, var expiresAt: Instant?) {
        /** The String bytes, safe to read once the caller's kind check has passed. */
        val str: ByteArray get() = (value as Value.Str).bytes

        /**
         * When a command last read or wrote this entry, from that command's own reading of the
         * clock. The sampling policy of spec 2.7 evicts the oldest of the keys it draws.
         */
        var lastAccess: Instant = Instant.EPOCH

        /** What this entry last contributed to [usedBytes]; see [charge]. */
        var accounted: Long = 0

        /** The one expiry rule: a key is readable through its deadline and gone after it. */
        fun expired(now: Instant): Boolean = expiresAt.let { it != null && now.isAfter(it) }
    }

    private val table = HashTable<Key, Entry>()

    /**
     * What the store holds, by the estimate of [Value.approximateBytes]: the sum over the live
     * entries of the key's own bytes, [ENTRY_BYTES] for the entry itself and the value's payload.
     * Kept as a running total rather than recounted, so `INFO` costs nothing and [evictUntil] can
     * be asked after every command whether the partition is over its share.
     */
    var usedBytes: Long = 0
        private set

    val size: Int get() = table.size

    /**
     * The W-TinyLFU bookkeeping, or null under [EvictionPolicy.LRU], which needs none: sampling
     * reads [Entry.lastAccess] off keys it draws and remembers nothing between evictions. A null
     * here is the whole cost of the policy this partition did not choose.
     */
    private val tinyLfu: WindowTinyLfu? =
        if (policy == EvictionPolicy.W_TINYLFU) {
            WindowTinyLfu(maxBytes, random.nextLong()) { key -> table.get(key)?.accounted ?: 0 }
        } else {
            null
        }

    /**
     * The active half of spec 5.4's belt and braces: the store's own wheel deletes a key as its
     * deadline falls due, so nothing has to be read to be removed. Only ever touched from the
     * partition executor -- by [put], [forget], [expireAt] and [tick] -- so its callback needs no
     * synchronisation of its own.
     *
     * Null until the first TTL, and born from that command's own reading of the clock: the engine
     * reads the clock once per command and never outside one, so a wheel cannot be built in the
     * constructor. A partition that has never held a TTL has no wheel to advance.
     */
    private var wheel: TimerWheel<Key>? = null

    private fun wheel(now: Instant): TimerWheel<Key> =
        wheel ?: TimerWheel<Key>(now, tickMillis) { key -> remove(key) }.also { wheel = it }

    /**
     * The entry under [key] if it is still alive at [now], deleting an expired one on this access
     * (spec 5.4's lazy check). Every read on the command path funnels through here, so this is
     * where "recently used" is written down, from the reading command's own instant.
     */
    fun get(key: Key, now: Instant): Entry? {
        val entry = table.get(key) ?: return null
        if (entry.expired(now)) {
            forget(key)
            return null
        }
        entry.lastAccess = now
        return entry
    }

    /**
     * The live entry under [key] without touching anything: what a replication or snapshot view
     * reads. Such a view is not a client access, so it neither ages the key nor collects it.
     */
    fun peek(key: Key, now: Instant): Entry? = table.get(key)?.takeUnless { it.expired(now) }

    /**
     * The one way an entry enters the store, and with it the one place a TTL reaches the wheel.
     * Every write goes through here, so no command can leave a deadline behind that would later
     * fire against a value it was never meant for.
     */
    fun put(key: Key, now: Instant, value: Value, expiresAt: Instant? = null): Entry {
        val entry = Entry(value, expiresAt)
        entry.lastAccess = now
        // The displaced entry's bytes leave with it; [charge] then bills for what replaced it.
        val displaced = table.put(key, entry)?.accounted ?: 0
        usedBytes -= displaced
        tinyLfu?.resized(key, -displaced)
        charge(key)
        // A write that carries no TTL clears the one the key had, wheel entry and all; a key with
        // no wheel entry has nothing to cancel, and so needs no wheel to be built.
        if (expiresAt == null) wheel?.cancel(key) else wheel(now).schedule(key, expiresAt)
        return entry
    }

    /** The one way an entry leaves the store: its bytes and its pending deadline leave with it. */
    fun forget(key: Key): Boolean {
        val gone = remove(key) != null
        if (gone) wheel?.cancel(key)
        return gone
    }

    /**
     * `EXPIRE` and `PERSIST`: the deadline under [key] is replaced, or cleared by a null one, and
     * the wheel is told. The entry itself is untouched, so a key does not lose what the policy
     * knows about it for having been given a new deadline.
     */
    fun expireAt(key: Key, now: Instant, deadline: Instant?) {
        val entry = table.get(key) ?: return
        entry.expiresAt = deadline
        if (deadline == null) wheel?.cancel(key) else wheel(now).schedule(key, deadline)
    }

    /**
     * One command's access to [key]: the policy is told, and [usedBytes] is brought level with
     * what the entry costs now. A command that grew or shrank an aggregate in place never passed
     * through [put], so this recount is what keeps the running total honest without a third path
     * into the table. Idempotent -- it books the difference from what the entry was last charged.
     */
    fun account(key: Key) {
        // One access per command, recorded here rather than inside [get] so a command that reads
        // its key twice -- the kind check and then the command itself -- counts once. Recency
        // needs no more than the single clock read; frequency is the sketch's.
        if (tinyLfu != null && table.get(key) != null) tinyLfu.touch(key)
        charge(key)
    }

    /**
     * One bounded eviction step, spec 5.5's order: every expired key goes first, and only then do
     * live keys, so no live key is ever taken while an expired one is still there (I6). The live
     * ones go by the policy this store was built with, until the partition is back under its share
     * or the store is empty.
     *
     * The key the command that crossed the threshold was writing is [keeping], and is never the
     * victim of its own write: a value too big for the share would otherwise delete itself and
     * reply as though it had been stored. It is an ordinary candidate for every step after this.
     *
     * At most [MAX_EVICTIONS] keys go in one step, so no single command stalls on a threshold it
     * cannot reach in one pass; the command after it runs the next step. Eviction is local to this
     * partition and to this thread: nothing here reads or writes another partition.
     */
    fun evictUntil(now: Instant, keeping: Key?) {
        if (usedBytes <= maxBytes) return
        purgeExpired(now)
        var evicted = 0
        while (usedBytes > maxBytes && table.size > 0 && evicted < MAX_EVICTIONS) {
            forget(coldest(keeping) ?: return)
            evicted++
        }
    }

    /**
     * The lazy check of [get], applied to the whole store at once: what a keyspace-wide command
     * sees afterwards is exactly the live keys, with no copy of the key set to filter.
     *
     * ponytail: O(n) in the keyspace, so `DBSIZE` costs a walk that Redis answers in O(1). The
     * wheel does not replace it: it removes a key at the first tick after its deadline, and a
     * keyspace-wide command asked in the gap before that tick must still not see the key. Only
     * a store that can find its expired keys without a walk would let this go.
     */
    fun purgeExpired(now: Instant) {
        // Collected before anything is removed: the table must not be mutated while its entries
        // are being walked.
        table.entries().filter { it.value.expired(now) }.map { it.key }.toList().forEach(::forget)
    }

    /** `FLUSHDB`: every key goes, so every deadline goes and the wheel is dropped whole. */
    fun clear() {
        wheel = null
        table.clear()
        usedBytes = 0
        tinyLfu?.clear()
    }

    fun randomKey(): Key? = table.randomKey(random)

    /** Every entry the table holds, expired ones included; callers filter at their own instant. */
    fun entries(): Sequence<Map.Entry<Key, Entry>> = table.entries()

    /** This store's share of a `SCAN`: [emit] sees the live keys found, and the cursor comes back. */
    fun scan(cursor: Long, count: Int, now: Instant, emit: (Key) -> Unit): Long =
        walk(table, cursor, count, { _, entry -> !entry.expired(now) }) { key, _ -> emit(key) }

    /** Advances the wheel to [now], firing the deadlines that have fallen due. */
    fun tick(now: Instant) {
        wheel?.advanceTo(now)
    }

    /**
     * Takes the entry out of the table and its bytes off the total. The one accounting line the
     * wheel's own removal shares with [forget]: the wheel has already dropped its own entry by
     * the time its callback runs, so it cannot cancel, but the bytes still have to go.
     */
    private fun remove(key: Key): Entry? = table.remove(key)?.also {
        usedBytes -= it.accounted
        tinyLfu?.forgotten(key, it.accounted)
    }

    /** Charges [usedBytes] the difference between what the entry under [key] cost and costs. */
    private fun charge(key: Key) {
        val entry = table.get(key) ?: return
        val size = key.bytes.size + ENTRY_BYTES + entry.value.approximateBytes()
        usedBytes += size - entry.accounted
        tinyLfu?.resized(key, size - entry.accounted)
        entry.accounted = size
    }

    /**
     * The one key this eviction takes, by whichever policy the store was built with, and never
     * [keeping]. The step around it -- when to run, in what order, how many -- is the same either
     * way, so the policy is this function and nothing else.
     */
    private fun coldest(keeping: Key?): Key? =
        (tinyLfu?.victim() ?: sampledColdest(keeping))?.takeIf { it != keeping }

    /**
     * Spec 2.7's sampling LRU: the least recently accessed of [SAMPLE] keys drawn at random. A
     * store no larger than the sample is taken whole -- drawing with replacement from it could
     * only miss a key that sampling "K random keys" was meant to include.
     */
    private fun sampledColdest(keeping: Key?): Key? {
        val drawn =
            if (table.size <= SAMPLE) table.entries().map { it.key }.toList()
            else List(SAMPLE) { table.randomKey(random) ?: return null }
        return drawn.filterNot { it == keeping }.minByOrNull { table.get(it)!!.lastAccess }
    }

    internal companion object {
        /** What an entry costs beyond its key's bytes and its value's: the entry, the table's node, the deadline. */
        const val ENTRY_BYTES = 48L

        /** Spec 2.7's K: how many keys one eviction draws before taking the coldest of them. */
        const val SAMPLE = 5

        /** The bound on one eviction step, so no one write pays for a whole keyspace. */
        const val MAX_EVICTIONS = 32
    }
}

/**
 * Redis's `SCAN` loop: buckets are walked until at least [count] entries came out of them or the
 * walk wrapped, then [keep] filters what came out. So a call may answer few keys, or none, with a
 * cursor that is not 0; the client keeps calling until it is. Shared by the store's walk of its
 * keys and the interpreter's walks of a hash's or a sorted set's own fields.
 */
internal fun <K, V> walk(
    table: HashTable<K, V>,
    cursor: Long,
    count: Int,
    keep: (K, V) -> Boolean,
    emit: (K, V) -> Unit,
): Long {
    var next = cursor
    var visited = 0
    do {
        next = table.scan(next) { key, value ->
            visited++
            if (keep(key, value)) emit(key, value)
        }
    } while (next != 0L && visited < count)
    return next
}
