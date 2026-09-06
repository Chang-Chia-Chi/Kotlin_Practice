package dynacache.cluster

import dynacache.cluster.proto.Replicate
import java.time.Instant

/**
 * One node's **hints** (spec 2.4, C5): the `Replicate` messages it accepted on behalf of a
 * preference-list node that was dead at the time, each the full write (key, tokens, version,
 * TTL as an instant) so replaying it is sending it unchanged. In memory, by arrival, keyed by
 * the id the holder will replay it under so the target's ack finds it.
 */
class HintStore {

    private class Hint(val target: NodeId, val write: Replicate)

    private val hints = LinkedHashMap<Long, Hint>()

    val size: Int @Synchronized get() = hints.size

    @Synchronized
    fun add(id: Long, target: NodeId, write: Replicate) {
        hints[id] = Hint(target, write)
    }

    /** The target acked the replay under [id]: the hint has been handed off. */
    @Synchronized
    fun remove(id: Long) {
        hints.remove(id)
    }

    /**
     * Up to [limit] of [target]'s hints in arrival order, by replay id. A hint whose TTL passed
     * at [now] is dropped on the way: the target would only expire it again.
     */
    @Synchronized
    fun pending(target: NodeId, now: Instant, limit: Int): Map<Long, Replicate> {
        hints.values.removeIf { it.write.expiresAtMillis != 0L && it.write.expiresAtMillis <= now.toEpochMilli() }
        return hints.filterValues { it.target == target }.entries.take(limit).associate { it.key to it.value.write }
    }
}
