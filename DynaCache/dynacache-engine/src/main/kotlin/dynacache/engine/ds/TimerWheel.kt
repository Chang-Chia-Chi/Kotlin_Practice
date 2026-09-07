package dynacache.engine.ds

import java.time.Instant

/**
 * A timer wheel: schedules a key to fire at a deadline, fires it from [advanceTo].
 *
 * Time only enters through the constructor's [now] and [advanceTo]; the wheel owns no thread
 * and reads no clock. A deadline is rounded up to the next tick boundary, so an entry never
 * fires before its deadline and fires at most one tick after it (C7), provided the caller
 * advances the wheel at least once per tick. Entries fire in deadline order (I7).
 * [schedule] and [cancel] are O(1); [advanceTo] is O(ticks crossed + entries fired).
 *
 * [onFire] runs on the thread calling [advanceTo] and may schedule or cancel freely: the tick
 * being fired has already been detached. Not thread-safe; one partition executor owns a wheel.
 * [K] is any hashable key, so tests can use cheap keys and T09 can pass the engine's `Key`.
 */
class TimerWheel<K>(
    now: Instant,
    private val tickMillis: Long = 1000,
    private val slots: Int = 256,
    private val onFire: (K) -> Unit,
) {
    /** A circular doubly linked list link; a [Slot] is the sentinel, a [Node] an entry. */
    private open class Link<K> {
        var prev: Link<K> = this
        var next: Link<K> = this

        fun unlink() {
            prev.next = next
            next.prev = prev
            prev = this
            next = this
        }
    }

    private class Node<K>(val key: K, val deadline: Long, val tick: Long) : Link<K>()

    private class Slot<K> : Link<K>() {
        fun add(node: Node<K>) {
            node.prev = prev
            node.next = this
            prev.next = node
            prev = node
        }

        /** Empties the slot, returning its nodes in insertion order. */
        fun drain(): List<Node<K>> {
            val out = ArrayList<Node<K>>()
            var link = next
            while (link !== this) {
                out += link as Node<K>
                link = link.next
            }
            prev = this
            next = this
            return out
        }
    }

    private val entries = HashMap<K, Node<K>>()

    /**
     * Level L has [slots] slots, each [span] `[L]` = slots^L ticks wide, and holds entries fewer
     * than slots^(L+1) ticks ahead; anything further waits in [overflow]. A level-0 slot spans
     * one tick, so it fires whole. A higher level is cascaded into the levels below whenever
     * the tick crosses one of its slot boundaries, and the overflow when level 2 wraps.
     */
    private val levels = Array(3) { Array(slots) { Slot<K>() } }
    private val overflow = Slot<K>()
    private val span = longArrayOf(1, slots.toLong(), slots.toLong() * slots, slots.toLong() * slots * slots)
    private var currentTick = Math.floorDiv(now.toEpochMilli(), tickMillis)

    /** Fires [key] once [deadline] has passed; an existing entry for [key] is replaced. */
    fun schedule(key: K, deadline: Instant) {
        cancel(key)
        val millis = deadline.toEpochMilli()
        val node = Node(key, millis, maxOf(Math.ceilDiv(millis, tickMillis), currentTick + 1))
        entries[key] = node
        place(node)
    }

    /** Removes [key]'s pending entry; false when there was none. */
    fun cancel(key: K): Boolean {
        val node = entries.remove(key) ?: return false
        node.unlink()
        return true
    }

    /** Fires, in deadline order, every entry whose tick boundary is at or before [now]. */
    fun advanceTo(now: Instant) {
        val target = Math.floorDiv(now.toEpochMilli(), tickMillis)
        while (currentTick < target) {
            if (entries.isEmpty()) {
                currentTick = target
                return
            }
            val t = ++currentTick
            if (t % span[3] == 0L) cascade(overflow)
            for (level in 2 downTo 1) {
                if (t % span[level] == 0L) cascade(levels[level][slotOf(t, level)])
            }
            // A level-0 slot holds one tick's deadlines; sorting keeps fire order by deadline
            // even below tick resolution (I7).
            for (node in levels[0][slotOf(t, 0)].drain().sortedBy { it.deadline }) {
                entries.remove(node.key)
                onFire(node.key)
            }
        }
    }

    private fun place(node: Node<K>) {
        val ahead = node.tick - currentTick
        val slot = when {
            ahead < span[1] -> levels[0][slotOf(node.tick, 0)]
            ahead < span[2] -> levels[1][slotOf(node.tick, 1)]
            ahead < span[3] -> levels[2][slotOf(node.tick, 2)]
            else -> overflow
        }
        slot.add(node)
    }

    private fun cascade(slot: Slot<K>) {
        for (node in slot.drain()) place(node)
    }

    private fun slotOf(tick: Long, level: Int): Int = ((tick / span[level]) % slots).toInt()
}
