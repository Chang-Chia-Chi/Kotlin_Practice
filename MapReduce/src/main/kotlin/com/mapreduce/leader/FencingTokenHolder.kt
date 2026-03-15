package com.mapreduce.leader

/**
 * ThreadLocal-based fencing token propagation for synchronous JDBI calls.
 *
 * The [FencedLeaderInterceptor] sets the epoch before proceeding and clears
 * it in a finally block. Repository code reads it via [require] or [get].
 *
 * This channel works for synchronous call stacks where the interceptor and
 * the repository execute on the same thread. For coroutine-based pipelines
 * that cross suspension points, use [FencingContext] instead.
 */
object FencingTokenHolder {

    private val holder = ThreadLocal<Long>()

    fun set(epoch: Long) {
        holder.set(epoch)
    }

    fun clear() {
        holder.remove()
    }

    /** Returns the current epoch, or null if not in a fenced context. */
    fun get(): Long? = holder.get()

    /**
     * Returns the current epoch, or throws if not set.
     * Use this in repository code that MUST run inside a fenced block.
     */
    fun require(): Long = holder.get()
        ?: throw IllegalStateException(
            "Fencing token not set — this code must run inside a @FencedLeader block or FencingTokenHolder.withToken()",
        )

    /**
     * Sets the epoch, executes [block], and clears in finally.
     * This is the primary entry point for propagating the epoch.
     */
    inline fun <T> withToken(epoch: Long, block: () -> T): T {
        set(epoch)
        try {
            return block()
        } finally {
            clear()
        }
    }
}
