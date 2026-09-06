package dynacache.engine.testkit

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.Collections

/**
 * The one clock every module's tests run on (plan rule 1.5): time moves only when a test says
 * so, by assigning [now] or calling [advance]. The engine's partition threads, the WAL writer's
 * callers, the cluster's hint sweeper and MicroRaft all read it off the test thread, hence
 * volatile.
 *
 * A clock built with [record] also keeps [readers], the thread behind every read in order, so a
 * test can assert who reads the clock and how often. It is off by default because a suite that
 * runs thousands of commands would otherwise keep a string per read, and the synchronized list
 * would add contention to the very threads a concurrency test is measuring.
 */
class MutableClock(@Volatile var now: Instant, private val record: Boolean = false) : Clock() {

    private val reads: MutableList<String> = Collections.synchronizedList(mutableListOf())

    /** The thread behind each read, in order; empty unless the clock was built to [record]. */
    val readers: List<String> get() = reads

    fun advance(by: Duration) {
        now += by
    }

    override fun instant(): Instant {
        if (record) reads += Thread.currentThread().name
        return now
    }

    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = this
}
