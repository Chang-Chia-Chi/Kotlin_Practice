import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Expiry
import com.github.benmanes.caffeine.cache.Ticker
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jboss.logging.Logger
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentMap
import kotlin.math.max
import kotlin.math.min

/**
 * Minimal in-memory task cache for a single-pod NATS → MinIO → SFTP pipeline.
 *
 * Purposes
 *  - Strictly avoid re-entrance (duplicates blocked while InProgress).
 *  - Provide last-status hint (Succeeded / Failed with failure count) for the caller to decide.
 *  - Enforce a max-failures threshold; when exceeded, signal the caller to TERM.
 *
 * Non-goals
 *  - No heartbeats or TTL refresh.
 *  - No backoff calculation.
 */
@ApplicationScoped
class InMemoryTaskCache @Inject constructor(
    private val registry: MeterRegistry,
    @ConfigProperty(name = "pipeline.cache.max-size", defaultValue = "200000")
    private val maxSize: Long,
    @ConfigProperty(name = "pipeline.cache.inflight-ttl", defaultValue = "PT10M")
    private val inFlightTtl: Duration,
    @ConfigProperty(name = "pipeline.cache.success-ttl", defaultValue = "PT6H")
    private val successTtl: Duration,
    @ConfigProperty(name = "pipeline.cache.failure-ttl", defaultValue = "PT30M")
    private val failureTtl: Duration,
    @ConfigProperty(name = "pipeline.cache.max-failures", defaultValue = "10")
    private val maxFailures: Int,
    @ConfigProperty(name = "pipeline.cache.debug-logs", defaultValue = "false")
    private val debugLogs: Boolean,
) : TaskCache {

    private val log = Logger.getLogger(InMemoryTaskCache::class.java)
    private val ownerId: UUID = UUID.randomUUID()
    private val ticker: Ticker = Ticker.systemTicker()

    private lateinit var map: ConcurrentMap<TaskKey, State>

    // Metrics
    private lateinit var mProceed: io.micrometer.core.instrument.Counter
    private lateinit var mShortCircuit: io.micrometer.core.instrument.Counter
    private lateinit var mWaitInProgress: io.micrometer.core.instrument.Counter
    private lateinit var mSuccess: io.micrometer.core.instrument.Counter
    private lateinit var mFailureRecorded: io.micrometer.core.instrument.Counter
    private lateinit var mFailureExhausted: io.micrometer.core.instrument.Counter

    @PostConstruct
    fun init() {
        val cache = Caffeine.newBuilder()
            .maximumSize(maxSize)
            .ticker(ticker)
            .expireAfter(object : Expiry<TaskKey, State> {
                override fun expireAfterCreate(key: TaskKey, value: State, currentTime: Long): Long = value.expireNanosFrom(currentTime)
                override fun expireAfterUpdate(key: TaskKey, value: State, currentTime: Long, currentDuration: Long): Long = value.expireNanosFrom(currentTime)
                override fun expireAfterRead(key: TaskKey, value: State, currentTime: Long, currentDuration: Long): Long = value.expireNanosFrom(currentTime)
            })
            .build<TaskKey, State>()

        this.map = cache.asMap()

        // Gauges & counters
        registry.gauge("task_cache_size", map) { it.size.toDouble() }
        mProceed = registry.counter("task_cache_acquire_proceed_total")
        mShortCircuit = registry.counter("task_cache_acquire_short_circuit_total")
        mWaitInProgress = registry.counter("task_cache_wait_in_progress_total")
        mSuccess = registry.counter("task_cache_record_success_total")
        mFailureRecorded = registry.counter("task_cache_record_failure_total", listOf())
        mFailureExhausted = registry.counter("task_cache_failures_exhausted_total")

        log.infof(
            "InMemoryTaskCache started (owner=%s, maxSize=%d, maxFailures=%d)",
            ownerId, maxSize, maxFailures
        )
    }

    override fun ownerId(): UUID = ownerId

    /**
     * Acquire decision:
     *  - If no entry → Proceed (becomes InProgress).
     *  - If Succeeded (within TTL) → Short-circuit success.
     *  - If InProgress (within TTL) → Wait(IN_PROGRESS, remainingTtl).
     *  - If Failed (within TTL) → Proceed (caller may choose to NAK or retry differently), becomes InProgress.
     */
    override fun tryStart(key: TaskKey): AcquireDecision {
        val now = ticker.read()
        var decision: AcquireDecision = AcquireDecision.Proceed

        map.compute(key) { _, cur ->
            val current = cur?.takeIf { !it.isExpired(now) }
            when (current) {
                null -> {
                    decision = AcquireDecision.Proceed
                    mProceed.increment()
                    InProgress(ownerId = ownerId, startedAtNanos = now, expireAtNanos = now + inFlightTtl.toNanos())
                }
                is Succeeded -> {
                    decision = AcquireDecision.ShortCircuitSuccess
                    mShortCircuit.increment()
                    current
                }
                is InProgress -> {
                    val wait = durationBetween(now, current.expireAtNanos)
                    decision = AcquireDecision.Wait(WaitReason.IN_PROGRESS, wait)
                    mWaitInProgress.increment()
                    current // no TTL refresh
                }
                is Failed -> {
                    decision = AcquireDecision.Proceed
                    mProceed.increment()
                    InProgress(ownerId = ownerId, startedAtNanos = now, expireAtNanos = now + inFlightTtl.toNanos())
                }
            }
        }

        if (debugLogs) log.debugf("tryStart key=%s → %s", key, decision)
        return decision
    }

    override fun recordSuccess(key: TaskKey): FinalizeResult.SuccessResult {
        val now = ticker.read()
        var result: FinalizeResult.SuccessResult = FinalizeResult.SuccessResult.Success

        map.compute(key) { _, cur ->
            when (cur) {
                is Succeeded -> {
                    result = FinalizeResult.SuccessResult.AlreadyFinalized
                    cur
                }
                else -> {
                    result = FinalizeResult.SuccessResult.Success
                    mSuccess.increment()
                    Succeeded(expireAtNanos = now + successTtl.toNanos())
                }
            }
        }
        if (debugLogs) log.debugf("recordSuccess key=%s → %s", key, result)
        return result
    }

    /**
     * Record a failure and increment the per-key failure count.
     * Returns whether the failure threshold was reached/exceeded so the caller can TERM immediately.
     */
    override fun recordFailure(key: TaskKey, cause: Throwable?): FinalizeResult.FailureResult {
        val now = ticker.read()
        var out = FinalizeResult.FailureResult.Recorded(failures = 1, shouldTerm = false)

        map.compute(key) { _, cur ->
            val prev = cur?.takeIf { !it.isExpired(now) }
            val failures = when (prev) {
                is Failed -> prev.failures + 1
                else -> 1
            }
            val exhausted = failures >= maxFailures
            out = FinalizeResult.FailureResult.Recorded(failures = failures, shouldTerm = exhausted)
            if (exhausted) mFailureExhausted.increment() else mFailureRecorded.increment()

            Failed(
                failures = failures,
                lastError = cause?.javaClass?.simpleName ?: "",
                lastMessage = cause?.message ?: "",
                atNanos = now,
                expireAtNanos = now + failureTtl.toNanos()
            )
        }

        if (debugLogs) log.debugf(
            "recordFailure key=%s failures=%d shouldTerm=%s cause=%s",
            key, out.failures, out.shouldTerm, cause?.message
        )
        return out
    }

    override fun snapshot(key: TaskKey): StateSnapshot? {
        val now = ticker.read()
        val s = map[key] ?: return null
        if (s.isExpired(now)) return null
        return when (s) {
            is InProgress -> StateSnapshot.InProgress(ownerId = s.ownerId, remainingTtl = durationBetween(now, s.expireAtNanos))
            is Succeeded -> StateSnapshot.Succeeded(remainingTtl = durationBetween(now, s.expireAtNanos))
            is Failed -> StateSnapshot.Failed(
                failures = s.failures,
                lastError = s.lastError,
                lastMessage = s.lastMessage,
                age = durationBetween(s.atNanos, now)
            )
        }
    }

    private fun durationBetween(startNanos: Long, endNanos: Long): Duration =
        Duration.ofNanos(max(0L, endNanos - startNanos))

    // ===== Model =====
    sealed interface State {
        val expireAtNanos: Long
        fun isExpired(nowNanos: Long): Boolean = nowNanos >= expireAtNanos
        fun expireNanosFrom(currentTime: Long): Long = max(1L, expireAtNanos - currentTime)
    }

    data class InProgress(
        val ownerId: UUID,
        val startedAtNanos: Long,
        override val expireAtNanos: Long,
    ) : State

    data class Succeeded(
        override val expireAtNanos: Long,
    ) : State

    data class Failed(
        val failures: Int,
        val lastError: String,
        val lastMessage: String,
        val atNanos: Long,
        override val expireAtNanos: Long,
    ) : State
}

// ===== Public SPI & DTOs =====
sealed interface TaskKey

data class MessageIdKey(val id: String) : TaskKey { override fun toString() = "msgId=$id" }

data class StreamSeqKey(val stream: String, val sequence: Long) : TaskKey { override fun toString() = "$stream#$sequence" }

sealed class AcquireDecision {
    data object Proceed : AcquireDecision()
    data object ShortCircuitSuccess : AcquireDecision()
    data class Wait(val reason: WaitReason, val delay: Duration) : AcquireDecision()
}

enum class WaitReason { IN_PROGRESS }

sealed class FinalizeResult {
    sealed class SuccessResult : FinalizeResult() {
        data object Success : SuccessResult()
        data object AlreadyFinalized : SuccessResult()
    }

    sealed class FailureResult : FinalizeResult() {
        data class Recorded(val failures: Int, val shouldTerm: Boolean) : FailureResult()
    }
}

sealed class StateSnapshot {
    data class InProgress(val ownerId: UUID, val remainingTtl: Duration) : StateSnapshot()
    data class Succeeded(val remainingTtl: Duration) : StateSnapshot()
    data class Failed(val failures: Int, val lastError: String, val lastMessage: String, val age: Duration) : StateSnapshot()
}

interface TaskCache {
    fun ownerId(): UUID
    fun tryStart(key: TaskKey): AcquireDecision
    fun recordSuccess(key: TaskKey): FinalizeResult.SuccessResult
    fun recordFailure(key: TaskKey, cause: Throwable? = null): FinalizeResult.FailureResult
    fun snapshot(key: TaskKey): StateSnapshot?
}

/* ---------------- Example usage in a JetStream handler ----------------
class JetStreamWorker @Inject constructor(
    private val cache: TaskCache,
    private val js: JetStreamClient,
    @ConfigProperty(name = "pipeline.wait-handling", defaultValue = "IGNORE")
    private val waitHandlingRaw: String,
) {
    private val waitHandling: WaitHandling = WaitHandling.valueOf(waitHandlingRaw.uppercase())

    suspend fun onMessage(msg: Message) {
        val key = msg.taskKey()
        when (val d = cache.tryStart(key)) {
            AcquireDecision.Proceed -> {
                try {
                    // do work → commit point (SFTP rename), then:
                    cache.recordSuccess(key)
                    msg.ack()
                } catch (e: Exception) {
                    val r = cache.recordFailure(key, e) as FinalizeResult.FailureResult.Recorded
                    if (r.shouldTerm) {
                        dlqPublish(msg, e)
                        js.term(msg)
                    } else {
                        // choose your own policy; fixed small NAK is common
                        msg.nakWithDelay(Duration.ofSeconds(2))
                    }
                }
            }
            AcquireDecision.ShortCircuitSuccess -> msg.ack()
            is AcquireDecision.Wait -> when (waitHandling) {
                WaitHandling.IGNORE -> return // safest for "no ghost acks"
                WaitHandling.NAK -> msg.nakWithDelay(d.delay)
                WaitHandling.ACK -> msg.ack() // quietest, but drops duplicate attempts by design
            }
        }
    }
}

# application.properties
# checksum.queue.capacity=20000              # legacy (unused by scheduler)
# checksum.queue.overflow=DROP_OLDEST
# checksum.bg.enabled=true
# checksum.bg.worker-concurrency=1
# checksum.bg.poll-interval=PT1S
# checksum.bg.reject-delay=PT2S
# sftp.max.concurrent=8
# sftp.bg.fg-threshold=0
# checksum.scheduler.capacity=50000
# checksum.scheduler.tick=PT0.2S
*/

// ========================= Tests: Dual Priority Queue Scheduler =========================
package com.mumu.pipeline.inmem

import com.mumu.pipeline.checksum.ChecksumRequest
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class ChecksumSchedulerTest {

    @Test
    fun ordersByPriorityThenFifo() = runBlocking {
        val reg = SimpleMeterRegistry()
        val sched = ChecksumScheduler(registry = reg, capacity = 100, tick = Duration.ofMillis(5))

        val a = ChecksumRequest("/a", "e1")
        val b = ChecksumRequest("/b", "e2")
        val c = ChecksumRequest("/c", "e3")

        // priorities: lower first; a(1), b(1), c(0) → c, a, b (FIFO for same priority)
        assertTrue(sched.schedule(a, notBefore = Duration.ZERO, priority = 1))
        assertTrue(sched.schedule(b, notBefore = Duration.ZERO, priority = 1))
        assertTrue(sched.schedule(c, notBefore = Duration.ZERO, priority = 0))

        val first = sched.takeReady(Duration.ofMillis(50))
        val second = sched.takeReady(Duration.ofMillis(50))
        val third = sched.takeReady(Duration.ofMillis(50))

        assertEquals("/c", first?.remotePath)
        assertEquals("/a", second?.remotePath)
        assertEquals("/b", third?.remotePath)
    }

    @Test
    fun respectsNotBeforeDelay() = runBlocking {
        val reg = SimpleMeterRegistry()
        val sched = ChecksumScheduler(registry = reg, capacity = 100, tick = Duration.ofMillis(5))
        val d = ChecksumRequest("/d", "e4")

        assertTrue(sched.schedule(d, notBefore = Duration.ofMillis(120), priority = 0))

        val none = sched.takeReady(Duration.ofMillis(30))
        assertNull(none)

        delay(130)
        val ready = sched.takeReady(Duration.ofMillis(50))
        assertNotNull(ready)
        assertEquals("/d", ready!!.remotePath)
    }

    @Test
    fun dropsOldestDelayedWhenAtCapacity() = runBlocking {
        val reg = SimpleMeterRegistry()
        val sched = ChecksumScheduler(registry = reg, capacity = 3, tick = Duration.ofMillis(5))

        // Fill with delayed entries: x(200ms), y(300ms)
        assertTrue(sched.schedule(ChecksumRequest("/x", "e"), notBefore = Duration.ofMillis(200), priority = 1))
        assertTrue(sched.schedule(ChecksumRequest("/y", "e"), notBefore = Duration.ofMillis(300), priority = 1))
        // One ready
        assertTrue(sched.schedule(ChecksumRequest("/z", "e"), notBefore = Duration.ZERO, priority = 1))

        // Capacity reached (3). Scheduling another ready should drop the oldest delayed (/x) and succeed.
        assertTrue(sched.schedule(ChecksumRequest("/w", "e"), notBefore = Duration.ZERO, priority = 1))

        // After enough time, only /y should emerge from delayed (since /x was dropped).
        delay(320)
        val r1 = sched.takeReady(Duration.ofMillis(50))
        val r2 = sched.takeReady(Duration.ofMillis(50))
        val r3 = sched.takeReady(Duration.ofMillis(50))
        val paths = listOfNotNull(r1?.remotePath, r2?.remotePath, r3?.remotePath)
        assertTrue(paths.contains("/y"))
        assertFalse(paths.contains("/x"))
    }

    @Test
    fun rejectsWhenCapacityAllReady() = runBlocking {
        val reg = SimpleMeterRegistry()
        val sched = ChecksumScheduler(registry = reg, capacity = 2, tick = Duration.ofMillis(5))
        assertTrue(sched.schedule(ChecksumRequest("/r1", "e"), notBefore = Duration.ZERO, priority = 0))
        assertTrue(sched.schedule(ChecksumRequest("/r2", "e"), notBefore = Duration.ZERO, priority = 0))
        // Now delayed is empty; adding another should return false (reject)
        assertFalse(sched.schedule(ChecksumRequest("/r3", "e"), notBefore = Duration.ZERO, priority = 0))
    }
}


// ========================= Additional Tests: Dual Priority Queue Scheduler =========================
package com.mumu.pipeline.inmem

import com.mumu.pipeline.checksum.ChecksumRequest
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Test

class ChecksumSchedulerTestMore {

    @Test
    fun multipleBecomeReadyAtSameInstant() = runBlocking {
        val reg = SimpleMeterRegistry()
        val sched = ChecksumScheduler(registry = reg, capacity = 100, tick = Duration.ofMillis(2))
        val t = Duration.ofMillis(80)
        // Same notBefore, different priorities
        assertTrue(sched.schedule(ChecksumRequest("/p1", "e"), notBefore = t, priority = 1))
        assertTrue(sched.schedule(ChecksumRequest("/p0", "e"), notBefore = t, priority = 0))
        assertTrue(sched.schedule(ChecksumRequest("/p2", "e"), notBefore = t, priority = 2))
        delay(90)
        val r1 = sched.takeReady(Duration.ofMillis(20))
        val r2 = sched.takeReady(Duration.ofMillis(20))
        val r3 = sched.takeReady(Duration.ofMillis(20))
        assertEquals("/p0", r1?.remotePath)
        assertEquals("/p1", r2?.remotePath)
        assertEquals("/p2", r3?.remotePath)
    }

    @Test
    fun fifoWithinSamePriorityUnderBurst() = runBlocking {
        val reg = SimpleMeterRegistry()
        val sched = ChecksumScheduler(registry = reg, capacity = 1000, tick = Duration.ofMillis(1))
        val names = (1..100).map { "/b$it" }
        names.forEach { assertTrue(sched.schedule(ChecksumRequest(it, "e"), notBefore = Duration.ZERO, priority = 5)) }
        val out = mutableListOf<String>()
        repeat(100) { out += sched.takeReady(Duration.ofMillis(20))!!.remotePath }
        assertEquals(names, out)
    }

    @Test
    fun concurrentScheduleAndTakeIsSafe() = runBlocking {
        val reg = SimpleMeterRegistry()
        val sched = ChecksumScheduler(registry = reg, capacity = 2000, tick = Duration.ofMillis(1))
        val total = 200
        val produced = mutableListOf<String>()
        val consumed = mutableListOf<String>()

        val producer = launch {
            repeat(total) {
                val id = "/c$it"; produced += id
                assertTrue(sched.schedule(ChecksumRequest(id, "e"), notBefore = Duration.ofMillis((it % 3).toLong()), priority = (it % 7).toLong()))
            }
        }
        val consumer = launch {
            var got = 0
            while (got < total) {
                val x = sched.takeReady(Duration.ofMillis(50)) ?: continue
                consumed += x.remotePath
                got++
            }
        }
        producer.join(); consumer.join()
        assertEquals(total, consumed.size)
        assertTrue(consumed.toSet().containsAll(produced.toSet()))
    }
}

// ========================= Tests: InMemoryTaskCache =========================
package com.mumu.pipeline.cache

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test

class InMemoryTaskCacheTest {

    private fun newCache(maxFailures: Int = 2): InMemoryTaskCache {
        val reg = SimpleMeterRegistry()
        val cache = InMemoryTaskCache(
            registry = reg,
            maxSize = 1000,
            inFlightTtl = Duration.ofSeconds(60),
            successTtl = Duration.ofMinutes(10),
            failureTtl = Duration.ofMinutes(5),
            maxFailures = maxFailures,
            debugLogs = false,
        )
        cache.init()
        return cache
    }

    @Test
    fun noReentrance_waitWhenInProgress() {
        val cache = newCache()
        val key = MessageIdKey("k1")
        assertEquals(AcquireDecision.Proceed, cache.tryStart(key))
        val d = cache.tryStart(key)
        require(d is AcquireDecision.Wait)
        assertEquals(WaitReason.IN_PROGRESS, d.reason)
        assertTrue(d.delay > Duration.ZERO)
    }

    @Test
    fun successShortCircuits() {
        val cache = newCache()
        val key = MessageIdKey("k2")
        assertEquals(AcquireDecision.Proceed, cache.tryStart(key))
        assertTrue(cache.recordSuccess(key) is FinalizeResult.SuccessResult.Success)
        assertEquals(AcquireDecision.ShortCircuitSuccess, cache.tryStart(key))
        val snap = cache.snapshot(key)
        require(snap is StateSnapshot.Succeeded)
        assertTrue(snap.remainingTtl > Duration.ZERO)
    }

    @Test
    fun failureCountsAndThreshold() {
        val cache = newCache(maxFailures = 2)
        val key = MessageIdKey("k3")
        assertEquals(AcquireDecision.Proceed, cache.tryStart(key))
        val f1 = cache.recordFailure(key, RuntimeException("boom1")) as FinalizeResult.FailureResult.Recorded
        assertEquals(1, f1.failures)
        assertFalse(f1.shouldTerm)
        val f2 = cache.recordFailure(key, RuntimeException("boom2")) as FinalizeResult.FailureResult.Recorded
        assertEquals(2, f2.failures)
        assertTrue(f2.shouldTerm)
        val snap = cache.snapshot(key)
        require(snap is StateSnapshot.Failed)
        assertEquals(2, snap.failures)
    }

    @Test
    fun proceedAfterFailedEntry() {
        val cache = newCache()
        val key = MessageIdKey("k4")
        assertEquals(AcquireDecision.Proceed, cache.tryStart(key))
        cache.recordFailure(key, RuntimeException("x"))
        // Next arrival proceeds (caller may choose to NAK externally)
        assertEquals(AcquireDecision.Proceed, cache.tryStart(key))
    }
}


// ========================= In-memory Caffeine State (TTL-based) =========================
package com.mumu.pipeline.inmem

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Expiry
import com.github.benmanes.caffeine.cache.Ticker
import com.mumu.pipeline.spi.*
import jakarta.inject.Named

/**
 * Caffeine-backed in-memory state with variable TTL by status.
 * - MATCHED → evict after [matchedTtl]
 * - FAILED  → evict after [failedTtl]
 * - PENDING/RUNNING → evict after [pendingTtl]
 */
@ApplicationScoped
@Named("caffeine")
class CaffeineChecksumState @Inject constructor(
    @ConfigProperty(name = "checksum.state.max-size", defaultValue = "200000")
    private val maxSize: Long,
    @ConfigProperty(name = "checksum.state.ttl.matched", defaultValue = "PT6H")
    private val matchedTtl: Duration,
    @ConfigProperty(name = "checksum.state.ttl.failed", defaultValue = "PT30M")
    private val failedTtl: Duration,
    @ConfigProperty(name = "checksum.state.ttl.pending", defaultValue = "PT30M")
    private val pendingTtl: Duration,
) : ChecksumState {

    private val ticker: Ticker = Ticker.systemTicker()
    private lateinit var map: ConcurrentMap<String, Record>

    @PostConstruct
    fun init() {
        val cache = Caffeine.newBuilder()
            .maximumSize(maxSize)
            .ticker(ticker)
            .expireAfter(object : Expiry<String, Record> {
                override fun expireAfterCreate(key: String, value: Record, currentTime: Long): Long = ttlFor(value).toNanos()
                override fun expireAfterUpdate(key: String, value: Record, currentTime: Long, currentDuration: Long): Long = ttlFor(value).toNanos()
                override fun expireAfterRead(key: String, value: Record, currentTime: Long, currentDuration: Long): Long = currentDuration
            })
            .build<String, Record>()
        map = cache.asMap()
    }

    private fun ttlFor(r: Record): Duration = when (r.status) {
        Status.MATCHED -> matchedTtl
        Status.FAILED -> failedTtl
        Status.PENDING, Status.RUNNING -> pendingTtl
    }

    override fun get(path: String): Record? = map[path]

    override fun upsertPending(path: String, expected: String, algo: String) {
        map.compute(path) { _, prev ->
            when (prev) {
                null -> Record(path, expected, algo, Status.PENDING)
                else -> prev.copy(expected = expected, algo = algo, status = Status.PENDING)
            }
        }
    }

    override fun update(path: String, status: Status, note: String?, failures: Int?) {
        map.compute(path) { _, prev ->
            val cur = prev ?: Record(path, expected = "", algo = "SHA-256", status = status)
            cur.copy(status = status, note = note, failures = failures ?: cur.failures)
        }
    }

    override fun all(): List<Record> = map.values.toList()
}

/* application.properties additions
# Caffeine state TTLs
checksum.state.max-size=200000
checksum.state.ttl.matched=PT6H
checksum.state.ttl.failed=PT30M
checksum.state.ttl.pending=PT30M
*/

// ========================= Property-based tests (jqwik) =========================
package com.mumu.pipeline.inmem

import com.mumu.pipeline.checksum.ChecksumRequest
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import net.jqwik.api.*
import net.jqwik.api.constraints.IntRange

class ChecksumSchedulerProperties {

    @Property(tries = 100)
    fun prioritiesOnly_respectsPriorityAndFifo(@ForAll @IntRange(min = 1, max = 50) n: Int,
                                               @ForAll @IntRange(min = 1, max = 50) m: Int) {
        val reg = SimpleMeterRegistry()
        val sched = ChecksumScheduler(registry = reg, capacity = 10000, tick = Duration.ofMillis(1))
        val count = min(n, m)
        val inserted = mutableListOf<Pair<Int, String>>()
        for (i in 0 until count) {
            val p = (i % 6)
            val id = "/i$i"
            assert(sched.schedule(ChecksumRequest(id, "e"), notBefore = Duration.ZERO, priority = p.toLong()))
            inserted += p to id
        }
        val taken = mutableListOf<String>()
        repeat(count) { taken += sched.takeReady(Duration.ofMillis(50))!!.remotePath }
        val expected = inserted.withIndex().sortedWith(compareBy({ it.value.first }, { it.index })).map { it.value.second }
        assertEquals(expected, taken)
    }

    @Property(tries = 30)
    fun twoBuckets_delayThenPriority(@ForAll @IntRange(min = 1, max = 20) aCount: Int,
                                     @ForAll @IntRange(min = 1, max = 20) bCount: Int) {
        val reg = SimpleMeterRegistry()
        val sched = ChecksumScheduler(registry = reg, capacity = 10000, tick = Duration.ofMillis(1))
        val ac = aCount
        val bc = bCount
        val bucketA = (0 until ac).map { i -> (i % 6) to "/a$i" }
        val bucketB = (0 until bc).map { i -> (i % 6) to "/b$i" }
        bucketA.forEach { (p, id) -> assert(sched.schedule(ChecksumRequest(id, "e"), notBefore = Duration.ZERO, priority = p.toLong())) }
        bucketB.forEach { (p, id) -> assert(sched.schedule(ChecksumRequest(id, "e"), notBefore = Duration.ofMillis(40), priority = p.toLong())) }
        val taken = mutableListOf<String>()
        repeat(ac) { taken += sched.takeReady(Duration.ofMillis(50))!!.remotePath }
        Thread.sleep(50)
        repeat(bc) { taken += sched.takeReady(Duration.ofMillis(50))!!.remotePath }
        val expectedA = bucketA.withIndex().sortedWith(compareBy({ it.value.first }, { it.index })).map { it.value.second }
        val expectedB = bucketB.withIndex().sortedWith(compareBy({ it.value.first }, { it.index })).map { it.value.second }
        assertEquals(expectedA + expectedB, taken)
    }
}

