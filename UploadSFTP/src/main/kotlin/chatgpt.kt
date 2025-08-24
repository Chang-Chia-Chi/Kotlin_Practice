here you go — a single drop-in **`PipelinePrimitives.kt`** that recaps and unifies what we built:

* **Waiting rate-limiter** helper (`RateLimiter.awaitPermit()`) – no failures on limit, just suspends
* **Per-message heartbeat** for NATS (`HeartbeatingNatsMessage`) – stop heartbeat first, then `ack/nak/nakWithDelay/term`
* **Multi-circuit-breaker admission** (`MultiBreakerGate`) with safe rollback and settle
* **Flow operators** to gate elements on **multiple CBs** with backoff + jitter (`withAdmissionAll`, `admitOnly`)
* **BreakerWatcher** to pause/resume your puller when any CB opens

```kotlin
// src/main/kotlin/com/example/pipeline/PipelinePrimitives.kt
@file:Suppress("MemberVisibilityCanBePrivate")

package com.example.pipeline

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.ratelimiter.RateLimiter
import io.nats.client.Message
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.transform
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/* =========================
 *  1) Rate limiter helper
 * ========================= */

/**
 * Await a Resilience4j permit without throwing when the limit is reached.
 * Suspends for the duration suggested by the limiter.
 */
suspend fun RateLimiter.awaitPermit() {
    // large timeout so reservePermission never rejects "because timeout"
    val nanos = reservePermission(Duration.ofDays(365))
    if (nanos < 0) error("RateLimiter rejected unexpectedly")
    if (nanos > 0) delay(nanos / 1_000_000)
}

/* =========================
 *  2) NATS heartbeat wrapper
 * ========================= */

/**
 * Wraps a JetStream message and keeps it alive by calling inProgress() periodically.
 * Always stops the heartbeat BEFORE ack/nak/term to avoid races.
 */
class HeartbeatingNatsMessage(
    private val msg: Message,
    private val ackWait: Duration,
    private val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.Default),
    private val minPeriod: kotlin.time.Duration = 2.seconds,
    private val maxPeriod: kotlin.time.Duration = 20.seconds,
    private val jitterPct: Double = 0.08
) {
    private val finished = AtomicBoolean(false)
    @Volatile private var hbJob: Job? = null

    /** Start background heartbeats immediately (idempotent). */
    fun startHeartbeat() {
        if (hbJob != null) return
        val base = (ackWait.toMillis().milliseconds / 3)
            .coerceAtLeast(minPeriod)
            .coerceAtMost(maxPeriod)

        hbJob = scope.launch {
            val rnd = Random
            while (isActive && !finished.get()) {
                runCatching { msg.inProgress() } // refresh redelivery timer
                val sign = if (rnd.nextBoolean()) 1.0 else -1.0
                val jitter = 1.0 + sign * rnd.nextDouble(0.0, jitterPct)
                val delayMs = max(1500L, (base.inWholeMilliseconds * jitter).toLong())
                delay(delayMs)
            }
        }
    }

    /** Stop heartbeat and wait for completion (idempotent). */
    suspend fun stopHeartbeat() {
        val job = hbJob ?: return
        job.cancel()
        runCatching { job.join() }
        hbJob = null
    }

    suspend fun ack() = finishOnce { stopHeartbeat(); msg.ack() }
    suspend fun nak() = finishOnce { stopHeartbeat(); msg.nak() }
    suspend fun nakWithDelay(delay: Duration) = finishOnce { stopHeartbeat(); msg.nakWithDelay(delay) }
    suspend fun term() = finishOnce { stopHeartbeat(); msg.term() }

    private suspend fun finishOnce(block: suspend () -> Unit) {
        if (finished.compareAndSet(false, true)) block()
    }
}

/* =========================
 *  3) Multi-CB admission
 * ========================= */

/**
 * AND-admission across multiple CircuitBreakers.
 * - Acquires permissions from ALL required CBs (using tryAcquirePermission()).
 * - If any denies, already-acquired permissions are released (no leaks).
 * - After work: call onSuccess()/onError() to record outcome; or release() if cancelled/not executed.
 */
class MultiBreakerGate(
    private val breakersByName: Map<String, CircuitBreaker> // e.g. "minio","sftp"
) {
    data class Ticket(val acquired: List<CircuitBreaker>) {
        private var finished = false
        fun onSuccess() {
            if (finished) return
            acquired.forEach { it.onSuccess(0, TimeUnit.NANOSECONDS) }
            finished = true
        }
        fun onError(t: Throwable) {
            if (finished) return
            acquired.forEach { it.onError(0, TimeUnit.NANOSECONDS, t) }
            finished = true
        }
        fun release() {
            if (finished) return
            acquired.forEach { it.releasePermission() }
            finished = true
        }
    }

    /** Try to admit across all names; returns a Ticket if ALL allow, otherwise null (after rolling back). */
    fun tryAdmit(vararg names: String): Ticket? {
        val got = mutableListOf<CircuitBreaker>()
        for (n in names) {
            val cb = breakersByName.getValue(n)
            if (!cb.tryAcquirePermission()) {
                got.forEach { it.releasePermission() }
                return null
            }
            got += cb
        }
        return Ticket(got)
    }

    /** Utility to check availability (no state change). */
    fun isAllAvailable(vararg names: String): Boolean =
        names.all { breakersByName.getValue(it).state != CircuitBreaker.State.OPEN }
}

/* =========================
 *  4) Flow operators
 * ========================= */

/**
 * Gate each element on ALL named circuit breakers; wait with backoff+jitter while denied.
 * After admission, run [transform] and automatically record success/error to ALL breakers.
 * If the coroutine is cancelled after admission, the ticket is released (no probe leak).
 */
fun <T, R> Flow<T>.withAdmissionAll(
    gate: MultiBreakerGate,
    names: (T) -> Array<String>,                 // which CBs this element needs
    initialBackoff: kotlin.time.Duration = 100.milliseconds,
    maxBackoff: kotlin.time.Duration = 2.seconds,
    jitterPct: Double = 0.15,
    onWaiting: (suspend (T) -> Unit)? = null,    // optional: send heartbeat/metrics while waiting
    transform: suspend (T) -> R
): Flow<R> = transform { value ->
    val ctx = currentCoroutineContext()
    var backoff = initialBackoff
    var ticket: MultiBreakerGate.Ticket? = null

    // admission loop
    while (ctx.isActive && ticket == null) {
        ticket = gate.tryAdmit(*names(value))
        if (ticket == null) {
            onWaiting?.let { it(value) }
            val jitter = 1.0 + (if (Random.nextBoolean()) 1 else -1) * Random.nextDouble(0.0, jitterPct)
            val waitMs = (backoff.inWholeMilliseconds * jitter).toLong().coerceAtLeast(25L)
            delay(waitMs)
            backoff = (backoff * 2.0).coerceAtMost(maxBackoff)
        }
    }
    check(ticket != null) { "Cancelled before admission" }

    try {
        val out = transform(value)
        ticket!!.onSuccess()
        emit(out)
    } catch (ce: CancellationException) {
        ticket!!.release() // don't burn half-open probe on cancel
        throw ce
    } catch (t: Throwable) {
        ticket!!.onError(t)
        throw t
    }
}

/**
 * Admit only (no transform). Emits the element after admission.
 * If downstream cancels before you manually settle, [finallyOnCancel] runs (default: release()).
 */
fun <T> Flow<T>.admitOnly(
    gate: MultiBreakerGate,
    names: (T) -> Array<String>,
    initialBackoff: kotlin.time.Duration = 100.milliseconds,
    maxBackoff: kotlin.time.Duration = 2.seconds,
    jitterPct: Double = 0.15,
    onWaiting: (suspend (T) -> Unit)? = null,
    finallyOnCancel: (suspend (T, MultiBreakerGate.Ticket) -> Unit)? = { _, ticket -> ticket.release() }
): Flow<T> = transform { value ->
    val ctx = currentCoroutineContext()
    var ticket: MultiBreakerGate.Ticket? = null
    var backoff = initialBackoff

    try {
        while (ctx.isActive && ticket == null) {
            ticket = gate.tryAdmit(*names(value))
            if (ticket == null) {
                onWaiting?.let { it(value) }
                val jitter = 1.0 + (if (Random.nextBoolean()) 1 else -1) * Random.nextDouble(0.0, jitterPct)
                val waitMs = (backoff.inWholeMilliseconds * jitter).toLong().coerceAtLeast(25L)
                delay(waitMs)
                backoff = (backoff * 2.0).coerceAtMost(maxBackoff)
            }
        }
        check(ticket != null) { "Cancelled before admission" }
        emit(value) // hand off; you must call ticket.onSuccess/onError later
    } finally {
        if (!currentCoroutineContext().isActive && ticket != null) {
            finallyOnCancel?.invoke(value, ticket!!)
        }
    }
}

/* =========================
 *  5) Breaker watcher → pause/resume pulls
 * ========================= */

/**
 * Listens for state transitions and calls [onPause] when ANY breaker opens,
 * [onResume] when all are closed/half-open. Useful to pause NATS pulls.
 */
class BreakerWatcher(
    private val breakers: List<CircuitBreaker>,
    private val onPause: () -> Unit,
    private val onResume: () -> Unit
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    @Volatile private var paused = false

    init {
        breakers.forEach { cb ->
            cb.eventPublisher.onStateTransition { e ->
                scope.launch {
                    val anyOpen = breakers.any { it.state == CircuitBreaker.State.OPEN }
                    if (anyOpen && !paused) {
                        paused = true; onPause()
                    } else if (!anyOpen && paused) {
                        paused = false; onResume()
                    }
                }
            }
        }
        // fire initial snapshot
        scope.launch {
            val anyOpen = breakers.any { it.state == CircuitBreaker.State.OPEN }
            paused = anyOpen
            if (anyOpen) onPause() else onResume()
        }
    }
}

/* =========================
 *  6) Tiny usage sketch (comments)
 * ========================= */
/*
val gate = MultiBreakerGate(mapOf("minio" to minioCB, "sftp" to sftpCB))

// Example: end-to-end in one step
sourceFlow.withAdmissionAll(
    gate,
    names = { arrayOf("minio", "sftp") },
    onWaiting = { /* optional: msg.inProgress() if you don't run a background heartbeat */ }
) { task ->
    minioLimiter.awaitPermit()
    val bytes = fetchFromMinio(task)
    sftpLimiter.awaitPermit()
    uploadToSftp(task, bytes)
    task.id
}.collect()

// Example: NATS heartbeat per message
suspend fun handle(msg: Message, ackWait: Duration) {
    val hb = HeartbeatingNatsMessage(msg, ackWait)
    hb.startHeartbeat()
    try {
        // do work...
        hb.ack()
    } catch (t: Throwable) {
        hb.nakWithDelay(Duration.ofSeconds(15))
    }
}

// Example: pause/resume pulls when any breaker is OPEN
BreakerWatcher(listOf(minioCB, sftpCB),
    onPause = { natsPuller.pause() },
    onResume = { natsPuller.resume() }
)
*/

// src/main/kotlin/com/example/pipeline/PipelineAllInOne.kt
@file:Suppress("MemberVisibilityCanBePrivate", "UnusedParameter")

package com.example.pipeline

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.nats.client.*
import io.nats.client.api.*
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.io.InputStream
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/* =========================
 *  Configuration (simplify)
 *  (Replace with @ConfigProperty as you wire Quarkus config)
 * ========================= */
data class PipelineConfig(
    val natsUrl: String = "nats://localhost:4222",
    val stream: String = "DATA",
    val subject: String = "data.request",
    val durable: String = "pipeline-worker",
    val ackWait: Duration = Duration.ofSeconds(60),
    val natsFetch: Int = 32,
    val natsFetchMaxWait: Duration = Duration.ofMillis(500),

    val s3Endpoint: String = "http://minio:9000",
    val s3Region: String = "us-east-1",
    val s3MetaBucket: String = "meta",
    val s3FileBucket: String = "files",

    val sftpHost: String = "sftp",
    val sftpPort: Int = 22,
    val sftpUser: String = "user",
    val sftpPassword: String = "pass",
    val sftpBaseDir: String = "/upload",
    val sftpChunk: Int = 64 * 1024,
    val sftpTmpSuffix: String = ".part",
    val sftpMaxBytesPerSec: Long = 0,    // 0 = unlimited

    // concurrency + rate limits
    val maxInFlight: Int = 256,
    val minioQps: Double = 200.0,
    val sftpQps: Double = 100.0,

    // retry
    val maxLocalRetries: Int = 3,
    val maxDeliveries: Int = 10, // NATS consumer MaxDeliver
    val nakDelay: Duration = Duration.ofSeconds(15)
)

/* =========================
 *  Metrics (Micrometer)
 * ========================= */
@ApplicationScoped
class PipelineMetrics @Inject constructor(
    private val registry: MeterRegistry
) {
    fun incParseFail(source: String, reason: String) {
        Counter.builder("pipeline_parse_fail_total")
            .tag("source", source)             // nats|api|retry
            .tag("reason", reason)             // json|schema|missing_field|unknown
            .description("Upstream parse failures")
            .register(registry).increment()
    }
    fun incDownloadFail(store: String, bucket: String, stage: String, reason: String) {
        Counter.builder("pipeline_download_fail_total")
            .tag("store", store)               // s3|minio
            .tag("bucket", bucket)
            .tag("stage", stage)               // metadata|object
            .tag("reason", reason)             // 404|timeout|io|empty|unknown
            .description("Download failures")
            .register(registry).increment()
    }
}

/* =========================
 *  RateLimiter helpers
 * ========================= */
fun rl(name: String, permitsPerSec: Double): RateLimiter =
    RateLimiter.of(name, RateLimiterConfig.custom()
        .timeoutDuration(Duration.ZERO) // we won't block here; we await manually
        .limitRefreshPeriod(Duration.ofMillis(250))
        .limitForPeriod(max(1, (permitsPerSec * 0.25).toInt()))
        .build())

suspend fun RateLimiter.awaitPermit() {
    val nanos = reservePermission(Duration.ofDays(365))
    if (nanos < 0) error("RateLimiter rejected unexpectedly")
    if (nanos > 0) delay(nanos / 1_000_000)
}

/* =========================
 *  CircuitBreakers
 * ========================= */
fun cb(name: String): CircuitBreaker =
    CircuitBreaker.of(name, CircuitBreakerConfig.custom()
        .failureRateThreshold(50.0f)
        .slowCallRateThreshold(50.0f)
        .slowCallDurationThreshold(Duration.ofSeconds(3))
        .permittedNumberOfCallsInHalfOpenState(5)
        .minimumNumberOfCalls(20)
        .waitDurationInOpenState(Duration.ofSeconds(15))
        .build())

/* =========================
 *  Multi-CB admission gate
 * ========================= */
class MultiBreakerGate(private val map: Map<String, CircuitBreaker>) {
    data class Ticket(val acquired: List<CircuitBreaker>) {
        private var finished = false
        fun onSuccess() { if (!finished) { acquired.forEach { it.onSuccess(0, TimeUnit.NANOSECONDS) }; finished = true } }
        fun onError(t: Throwable) { if (!finished) { acquired.forEach { it.onError(0, TimeUnit.NANOSECONDS, t) }; finished = true } }
        fun release() { if (!finished) { acquired.forEach { it.releasePermission() }; finished = true } }
    }
    fun tryAdmit(vararg names: String): Ticket? {
        val got = mutableListOf<CircuitBreaker>()
        for (n in names) {
            val b = map.getValue(n)
            if (!b.tryAcquirePermission()) { got.forEach { it.releasePermission() }; return null }
            got += b
        }
        return Ticket(got)
    }
    fun anyOpen(vararg names: String): Boolean = names.any { map.getValue(it).state == CircuitBreaker.State.OPEN }
}

/* =========================
 *  NATS heartbeat wrapper
 * ========================= */
class HeartbeatingNatsMessage(
    private val msg: Message,
    private val ackWait: Duration,
    private val scope: CoroutineScope,
    private val minPeriod: kotlin.time.Duration = 2.seconds,
    private val maxPeriod: kotlin.time.Duration = 20.seconds,
    private val jitterPct: Double = 0.08
) {
    private val finished = AtomicBoolean(false)
    @Volatile private var job: Job? = null

    fun start() {
        if (job != null) return
        val base = (ackWait.toMillis().milliseconds / 3).coerceAtLeast(minPeriod).coerceAtMost(maxPeriod)
        job = scope.launch {
            while (isActive && !finished.get()) {
                runCatching { msg.inProgress() }
                val sign = if (Random.nextBoolean()) 1.0 else -1.0
                val jitter = 1.0 + sign * Random.nextDouble(0.0, jitterPct)
                val delayMs = max(1500L, (base.inWholeMilliseconds * jitter).toLong())
                delay(delayMs)
            }
        }
    }

    suspend fun stop() { job?.cancel(); runCatching { job?.join() }; job = null }
    suspend fun ack() { finishOnce { stop(); msg.ack() } }
    suspend fun nak() { finishOnce { stop(); msg.nak() } }
    suspend fun nakWithDelay(delay: Duration) { finishOnce { stop(); msg.nakWithDelay(delay) } }
    suspend fun term() { finishOnce { stop(); msg.term() } }
    private suspend fun finishOnce(block: suspend () -> Unit) { if (finished.compareAndSet(false, true)) block() }
}

/* =========================
 *  SFTP (JSch) with atomic rename + optional byte pacing
 * ========================= */
class BytePacer(rateBytesPerSec: Long) {
    private val enabled = rateBytesPerSec > 0
    private val tokens = java.util.concurrent.atomic.AtomicLong(if (enabled) rateBytesPerSec else Long.MAX_VALUE)
    private val ticker = if (enabled) CoroutineScope(Dispatchers.Default).launch {
        while (isActive) { delay(1000); tokens.set(rateBytesPerSec) }
    } else null
    suspend fun acquire(bytes: Int) {
        if (!enabled) return
        while (true) {
            val cur = tokens.get()
            if (cur >= bytes) { if (tokens.compareAndSet(cur, cur - bytes)) return }
            else delay(5)
        }
    }
}

@ApplicationScoped
class SftpService @Inject constructor(cfg: PipelineConfig = PipelineConfig()) {
    private val jsch = com.jcraft.jsch.JSch()
    private val sessions = Channel<com.jcraft.jsch.ChannelSftp>(capacity = 4)
    private val chunk = cfg.sftpChunk
    private val baseDir = cfg.sftpBaseDir
    private val tmpSuffix = cfg.sftpTmpSuffix
    private val pacer = BytePacer(cfg.sftpMaxBytesPerSec)

    @PostConstruct
    fun init() {
        repeat(4) {
            val s = jsch.getSession(cfg.sftpUser, cfg.sftpHost, cfg.sftpPort).apply {
                setPassword(cfg.sftpPassword)
                setConfig("StrictHostKeyChecking", "no")
                connect(10_000)
            }
            val ch = (s.openChannel("sftp") as com.jcraft.jsch.ChannelSftp).apply { connect(10_000) }
            sessions.trySend(ch)
        }
    }

    @PreDestroy
    fun shutdown() {
        while (!sessions.isEmpty) runCatching { sessions.tryReceive().getOrNull()?.disconnect() }
    }

    private suspend fun <T> withChan(block: suspend (com.jcraft.jsch.ChannelSftp) -> T): T {
        val ch = sessions.receive()
        return try { block(ch) } finally { sessions.trySend(ch) }
    }

    suspend fun uploadAtomic(remotePath: String, fileName: String, src: InputStream) = withContext(Dispatchers.IO) {
        withChan { sftp ->
            sftp.setFilenameEncoding("UTF-8")
            val dir = normalize("$baseDir/$remotePath")
            mkdirs(sftp, dir)

            val tmp = "$dir/$fileName$tmpSuffix"
            val fin = "$dir/$fileName"

            sftp.put(tmp).use { out ->
                val buf = ByteArray(chunk)
                while (true) {
                    val n = src.read(buf)
                    if (n <= 0) break
                    pacer.acquire(n)
                    out.write(buf, 0, n)
                }
                out.flush()
            }
            sftp.rename(tmp, fin)
        }
    }

    private fun mkdirs(sftp: com.jcraft.jsch.ChannelSftp, dir: String) {
        val parts = dir.split("/").filter { it.isNotBlank() }
        var cur = if (dir.startsWith("/")) "/" else ""
        for (p in parts) {
            cur = if (cur.endsWith("/")) "$cur$p" else "$cur/$p"
            try { sftp.cd(cur) } catch (_: Exception) { sftp.mkdir(cur) }
        }
    }
    private fun normalize(p: String) = p.replace("//", "/")
}

/* =========================
 *  S3 (AWS SDK for Kotlin, CRT engine)
 *  NOTE: add deps:
 *    implementation("aws.sdk.kotlin:s3")
 *    implementation("aws.smithy.kotlin:http-client-engine-crt-jvm")
 * ========================= */
@ApplicationScoped
class S3Service(cfg: PipelineConfig = PipelineConfig()) {
    private val engine = aws.smithy.kotlin.runtime.http.engine.crt.CrtHttpEngine {
        maxConnections = 128u
        connectTimeout = 5.seconds
    }
    val client = aws.sdk.kotlin.services.s3.S3Client {
        endpointUrl = aws.smithy.kotlin.runtime.net.Url.parse(cfg.s3Endpoint)
        region = cfg.s3Region
        httpClient = engine
    }
    suspend fun getText(bucket: String, key: String): String? =
        client.getObject(aws.sdk.kotlin.services.s3.model.GetObjectRequest { this.bucket = bucket; this.key = key }) { resp ->
            resp.body?.readAll()?.decodeToString()
        }
    suspend fun getBytes(bucket: String, key: String): ByteArray? =
        client.getObject(aws.sdk.kotlin.services.s3.model.GetObjectRequest { this.bucket = bucket; this.key = key }) { resp ->
            resp.body?.readAll()
        }
}

/* =========================
 *  NATS connection holder
 * ========================= */
@ApplicationScoped
class NatsHolder(cfg: PipelineConfig = PipelineConfig()) {
    val conn: Connection = Nats.connect(cfg.natsUrl)
    @PreDestroy fun close() { runCatching { conn.drain() }; runCatching { conn.close() } }
}

/* =========================
 *  Task model
 * ========================= */
enum class Source { NATS, API, RETRY }
data class Task(
    val id: String,
    val source: Source,
    val natsMsg: Message? = null,        // present if Source.NATS
    val metaKey: String,                 // key in meta bucket
    val destPath: String,                // SFTP dir
    val outName: String,                 // SFTP file name
    val fileKey: String? = null,         // optional – if absent, we’ll read from meta JSON
    val attempts: Int = 0
)

/* =========================
 *  Flow operators — multi-CB gate
 * ========================= */
fun <T, R> Flow<T>.withAdmissionAll(
    gate: MultiBreakerGate,
    names: (T) -> Array<String>,
    initialBackoff: kotlin.time.Duration = 100.milliseconds,
    maxBackoff: kotlin.time.Duration = 2.seconds,
    jitterPct: Double = 0.15,
    onWaiting: (suspend (T) -> Unit)? = null,
    transform: suspend (T) -> R
): Flow<R> = transform { value ->
    val ctx = currentCoroutineContext()
    var backoff = initialBackoff
    var ticket: MultiBreakerGate.Ticket? = null
    while (ctx.isActive && ticket == null) {
        ticket = gate.tryAdmit(*names(value))
        if (ticket == null) {
            onWaiting?.invoke(value)
            val jitter = 1.0 + (if (Random.nextBoolean()) 1 else -1) * Random.nextDouble(0.0, jitterPct)
            val waitMs = (backoff.inWholeMilliseconds * jitter).toLong().coerceAtLeast(25L)
            delay(waitMs)
            backoff = (backoff * 2.0).coerceAtMost(maxBackoff)
        }
    }
    check(ticket != null) { "Cancelled before admission" }
    try {
        val out = transform(value)
        ticket!!.onSuccess()
        emit(out)
    } catch (ce: CancellationException) {
        ticket!!.release()
        throw ce
    } catch (t: Throwable) {
        ticket!!.onError(t)
        throw t
    }
}

/* =========================
 *  Coordinator
 * ========================= */
@ApplicationScoped
class Coordinator @Inject constructor(
    private val cfg: PipelineConfig,
    private val natsHolder: NatsHolder,
    private val s3: S3Service,
    private val sftp: SftpService,
    private val metrics: PipelineMetrics
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    // admission controls
    private val cbMinio = cb("minio")
    private val cbSftp  = cb("sftp")
    private val gate = MultiBreakerGate(mapOf("minio" to cbMinio, "sftp" to cbSftp))

    // rate limits (wait-not-fail)
    private val rlMinio = rl("minio-ops", cfg.minioQps)
    private val rlSftp  = rl("sftp-ops", cfg.sftpQps)

    // concurrency
    private val inFlightGate = Semaphore(cfg.maxInFlight)

    // retry pool
    private val retryCh = Channel<Task>(capacity = Channel.UNLIMITED)

    // API injection
    private val apiCh = Channel<Task>(capacity = Channel.UNLIMITED)

    // NATS control
    private val paused = AtomicBoolean(false)

    @PostConstruct
    fun start() {
        // Pause NATS when any breaker is OPEN
        fun checkPause() {
            val anyOpen = cbMinio.state == CircuitBreaker.State.OPEN || cbSftp.state == CircuitBreaker.State.OPEN
            val was = paused.getAndSet(anyOpen)
            if (anyOpen && !was) println("Coordinator: PAUSE pulls (CB OPEN)")
            if (!anyOpen && was) println("Coordinator: RESUME pulls (CB CLOSED)")
        }
        cbMinio.eventPublisher.onStateTransition { checkPause() }
        cbSftp.eventPublisher.onStateTransition { checkPause() }
        checkPause()

        // Start NATS puller
        scope.launch { natsPullLoop() }

        // Start processing loop (priority: API > RETRY > NATS)
        scope.launch { processLoop() }
    }

    @PreDestroy
    fun stop() {
        scope.cancel()
    }

    /* ----- Public API to enqueue manual retry/requests ----- */
    suspend fun submitApiTask(t: Task) { apiCh.send(t.copy(source = Source.API)) }

    /* ----- Public API to push into retry pool ----- */
    suspend fun enqueueRetry(t: Task) { retryCh.send(t.copy(source = Source.RETRY, attempts = t.attempts + 1)) }

    /* ----- NATS puller → emits Tasks into natsCh ----- */
    private val natsCh = Channel<Task>(capacity = Channel.UNLIMITED)

    private suspend fun natsPullLoop() {
        val conn = natsHolder.conn
        val js = conn.jetStream()
        val jsm = conn.jetStreamManagement()
        // Ensure stream + consumer
        runCatching {
            jsm.addStream(StreamConfiguration.builder()
                .name(cfg.stream)
                .subjects(cfg.subject)
                .storageType(StorageType.Memory)
                .build())
        }
        val cc = ConsumerConfiguration.builder()
            .durable(cfg.durable)
            .ackPolicy(AckPolicy.Explicit)
            .ackWait(cfg.ackWait)
            .deliverPolicy(DeliverPolicy.All)
            .maxDeliver(cfg.maxDeliveries)
            .build()
        val pso = PullSubscribeOptions.builder()
            .stream(cfg.stream)
            .durable(cfg.durable)
            .configuration(cc)
            .build()
        val sub = js.pullSubscribe(cfg.subject, pso)

        while (scope.isActive) {
            if (paused.get()) { delay(200); continue }
            try {
                sub.pull(min(1, cfg.natsFetch), cfg.natsFetchMaxWait) // small pulls avoid over-queuing
                val msgs = sub.fetch(cfg.natsFetch, cfg.natsFetchMaxWait)
                for (m in msgs) {
                    // Parse the message payload → Task; record parse fails
                    val t = parseTaskFromNats(m)
                    if (t == null) {
                        metrics.incParseFail("nats", "json")
                        m.term() // drop bad payloads
                    } else {
                        natsCh.trySend(t)
                    }
                }
            } catch (_: Exception) {
                delay(200)
            }
        }
    }

    private fun parseTaskFromNats(msg: Message): Task? = try {
        val body = msg.data?.toString(StandardCharsets.UTF_8) ?: return null
        // Example JSON: {"id":"...","metaKey":"k","destPath":"p","outName":"f"}
        val id = body.substringAfter("\"id\":\"").substringBefore("\"")
        val metaKey = body.substringAfter("\"metaKey\":\"").substringBefore("\"")
        val dest = body.substringAfter("\"destPath\":\"").substringBefore("\"")
        val name = body.substringAfter("\"outName\":\"").substringBefore("\"")
        if (id.isBlank() || metaKey.isBlank() || dest.isBlank() || name.isBlank()) null
        else Task(id = id, source = Source.NATS, natsMsg = msg, metaKey = metaKey, destPath = dest, outName = name)
    } catch (_: Exception) { null }

    /* ----- Priority scheduler: API > RETRY > NATS ----- */
    private suspend fun receivePriority(): Task {
        // simple polling preference; avoids starving NATS completely
        repeat(3) {
            apiCh.tryReceive().getOrNull()?.let { return it }
            retryCh.tryReceive().getOrNull()?.let { return it }
        }
        // if nothing urgent, block on next available (API first, then RETRY, then NATS)
        select@ while (true) {
            apiCh.tryReceive().getOrNull()?.let { return it }
            retryCh.tryReceive().getOrNull()?.let { return it }
            natsCh.receive().let { return it }
        }
    }

    /* ----- Main processing loop ----- */
    private suspend fun processLoop() {
        val minioName = "minio"
        val sftpName = "sftp"

        while (scope.isActive) {
            val task = receivePriority()
            inFlightGate.withPermit {
                scope.launch {
                    when (task.source) {
                        Source.NATS -> processNatsTask(task, minioName, sftpName)
                        Source.API, Source.RETRY -> processApiOrRetryTask(task, minioName, sftpName)
                    }
                }
            }
        }
    }

    /* ----- Processing: NATS variant (with heartbeat + ack/nak) ----- */
    private suspend fun processNatsTask(task: Task, minioCB: String, sftpCB: String) {
        val msg = requireNotNull(task.natsMsg)
        val hb = HeartbeatingNatsMessage(msg, cfg.ackWait, scope).apply { start() }
        try {
            runEndToEnd(task, minioCB, sftpCB)
            hb.ack()
        } catch (t: Throwable) {
            // Local retry inside processing already attempted; decide NAK delay vs RETRY pool
            hb.nakWithDelay(cfg.nakDelay)
        }
    }

    /* ----- Processing: API/RETRY variant ----- */
    private suspend fun processApiOrRetryTask(task: Task, minioCB: String, sftpCB: String) {
        try {
            runEndToEnd(task, minioCB, sftpCB)
        } catch (t: Throwable) {
            if (task.attempts + 1 < cfg.maxLocalRetries) {
                enqueueRetry(task)
            } else {
                // give up: emit metric, or publish to DLQ
                println("Task ${task.id} abandoned after ${task.attempts} retries: ${t.message}")
            }
        }
    }

    /* ----- Core end-to-end (admission+limits+IO+retries) ----- */
    private suspend fun runEndToEnd(task: Task, minioCB: String, sftpCB: String) {
        // Multi-CB admission upfront (both MinIO & SFTP must be ok)
        flowOf(task).withAdmissionAll(
            gate = gate,
            names = { arrayOf(minioCB, sftpCB) }
        ) { t ->
            // Step 1: download metadata JSON
            rlMinio.awaitPermit()
            val metaJson = s3.getText(cfg.s3MetaBucket, t.metaKey)
            if (metaJson == null || metaJson.isEmpty()) {
                metrics.incDownloadFail("s3", cfg.s3MetaBucket, "metadata", if (metaJson == null) "404" else "empty")
                error("metadata missing/empty")
            }
            // Parse metadata (minimal demo)
            val fileKey = t.fileKey ?: metaJson.substringAfter("\"fileKey\":\"", "")
                .substringBefore("\"")
                .ifBlank {
                    metrics.incParseFail(source = when (t.source) { Source.NATS -> "nats"; Source.API -> "api"; Source.RETRY -> "retry" }, reason = "missing_field")
                    error("fileKey missing")
                }

            // Step 2: object bytes (with local retries)
            val bytes = fetchObjectWithRetries(fileKey)

            // Step 3: SFTP upload (with local retries)
            uploadWithRetries(t.destPath, t.outName, bytes)

            t.id
        }.single()
    }

    private suspend fun fetchObjectWithRetries(key: String): ByteArray {
        var attempt = 0
        var last: Throwable? = null
        while (attempt < cfg.maxLocalRetries) {
            try {
                rlMinio.awaitPermit()
                val b = s3.getBytes(cfg.s3FileBucket, key)
                    ?: run {
                        metrics.incDownloadFail("s3", cfg.s3FileBucket, "object", "404")
                        error("NoSuchKey")
                    }
                return b
            } catch (e: Exception) {
                last = e
                val reason = when (e) {
                    is aws.sdk.kotlin.services.s3.model.NoSuchKey -> "404"
                    is java.net.SocketTimeoutException -> "timeout"
                    else -> "unknown"
                }
                metrics.incDownloadFail("s3", cfg.s3FileBucket, "object", reason)
                delay(200L * (attempt + 1))
            } finally {
                attempt++
            }
        }
        throw last ?: IllegalStateException("download failed")
    }

    private suspend fun uploadWithRetries(destPath: String, outName: String, bytes: ByteArray) {
        var attempt = 0
        while (attempt < cfg.maxLocalRetries) {
            try {
                rlSftp.awaitPermit()
                sftp.uploadAtomic(destPath, outName, bytes.inputStream())
                return
            } catch (e: Exception) {
                if (attempt + 1 >= cfg.maxLocalRetries) throw e
                delay(200L * (attempt + 1))
            } finally {
                attempt++
            }
        }
    }
}

/* =========================
 *  Tiny demo main (optional)
 * ========================= */
// fun main() = runBlocking {
//     val cfg = PipelineConfig()
//     val coordinator = Coordinator(cfg, NatsHolder(cfg), S3Service(cfg), SftpService(cfg), PipelineMetrics(object : MeterRegistry(io.micrometer.core.instrument.Clock.SYSTEM) {
//         override fun newMeter(id: io.micrometer.core.instrument.Meter.Id, type: io.micrometer.core.instrument.Meter.Type, measurements: MutableIterable<io.micrometer.core.instrument.Measurement>): io.micrometer.core.instrument.Meter = TODO()
//         override fun newMeter(id: io.micrometer.core.instrument.Meter.Id, type: io.micrometer.core.instrument.Meter.Type, baseUnit: String?, measurements: MutableIterable<io.micrometer.core.instrument.Measurement>): io.micrometer.core.instrument.Meter = TODO()
//         override fun getBaseTimeUnit(): java.util.concurrent.TimeUnit = TimeUnit.SECONDS
//     }))
// }
