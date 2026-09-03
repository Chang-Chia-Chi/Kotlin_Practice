package sftp.connector.pressure

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.SftpConnector
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.source.Readiness
import sftp.connector.source.ReadinessCheck
import sftp.connector.source.SftpEvent.FileSeen
import sftp.connector.source.SftpEvent.PollCompleted
import sftp.connector.testkit.EmbeddedSftpServer
import sftp.connector.testkit.LoopbackConnectProxy
import sftp.connector.transport.jsch.JschTransport
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.createDirectories
import kotlin.io.path.writeText
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * A watch left running behind a tunnel that a seeded schedule keeps breaking, for as many minutes
 * as `-Dsftp.soak.minutes` says, with a producer writing files at a steady rate the whole time.
 * Every minute the threads, the post-GC heap and the `sftp_*` meters are sampled into
 * `target/soak/samples.csv`; every fault's heal-to-next-`PollCompleted` time goes into
 * `target/soak/recoveries.csv`. What is asserted is what a run of hours would show: threads and
 * heap flat by slope, sessions created in proportion to the sessions killed, recovery inside its
 * bound, and every file produced delivered once.
 *
 * Real time throughout, on purpose: this tier reads the meters as an operator would.
 */
@Tag("soak")
class SoakTest {

    @TempDir
    lateinit var remoteRoot: Path

    @TempDir
    lateinit var stage: Path

    @TempDir
    lateinit var delivered: Path

    private val meters = SimpleMeterRegistry()

    @Test
    fun `soak_a watch behind a faulty tunnel keeps threads and heap flat, recovers inside the bound, and delivers every file once`() =
        runBlocking<Unit> {
            val minutes = System.getProperty("sftp.soak.minutes")?.toIntOrNull() ?: 0
            assumeTrue(minutes > 0, "SKIPPED, NOT PASSED: the soak runs only with -Dsftp.soak.minutes=N")
            val out = Path.of("target", "soak").createDirectories()
            remoteRoot.resolve("drop").createDirectories()

            EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
                LoopbackConnectProxy.start().use { tunnel ->
                    val config = configFor(server, tunnel)
                    val connector = SftpConnector.start(config, JschTransport(config, meters), meters)
                    val produced = AtomicInteger()
                    val deliveries = ConcurrentHashMap<String, Int>()
                    var lastCompleted = TimeSource.Monotonic.markNow()

                    val producer = launch(Dispatchers.IO) {
                        while (isActive) {
                            remoteRoot.resolve("drop/f${produced.incrementAndGet()}.csv").writeText("id,amount\n1,42\n")
                            delay(PRODUCE_EVERY)
                        }
                    }
                    val consumer = launch {
                        connector.source.watch("/drop", TICK).collect { event ->
                            when (event) {
                                is FileSeen -> {
                                    if (event.download(delivered.resolve(event.file.name)) != null) deliveries.merge(event.file.name, 1, Int::plus)
                                    runCatching { event.ack() }
                                }
                                is PollCompleted -> lastCompleted = TimeSource.Monotonic.markNow()
                                else -> Unit
                            }
                        }
                    }

                    val recoveries = CopyOnWriteArrayList<String>()
                    var sessionsKilled = 0
                    val faults = launch {
                        val rnd = Random(SEED)
                        while (isActive) {
                            delay((2 + rnd.nextInt(8)).seconds)
                            val fault = FAULTS[rnd.nextInt(FAULTS.size)]
                            val live = server.liveSessions
                            when (fault) {
                                "stall" -> { tunnel.stall(); sessionsKilled += live }
                                "kill" -> { server.killLiveSessions(); sessionsKilled += live }
                                "refuse" -> { tunnel.refuseConnections(); delay(2.seconds); tunnel.acceptConnections() }
                            }
                            val healed = TimeSource.Monotonic.markNow()
                            while (lastCompleted < healed) delay(20)
                            recoveries += "$fault,${healed.elapsedNow().inWholeMilliseconds}"
                        }
                    }

                    val samples = mutableListOf<Sample>()
                    repeat(minutes) { minute ->
                        delay(1.minutes)
                        samples += Sample(
                            minute + 1,
                            Thread.activeCount(),
                            usedHeapAfterGc(),
                            gauge("sftp_pool_active"), gauge("sftp_pool_idle"), gauge("sftp_pool_pending"),
                            counter("sftp_pool_created_total"), counter("sftp_pool_evicted_total"), counter("sftp_retry_total"),
                            gauge("sftp_breaker_state"), gauge("sftp_inflight"),
                        )
                    }
                    faults.cancelAndJoin()
                    producer.cancelAndJoin()
                    // A quiet tail: nothing breaks, so whatever was in flight lands and is acked.
                    delay(SETTLE)
                    consumer.cancelAndJoin()
                    connector.close()

                    out.resolve("samples.csv").writeText("minute,threads,heap_bytes,pool_active,pool_idle,pool_pending,created_total,evicted_total,retry_total,breaker_state,inflight\n" + samples.joinToString("\n") { it.csv() })
                    out.resolve("recoveries.csv").writeText("fault,recovered_in_ms\n" + recoveries.joinToString("\n"))
                    println("soak: ${samples.size} samples, ${recoveries.size} faults, ${produced.get()} produced, ${deliveries.size} names delivered; samples:\n" + samples.joinToString("\n") { it.csv() } + "\nrecoveries:\n" + recoveries.joinToString("\n"))

                    val threadSlope = slope(samples.map { it.threads.toDouble() })
                    val heapSlope = slope(samples.map { it.heap.toDouble() })
                    assertThat(threadSlope).describedAs("threads per minute").isLessThan(1.0)
                    assertThat(heapSlope).describedAs("post-GC heap bytes per minute").isLessThan((2L shl 20).toDouble())
                    assertThat(counter("sftp_pool_created_total")).describedAs("sessions created against sessions killed ($sessionsKilled) plus the pool's size and the tail")
                        .isLessThanOrEqualTo((sessionsKilled + MAX_SIZE + CREATED_SLACK).toDouble())
                    assertThat(recoveries.map { it.substringAfter(',').toLong() }).describedAs("heal to next PollCompleted, ms")
                        .allMatch { it <= RECOVERY_BOUND.inWholeMilliseconds }
                    val names = (1..produced.get()).map { "f$it.csv" }
                    assertThat(deliveries.keys).describedAs("every produced file delivered").containsAll(names)
                    assertThat(deliveries.filterValues { it != 1 }).describedAs("files delivered more than once").isEmpty()
                }
            }
        }

    private class Sample(
        val minute: Int, val threads: Int, val heap: Long,
        val active: Double, val idle: Double, val pending: Double,
        val created: Double, val evicted: Double, val retries: Double, val breaker: Double, val inFlight: Double,
    ) {
        fun csv() = "$minute,$threads,$heap,$active,$idle,$pending,$created,$evicted,$retries,$breaker,$inFlight"
    }

    /** Least-squares slope per sample; zero for fewer than two samples. */
    private fun slope(ys: List<Double>): Double {
        if (ys.size < 2) return 0.0
        val n = ys.size
        val xMean = (n - 1) / 2.0
        val yMean = ys.average()
        val num = ys.withIndex().sumOf { (i, y) -> (i - xMean) * (y - yMean) }
        val den = ys.indices.sumOf { (it - xMean) * (it - xMean) }
        return num / den
    }

    private fun gauge(name: String) = meters.find(name).gauge()?.value() ?: 0.0
    private fun counter(name: String) = meters.find(name).counters().sumOf { it.count() }

    private fun usedHeapAfterGc(): Long {
        System.gc()
        return Runtime.getRuntime().let { it.totalMemory() - it.freeMemory() }
    }

    private fun configFor(server: EmbeddedSftpServer, tunnel: LoopbackConnectProxy): SftpConnectorConfig = sftpConnector("soak") {
        endpoint { host = server.host; port = server.port; proxy { httpConnect(tunnel.host, tunnel.port) } }
        auth { password(USER, PASSWORD) }
        hostKey = HostKeyPolicy.AcceptAll
        pool { maxSize = MAX_SIZE; keepAlive = KEEPALIVE; validationBypass = 1.minutes; cancelGrace = 1.seconds; drainTimeout = 5.seconds }
        resilience {
            retry { backoff = exponential(250.milliseconds, max = MAX_BACKOFF, jitter = false) }
            circuitBreaker { slidingWindow = 5; failureRateThreshold = 50; waitInOpen = WAIT_IN_OPEN }
        }
        polling { staging { dir = stage }; directories("/drop"); onAck = move("temp/"); readiness = ReadinessCheck { _, _ -> Readiness.Ready } }
    }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"
        private const val SEED = 20260903L
        private const val MAX_SIZE = 2
        private const val CREATED_SLACK = 2
        private val FAULTS = listOf("stall", "kill", "refuse")
        private val KEEPALIVE = 500.milliseconds
        private val MAX_BACKOFF = 2.seconds
        private val WAIT_IN_OPEN = 2.seconds
        private val TICK = 1.seconds
        private val PRODUCE_EVERY = 500.milliseconds
        private val SETTLE = 20.seconds

        /**
         * The ticket's `2 x keepAlive + max backoff` is what the pool needs to notice and redial;
         * a watch then needs its next tick, and a breaker that opened needs its wait in open.
         */
        private val RECOVERY_BOUND: Duration = KEEPALIVE * 2 + MAX_BACKOFF + TICK + WAIT_IN_OPEN + 2.seconds
    }
}
