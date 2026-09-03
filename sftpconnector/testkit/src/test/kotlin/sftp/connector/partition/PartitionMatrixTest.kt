package sftp.connector.partition

import eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM
import eu.rekawek.toxiproxy.model.ToxicDirection.UPSTREAM
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import sftp.connector.SftpConnector
import sftp.connector.client.Overwrite
import sftp.connector.config.SftpConnectorBuilder
import sftp.connector.error.ConnectFailed
import sftp.connector.error.PoolExhausted
import sftp.connector.source.Readiness
import sftp.connector.source.ReadinessCheck
import sftp.connector.source.SftpEvent
import sftp.connector.source.SftpEvent.FileSeen
import sftp.connector.source.SftpEvent.PollCompleted
import sftp.connector.source.SftpEvent.PollFailed
import sftp.connector.source.SftpEvent.PollSkipped
import sftp.connector.source.SftpEvent.PollStarted
import sftp.connector.source.SftpSource
import sftp.connector.source.SkipCause
import sftp.connector.transport.jsch.JschTransport
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.io.path.createDirectory
import kotlin.io.path.exists
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.writeBytes
import kotlin.io.path.writeText
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * The rest of the partition matrix, P2 to P6, on the base P1 was built on. Each row asserts what
 * the connector did and what it cost - the disposition, the counter that moved, the time it took
 * to recover - never the toxic. The network is healed once the connector has noticed.
 */
class PartitionMatrixTest : PartitionTest() {

    /** P2. The peer resets the connection mid-download: as P1, and noticed at once rather than at the keepalive's bound. */
    @Test
    fun `P2_a connection reset mid-download is noticed at once, poisoned, and the download retried on a fresh session`() =
        runBlocking<Unit> {
            val watching = stageDrop(FILE_BYTES)
            withPartitionedClient(watching) { client, partition ->
                val source = SftpSource(client, config, meters)
                partition.tunnel.holdAfter(HOLD_AFTER_BYTES) {
                    partition.damage { resetPeer("reset", UPSTREAM, 0) }
                    partition.tunnel.resume()
                }

                val seen = mutableListOf<FileSeen>()
                var noticedAfter: Duration? = null
                source.poll("/drop").collect { event ->
                    if (event !is FileSeen) return@collect
                    seen += event
                    val healer = launch { noticedAfter = partition.healOnceNoticed() }
                    assertThat(event.download(stage.resolve("big.bin"))?.size).isEqualTo(FILE_BYTES.toLong())
                    healer.join()
                    event.ack()
                }

                assertThat(noticedAfter!!).describedAs("a reset is noticed well inside the keepalive bound").isLessThan(KEEPALIVE * 2)
                assertThat(seen).hasSize(1)
                assertThat(remoteRoot.resolve("drop/temp/big.bin").exists()).isTrue()
                assertThat(retries("download")).isEqualTo(1.0)
                assertThat(evictedAsPoisoned()).isEqualTo(1.0)
                assertThat(unmappedFailures()).describedAs("the reset was classified, not defaulted").isZero()
                assertThat(sessionsOpened()).isEqualTo(2.0)
            }
        }

    /**
     * P3. The rename reaches the server, its reply is lost, and the connection is reset. The
     * reply is held in the proxy by a latency toxic armed before the request goes out, so that
     * the reset added behind it the moment the request has passed the tunnel is what the reply
     * runs into; the server heard the rename either way, so the retry's discriminator is what is
     * on trial. The retry finds the file at the target with the size it was told, and reports
     * success once (I11).
     */
    @Test
    fun `P3_a rename whose reply is lost to a reset is retried, finds its landed file, and reports success once`() =
        runBlocking<Unit> {
            remoteRoot.resolve("drop").createDirectory()
            remoteRoot.resolve("drop/temp").createDirectory()
            remoteRoot.resolve("drop/a.csv").writeText(CONTENT)

            withPartitionedClient({ pool { validationBypass = 1.minutes } }) { client, partition ->
                pool.withLease { it.connection.realpath(".") }
                partition.damage { latency("hold-the-reply", DOWNSTREAM, REPLY_HOLD.inWholeMilliseconds) }
                partition.tunnel.onNextClientRequest { partition.damage { resetPeer("reset", DOWNSTREAM, 0) } }
                var noticedAfter: Duration? = null
                val healer = launch { noticedAfter = partition.healOnceNoticed() }

                client.rename("/drop/a.csv", "/drop/temp/a.csv", Overwrite.REPLACE, expectedSize = CONTENT.length.toLong())
                healer.join()

                assertThat(noticedAfter!!).describedAs("the held reply ran into the reset, and the session was written off at once").isLessThan(REPLY_HOLD + HEAL_SLACK)
                assertThat(remoteRoot.resolve("drop/temp/a.csv").exists()).isTrue()
                assertThat(remoteRoot.resolve("drop/a.csv").exists()).isFalse()
                assertThat(retries("rename")).describedAs("one retry, which found the landed file").isEqualTo(1.0)
                assertThat(meters.get("sftp_op_seconds").tags("op", "rename", "result", "ok").timer().count()).describedAs("success reported once").isEqualTo(1L)
                assertThat(evictedAsPoisoned()).isEqualTo(1.0)
                assertThat(unmappedFailures()).isZero()
                assertThat(sessionsOpened()).isEqualTo(2.0)
            }
        }

    /** P4. The proxy is down as a poll starts on an empty pool: the tick fails, the breaker counts and opens, and the first tick let through after the proxy is back completes. */
    @Test
    fun `P4_a proxy that is down fails the tick with ConnectFailed, the breaker opens, and the first tick after it is back completes`() =
        runBlocking<Unit> {
            val watching = stageDrop(fileBytes = null)
            val breaking: SftpConnectorBuilder.() -> Unit = {
                watching()
                resilience { circuitBreaker { slidingWindow = 3; failureRateThreshold = 50; waitInOpen = WAIT_IN_OPEN } }
            }
            withPartitionedClient(breaking) { client, partition ->
                partition.proxy.disable()
                val source = SftpSource(client, config, meters)
                val events = CopyOnWriteArrayList<SftpEvent>()
                val collector = launch { source.watch("/drop", EVERY).collect { events += it } }

                untilEvent(events) { it is PollSkipped && it.cause == SkipCause.BREAKER_OPEN }
                assertThat(events.filterIsInstance<PollFailed>().map { it.error::class }).describedAs("the disposition").containsOnly(ConnectFailed::class)
                assertThat(meters.get("sftp_breaker_state").gauge().value()).describedAs("the breaker counted").isEqualTo(2.0)
                assertThat(sessionsOpened()).describedAs("no session was opened while the proxy was down").isZero()

                val eventsBefore = events.size
                val enabledAt = TimeSource.Monotonic.markNow()
                partition.proxy.enable()
                untilEvent(events) { it is PollCompleted }
                val recovered = enabledAt.elapsedNow()
                collector.cancelAndJoin()

                val after = events.drop(eventsBefore).filter { it is PollCompleted || it is PollFailed }
                assertThat(after.first()).describedAs("the first tick let through after the proxy came back").isInstanceOf(PollCompleted::class.java)
                assertThat(recovered).describedAs("from enable() to the first PollCompleted").isLessThan(WAIT_IN_OPEN + EVERY + KEEPALIVE * 2 + HEAL_SLACK)
                assertThat(unmappedFailures()).isZero()
                println("partition measured: P4 recovered ${recovered.inWholeMilliseconds} ms after enable(), ${events.count { it is PollSkipped }} ticks skipped while open")
            }
        }

    /** P5. The proxy flaps every three seconds for a minute under a watch: the breaker cycles, the pool never overfills, every tick ends as one of the three events, and the flow never ends. */
    @Test
    fun `P5_a proxy flapping for a minute cycles the breaker, keeps the pool inside maxSize, and never ends the watch`() =
        runBlocking<Unit> {
            val watching = stageDrop(fileBytes = null)
            val flapping: SftpConnectorBuilder.() -> Unit = {
                watching()
                pool { maxSize = 2 }
                resilience { circuitBreaker { slidingWindow = 3; failureRateThreshold = 50; waitInOpen = WAIT_IN_OPEN } }
            }
            withPartitionedClient(flapping) { client, partition ->
                val source = SftpSource(client, config, meters)
                val events = CopyOnWriteArrayList<SftpEvent>()
                val collector = launch { source.watch("/drop", 1.seconds).collect { events += it } }
                var mostSessions = 0
                val breakerStates = CopyOnWriteArrayList<Int>()
                val sampler = launch {
                    while (true) {
                        mostSessions = maxOf(mostSessions, pool.stats().total)
                        breakerStates += meters.get("sftp_breaker_state").gauge().value().toInt()
                        delay(50)
                    }
                }

                var lastEnabledAt = TimeSource.Monotonic.markNow()
                repeat(FLAPS) {
                    partition.proxy.disable()
                    delay(FLAP_EVERY)
                    lastEnabledAt = TimeSource.Monotonic.markNow()
                    partition.proxy.enable()
                    delay(FLAP_EVERY)
                }
                val quietFrom = events.size
                untilEvent(events) { it is PollCompleted && events.indexOf(it) >= quietFrom }
                val recovered = lastEnabledAt.elapsedNow()
                sampler.cancelAndJoin()
                assertThat(collector.isActive).describedAs("the watch never ended").isTrue()
                collector.cancelAndJoin()

                val started = events.count { it is PollStarted }
                val completed = events.count { it is PollCompleted }
                val failed = events.count { it is PollFailed }
                val skippedByBreaker = events.count { it is PollSkipped && it.cause == SkipCause.BREAKER_OPEN }
                assertThat(completed + failed + skippedByBreaker).describedAs("every tick ended as completed, failed or skipped").isEqualTo(started)
                assertThat(mostSessions).describedAs("sessions never exceeded maxSize").isLessThanOrEqualTo(2)
                assertThat(breakerStates).describedAs("the breaker opened").contains(2)
                assertThat(breakerStates.lastIndexOf(0)).describedAs("and closed again after opening").isGreaterThan(breakerStates.indexOf(2))
                assertThat(unmappedFailures()).describedAs("every failure of the flapping was classified").isZero()
                assertThat(recovered).describedAs("from the last enable() to a completed poll").isLessThan(WAIT_IN_OPEN + 1.seconds + KEEPALIVE * 2 + HEAL_SLACK)
                println(
                    "partition measured: P5 recovered ${recovered.inWholeMilliseconds} ms after the last enable(); $started ticks - $completed completed, $failed failed " +
                        "(${events.filterIsInstance<PollFailed>().map { it.error::class.simpleName }.distinct()}), $skippedByBreaker skipped by the breaker; " +
                        "breaker half-open seen: ${1 in breakerStates}; sessions opened ${sessionsOpened()}, evicted as poisoned ${evictedAsPoisoned()}",
                )
            }
        }

    /** P6. Both directions go dark under a download while the connector closes: I9 holds on a real partition, the partial file goes, and the session is counted as shutdown. */
    @Test
    fun `P6_closing under a partition with a download in flight returns within the bound, leaves no partial file, and counts the cut as shutdown`() =
        runBlocking<Unit> {
            val watching = stageDrop(FILE_BYTES)
            val closing: SftpConnectorBuilder.() -> Unit = { watching(); pool { drainTimeout = DRAIN; cancelGrace = GRACE } }
            withPartitionedClient(closing) { _, partition ->
                val connector = SftpConnector.start(config, JschTransport(config, meters), meters)
                val listed = connector.client.list("/drop").single()
                partition.tunnel.holdAfter(HOLD_AFTER_BYTES) { partition.drop(UPSTREAM, DOWNSTREAM) }
                val download = async { runCatching { connector.client.download(listed, stage.resolve("big.bin")) } }
                withTimeout(10.seconds) { while (stage.listDirectoryEntries().isEmpty()) delay(20) }

                val began = TimeSource.Monotonic.markNow()
                connector.close()
                val took = began.elapsedNow()

                assertThat(took).describedAs("I9 under a real partition").isBetween(DRAIN, DRAIN + GRACE + CLOSE_SLACK)
                val ended = withTimeout(10.seconds) { download.await() }.exceptionOrNull()
                assertThat(ended).describedAs("the cut download").isInstanceOfSatisfying(PoolExhausted::class.java) { assertThat(it.closing).isTrue() }
                assertThat(stage.listDirectoryEntries()).describedAs("partial files left behind").isEmpty()
                assertThat(connector.pool.stats().total).isZero()
                assertThat(meters.get("sftp_pool_evicted_total").tag("reason", "shutdown").counter().count()).isEqualTo(1.0)
                partition.heal()
                println("partition measured: P6 closed in ${took.inWholeMilliseconds} ms under drainTimeout $DRAIN + cancelGrace $GRACE")
            }
        }

    /** Recorded, not asserted: one round trip under each toxic, its class and its latency. */
    @Test
    @Tag("measure")
    fun `measure_op latency by failure class under each toxic`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        withPartitionedClient({ pool { keepAlive = KEEPALIVE; validationBypass = 1.minutes }; resilience { retry { maxAttempts = 1 } } }) { client, partition ->
            val rows = mutableListOf<String>()
            fun opSecondsMillis() = meters.find("sftp_op_seconds").tag("op", "exists").timers().sumOf { it.totalTime(TimeUnit.MILLISECONDS) }
            suspend fun measure(toxic: String, arm: () -> Unit) {
                pool.withLease { it.connection.realpath(".") }
                arm()
                val timedBefore = opSecondsMillis()
                val outcome = runCatching { client.exists("/drop") }
                rows += "| $toxic | ${outcome.fold({ "ok" }, { it::class.simpleName })} | ${(opSecondsMillis() - timedBefore).toLong()} ms |"
                partition.heal()
            }
            measure("none") {}
            measure("latency 200 ms downstream") { partition.damage { latency("lag", DOWNSTREAM, 200) } }
            measure("reset_peer upstream") { partition.damage { resetPeer("reset", UPSTREAM, 0) } }
            measure("timeout 0 both directions") { partition.drop(UPSTREAM, DOWNSTREAM) }
            println("measured: sftp_op_seconds of one stat under each toxic, keepAlive $KEEPALIVE, one attempt\n| toxic | outcome | op_seconds |\n|---|---|---|\n${rows.joinToString("\n")}")
        }
    }

    private fun stageDrop(fileBytes: Int?): SftpConnectorBuilder.() -> Unit {
        remoteRoot.resolve("drop").createDirectory()
        remoteRoot.resolve("drop/temp").createDirectory()
        if (fileBytes != null) remoteRoot.resolve("drop/big.bin").writeBytes(ByteArray(fileBytes) { it.toByte() })
        return {
            pool { keepAlive = KEEPALIVE; validationBypass = 1.minutes }
            polling { directories("/drop"); onAck = move("temp/"); readiness = ReadinessCheck { _, _ -> Readiness.Ready } }
        }
    }

    private suspend fun untilEvent(events: List<SftpEvent>, matches: (SftpEvent) -> Boolean) {
        withTimeout(EVENT_BOUND) { while (events.none(matches)) delay(20) }
    }

    private companion object {
        private const val FILE_BYTES = 2 * 1024 * 1024
        private const val HOLD_AFTER_BYTES = 64L * 1024
        private const val CONTENT = "id,amount\n1,42\n"
        private const val FLAPS = 10

        private val KEEPALIVE = 500.milliseconds
        private val WAIT_IN_OPEN = 1.seconds
        private val REPLY_HOLD = 500.milliseconds
        private val EVERY = 300.milliseconds
        private val FLAP_EVERY = 3.seconds
        private val EVENT_BOUND = 30.seconds
        private val HEAL_SLACK = 1.seconds
        private val DRAIN = 1.seconds
        private val GRACE = 300.milliseconds
        private val CLOSE_SLACK = 2.seconds
    }
}
