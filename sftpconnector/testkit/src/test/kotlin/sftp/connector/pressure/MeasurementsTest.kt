package sftp.connector.pressure

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.client.SftpClient
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.pool.SftpPool
import sftp.connector.testkit.EmbeddedSftpServer
import sftp.connector.transport.jsch.JschTransport
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.createDirectory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * Degradation, measured and printed for the progress log, never asserted: what a caller pays at
 * the pool's door as concurrency passes its size, and what three listings of a very large
 * directory at once cost in memory. Opt-in, because the numbers are observations and the
 * directory takes a while to make.
 */
@Tag("measure")
class MeasurementsTest {

    @TempDir
    lateinit var remoteRoot: Path

    @TempDir
    lateinit var stage: Path

    @Test
    fun `measure_acquire p50 and p99 for concurrency one to maxSize plus two`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectory()
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
            val config = configFor(server)
            val pool = SftpPool(JschTransport(config), config)
            // Filled first, so what is measured is the wait at the door and not the handshakes.
            List(MAX_SIZE) { pool.acquire() }.forEach { it.release() }
            val rows = (1..MAX_SIZE + 2).map { concurrency ->
                val waits = List(concurrency) {
                    async {
                        List(ROUNDS) {
                            val began = TimeSource.Monotonic.markNow()
                            val lease = pool.acquire()
                            val waited = began.elapsedNow()
                            lease.connection.realpath(".")
                            lease.release()
                            waited
                        }
                    }
                }.awaitAll().flatten().sorted()
                "| $concurrency | ${waits.percentile(50).inWholeMicroseconds} us | ${waits.percentile(99).inWholeMicroseconds} us |"
            }
            pool.close()
            println("measured: acquire wait, maxSize $MAX_SIZE, $ROUNDS rounds per caller\n| concurrency | p50 | p99 |\n|---|---|---|\n${rows.joinToString("\n")}")
        }
    }

    @Test
    fun `measure_heap under three concurrent listings of a hundred thousand entries`() = runBlocking<Unit> {
        val drop = remoteRoot.resolve("drop").createDirectory()
        repeat(ENTRIES) { Files.createFile(drop.resolve("file-$it.csv")) }
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
            val config = configFor(server)
            val client = SftpClient(SftpPool(JschTransport(config), config), config)
            val before = usedHeapAfterGc()
            var peak = 0L
            val sampler = launch { while (true) { peak = maxOf(peak, usedHeap()); delay(100) } }
            val began = TimeSource.Monotonic.markNow()
            val counts = List(3) { async { client.list("/drop").count() } }.awaitAll()
            val took = began.elapsedNow()
            sampler.cancelAndJoin()
            val after = usedHeapAfterGc()
            println(
                "measured: three concurrent listings of $ENTRIES entries counted $counts in $took; " +
                    "post-GC heap before ${before shr 20} MB, peak used during ${peak shr 20} MB, post-GC after ${after shr 20} MB",
            )
        }
    }

    private fun List<Duration>.percentile(p: Int): Duration = this[((size - 1) * p / 100.0).toInt()]

    private fun usedHeap(): Long = Runtime.getRuntime().let { it.totalMemory() - it.freeMemory() }

    private fun usedHeapAfterGc(): Long {
        System.gc()
        return usedHeap()
    }

    private fun configFor(server: EmbeddedSftpServer): SftpConnectorConfig = sftpConnector("measure") {
        endpoint { host = server.host; port = server.port }
        auth { password(USER, PASSWORD) }
        hostKey = HostKeyPolicy.AcceptAll
        pool { maxSize = MAX_SIZE; acquireTimeout = 60.seconds }
        resilience { operationTimeout = 2.minutes; transferTimeout = 2.minutes }
        polling { staging { dir = stage } }
    }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"
        private const val MAX_SIZE = 3
        private const val ROUNDS = 30
        private const val ENTRIES = 100_000
    }
}
