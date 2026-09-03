package sftp.connector.partition

import eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM
import eu.rekawek.toxiproxy.model.ToxicDirection.UPSTREAM
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import sftp.connector.config.SftpConnectorBuilder
import sftp.connector.source.Readiness
import sftp.connector.source.ReadinessCheck
import sftp.connector.source.SftpEvent.FileSeen
import sftp.connector.source.SftpSource
import kotlin.io.path.createDirectory
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.writeBytes
import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes

/**
 * The production failure: the tunnel is up, the far side is gone, and the network says nothing.
 * A loopback stall imitates it from inside one process; here it is done to real bytes on a real
 * socket, and the connector is asked for the same answer.
 *
 * What is asserted is what the caller got and what it cost - the session thrown away, the retry,
 * the second handshake, and the time it took to notice - never the toxic. The network is healed
 * the moment the pool has noticed, which is what a half-open connection is: the old flow is dead
 * and a new one works.
 */
class HalfOpenPartitionTest : PartitionTest() {

    /** P1. Both directions go dark mid-download. */
    @Test
    fun `P1_a half-open connection mid-download is noticed within two keepalives, poisoned, and the download retried on a fresh session`() =
        runBlocking<Unit> {
            remoteRoot.resolve("drop").createDirectory()
            remoteRoot.resolve("drop/big.bin").writeBytes(ByteArray(FILE_BYTES) { it.toByte() })
            // What a started connector would have made before the first poll.
            remoteRoot.resolve("drop/temp").createDirectory()
            val watching: SftpConnectorBuilder.() -> Unit = {
                pool { keepAlive = KEEPALIVE; validationBypass = 1.minutes }
                polling { directories("/drop"); onAck = move("temp/"); readiness = ReadinessCheck { _, _ -> Readiness.Ready } }
            }

            withPartitionedClient(watching) { client, partition ->
                val source = SftpSource(client, config, meters)
                partition.tunnel.holdAfter(HOLD_AFTER_BYTES) {
                    partition.drop(UPSTREAM, DOWNSTREAM)
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

                assertThat(noticedAfter!!).describedAs("time from the partition to the session being written off").isLessThan(KEEPALIVE * 2 + NOTICE_SLACK)
                assertThat(seen).describedAs("the file, handed over once").hasSize(1)
                assertThat(names(stage)).containsExactly("big.bin")
                assertThat(names(remoteRoot.resolve("drop/temp"))).containsExactly("big.bin")
                assertThat(retries("download")).isEqualTo(1.0)
                assertThat(evictedAsPoisoned()).isEqualTo(1.0)
                assertThat(unmappedFailures()).describedAs("the loss was classified, not defaulted").isZero()
                assertThat(sessionsOpened()).describedAs("the first, and the one the retry ran on").isEqualTo(2.0)
            }
        }

    /**
     * The proxy swallows replies under a live request while the request direction stays open.
     * The keepalives keep reaching the server, which is what tells this apart from P1 and is
     * exactly why nothing but the clock can end it; the outcome is the same as P1's.
     */
    @Test
    fun `a proxy that swallows replies under a live request loses the session within two keepalives, and the call is retried`() =
        runBlocking<Unit> {
            remoteRoot.resolve("drop").createDirectory()

            withPartitionedClient({ pool { keepAlive = KEEPALIVE; validationBypass = 1.minutes } }) { client, partition ->
                pool.withLease { it.connection.realpath(".") }
                val heardBefore = partition.globalRequestsHeard.size
                partition.tunnel.onNextClientRequest { partition.drop(DOWNSTREAM) }
                var noticedAfter: Duration? = null
                val healer = launch { noticedAfter = partition.healOnceNoticed() }

                assertThat(client.exists("/drop")).isTrue()
                healer.join()

                assertThat(noticedAfter!!).isLessThan(KEEPALIVE * 2 + NOTICE_SLACK)
                assertThat(partition.globalRequestsHeard.drop(heardBefore)).describedAs("keepalives that reached the server through the stall").isNotEmpty()
                assertThat(retries("exists")).isEqualTo(1.0)
                assertThat(evictedAsPoisoned()).isEqualTo(1.0)
                assertThat(unmappedFailures()).isZero()
                assertThat(sessionsOpened()).isEqualTo(2.0)
            }
        }

    private fun names(directory: Path): List<String> = directory.listDirectoryEntries().map { it.fileName.toString() }

    private companion object {
        private const val FILE_BYTES = 2 * 1024 * 1024
        private const val HOLD_AFTER_BYTES = 64L * 1024

        private val KEEPALIVE = 500.milliseconds

        /**
         * Two intervals is when the keepalive gives up; what follows is the disconnect, the
         * eviction and this test noticing, and the bytes held at the tunnel landing after the
         * partition restarts the first interval. One more interval covers all of that.
         */
        private val NOTICE_SLACK = KEEPALIVE
    }
}
