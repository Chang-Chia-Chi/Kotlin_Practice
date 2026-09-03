package sftp.connector.quarkus

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import sftp.connector.SftpConnector
import sftp.connector.error.PoolExhausted
import sftp.connector.source.SftpEvent
import kotlin.io.path.exists
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.writeText

/**
 * The adapter doing the whole of its job in a running Quarkus application: properties in, a
 * started connector injected, its meters in the host's registry, and a close on the shutdown
 * event that leaves nothing behind.
 *
 * It is one test rather than several because it is one claim - that a host gets a working
 * connector out of its properties file - and because the last step stops the connector, which
 * every other test in the class would then be running against.
 */
@QuarkusTest
@QuarkusTestResource(EmbeddedSftpServerResource::class)
class SftpConnectorAdapterTest {

    @Inject
    lateinit var connector: SftpConnector

    @Inject
    lateinit var lifecycle: SftpConnectorLifecycle

    /** The host's own, which is what the pool was handed. */
    @Inject
    lateinit var meters: MeterRegistry

    /** The same registry seen as what a scrape reads, which is where the checkbox lives. */
    @Inject
    lateinit var scraped: PrometheusMeterRegistry

    @Test
    fun `a host gets a connector from its properties, polls with it, and closes it on shutdown`() =
        runBlocking<Unit> {
            val drop = EmbeddedSftpServerResource.watchedDirectory()
            drop.resolve("vendor.csv").writeText("id,amount\n1,42\n")
            // The uploader saying it has finished. Configured readiness with no clock in it, so
            // the file is handed over on the first poll or the test has found a real fault.
            drop.resolve("vendor.csv.ready").writeText("")

            val seen = mutableListOf<SftpEvent.FileSeen>()
            connector.source.poll(WATCHED).collect { event ->
                if (event is SftpEvent.FileSeen) {
                    seen += event
                    event.ack()
                }
            }

            // The marker is skipped rather than handed over, so one file and not two.
            assertThat(seen.map { it.file.path }).containsExactly("/drop/vendor.csv")
            // The ack ran the configured move, into a folder the start-up probe made.
            assertThat(drop.resolve("done/vendor.csv").exists()).isTrue()
            assertThat(drop.resolve("vendor.csv").exists()).isFalse()

            // The pool's gauges are on the host's registry, under the endpoint they describe,
            // and they are in what a scrape returns rather than merely registered somewhere.
            val endpoint = "${EmbeddedSftpServerResource.server.host}:${EmbeddedSftpServerResource.server.port}"
            POOL_GAUGES.forEach { name ->
                assertThat(meters.find(name).tag("endpoint", endpoint).gauge())
                    .describedAs("%s on the host registry", name)
                    .isNotNull
            }
            // One session, borrowed for the poll and handed back.
            assertThat(meters.find("sftp_pool_idle").tag("endpoint", endpoint).gauge()!!.value()).isEqualTo(1.0)
            assertThat(scraped.scrape()).contains(*POOL_GAUGES)

            // What the host's shutdown does, on the thread the host would deliver it on.
            lifecycle.stop(ShutdownEvent())

            val left = connector.pool.stats()
            assertThat(left.idle + left.inUse + left.connecting).isZero()
            assertThat(meters.find("sftp_pool_evicted_total").tag("reason", "shutdown").counter()!!.count())
                .isGreaterThanOrEqualTo(1.0)
            // Nothing half-downloaded, and nothing staged: the one file was never fetched.
            assertThat(EmbeddedSftpServerResource.stagingDir.listDirectoryEntries()).isEmpty()
            // The pool has stopped lending, which is what a closed connector looks like to a caller.
            assertThatThrownBy { runBlocking { connector.client.exists(WATCHED) } }
                .isInstanceOf(PoolExhausted::class.java)

            // The application's own shutdown event follows this test and finds nothing to do.
            lifecycle.stop(ShutdownEvent())
        }

    private companion object {
        private const val WATCHED = "/drop"
        private val POOL_GAUGES = arrayOf("sftp_pool_active", "sftp_pool_idle", "sftp_pool_pending")
    }
}
