package sftp.connector.quarkus

import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.Startup
import jakarta.enterprise.event.Observes
import jakarta.enterprise.inject.Produces
import jakarta.inject.Singleton
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import sftp.connector.SftpConnector
import sftp.connector.config.SftpConnectorConfig
import kotlin.time.Duration.Companion.seconds

/**
 * The connector's life inside a Quarkus host: started with the application, injectable while it
 * runs, and stopped when the application stops.
 *
 * A host that has this module on its classpath writes `sftp.connector.*` in its application
 * properties and injects [SftpConnector]. Everything else - reading the properties, putting them
 * to the builder, handing the host's own meter registry to the pool, and closing within the bound
 * the connector promises - happens here, once, rather than in each host that wanted a connector.
 *
 * **One connector per application, by construction.** There is a single `sftp.connector` prefix,
 * so there is a single configuration and a single produced connector, and the question of two
 * connectors sharing a registry does not arise from this module. It matters because the pool's
 * gauges and the client's breaker gauge are identified by the server they talk to and nothing
 * else: a second connector to the same server on the same registry would find those gauge names
 * already taken, keep reading the first connector's numbers, and report it silently. A host that
 * builds a second connector by hand takes that on; the adapter never does.
 */
@Singleton
class SftpConnectorLifecycle(properties: SftpConnectorProperties) {

    /**
     * Built as this bean is, which is at start-up, so a properties file with faults in it stops
     * the deployment there and names every one of them at once.
     */
    private val configuration: SftpConnectorConfig = properties.toConnectorConfig()

    /**
     * Kept because the shutdown observer must not ask for the connector by injection: at that
     * point a start-up that had already failed would be asked to start a second one.
     */
    @Volatile
    private var running: SftpConnector? = null

    /**
     * Started with the application rather than at the first injection, because a connector nobody
     * has injected yet is still a connector that should be polling - and a start-up fault should
     * stop the deployment rather than wait for the first caller to discover it.
     *
     * [Singleton] rather than an application-scoped bean: the connector is a final class and a
     * normal-scoped bean would need a proxy that cannot be made from one.
     */
    @Produces
    @Singleton
    @Startup
    fun connector(meterRegistry: MeterRegistry): SftpConnector =
        runBlocking { SftpConnector.start(configuration, meterRegistry = meterRegistry) }
            .also { running = it }

    /**
     * Closes the connector as the application stops, and waits for it.
     *
     * The wait is the point. Closing is bounded by the drain timeout plus one cancel grace and
     * cannot be cancelled, so the timeout here is not a way of returning sooner - it is the
     * assertion that the close kept its own promise, and the log line below is what a close that
     * overran would leave behind.
     *
     * The work is dispatched rather than run on the thread that delivered the event, because
     * cutting a session loose is a blocking socket close made on whichever thread is doing the
     * closing, and a host whose event loop delivered this event must not be the thread that
     * blocks on it.
     */
    fun stop(@Observes event: ShutdownEvent) {
        val connector = running ?: return
        val promised = configuration.pool.drainTimeout + configuration.pool.cancelGrace
        runBlocking(Dispatchers.IO) {
            if (withTimeoutOrNull(promised + SLACK) { connector.close() } == null) {
                LOG.warn(
                    "Closing the SFTP connector took longer than the {} it is bounded by. The " +
                        "application is stopping anyway; sessions may have been left for the " +
                        "server to time out.",
                    promised,
                )
            }
        }
        running = null
    }

    private companion object {
        private val LOG = LoggerFactory.getLogger(SftpConnectorLifecycle::class.java)

        /**
         * Room for the scheduling either side of the bound, so an ordinary close on a loaded
         * machine does not report itself as an overrun. It is not part of what the connector
         * promises.
         */
        private val SLACK = 5.seconds
    }
}
