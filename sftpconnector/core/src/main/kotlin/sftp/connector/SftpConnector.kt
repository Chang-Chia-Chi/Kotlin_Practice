package sftp.connector

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import sftp.connector.client.SftpClient
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.pool.SftpPool
import sftp.connector.source.SftpSource
import sftp.connector.transport.SftpTransport
import sftp.connector.transport.jsch.JschTransport
import java.time.Clock
import kotlin.coroutines.CoroutineContext

/**
 * One connector to one server, running.
 *
 * Everything before this was behaviour: a pool that lends sessions when asked, a client that
 * performs operations when called. This is the first thing with a life of its own - it is started,
 * it keeps running when nobody is asking it for anything, and it will one day be stopped. That is
 * why it exists rather than being an assembly a caller writes by hand: the pool looks after itself
 * only while something runs [SftpPool.housekeep], and a pool that started that coroutine in its own
 * constructor would be a pool nothing could stop.
 *
 * Starting one is configuration in, a running thing out, and it either returns something usable or
 * it refuses. The refusing is most of the point: see [StartupProbe].
 */
class SftpConnector private constructor(

    /** The file operations, over the pool below. */
    val client: SftpClient,

    /** The sessions, for a caller that wants to see how full the pool is. */
    val pool: SftpPool,

    /** The watched directories as flows of events, with per-file ack and nack. */
    val source: SftpSource,

    private val scope: CoroutineScope,
) {

    /**
     * Everything the connector runs on its own behalf, under one supervisor job so that one such
     * task failing does not take the others with it. Today that is the housekeeper; the watchers
     * join it when there are any.
     *
     * Cancelling it stops all of that, and it is where a graceful shutdown ends - but it is not
     * that shutdown. Nothing is drained, no caller is waited for and no session is hung up on by
     * cancelling this; the sessions the pool holds are closed by the phased close that does not
     * exist yet.
     */
    val backgroundWork: Job get() = scope.coroutineContext.job

    companion object {

        /**
         * Checks that this configuration describes something this server can do, then starts the
         * connector's own background work and hands back the running connector.
         *
         * A configuration that cannot start one never reaches here: [SftpConnectorConfig] has one
         * producer, the DSL, which refuses everything it can decide on its own before any session
         * is opened. What is left is what only the server can settle, and that is what the probe
         * asks it - so a connector that returns from this call has had both halves checked.
         *
         * The pool is left to fill itself to its minimum in the background afterwards. Readiness
         * does not wait for that: a connector with an empty pool works, it just pays for a
         * handshake on the first call, and holding up a deployment for warm spares would trade
         * something that matters for something that does not.
         *
         * @param background where the connector's own coroutines run. Injected for the same reason
         *   the clock is: a test that would otherwise wait half a minute for the first
         *   housekeeping round can hold the scheduler instead.
         * @throws sftp.connector.error.ConfigurationError when a check against the server fails,
         *   naming the check, the path and the remedy.
         */
        suspend fun start(
            config: SftpConnectorConfig,
            transport: SftpTransport = JschTransport(config),
            /** Whatever the host supplies; a private one when the connector is used on its own. */
            meterRegistry: MeterRegistry = SimpleMeterRegistry(),
            clock: Clock = Clock.systemUTC(),
            background: CoroutineContext = Dispatchers.Default,
        ): SftpConnector {
            val pool = SftpPool(transport, config, meterRegistry, clock)
            val client = SftpClient(pool, config, meterRegistry, clock)
            val source = SftpSource(client, config, meterRegistry, clock)

            StartupProbe(client, config).run()

            val scope = CoroutineScope(background + SupervisorJob() + CoroutineName("sftp-${config.name}"))
            scope.launch { pool.housekeep() }
            LOG.info(
                "Connector \"{}\" is up against {} and looking after {} watched {}.",
                config.name,
                config.endpoint.address,
                config.polling.directories.size,
                if (config.polling.directories.size == 1) "directory" else "directories",
            )
            return SftpConnector(client, pool, source, scope)
        }

        private val LOG = LoggerFactory.getLogger(SftpConnector::class.java)
    }
}
