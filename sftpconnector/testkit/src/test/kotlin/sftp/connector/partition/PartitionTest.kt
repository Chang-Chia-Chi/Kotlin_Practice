package sftp.connector.partition

import eu.rekawek.toxiproxy.Proxy
import eu.rekawek.toxiproxy.ToxiproxyClient
import eu.rekawek.toxiproxy.model.ToxicDirection
import eu.rekawek.toxiproxy.model.ToxicList
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.io.TempDir
import org.testcontainers.DockerClientFactory
import org.testcontainers.Testcontainers
import org.testcontainers.toxiproxy.ToxiproxyContainer
import sftp.connector.client.SftpClient
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorBuilder
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.pool.SftpPool
import sftp.connector.testkit.EmbeddedSftpServer
import sftp.connector.testkit.LoopbackConnectProxy
import sftp.connector.transport.jsch.JschTransport
import java.nio.file.Path
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * The partition tier's ground: a real client, through the CONNECT proxy the production network
 * has, through a Toxiproxy container that can do to the bytes what a network does, to a real
 * server on this host. A test extends this and gets [withPartitionedClient]; everything about
 * containers, host ports and warm-up is in here and nowhere else.
 *
 * Docker is expected. Without it every test in the class is skipped, and the skip's message says
 * so, because a skipped partition tier is not a passed one.
 */
abstract class PartitionTest {

    @TempDir
    lateinit var remoteRoot: Path

    @TempDir
    lateinit var stage: Path

    protected val meters = SimpleMeterRegistry()

    protected lateinit var pool: SftpPool

    protected lateinit var config: SftpConnectorConfig

    /** The network between the client and the server, as a test can damage and repair it. */
    inner class Partition internal constructor(
        val server: EmbeddedSftpServer,
        val tunnel: LoopbackConnectProxy,
        /** The Toxiproxy handle, for every fault the matrix names by its toxic. */
        val proxy: Proxy,
        private val heard: List<String>,
    ) {
        /** Every global request the server has heard, in order; a keepalive arriving is proof the request direction is alive. */
        val globalRequestsHeard: List<String> get() = heard.toList()

        @Volatile
        private var partitionedAt: TimeSource.Monotonic.ValueTimeMark? = null

        /**
         * Drops every byte in [directions] and closes nothing: the connection is half-open, and
         * neither side is told. Bytes already past the proxy still arrive. The moment the toxics
         * are in place is what [healOnceNoticed] measures from.
         */
        fun drop(vararg directions: ToxicDirection) = damage {
            directions.forEach { timeout("drop-${it.name.lowercase()}", it, 0) }
        }

        /**
         * Does [what] to the bytes - any toxic the matrix names - and takes the mark that
         * [healOnceNoticed] measures from. A reset, a delay: whatever the row is about.
         */
        fun damage(what: ToxicList.() -> Unit) {
            proxy.toxics().what()
            partitionedAt = TimeSource.Monotonic.markNow()
        }

        /**
         * Waits for the pool to write a session off as poisoned, heals the network, and returns
         * how long the connector took to notice, counted from [drop]. The number is printed too,
         * because it is an observation worth reading off a run on any machine.
         */
        suspend fun healOnceNoticed(): Duration {
            untilEvictedAsPoisoned(1)
            val noticedAfter = checkNotNull(partitionedAt) { "nothing was dropped" }.elapsedNow()
            heal()
            println("partition measured: session written off ${noticedAfter.inWholeMilliseconds} ms after the drop")
            return noticedAfter
        }

        /**
         * The network whole again: every toxic gone. Bytes a toxic was holding are delivered, so
         * a connection that was waiting under it carries on; one the client already gave up on
         * is closed by the client's own disconnect. A proxy taken down with `disable()` is the
         * caller's to bring back, since re-enabling restarts the listener and cuts every
         * connection through it.
         */
        fun heal() {
            proxy.toxics().all.forEach { it.remove() }
        }
    }

    /**
     * A client over a pool of one session through the whole topology. The handshake is warmed at
     * the shipped keepalive first, because a keepalive shortened for a test bounds the key
     * exchange too, and the first key exchange in a JVM is slow.
     *
     * A retry waits [HEAL_WINDOW] before it dials again. That is the time a test has, once the
     * pool has written the session off, to repair the network before the fresh dial goes out -
     * which is what a partition of the old flow alone looks like. A toxic removed from under a
     * handshake in progress ends that connection instead, and the retry that follows is then
     * the toxic's doing rather than the partition's.
     */
    protected suspend fun withPartitionedClient(
        extra: SftpConnectorBuilder.() -> Unit = {},
        block: suspend (SftpClient, Partition) -> Unit,
    ) {
        val heard = CopyOnWriteArrayList<String>()
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD, onGlobalRequest = { heard += it }).use { server ->
            Testcontainers.exposeHostPorts(server.port)
            val proxy = toxiproxy().createProxy("sftp-${proxies.incrementAndGet()}", "0.0.0.0:$LISTEN_PORT", "$HOST_FROM_CONTAINER:${server.port}")
            try {
                LoopbackConnectProxy.start().use { tunnel ->
                    config = configFor(tunnel, extra)
                    JschTransport(configFor(tunnel, extra = {})).connect().close()
                    pool = SftpPool(JschTransport(config, meters), config, meters)
                    try {
                        block(SftpClient(pool, config, meters), Partition(server, tunnel, proxy, heard))
                    } finally {
                        pool.close()
                    }
                }
            } finally {
                proxy.delete()
            }
        }
    }

    /** Waits for the pool to have thrown [count] sessions away as poisoned, which is the moment a lost session has been noticed. */
    protected suspend fun untilEvictedAsPoisoned(count: Int) {
        withTimeout(NOTICE_BOUND) { while (evictedAsPoisoned() < count) delay(POLL) }
    }

    protected fun retries(op: String): Double = meters.find("sftp_retry_total").tag("op", op).counter()?.count() ?: 0.0

    /** Failures the error table did not recognise; zero is what proves a lost session was classified rather than defaulted. */
    protected fun unmappedFailures(): Double = meters.find("sftp_error_unmapped_total").counter()?.count() ?: 0.0

    protected fun sessionsOpened(): Double = meters.find("sftp_pool_created_total").counter()?.count() ?: 0.0

    protected fun evictedAsPoisoned(): Double =
        meters.find("sftp_pool_evicted_total").tag("reason", "poisoned").counter()?.count() ?: 0.0

    private fun configFor(tunnel: LoopbackConnectProxy, extra: SftpConnectorBuilder.() -> Unit): SftpConnectorConfig =
        sftpConnector("partition-demo") {
            endpoint {
                host = container.host
                port = container.getMappedPort(LISTEN_PORT)
                proxy { httpConnect(tunnel.host, tunnel.port) }
            }
            auth { password(USER, PASSWORD) }
            hostKey = HostKeyPolicy.AcceptAll
            pool { maxSize = 1 }
            polling { staging { dir = stage } }
            resilience { retry { backoff = exponential(HEAL_WINDOW, max = HEAL_WINDOW, jitter = false) } }
            extra()
        }

    protected companion object {
        const val USER = "etl"
        const val PASSWORD = "s3cret"

        private const val LISTEN_PORT = 8666
        private const val HOST_FROM_CONTAINER = "host.testcontainers.internal"
        private val POLL = 10.milliseconds
        private val NOTICE_BOUND = 30.seconds
        private val HEAL_WINDOW = 250.milliseconds

        private val proxies = AtomicInteger()

        /**
         * One container for the JVM, started on first use and reaped by Testcontainers at exit.
         * First use is after a host port has been exposed, which is what lets the container reach
         * a server on this host at all.
         */
        private val container: ToxiproxyContainer by lazy { ToxiproxyContainer(IMAGE).also { it.start() } }
        private const val IMAGE = "ghcr.io/shopify/toxiproxy:2.9.0"

        private fun toxiproxy() = ToxiproxyClient(container.host, container.controlPort)

        @JvmStatic
        @BeforeAll
        fun requireDocker() {
            assumeTrue(
                DockerClientFactory.instance().isDockerAvailable,
                "SKIPPED, NOT PASSED: Docker is not available here, so the Toxiproxy partition tier did not run. " +
                    "It is a gate wherever Docker exists; run it on a machine that has it.",
            )
        }
    }
}
