package sftp.connector.source

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.SftpConnector
import sftp.connector.client.SftpClient
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorBuilder
import sftp.connector.config.sftpConnector
import sftp.connector.error.AuthenticationFailed
import sftp.connector.error.SessionLost
import sftp.connector.pool.SftpPool
import sftp.connector.source.SftpEvent.FileSeen
import sftp.connector.source.SftpEvent.PollCompleted
import sftp.connector.source.SftpEvent.PollFailed
import sftp.connector.testkit.EmbeddedSftpServer
import sftp.connector.transport.jsch.JschTransport
import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.writeText
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes

/**
 * A watch against a real server: the two endings a real connection can give it, one of which
 * must not end it. The interval is short and real, since a server is not on virtual time; what is
 * asserted is the sequence of events, never a timing.
 */
class WatchAgainstServerTest {

    @TempDir
    lateinit var remoteRoot: Path

    @TempDir
    lateinit var stage: Path

    private val registry = SimpleMeterRegistry()

    /**
     * The server drops every session between two ticks, as a restart does. The pool still holds
     * the dead one and, with validation skipped, hands it to the next tick; that tick fails with
     * the lost session, is reported, and the tick after it lists on a fresh session and hands
     * over the file that arrived in the meantime. Which tick numbers those are depends on how
     * long a real ack takes against a real interval, so the order of events is what is asserted.
     */
    @Test
    fun `the watch survives a server restart between ticks`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectories()
        remoteRoot.resolve("drop/first.csv").writeText(CONTENT)

        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
            val config = configFor(server, PASSWORD) {
                pool { validationBypass = 1.minutes }
                resilience { retry { maxAttempts = 1 } }
            }
            val connector = SftpConnector.start(config, meterRegistry = registry)
            val events = mutableListOf<SftpEvent>()
            var restarted = false
            var secondSeen = false
            try {
                connector.source.watch("/drop", EVERY)
                    .takeWhile { !(secondSeen && it is PollCompleted) }
                    .collect { event ->
                        events += event
                        if (event is FileSeen) {
                            event.ack()
                            secondSeen = event.file.name == "second.csv"
                        }
                        if (event is PollCompleted && !restarted) {
                            restarted = true
                            server.killLiveSessions()
                            remoteRoot.resolve("drop/second.csv").writeText(CONTENT)
                        }
                    }
            } finally {
                connector.backgroundWork.cancelAndJoin()
            }

            assertThat(events.filterIsInstance<PollFailed>().map { it.error::class }).containsExactly(SessionLost::class)
            assertThat(events.filterIsInstance<FileSeen>().map { it.file.name }).containsExactly("first.csv", "second.csv")
            assertThat(events.indexOfFirst { it is PollFailed })
                .describedAs("the lost session comes after the first file and before the second")
                .isBetween(events.indexOfFirst { it is FileSeen }, events.indexOfLast { it is FileSeen })
            assertThat(remoteRoot.resolve("drop/temp/second.csv").exists()).isTrue()
        }
    }

    /** A rejected password is not a flaky network: the watch ends with the rejection, and nothing asks again. */
    @Test
    fun `a wrong password ends the watch with the rejection, and no tick asks again`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectories()

        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
            val config = configFor(server, "not the password") { resilience { circuitBreaker { slidingWindow = 1 } } }
            runCatching { JschTransport(config).connect() }
            val perConnect = server.authAttempts
            val pool = SftpPool(JschTransport(config, registry), config, registry)
            val source = SftpSource(SftpClient(pool, config, registry), config, registry)
            val events = mutableListOf<SftpEvent>()

            val ended = runCatching { source.watch("/drop", EVERY).collect { events += it } }.exceptionOrNull()

            assertThat(ended).isInstanceOf(AuthenticationFailed::class.java)
            assertThat(events.filterIsInstance<PollFailed>()).describedAs("a rejection reported as something the next tick could survive").isEmpty()
            assertThat(server.authAttempts).describedAs("one connect's worth of passwords, and no more").isEqualTo(perConnect * 2)
            assertThat(registry.get("sftp_breaker_state").gauge().value()).isZero()
        }
    }

    private fun configFor(server: EmbeddedSftpServer, password: String, extra: SftpConnectorBuilder.() -> Unit) =
        sftpConnector("watch-demo") {
            endpoint { host = server.host; port = server.port }
            auth { password(USER, password) }
            hostKey = HostKeyPolicy.AcceptAll
            pool { maxSize = 1 }
            polling {
                staging { dir = stage }
                directories("/drop")
                onAck = move("temp/")
                readiness = ReadinessCheck { _, _ -> Readiness.Ready }
            }
            extra()
        }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"
        private const val CONTENT = "id,amount\n1,42\n"
        private val EVERY = 200.milliseconds
    }
}
