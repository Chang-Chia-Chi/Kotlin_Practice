package sftp.connector.testkit

import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.sftpConnector
import sftp.connector.error.PoolExhausted
import sftp.connector.pool.PoolStats
import sftp.connector.pool.SftpPool
import sftp.connector.transport.jsch.JschTransport
import java.nio.file.Path
import kotlin.time.Duration.Companion.milliseconds

/**
 * The pool against a real server rather than a script.
 *
 * Everything the fake transport proves about accounting it proves without a socket, which is what
 * makes those tests fast; what it cannot prove is that two sessions to one server can be open and
 * in use at the same time, and that the third caller's failure is the one an operator would read.
 * That is what this is for, and it is why the acquire timeout here is short: the wait is the
 * subject, so it happens for real.
 */
class PoolAgainstServerTest {

    @TempDir
    lateinit var remoteRoot: Path

    @Test
    fun `two callers hold two sessions at once and the third is told why there is no more`() = runBlocking<Unit> {
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
            val config = sftpConnector("pool-demo") {
                endpoint { host = server.host; port = server.port }
                auth { password(USER, PASSWORD) }
                // The embedded server generates a fresh key per instance, so there is no key a
                // test could have recorded in advance.
                hostKey = HostKeyPolicy.AcceptAll
                pool { maxSize = 2; acquireTimeout = ACQUIRE_TIMEOUT }
            }
            val pool = SftpPool(JschTransport(config), config)

            val first = pool.acquire()
            val second = pool.acquire()

            // Both are real sessions and both answer, which is the claim: two, at the same time.
            assertThat(first.connection.realpath(".")).isEqualTo("/")
            assertThat(second.connection.realpath(".")).isEqualTo("/")
            assertThat(first.connection).isNotSameAs(second.connection)
            assertThat(pool.stats()).isEqualTo(PoolStats(idle = 0, inUse = 2, connecting = 0, pending = 0))

            val turnedAway = runCatching { pool.acquire() }.exceptionOrNull()

            assertThat(turnedAway).isInstanceOf(PoolExhausted::class.java)
            assertThat(turnedAway)
                .hasMessageContaining("no session came free in $ACQUIRE_TIMEOUT")
                .hasMessageContaining("2 in use, 0 still opening, 0 idle, 1 waiting including this one")
                .hasMessageContaining("nothing came free at all, so the work already holding the sessions is not finishing")
                .hasMessageContaining("endpoint=${server.host}:${server.port}")

            // And the pool is not broken by having refused someone: the moment one comes back,
            // the next caller gets it.
            first.release()
            val afterTheWait = pool.acquire()
            assertThat(afterTheWait.connection.realpath(".")).isEqualTo("/")

            afterTheWait.release()
            second.release()
        }
    }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"

        /** Short on purpose: this is the one test where the wait is not virtual. */
        private val ACQUIRE_TIMEOUT = 300.milliseconds
    }
}
