package sftp.connector.source

import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import sftp.connector.client.SftpClient
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.sftpConnector
import sftp.connector.pool.SftpPool
import sftp.connector.source.Readiness.NotReady
import sftp.connector.source.Readiness.Ready
import sftp.connector.source.Readiness.Skip
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.transport.RemoteFile
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * The built-in readiness checks, each asked about a file at a moment of the test's choosing.
 * Time is a fixed clock handed in, so a check that has to see a size hold still for ten seconds
 * is shown three instants and never waits for any of them.
 */
class ReadinessTest {

    private val transport = FakeSftpTransport().directory("/drop").file("/drop/a.csv", "12345")

    @Test
    fun `a size is stable once it has held still across the interval, and a change starts over`() = runBlocking<Unit> {
        val check = SizeStable(checks = 2, interval = 10.seconds)

        assertThat(check.check(file(size = 5), at(T0))).isInstanceOf(NotReady::class.java)
        assertThat(check.check(file(size = 5), at(T0 + 5.seconds))).describedAs("too soon to count").isInstanceOf(NotReady::class.java)
        assertThat(check.check(file(size = 5), at(T0 + 10.seconds))).isEqualTo(Ready)

        assertThat(check.check(file(size = 6), at(T0 + 20.seconds))).describedAs("size changed").isInstanceOf(NotReady::class.java)
        assertThat(check.check(file(size = 6), at(T0 + 30.seconds))).isEqualTo(Ready)
    }

    @Test
    fun `a file is old enough once its modification time is the duration behind the clock`() = runBlocking<Unit> {
        val check = MinAge(1.minutes)

        assertThat(check.check(file(), at(MODIFIED_AT + 59.seconds))).isInstanceOf(NotReady::class.java)
        assertThat(check.check(file(), at(MODIFIED_AT + 60.seconds))).isEqualTo(Ready)
    }

    @Test
    fun `a marker beside the file makes it ready, and the marker itself is skipped`() = runBlocking<Unit> {
        val check = MarkerFile(".done")

        assertThat(check.check(file(), at(T0))).isInstanceOf(NotReady::class.java)
        transport.file("/drop/a.csv.done", "")
        assertThat(check.check(file(), at(T0))).isEqualTo(Ready)
        assertThat(check.check(file(path = "/drop/a.csv.done"), at(T0))).isEqualTo(Skip)
    }

    @Test
    fun `a composite answers with the first check that is not ready, and plus flattens`() = runBlocking<Unit> {
        val never = ReadinessCheck { _, _ -> NotReady("never") }
        val always = ReadinessCheck { _, _ -> Ready }

        assertThat((always + never + always).check(file(), at(T0))).isEqualTo(NotReady("never"))
        assertThat((always + always).check(file(), at(T0))).isEqualTo(Ready)
        assertThat(((always + never) + always) as AllOf).extracting { it.checks.size }.isEqualTo(3)
    }

    private fun file(path: String = "/drop/a.csv", size: Long = 5) =
        RemoteFile(path, size, MODIFIED_AT, isDirectory = false)

    private fun at(instant: Instant): ReadinessContext {
        val config = sftpConnector("readiness-demo") {
            endpoint { host = "fake.example"; port = 22 }
            auth { password("etl", "s3cret") }
            hostKey = HostKeyPolicy.AcceptAll
        }
        return ReadinessContext(SftpClient(SftpPool(transport, config), config), Clock.fixed(instant, ZoneOffset.UTC))
    }

    private operator fun Instant.plus(duration: kotlin.time.Duration): Instant = plusMillis(duration.inWholeMilliseconds)

    private companion object {
        private val T0: Instant = Instant.parse("2024-06-01T12:00:00Z")

        /** What the fake transport stamps on everything. */
        private val MODIFIED_AT: Instant = Instant.parse("2024-01-01T00:00:00Z")
    }
}
