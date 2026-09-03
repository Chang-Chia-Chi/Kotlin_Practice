package sftp.connector.source

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
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
 * The clock is a fixed one handed in, and the wait a size check makes is virtual time, so a
 * check that has to see a size hold still for ten seconds never waits for any of them.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class ReadinessTest {

    private val transport = FakeSftpTransport().directory("/drop").file("/drop/a.csv", "12345")

    /**
     * The whole batch is stated, one interval passes, the whole batch is stated again: three files
     * cost one interval, not three, and the one that grew in the meantime is the one turned away.
     * A file asked about on its own is a batch of one and costs the same interval.
     */
    @Test
    fun `a batch is stated twice one interval apart, and only the size that moved is not ready`() = runTest {
        transport.file("/drop/b.csv", "12345").file("/drop/c.csv", "12345")
        val check = SizeStable(checks = 2, interval = 10.seconds)
        val (a, b, c) = listOf(file("/drop/a.csv"), file("/drop/b.csv"), file("/drop/c.csv"))
        launch { delay(5.seconds); transport.file("/drop/b.csv", "123456") }

        val verdicts = check.check(listOf(a, b, c), at(T0))

        assertThat(verdicts[a]).isEqualTo(Ready)
        assertThat(verdicts[b]).describedAs("grew between the two stats").isInstanceOf(NotReady::class.java)
        assertThat(verdicts[c]).isEqualTo(Ready)
        assertThat(currentTime).describedAs("virtual time the whole batch took").isEqualTo(10_000)

        assertThat(check.check(a, at(T0))).isEqualTo(Ready)
        assertThat(currentTime).isEqualTo(20_000)
    }

    @Test
    fun `a size check over a batch remembers nothing between calls`() = runTest {
        val check = SizeStable(checks = 2, interval = 10.seconds)
        val a = file("/drop/a.csv")

        check.check(listOf(a), at(T0))
        transport.file("/drop/a.csv", "1234567")

        assertThat(check.check(listOf(a), at(T0))[a]).describedAs("the earlier size must not count").isEqualTo(Ready)
    }

    /** The checks after the first are only asked about the files the first let through. */
    @Test
    fun `a composite over a batch asks each check only about the files still ready`() = runTest {
        val a = file("/drop/a.csv")
        val b = file("/drop/b.csv")
        val asked = mutableListOf<String>()
        val onlyA = ReadinessCheck { f, _ -> if (f == a) Ready else NotReady("not a") }
        val recording = ReadinessCheck { f, _ -> asked += f.path; Ready }

        val verdicts = (onlyA + recording).check(listOf(a, b), at(T0))

        assertThat(verdicts).containsEntry(a, Ready).containsEntry(b, NotReady("not a"))
        assertThat(asked).containsExactly("/drop/a.csv")
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
