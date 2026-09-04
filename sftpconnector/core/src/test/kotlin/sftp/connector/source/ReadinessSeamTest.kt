package sftp.connector.source

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import sftp.connector.source.Readiness.NotReady
import sftp.connector.source.Readiness.Ready
import sftp.connector.transport.RemoteFile
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * The built-in checks asked about a map, in the module that has no server to ask.
 *
 * This class is the seam itself rather than a test of it. It lives in `core`, which cannot see
 * the testkit and so has neither the embedded server nor the fake transport, and it constructs no
 * pool, no client and no transport: a readiness check is handed what the server says about a path
 * and the time, and those two are a lambda and a clock. Before that a check's world was the whole
 * client, so this file could not have been written at all - which is exactly how a seam that has
 * been widened past its purpose shows itself.
 *
 * The behaviour these assert is covered against a real server elsewhere; what they add is that
 * the cheap way to ask now exists.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class ReadinessSeamTest {

    @Test
    fun `a size that holds still is ready, one that grows is not, and the batch pays one interval`() = runTest {
        val sizes = mutableMapOf("/drop/a.csv" to 5L, "/drop/b.csv" to 5L)
        val (a, b) = listOf(listed("/drop/a.csv"), listed("/drop/b.csv"))
        launch { delay(5.seconds); sizes["/drop/b.csv"] = 12L }

        val verdicts = SizeStable(checks = 2, interval = 10.seconds).check(listOf(a, b), contextOver(sizes))

        assertThat(verdicts[a]).isEqualTo(Ready)
        assertThat(verdicts[b]).describedAs("grew between the two stats").isInstanceOf(NotReady::class.java)
        assertThat(currentTime).describedAs("one interval for the pair, not one each").isEqualTo(10_000)
    }

    @Test
    fun `a file the map no longer holds is turned away rather than handed over`() = runTest {
        val sizes = mutableMapOf("/drop/a.csv" to 5L)
        val a = listed("/drop/a.csv")
        launch { delay(5.seconds); sizes.remove("/drop/a.csv") }

        val verdicts = SizeStable(checks = 2, interval = 10.seconds).check(listOf(a), contextOver(sizes))

        assertThat(verdicts[a]).isInstanceOf(NotReady::class.java)
    }

    /**
     * The clock is the test scheduler's, so the wait a size check makes is also the time a file
     * ages by. Nothing here waits for a real second.
     */
    @Test
    fun `a file is old enough once the clock has moved a full duration past its modification time`() = runTest {
        val check = MinAge(1.minutes)
        val ctx = contextOver(mapOf("/drop/a.csv" to 5L))
        val a = listed("/drop/a.csv", modifiedAt = Instant.EPOCH)

        assertThat(check.check(a, ctx)).describedAs("modified this instant").isInstanceOf(NotReady::class.java)
        delay(59.seconds)
        assertThat(check.check(a, ctx)).describedAs("a second short").isInstanceOf(NotReady::class.java)
        delay(1.seconds)
        assertThat(check.check(a, ctx)).isEqualTo(Ready)
    }

    /** What the server would say, read out of a map instead. Absent from the map is absent from the server. */
    private fun TestScope.contextOver(sizes: Map<String, Long>) = ReadinessContext(
        stat = { path -> sizes[path]?.let { listed(path, size = it) } },
        clock = virtualClock(),
    )

    private fun listed(path: String, size: Long = 5, modifiedAt: Instant = Instant.EPOCH) =
        RemoteFile(path, size, modifiedAt, isDirectory = false)

    /** The scheduler's own time, so that a check reading the clock and a check waiting agree. */
    private fun TestScope.virtualClock(): Clock = object : Clock() {
        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId): Clock = this
        override fun millis(): Long = testScheduler.currentTime
        override fun instant(): Instant = Instant.ofEpochMilli(millis())
    }
}
