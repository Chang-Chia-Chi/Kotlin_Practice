package infra.shuttle.testkit

import infra.shuttle.core.DigestAlgorithm
import infra.shuttle.core.RouteEvent
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.IOException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path

class ScriptedSourceTest {
    @TempDir lateinit var dir: Path
    private val clock = ClockFixture()

    @Test
    fun emits_the_scripted_flow_and_records_every_ack_and_nack_with_its_flag() = runTest {
        val a = ScriptedSource.identity("a.csv")
        val b = ScriptedSource.identity("b.csv")
        val down = IllegalStateException("auth")
        val source = ScriptedSource(clock)
            .seen(a).seen(b)
            .pollCompleted(setOf(a, b))
            .pollFailed(IOException("listing"))
            .pollSkipped()
            .seen(a).pollCompleted(setOf(a), truncated = true)
            .routeDown(down)

        val events = source.events().toList()
        assertEquals(8, events.size)
        val seen = events.filterIsInstance<RouteEvent.Seen>()
        assertEquals(listOf(a, b, a), seen.map { it.identity })
        assertEquals("a.csv", seen[0].source.path)
        assertEquals(RouteEvent.PollCompleted(clock.instant(), setOf(a, b), truncated = false), events[2])
        assertTrue(events[3] is RouteEvent.PollFailed)
        assertSame(RouteEvent.PollSkipped, events[4])
        assertEquals(RouteEvent.PollCompleted(clock.instant(), setOf(a), truncated = true), events[6])
        assertSame(down, (events[7] as RouteEvent.RouteDown).cause)

        seen[0].ack()
        seen[1].nack(true)
        seen[2].nack(false)
        assertEquals(listOf(a), source.acks)
        assertEquals(listOf(ScriptedSource.Nack(b, redeliver = true), ScriptedSource.Nack(a, redeliver = false)), source.nacks)
        assertEquals(8, source.events().toList().size) // the flow is cold and replays
    }

    @Test
    fun the_fetcher_copies_scripted_bytes_digests_them_and_can_fail_or_report_a_file_gone() = runTest {
        val fetcher = ScriptedFetcher(clock).file("in/a.csv", "hello".toByteArray()).gone("in/b.csv")
        val staged = fetcher("in/a.csv", dir.resolve("a.csv"), DigestAlgorithm.MD5)
        assertEquals("a.csv", staged.name)
        assertEquals(dir.resolve("a.csv"), staged.path)
        assertEquals(5, staged.size)
        assertEquals("5d41402abc4b2a76b9719d911017c592", staged.digest.hex)
        assertEquals(DigestAlgorithm.MD5, staged.digest.algorithm)
        assertEquals(clock.instant(), staged.mtime)
        assertArrayEquals("hello".toByteArray(), Files.readAllBytes(staged.path))
        assertEquals("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", fetcher("in/a.csv", dir.resolve("a2"), DigestAlgorithm.SHA256).digest.hex)

        assertTrue(runCatching { fetcher("in/b.csv", dir.resolve("b.csv"), DigestAlgorithm.MD5) }.exceptionOrNull() is NoSuchFileException)
        fetcher.failNext = true
        assertTrue(runCatching { fetcher("in/a.csv", dir.resolve("a3"), DigestAlgorithm.MD5) }.exceptionOrNull() is IOException)
        assertEquals(4, fetcher.calls.size)
    }
}
