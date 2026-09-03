package infra.shuttle.testkit

import infra.shuttle.core.DigestAlgorithm
import infra.shuttle.core.Payload
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

class FakeProcessContextTest {
    @TempDir lateinit var dir: Path
    private val clock = ClockFixture()
    private val fetcher = ScriptedFetcher(clock).file("in/a.csv", "a,b".toByteArray())

    @Test
    fun I18_allocates_staged_files_in_the_directory_tracks_them_and_deletes_them_on_close() = runTest {
        val ctx = FakeProcessContext(dir, fetcher, clock)
        val out = ctx.newStagedFile("out.zip")
        assertTrue(out.startsWith(dir))
        Files.writeString(out, "zip")
        val fetched = ctx.fetch("s3", "in/a.csv")
        assertTrue(fetched.path.startsWith(dir))
        assertEquals("a.csv", fetched.name)
        assertEquals(listOf(out, fetched.path), ctx.createdFiles)
        ctx.setAttribute("k", "v")
        assertEquals(mapOf("k" to "v"), ctx.attributes)
        assertEquals(clock, ctx.clock)
        assertEquals("a.csv", ctx.source.path)
        ctx.close()
        assertFalse(Files.exists(out))
        assertFalse(Files.exists(fetched.path))
    }

    @Test
    fun I18_detects_a_processor_writing_into_an_input() = runTest {
        val ctx = FakeProcessContext(dir, fetcher, clock)
        val input = fetcher("in/a.csv", dir.resolve("input"), DigestAlgorithm.MD5)
        val payload = Payload(listOf(input))
        ctx.snapshot(payload)
        assertTrue(ctx.inputsUntouched())
        Files.writeString(input.path, "a,c") // same size, different bytes
        assertFalse(ctx.inputsUntouched())
    }
}
