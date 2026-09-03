package infra.shuttle.testkit

import infra.shuttle.core.ObjectStoreTarget
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/**
 * Spec 7.1 through the seam alone, so every target proves the same promise: a fresh ref per store,
 * the newest content current at the key, `verify` true for a ref that still exists and false for
 * one that does not. Anything a target can additionally observe belongs in its own test class.
 */
abstract class ObjectStoreTargetContract {
    @TempDir lateinit var dir: Path

    abstract fun target(): ObjectStoreTarget
    abstract fun location(): String
    /** What a GET by key returns now; the seam has no read, so each target says how to look. */
    abstract suspend fun currentBytes(key: String): ByteArray

    protected fun file(name: String, content: String): Path = Files.writeString(dir.resolve(name), content)

    @Test
    fun I6_a_fresh_ref_per_store_and_the_newest_content_current_at_the_key() = runTest {
        val target = target()
        val first = target.store("out/a.csv", file("a1", "one"), emptyMap())
        val second = target.store("out/a.csv", file("a2", "two!"), emptyMap())
        assertNotEquals(first, second)
        assertEquals("out/a.csv", second.key)
        assertEquals(location(), second.location)
        assertEquals(4, second.size)
        assertTrue(target.verify(second))
        assertFalse(target.verify(second.copy(ref = "no-such-version")))
        assertEquals("two!", String(currentBytes("out/a.csv")))
        target.probe()
    }
}
