package infra.shuttle.testkit

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

class InMemoryTargetTest {
    @TempDir lateinit var dir: Path
    private val target = InMemoryTarget("bucket")

    private fun file(name: String, content: String) = Files.writeString(dir.resolve(name), content)

    @Test
    fun I6_a_fresh_ref_per_store_one_copy_per_key_and_verify_follows_the_current_ref() = runTest {
        val first = target.store("out/a.csv", file("a1", "one"), mapOf("digest" to "x"))
        val second = target.store("out/a.csv", file("a2", "two!"), emptyMap())
        assertNotEquals(first, second)
        assertEquals("out/a.csv", second.key)
        assertEquals("bucket", second.location)
        assertEquals(4, second.size)
        assertTrue(target.verify(second))
        assertFalse(target.verify(first))
        assertArrayEquals("two!".toByteArray(), target.bytes("out/a.csv"))
        assertEquals(setOf("out/a.csv"), target.keys)
        assertEquals(emptyMap<String, String>(), target.metadata("out/a.csv")) // the newest store's metadata
        assertEquals(2, target.calls.count { it.method == "store" })
    }

    @Test
    fun a_failed_store_writes_nothing_and_the_switch_is_one_shot() = runTest {
        target.failNextStore = true
        assertTrue(runCatching { target.store("k", file("a", "x"), emptyMap()) }.exceptionOrNull() is IOException)
        assertTrue(target.keys.isEmpty())
        val ref = target.store("k", file("b", "y"), emptyMap())
        assertTrue(target.verify(ref))
        target.probe()
        assertEquals(listOf("store", "store", "verify", "probe"), target.calls.map { it.method })
    }
}
