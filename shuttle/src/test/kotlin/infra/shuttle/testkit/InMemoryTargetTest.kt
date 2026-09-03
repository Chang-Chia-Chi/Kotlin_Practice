package infra.shuttle.testkit

import infra.shuttle.core.ObjectStoreTarget
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException

class InMemoryTargetTest : ObjectStoreTargetContract() {
    private val target = InMemoryTarget("bucket")
    override fun target(): ObjectStoreTarget = target
    override fun location() = "bucket"
    override suspend fun currentBytes(key: String) = target.bytes(key)

    @Test
    fun I6_one_copy_per_key_a_superseded_ref_no_longer_verifies_and_the_newest_metadata_wins() = runTest {
        val first = target.store("out/a.csv", file("a1", "one"), mapOf("digest" to "x"))
        target.store("out/a.csv", file("a2", "two!"), emptyMap())
        assertFalse(target.verify(first))
        assertArrayEquals("two!".toByteArray(), target.bytes("out/a.csv"))
        assertEquals(setOf("out/a.csv"), target.keys)
        assertEquals(emptyMap<String, String>(), target.metadata("out/a.csv"))
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
