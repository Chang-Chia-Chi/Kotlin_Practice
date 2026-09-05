package dynacache.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class KeyTest {

    @Test
    fun `keys compare by their bytes and are binary-safe`() {
        assertEquals(Key("user:1"), Key("user:1".toByteArray()))
        assertEquals(Key("user:1").hashCode(), Key("user:1").hashCode())
        assertNotEquals(Key("user:1"), Key("user:2"))

        val raw = byteArrayOf(0, -1, 10, 0)
        assertEquals(Key(raw), Key(byteArrayOf(0, -1, 10, 0)))
        assertNotEquals(Key(raw), Key(byteArrayOf(0, -1, 10)))
    }

    @Test
    fun `a hash tag makes only the tag decide the hash`() {
        assertEquals(Key("{user1}.a").hash, Key("{user1}.b").hash)
        assertEquals(Key("user1").hash, Key("{user1}.a").hash)
        assertEquals("user1", String(Key("{user1}.a").hashedBytes))
        assertNotEquals(Key("{user1}.a"), Key("{user1}.b"))
    }

    @Test
    fun `a key without a hash tag hashes whole`() {
        assertEquals("user1.a", String(Key("user1.a").hashedBytes))
        assertNotEquals(Key("user1.a").hash, Key("user1.b").hash)
    }

    @Test
    fun `an empty or unclosed tag falls back to the whole key`() {
        assertEquals("{}.a", String(Key("{}.a").hashedBytes))
        assertNotEquals(Key("{}.a").hash, Key("{}.b").hash)
        assertEquals("{user1.a", String(Key("{user1.a").hashedBytes))
        assertEquals("}{a", String(Key("}{a").hashedBytes))
    }

    @Test
    fun `the first tag wins and only its first closing brace ends it`() {
        assertEquals("first", String(Key("{first}{second}").hashedBytes))
        assertEquals("a", String(Key("x{a}b}c").hashedBytes))
        assertEquals("foo{}{bar}", String(Key("foo{}{bar}").hashedBytes))
    }

    @Test
    fun `the partition hash is never negative`() {
        repeat(1000) { i -> assertTrue(Key("key:" + i).hash >= 0) }
    }
}
