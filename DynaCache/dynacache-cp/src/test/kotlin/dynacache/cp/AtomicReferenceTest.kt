package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * The AtomicReference (CP spec 3.5, 6.5) at the state machine: opaque bytes, compared for a CAS
 * byte by byte. The clients racing the same expected bytes are [CpConcurrencyTest], which needs
 * the log to serialize them.
 */
class AtomicReferenceTest {

    private val cp = Primitives()
    private val ref = Key("cp:ref:r")

    private fun set(value: String) = cp.apply(Command.Cp.RefSet(ref, value.toByteArray()))

    private fun get() = cp.apply(Command.Cp.RefGet(ref))

    private fun cas(expected: String, new: String) =
        cp.apply(Command.Cp.RefCas(ref, expected.toByteArray(), new.toByteArray()))

    private fun bulk(value: String) = Reply.Bulk(value.toByteArray())

    @Test
    fun ref_set_get_roundtrip() {
        assertEquals(Reply.Bulk(null), get(), "never set")
        assertEquals(Reply.Simple("OK"), set("hello"))
        assertEquals(bulk("hello"), get())
    }

    /** The bytes are opaque: nothing is trimmed, cased or decoded before the comparison. */
    @Test
    fun ref_cas_byte_equality() {
        assertEquals(Reply.Integer(0), cas("hello", "world"), "a reference never set matches nothing")

        assertEquals(Reply.Simple("OK"), set("hello"))
        assertEquals(Reply.Integer(0), cas("Hello", "world"), "one byte differs")
        assertEquals(Reply.Integer(0), cas("hello ", "world"), "one byte longer")
        assertEquals(Reply.Integer(0), cas("hell", "world"), "a prefix is not the value")
        assertEquals(bulk("hello"), get(), "none of those swapped")

        assertEquals(Reply.Integer(1), cas("hello", "world"))
        assertEquals(bulk("world"), get())
    }

    /** A reference's TTL is measured against log time, so every member expires it at the same index (C23). */
    @Test
    fun ref_ttl_expires() {
        assertEquals(Reply.Simple("OK"), cp.apply(Command.Cp.RefSet(ref, "hello".toByteArray(), Duration.ofSeconds(1))))
        assertEquals(bulk("hello"), get())

        cp.tick(after = Duration.ofSeconds(2))

        assertEquals(Reply.Bulk(null), get())
    }
}
