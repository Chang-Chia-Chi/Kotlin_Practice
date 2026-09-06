package dynacache.server

import dynacache.engine.Command
import dynacache.engine.CommandEngine
import dynacache.engine.Key
import dynacache.engine.PartitionContext
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.CompletableFuture

private val FIXED: Clock = Clock.fixed(Instant.ofEpochSecond(1_000_000), ZoneOffset.UTC)

private fun bytes(text: String) = text.toByteArray(Charsets.ISO_8859_1)

/**
 * One side of the dispatcher seam: it records what reached it and answers one canned reply. Both
 * engines are one of these, so a test says which engine saw a command by naming the list it is in.
 */
private class Recording(private val reply: Reply) : CommandEngine {

    val seen = mutableListOf<Command>()
    val batches = mutableListOf<List<Key>>()

    override fun submit(command: Command): CompletableFuture<Reply> {
        seen += command
        return CompletableFuture.completedFuture(reply)
    }

    override fun <R> atomically(keys: List<Key>, block: (PartitionContext) -> R): CompletableFuture<R> {
        batches += keys
        return CompletableFuture.failedFuture(NotImplementedError("the recording engine runs no batch"))
    }

    override fun close() = Unit
}

class CommandDispatcherTest {

    private val ap = Recording(Reply.Integer(1))
    private val cp = Recording(Reply.Integer(7))
    private val dispatcher = CommandDispatcher(ap, cp, FIXED)

    private fun answer(command: Command): Reply = dispatcher.submit(command).get()

    private fun errorKind(command: Command): String = (answer(command) as Reply.Error).kind

    // ---- CP spec 10.8 --------------------------------------------------------------------------

    @Test
    fun dispatch_cp_verb_routes_to_cp() {
        val command = Command.Cp.LongIncr(Key("cp:x"))
        assertEquals(Reply.Integer(7), answer(command))
        assertEquals(listOf<Command>(command), cp.seen)
        assertTrue(ap.seen.isEmpty(), "the AP engine saw ${ap.seen}")
    }

    @Test
    fun dispatch_cp_prefix_routes_to_cp() {
        assertEquals(Reply.Integer(7), answer(Command.IncrBy(Key("cp:x"), 1)))
        // The compat command reaches the CP engine as the verb it means, so both spellings are
        // one command by the time an engine sees them (CP spec 6.2).
        assertEquals(listOf<Command>(Command.Cp.LongIncr(Key("cp:x"))), cp.seen)
        assertTrue(ap.seen.isEmpty(), "the AP engine saw ${ap.seen}")
    }

    @Test
    fun dispatch_ap_key_routes_to_ap() {
        val command = Command.IncrBy(Key("x"), 1)
        assertEquals(Reply.Integer(1), answer(command))
        assertEquals(listOf<Command>(command), ap.seen)
        assertTrue(cp.seen.isEmpty(), "the CP engine saw ${cp.seen}")
    }

    @Test
    fun dispatch_cp_verb_bad_namespace_rejected() {
        assertEquals("NOTCP", errorKind(Command.Cp.LongIncr(Key("foo"))))
        assertTrue(ap.seen.isEmpty() && cp.seen.isEmpty(), "a rejected command reached an engine")
    }

    @Test
    fun dispatch_unsupported_redis_cmd_on_cp_rejected() {
        val lpush = Command.Push(Key("cp:foo"), listOf(bytes("a")), Command.End.HEAD)
        assertEquals("NOTCP", errorKind(lpush))
        assertTrue(ap.seen.isEmpty() && cp.seen.isEmpty(), "a rejected command reached an engine")
    }

    // ---- Constraints ---------------------------------------------------------------------------

    @Test
    fun C16_ap_engine_never_sees_cp_key() {
        val overCpKeys = listOf(
            Command.Cp.LongIncr(Key("cp:counter:x")),
            Command.Get(Key("cp:counter:x")),
            Command.Set(Key("cp:ref:r"), bytes("v")),
            Command.IncrBy(Key("cp:counter:x"), 3),
            Command.Del(Key("cp:counter:x")),
            Command.Type(Key("cp:counter:x")),
            Command.Push(Key("cp:l"), listOf(bytes("a")), Command.End.HEAD),
            // A fanned command is CP-bound when any of its keys is, not only the first: the AP
            // engine must never see the cp: half of an MGET either.
            Command.MGet(listOf(Key("plain"), Key("cp:counter:x"))),
            Command.DelKeys(listOf(Key("cp:counter:x"), Key("plain"))),
        )
        overCpKeys.forEach { dispatcher.submit(it).get() }
        assertTrue(ap.seen.isEmpty(), "the AP engine saw ${ap.seen}")

        // A batch declaring a cp: key never runs on the AP engine either.
        val batch = dispatcher.atomically(listOf(Key("plain"), Key("cp:counter:x"))) { Reply.Simple("OK") }
        assertTrue(batch.isCompletedExceptionally, "the batch ran")
        assertTrue(ap.batches.isEmpty(), "the AP engine was given ${ap.batches}")
    }

    /**
     * C22 under its own name: neither engine reads the other's state, so one key name in both
     * namespaces is two independent keys. [I22_namespaces_never_cross] is that assertion -- the
     * same name written on both sides, each engine seeing only its own -- so this delegates.
     */
    @Test
    fun C22_no_cross_engine_state_leakage() = I22_namespaces_never_cross()

    @Test
    fun I22_namespaces_never_cross() {
        answer(Command.Set(Key("cp:counter:foo"), bytes("1")))
        answer(Command.Set(Key("foo"), bytes("2")))
        answer(Command.Get(Key("cp:counter:foo")))
        answer(Command.Get(Key("foo")))

        assertEquals(
            listOf(Command.Cp.LongSet(Key("cp:counter:foo"), 1), Command.Cp.LongGet(Key("cp:counter:foo"))),
            cp.seen,
        )
        // The AP engine got both plain commands and neither cp: one, under the same key name.
        assertEquals(listOf("foo", "foo"), ap.seen.map { (it as Command.Keyed).key.toString() })
        assertEquals(listOf(Command.Set::class, Command.Get::class), ap.seen.map { it::class })
    }

    // ---- The compat set ------------------------------------------------------------------------

    @Test
    fun `the compat set reaches the CP engine as the verb it means`() {
        val counter = Key("cp:counter:x")
        val reference = Key("cp:ref:r")
        val expected = listOf<Pair<Command, Command>>(
            Command.Get(counter) to Command.Cp.LongGet(counter),
            Command.Set(counter, bytes("5")) to Command.Cp.LongSet(counter, 5),
            Command.Set(counter, bytes("5"), ttl = Duration.ofSeconds(30)) to
                Command.Cp.LongSet(counter, 5, Duration.ofSeconds(30)),
            Command.IncrBy(counter, 1) to Command.Cp.LongIncr(counter),
            Command.IncrBy(counter, -1) to Command.Cp.LongDecr(counter),
            Command.IncrBy(counter, 5) to Command.Cp.LongIncrBy(counter, 5),
            Command.IncrBy(counter, -5) to Command.Cp.LongIncrBy(counter, -5),
            Command.Expire(counter, FIXED.instant().plusSeconds(10)) to
                Command.Cp.LongExpire(counter, Duration.ofSeconds(10)),
            Command.Ttl(counter, Command.Ttl.Precision.SECONDS) to
                Command.Cp.LongTtl(counter, Command.Ttl.Precision.SECONDS),
            Command.Ttl(counter, Command.Ttl.Precision.MILLIS) to
                Command.Cp.LongTtl(counter, Command.Ttl.Precision.MILLIS),
            Command.Persist(counter) to Command.Cp.LongPersist(counter),
            // A cp:ref: key is the AtomicReference's, so SET and GET mean the reference verbs
            // rather than the counter's (CP spec 6.5).
            Command.Get(reference) to Command.Cp.RefGet(reference),
            Command.Set(reference, bytes("v")) to Command.Cp.RefSet(reference, bytes("v")),
            // The TTL verbs read the key's kind too: CP spec 9.4 gives them to the owning state
            // machine, so a reference's lease is the reference's own and not a missing counter's.
            Command.Expire(reference, FIXED.instant().plusSeconds(10)) to
                Command.Cp.RefExpire(reference, Duration.ofSeconds(10)),
            Command.Ttl(reference, Command.Ttl.Precision.SECONDS) to
                Command.Cp.RefTtl(reference, Command.Ttl.Precision.SECONDS),
            Command.Ttl(reference, Command.Ttl.Precision.MILLIS) to
                Command.Cp.RefTtl(reference, Command.Ttl.Precision.MILLIS),
            Command.Persist(reference) to Command.Cp.RefPersist(reference),
        )
        expected.forEach { (sent, _) -> dispatcher.submit(sent).get() }
        assertEquals(expected.map { it.second }, cp.seen)
        assertTrue(ap.seen.isEmpty(), "the AP engine saw ${ap.seen}")
    }

    @Test
    fun `a cp key outside the compat set is NOTCP`() {
        // DEL, EXISTS and TYPE are Redis commands no CP primitive answers yet; each is a
        // rejection rather than a silent trip to AP.
        assertEquals("NOTCP", errorKind(Command.Del(Key("cp:counter:x"))))
        assertEquals("NOTCP", errorKind(Command.Exists(Key("cp:counter:x"))))
        assertEquals("NOTCP", errorKind(Command.Type(Key("cp:counter:x"))))
        assertEquals("NOTCP", errorKind(Command.StrLen(Key("cp:counter:x"))))
        // A counter's value is a number; anything else is the error Redis gives for one, and NX
        // does not excuse it: the value is read before the condition is.
        assertEquals("ERR", errorKind(Command.Set(Key("cp:counter:x"), bytes("banana"))))
        assertEquals(
            "ERR",
            errorKind(Command.Set(Key("cp:counter:x"), bytes("banana"), Command.Set.Condition.NX)),
        )
        assertTrue(cp.seen.isEmpty(), "the CP engine saw ${cp.seen}")
    }

    /**
     * CP spec 1 and 9.5: `SET ... NX|XX` on a `cp:` key is the compat set's, and it re-targets to
     * the conditional form of the SET verb the key's kind names (T61). The condition and the TTL
     * ride on that one command, so the state machine applies both in one committed entry (I21).
     */
    @Test
    fun compat_conditional_set_retargets_to_the_kinds_set_verb() {
        val counter = Key("cp:counter:x")
        val reference = Key("cp:ref:r")
        val expected = listOf<Pair<Command, Command>>(
            Command.Set(counter, bytes("5"), Command.Set.Condition.NX) to
                Command.Cp.LongSet(counter, 5, condition = Command.Set.Condition.NX),
            Command.Set(counter, bytes("5"), Command.Set.Condition.XX, ttl = Duration.ofSeconds(30)) to
                Command.Cp.LongSet(counter, 5, Duration.ofSeconds(30), Command.Set.Condition.XX),
            Command.Set(reference, bytes("v"), Command.Set.Condition.NX, ttl = Duration.ofMillis(30_000)) to
                Command.Cp.RefSet(reference, bytes("v"), Duration.ofMillis(30_000), Command.Set.Condition.NX),
            Command.Set(reference, bytes("v"), Command.Set.Condition.XX) to
                Command.Cp.RefSet(reference, bytes("v"), condition = Command.Set.Condition.XX),
        )
        expected.forEach { (sent, _) -> dispatcher.submit(sent).get() }
        assertEquals(expected.map { it.second }, cp.seen)
        assertTrue(ap.seen.isEmpty(), "the AP engine saw ${ap.seen}")
    }

    @Test
    fun `a node without a CP engine answers NOTCP`() {
        val apOnly = CommandDispatcher(ap, null, FIXED)
        assertEquals("NOTCP", (apOnly.submit(Command.Cp.LongIncr(Key("cp:x"))).get() as Reply.Error).kind)
        assertEquals("NOTCP", (apOnly.submit(Command.IncrBy(Key("cp:x"), 1)).get() as Reply.Error).kind)
        assertEquals(Reply.Integer(1), apOnly.submit(Command.IncrBy(Key("x"), 1)).get())
        assertEquals(listOf<Command>(Command.IncrBy(Key("x"), 1)), ap.seen)
    }
}
