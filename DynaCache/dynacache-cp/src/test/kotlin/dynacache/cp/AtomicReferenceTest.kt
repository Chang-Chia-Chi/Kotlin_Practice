package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.SECONDS

/**
 * The AtomicReference (CP spec 3.5, 6.5) through the CP engine's seam: opaque bytes, compared for
 * a CAS byte by byte.
 */
class AtomicReferenceTest {

    private val kit = CpTestKit()
    private val ref = Key("cp:ref:r")

    @AfterEach
    fun tearDown() = kit.close()

    private fun submit(command: Command): Reply =
        kit.leaderEngine().submit(command).get(REPLY_TIMEOUT_SECS, SECONDS)

    private fun set(value: String) = submit(Command.Cp.RefSet(ref, value.toByteArray()))

    private fun get() = submit(Command.Cp.RefGet(ref))

    private fun cas(expected: String, new: String) =
        submit(Command.Cp.RefCas(ref, expected.toByteArray(), new.toByteArray()))

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
        val leader = kit.leader()
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.RefSet(ref, "hello".toByteArray(), Duration.ofSeconds(1))))
        assertEquals(bulk("hello"), get())

        kit.clock(leader.config.nodeId).advance(Duration.ofSeconds(2))
        leader.tick().get(REPLY_TIMEOUT_SECS, SECONDS)

        assertEquals(Reply.Bulk(null), get())
    }

    /** I21: N clients swap the same expected bytes; the log serializes them, so one wins and the rest see the winner's. */
    @Test
    fun ref_concurrent_cas_exactly_one_wins() {
        assertEquals(Reply.Simple("OK"), set("start"))
        val replies = race { client -> Command.Cp.RefCas(ref, "start".toByteArray(), "won-by-$client".toByteArray()) }

        assertEquals(1, replies.count { it == Reply.Integer(1) }, "exactly one swap: $replies")
        val winner = replies.indexOf(Reply.Integer(1)) + CONTENDERS.first
        assertEquals(bulk("won-by-$winner"), get(), "the reference holds the winner's bytes and nobody else's")
    }

    /** I21 over both CAS-carrying primitives: the counter's compare-and-set races the reference's. */
    @Test
    fun I21_concurrent_cas_exactly_one_wins() {
        val counter = Key("cp:counter:c")
        assertEquals(Reply.Simple("OK"), submit(Command.Cp.LongSet(counter, 0)))
        assertEquals(1, race { client -> Command.Cp.LongCas(counter, expected = 0, new = client.toLong()) }
            .count { it == Reply.Integer(1) }, "exactly one LONG_CAS")
        assertEquals(Reply.Integer(0), submit(Command.Cp.LongCas(counter, expected = 0, new = 99)), "0 is gone")

        assertEquals(Reply.Simple("OK"), set("start"))
        assertEquals(1, race { client -> Command.Cp.RefCas(ref, "start".toByteArray(), "$client".toByteArray()) }
            .count { it == Reply.Integer(1) }, "exactly one REF_CAS")
        assertEquals(Reply.Integer(0), cas("start", "late"), "the expected bytes are gone")
    }

    /** Every contender's command submitted at once through the leader's engine. */
    private fun race(command: (Int) -> Command): List<Reply> {
        val engine = kit.leaderEngine()
        val clients = Executors.newFixedThreadPool(CLIENT_THREADS)
        return try {
            CONTENDERS
                .map { client -> CompletableFuture.supplyAsync({ engine.submit(command(client)) }, clients) }
                .map { it.get(REPLY_TIMEOUT_SECS, SECONDS).get(REPLY_TIMEOUT_SECS, SECONDS) }
        } finally {
            clients.shutdownNow()
        }
    }

    private companion object {
        const val REPLY_TIMEOUT_SECS = 10L
        const val CLIENT_THREADS = 8
        val CONTENDERS = 1..10
    }
}
