package dynacache.server

import dynacache.cp.CpTestKit
import dynacache.engine.ApEngine
import dynacache.engine.Reply
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * A connection's session over its whole life (CP spec 4): the handler makes one on the first verb
 * that needs it, hands the same one to every later verb, and forgets it once it is gone -- closed
 * through this connection, or lapsed at a TTL tick. The arrangement is [CpRoutingTest]'s: a real
 * AP engine, a real three-member CP group and the socket in front of both, so what is asserted is
 * what an unmodified client would see.
 */
class CpSessionLifecycleTest {

    private val kit = CpTestKit()
    private val ap = ApEngine(partitionCount = 4, clock = Clock.systemUTC())
    private val server = DynaCacheServer(port = 0, engine = ap, cp = kit.leaderEngine())

    @BeforeEach
    fun start() = server.start()

    @AfterEach
    fun stop() {
        server.close()
        ap.close()
        kit.close()
    }

    /**
     * CP spec 4 and 6.6: `CP.SESSION.CLOSE` ends the session it names, so the next `CREATE` on the
     * same connection is a new session and not the closed one handed back.
     */
    @Test
    fun session_create_after_close_returns_a_new_session() {
        RespClient(server.boundPort).use { client ->
            val first = create(client)
            close(client, first)

            val second = create(client)

            assertNotEquals(first, second, "CREATE after CLOSE handed back the closed session")
            client.send("CP.LOCK.TRY", LOCK, "30000")
            val granted = assertInstanceOf(Reply.Array::class.java, client.read(), "the new session is not usable")
            assertEquals(Reply.Integer(1), granted.items[0], "the lock was refused to the new session")
        }
    }

    /**
     * The connection's session is what a lock verb carries (CP spec 4), so after a CLOSE the lock
     * is held by the session the second CREATE made -- which is the one `CP.LOCK.STATE` reports.
     */
    @Test
    fun session_verbs_after_close_use_the_new_session() {
        RespClient(server.boundPort).use { client ->
            val first = create(client)
            close(client, first)
            val second = create(client)

            client.send("CP.LOCK.TRY", LOCK, "30000")
            assertInstanceOf(Reply.Array::class.java, client.read())

            client.send("CP.LOCK.STATE", LOCK)
            val state = assertInstanceOf(Reply.Array::class.java, client.read())
            assertEquals(Reply.Integer(second), state.items[0], "the lock is not held by the new session")
        }
    }

    /**
     * CP spec 4 and 5: a session that misses its timeout of log time lapses at the next TTL tick.
     * The verb that finds it gone answers `-NOSESSION` once, and because the handler forgets it
     * there, the next `CREATE` starts clean instead of naming the lapsed session for ever.
     */
    @Test
    fun session_lapse_clears_the_cache() {
        RespClient(server.boundPort).use { client ->
            val lapsing = create(client)

            val leader = kit.leader()
            kit.clock(leader.config.nodeId).advance(SESSION_TIMEOUT.plusSeconds(5))
            leader.tick().get(REPLY_TIMEOUT_SECONDS, TimeUnit.SECONDS)

            client.send("CP.LOCK.TRY", LOCK, "30000")
            assertEquals("NOSESSION", assertInstanceOf(Reply.Error::class.java, client.read()).kind)

            val fresh = create(client)
            assertNotEquals(lapsing, fresh, "CREATE after the lapse handed back the lapsed session")
            client.send("CP.LOCK.TRY", LOCK, "30000")
            val granted = assertInstanceOf(Reply.Array::class.java, client.read(), "the fresh session is not usable")
            assertEquals(Reply.Integer(1), granted.items[0], "the lock was refused to the fresh session")
        }
    }

    private fun create(client: RespClient): Long {
        client.send("CP.SESSION.CREATE")
        return assertInstanceOf(Reply.Integer::class.java, client.read()).value
    }

    private fun close(client: RespClient, session: Long) {
        client.send("CP.SESSION.CLOSE", session.toString())
        assertEquals(Reply.Simple("OK"), client.read())
    }

    private companion object {
        const val LOCK = "cp:lock:k"
        const val REPLY_TIMEOUT_SECONDS = 10L

        /** `CP.SESSION.CREATE` takes no timeout on the wire, so every session gets the default. */
        val SESSION_TIMEOUT: Duration = Duration.ofSeconds(15)
    }
}
