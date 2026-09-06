package dynacache.server

import dynacache.cp.CpTestKit
import dynacache.engine.ApEngine
import dynacache.engine.Reply
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Clock

/**
 * Both engines behind one socket: a real AP engine, a real three-member CP group and the
 * dispatcher between them. Nothing here reaches past the wire, so what is asserted is what an
 * unmodified Redis client would see.
 */
class CpRoutingTest {

    private val kit = CpTestKit()
    private val ap = ApEngine(partitionCount = 4, clock = Clock.systemUTC())

    // The leader is the member that may replicate; a follower would answer -NOTLEADER and the
    // client would retry there, which is CP spec 9.1 step 3 and not this test's subject.
    private val server = DynaCacheServer(port = 0, engine = ap, cp = kit.leaderEngine())

    @BeforeEach
    fun start() = server.start()

    @AfterEach
    fun stop() {
        server.close()
        ap.close()
        kit.close()
    }

    private fun bulk(text: String) = Reply.Bulk(text.toByteArray(Charsets.ISO_8859_1))

    @Test
    fun long_redis_compat_incr() {
        RespClient(server.boundPort).use { client ->
            // The compat spelling and the CP verb are one counter: the second call sees the first.
            client.send("INCR", "cp:counter:x")
            assertEquals(Reply.Integer(1), client.read())
            client.send("CP.LONG.INCR", "cp:counter:x")
            assertEquals(Reply.Integer(2), client.read())
            client.send("CP.LONG.GET", "cp:counter:x")
            assertEquals(Reply.Integer(2), client.read())

            // A key of the same shape without the prefix is the AP engine's, and counts alone.
            client.send("INCR", "counter:x")
            assertEquals(Reply.Integer(1), client.read())
            client.send("GET", "counter:x")
            assertEquals(bulk("1"), client.read())

            // SET and GET on the compat path reach the same counter as the verbs.
            client.send("SET", "cp:counter:x", "41")
            assertEquals(Reply.Simple("OK"), client.read())
            client.send("GET", "cp:counter:x")
            assertEquals(Reply.Integer(41), client.read())
        }
    }

    @Test
    fun dispatch_auto_creates_session_on_first_cp_verb() {
        RespClient(server.boundPort).use { client ->
            // No CP.SESSION.CREATE first: the connection's session is made on the way to the lock
            // (CP spec 4), and the lock verb carries no session on the wire at all.
            client.send("CP.LOCK.TRY", "cp:lock:k", "30000")
            assertEquals(Reply.Array(listOf(Reply.Integer(1), Reply.Integer(1))), client.read())

            // The connection's session is the one CP.SESSION.CREATE names, not a second one.
            client.send("CP.SESSION.CREATE")
            val session = client.read() as Reply.Integer
            client.send("CP.SESSION.HEARTBEAT", session.value.toString())
            assertEquals(Reply.Simple("OK"), client.read())

            // The same session holds the lock, so trying again is reentrance and not a refusal.
            client.send("CP.LOCK.TRY", "cp:lock:k", "30000")
            assertEquals(Reply.Array(listOf(Reply.Integer(1), Reply.Integer(1))), client.read())
        }
    }

    @Test
    fun `the CP error kinds reach the wire as they are`() {
        RespClient(server.boundPort).use { client ->
            client.send("CP.LONG.INCR", "foo")
            assertEquals("NOTCP", (client.read() as Reply.Error).kind)
            client.send("LPUSH", "cp:foo", "a")
            assertEquals("NOTCP", (client.read() as Reply.Error).kind)
            // A CP.INFO names the leader the group elected, so the introspection verbs are wired.
            client.send("CP.MEMBERS")
            assertEquals(Reply.Array(kit.members.map { bulk(it.name) }), client.read())
        }
    }
}
