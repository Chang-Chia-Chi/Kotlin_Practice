package dynacache.server

import dynacache.cp.CpTestKit
import dynacache.engine.ApEngine
import dynacache.engine.Reply
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

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

    /**
     * CP spec 9.4: `TTL` and `PTTL` on a `cp:ref:` key operate on the AtomicReference, so a
     * reference set with `EX` reports its own lease. The lease is measured on log time, which
     * only the leader's clock moves.
     */
    @Test
    fun ref_ttl_via_compat_reports_reference_ttl() {
        RespClient(server.boundPort).use { client ->
            client.send("SET", "cp:ref:x", "v", "EX", "100")
            assertEquals(Reply.Simple("OK"), client.read())
            client.send("GET", "cp:ref:x")
            assertEquals(bulk("v"), client.read())

            client.send("TTL", "cp:ref:x")
            assertEquals(Reply.Integer(100), client.read(), "the reference's lease, not the counter's -2")
            // Every appended entry moves log time on by at least a millisecond (CP spec 5), so
            // PTTL is the lease minus the handful of entries this test has already written.
            client.send("PTTL", "cp:ref:x")
            val pttl = (client.read() as Reply.Integer).value
            assertTrue(pttl in 99_900..100_000, "PTTL of a 100 s reference lease, got $pttl")

            // Log time moves, and the lease reported moves with it.
            kit.clock(kit.leader().config.nodeId).advance(Duration.ofSeconds(40))
            client.send("TTL", "cp:ref:x")
            assertEquals(Reply.Integer(60), client.read())

            // A reference nobody set has no lease, and one set without EX has none to report.
            client.send("TTL", "cp:ref:missing")
            assertEquals(Reply.Integer(-2), client.read())
            client.send("SET", "cp:ref:plain", "v")
            assertEquals(Reply.Simple("OK"), client.read())
            client.send("TTL", "cp:ref:plain")
            assertEquals(Reply.Integer(-1), client.read())
        }
    }

    /**
     * CP spec 9.4 for the other two verbs: `EXPIRE` gives or shortens the reference's lease,
     * `PERSIST` takes it away, and a tick past the deadline is what removes the reference.
     */
    @Test
    fun ref_expire_and_persist_via_compat() {
        val leader = kit.leader()
        val clock = kit.clock(leader.config.nodeId)
        RespClient(server.boundPort).use { client ->
            client.send("EXPIRE", "cp:ref:y", "100")
            assertEquals(Reply.Integer(0), client.read(), "no reference to give a lease to")

            client.send("SET", "cp:ref:y", "v")
            assertEquals(Reply.Simple("OK"), client.read())
            client.send("EXPIRE", "cp:ref:y", "100")
            assertEquals(Reply.Integer(1), client.read())
            client.send("EXPIRE", "cp:ref:y", "5")
            assertEquals(Reply.Integer(1), client.read())
            client.send("TTL", "cp:ref:y")
            assertEquals(Reply.Integer(5), client.read(), "EXPIRE shortened the lease")

            client.send("PERSIST", "cp:ref:y")
            assertEquals(Reply.Integer(1), client.read())
            client.send("PERSIST", "cp:ref:y")
            assertEquals(Reply.Integer(0), client.read(), "nothing left to remove")
            clock.advance(Duration.ofSeconds(10))
            client.send("GET", "cp:ref:y")
            assertEquals(bulk("v"), client.read(), "PERSIST outlived the old lease")

            // PEXPIRE arrives as the same deadline, and the tick past it is what deletes.
            client.send("PEXPIRE", "cp:ref:y", "1000")
            assertEquals(Reply.Integer(1), client.read())
            clock.advance(Duration.ofSeconds(2))
            leader.tick().get(5, TimeUnit.SECONDS)
            client.send("GET", "cp:ref:y")
            assertEquals(Reply.Bulk(null), client.read())
            client.send("TTL", "cp:ref:y")
            assertEquals(Reply.Integer(-2), client.read())
        }
    }

    /**
     * T61, CP spec 1 and 9.5: the Redis lock idiom on the CP namespace. The log serializes the
     * entries, so of N clients racing `SET cp:ref:lock v NX` exactly one is told `+OK` and the
     * rest nil, and the reference holds the winner's bytes and nobody else's (I21).
     */
    @Test
    fun compat_set_nx_on_ref_key_acquires_once() {
        val answers = race(9) { client, n ->
            client.send("SET", "cp:ref:lock", "owner-$n", "NX")
            client.read()
        }
        val winners = answers.filterValues { it == Reply.Simple("OK") }.keys
        assertEquals(1, winners.size, "one +OK among $answers")
        assertTrue(
            answers.filterKeys { it !in winners }.values.all { it == Reply.Bulk(null) },
            "the losers are nil, got $answers",
        )
        RespClient(server.boundPort).use { client ->
            client.send("GET", "cp:ref:lock")
            assertEquals(bulk("owner-${winners.single()}"), client.read(), "the winner's bytes")
        }
    }

    /** The lease of a `SET NX PX` runs on log time, and the lock is free again a tick past it. */
    @Test
    fun compat_set_nx_px_expires_on_log_time() {
        val leader = kit.leader()
        val clock = kit.clock(leader.config.nodeId)
        RespClient(server.boundPort).use { client ->
            client.send("SET", "cp:ref:lease", "first", "NX", "PX", "30000")
            assertEquals(Reply.Simple("OK"), client.read())
            client.send("SET", "cp:ref:lease", "second", "NX", "PX", "30000")
            assertEquals(Reply.Bulk(null), client.read(), "the lease is still held")
            client.send("PTTL", "cp:ref:lease")
            assertTrue((client.read() as Reply.Integer).value in 29_900..30_000, "the lease was applied")

            // Log time only moves when the leader's clock does; the tick past the deadline is what
            // removes the reference, and the lock is then there to be taken again.
            clock.advance(Duration.ofSeconds(31))
            leader.tick().get(5, TimeUnit.SECONDS)
            client.send("GET", "cp:ref:lease")
            assertEquals(Reply.Bulk(null), client.read(), "the lease ran out")
            client.send("SET", "cp:ref:lease", "second", "NX", "PX", "30000")
            assertEquals(Reply.Simple("OK"), client.read())
            client.send("GET", "cp:ref:lease")
            assertEquals(bulk("second"), client.read())
        }
    }

    /** `XX` refuses what is not there, and Redis's nil is what the client sees. */
    @Test
    fun compat_set_xx_on_missing_key_is_nil() {
        RespClient(server.boundPort).use { client ->
            client.send("SET", "cp:ref:absent", "v", "XX")
            assertEquals(Reply.Bulk(null), client.read())
            client.send("GET", "cp:ref:absent")
            assertEquals(Reply.Bulk(null), client.read(), "the refusal wrote nothing")
        }
    }

    /** `XX` over a live reference replaces its bytes, and its lease with them, as Redis does. */
    @Test
    fun compat_set_xx_on_present_key_replaces() {
        RespClient(server.boundPort).use { client ->
            client.send("SET", "cp:ref:held", "first", "PX", "30000")
            assertEquals(Reply.Simple("OK"), client.read())
            client.send("SET", "cp:ref:held", "second", "XX")
            assertEquals(Reply.Simple("OK"), client.read())
            client.send("GET", "cp:ref:held")
            assertEquals(bulk("second"), client.read())
            client.send("TTL", "cp:ref:held")
            assertEquals(Reply.Integer(-1), client.read(), "a plain SET clears the lease it replaces")
        }
    }

    /** The counter answers the same four behaviours: NX takes once, over a numeric value. */
    @Test
    fun compat_set_nx_on_counter_key_acquires_once() {
        val answers = race(9) { client, n ->
            client.send("SET", "cp:counter:lock", n.toString(), "NX")
            client.read()
        }
        val winners = answers.filterValues { it == Reply.Simple("OK") }.keys
        assertEquals(1, winners.size, "one +OK among $answers")
        assertTrue(
            answers.filterKeys { it !in winners }.values.all { it == Reply.Bulk(null) },
            "the losers are nil, got $answers",
        )
        RespClient(server.boundPort).use { client ->
            client.send("GET", "cp:counter:lock")
            assertEquals(Reply.Integer(winners.single().toLong()), client.read(), "the winner's value")
        }
    }

    /** The counter's lease runs on log time too, so its NX lock frees itself the same way. */
    @Test
    fun compat_set_nx_px_on_counter_expires_on_log_time() {
        val leader = kit.leader()
        val clock = kit.clock(leader.config.nodeId)
        RespClient(server.boundPort).use { client ->
            client.send("SET", "cp:counter:lease", "1", "NX", "PX", "30000")
            assertEquals(Reply.Simple("OK"), client.read())
            client.send("SET", "cp:counter:lease", "2", "NX", "PX", "30000")
            assertEquals(Reply.Bulk(null), client.read(), "the lease is still held")

            clock.advance(Duration.ofSeconds(31))
            leader.tick().get(5, TimeUnit.SECONDS)
            client.send("GET", "cp:counter:lease")
            assertEquals(Reply.Bulk(null), client.read(), "the lease ran out")
            client.send("SET", "cp:counter:lease", "2", "NX")
            assertEquals(Reply.Simple("OK"), client.read())
            client.send("GET", "cp:counter:lease")
            assertEquals(Reply.Integer(2), client.read())
        }
    }

    /** And `XX` on a counter: nil for one that is not there, a replacement for one that is. */
    @Test
    fun compat_set_xx_on_counter_key_is_nil_then_replaces() {
        RespClient(server.boundPort).use { client ->
            client.send("SET", "cp:counter:c", "7", "XX")
            assertEquals(Reply.Bulk(null), client.read())
            client.send("GET", "cp:counter:c")
            assertEquals(Reply.Bulk(null), client.read(), "the refusal wrote nothing")

            client.send("SET", "cp:counter:c", "7")
            assertEquals(Reply.Simple("OK"), client.read())
            client.send("SET", "cp:counter:c", "8", "XX")
            assertEquals(Reply.Simple("OK"), client.read())
            client.send("GET", "cp:counter:c")
            assertEquals(Reply.Integer(8), client.read())

            // A counter's value is a number whatever the condition says, so this is -ERR and not nil.
            client.send("SET", "cp:counter:c", "banana", "NX")
            assertEquals("ERR", (client.read() as Reply.Error).kind)
        }
    }

    /**
     * [clients] connections send at once and their replies come back by client number. The threads
     * are released together by a latch rather than by a sleep, so the race is a real one.
     */
    private fun race(clients: Int, exchange: (RespClient, Int) -> Reply): Map<Int, Reply> {
        val pool = Executors.newFixedThreadPool(clients)
        val start = CountDownLatch(1)
        return try {
            val replies = (1..clients).associateWith { n ->
                pool.submit<Reply> {
                    RespClient(server.boundPort).use { client ->
                        start.await()
                        exchange(client, n)
                    }
                }
            }
            start.countDown()
            replies.mapValues { (_, reply) -> reply.get(20, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }
    }
}
