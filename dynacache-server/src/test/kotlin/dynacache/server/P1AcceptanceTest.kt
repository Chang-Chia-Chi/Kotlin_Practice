package dynacache.server

import dynacache.engine.ApEngine
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.params.SetParams
import redis.clients.jedis.params.ZAddParams
import java.time.Clock
import java.time.Duration

/**
 * Spec 9's single-node demo, driven by a Redis client that has never heard of DynaCache: Jedis,
 * unmodified and unconfigured, off Maven Central in test scope. That is the whole point of the
 * tier (C8). Every assertion is on a value Jedis handed back, so a reply spelled differently
 * from Redis's shows up here as a parse failure or a wrong value, not as a byte comparison whose
 * expectation the server itself could have taught the test.
 *
 * The seam is the socket. Nothing below it is addressed: the engine is the default one, the
 * scheduler is the server's own, and the port is whatever 0 was given.
 */
class P1AcceptanceTest {

    private val engine = ApEngine(partitionCount = 16, clock = Clock.systemUTC())
    private val server = DynaCacheServer(0, engine).apply { start() }

    @AfterEach
    fun stop() {
        server.close()
        engine.close()
    }

    @Test
    fun P1_acceptance_redis_client_unmodified() {
        Jedis("127.0.0.1", server.boundPort).use { redis ->
            assertEquals("PONG", redis.ping())
            aStringWithATtl(redis)
            theLeaderboard(redis)
            theCounterScript(redis)
            aScanOverAThousandKeys(redis)
            oneBatchOverTwoKeys(redis)
            aTtlThatFires(redis)
            introspection(redis)
        }
    }

    /** `SET foo bar EX 60`, `GET foo`: spec 9's first two commands. */
    private fun aStringWithATtl(redis: Jedis) {
        assertEquals("OK", redis.set("foo", "bar", SetParams.setParams().ex(60)))
        assertEquals("bar", redis.get("foo"))
        val remaining = redis.ttl("foo")
        assertTrue(remaining in 1L..60L, "TTL after SET ... EX 60 was $remaining")
    }

    /** `ZADD leaderboard 100 alice 200 bob`, `ZRANGE leaderboard 0 -1 WITHSCORES` sorted. */
    private fun theLeaderboard(redis: Jedis) {
        assertEquals(1L, redis.zadd("leaderboard", 100.0, "alice"))
        assertEquals(1L, redis.zadd("leaderboard", 200.0, "bob"))
        val ranked = redis.zrangeWithScores("leaderboard", 0, -1)
        assertEquals(listOf("alice", "bob"), ranked.map { it.element })
        assertEquals(listOf(100.0, 200.0), ranked.map { it.score })
        assertEquals(2L, redis.zcard("leaderboard"))
        assertEquals(0L, redis.zrank("leaderboard", "alice").toLong())
        assertEquals(200.0, redis.zscore("leaderboard", "bob").toDouble())
        // The flags this ticket added, read through the client that names them.
        assertEquals(0L, redis.zadd("leaderboard", 150.0, "alice", ZAddParams.zAddParams().nx()))
        assertEquals(100.0, redis.zscore("leaderboard", "alice").toDouble())
        assertEquals(1L, redis.zadd("leaderboard", 150.0, "alice", ZAddParams.zAddParams().xx().ch()))
        assertEquals(150.0, redis.zscore("leaderboard", "alice").toDouble())
        assertEquals(listOf("alice", "bob"), redis.zrange("leaderboard", 0, -1))
    }

    /** Spec 9's atomic increment, verbatim, through `EVAL`. */
    private fun theCounterScript(redis: Jedis) {
        assertEquals("OK", redis.set("counter", "0"))
        val script =
            "redis.call('SET', KEYS[1], redis.call('GET', KEYS[1]) + 1); return redis.call('GET', KEYS[1])"
        assertEquals("1", redis.eval(script, 1, "counter"))
        assertEquals("2", redis.eval(script, 1, "counter"))
        assertEquals("2", redis.get("counter"))
    }

    /**
     * C15 from outside: a full cursor walk returns every key that was there for the whole of it.
     * Duplicates are permitted, which is why the client collects into a set, exactly as the spec
     * says a client must.
     */
    private fun aScanOverAThousandKeys(redis: Jedis) {
        val written = (0 until 1_000).map { "scan:$it" }.toSet()
        written.forEach { assertEquals("OK", redis.set(it, "v")) }
        val seen = HashSet<String>()
        var cursor = ScanParams.SCAN_POINTER_START
        var pages = 0
        do {
            val page = redis.scan(cursor, ScanParams().match("scan:*").count(64))
            seen += page.result
            cursor = page.cursor
            assertTrue(++pages <= 10_000, "the SCAN cursor never came back to 0")
        } while (cursor != ScanParams.SCAN_POINTER_START)
        assertTrue(seen.containsAll(written), "the scan missed ${(written - seen).size} of 1000 keys")
    }

    /** `MULTI`/`EXEC` over two keys sharing a hash tag, so C12's span check lets the batch run. */
    private fun oneBatchOverTwoKeys(redis: Jedis) {
        val batch = redis.multi()
        batch.set("{account}.balance", "100")
        batch.set("{account}.owner", "alice")
        assertEquals(listOf("OK", "OK"), batch.exec())
        assertEquals("100", redis.get("{account}.balance"))
        assertEquals("alice", redis.get("{account}.owner"))
    }

    /**
     * The one place this tier awaits real time. The timer wheel is advanced by the server's own
     * scheduler thread, so no test can move it; a deadline bounds the wait and there is no sleep.
     * The read before the deadline is C7's other half: a key stays readable until its TTL elapses.
     */
    private fun aTtlThatFires(redis: Jedis) {
        assertEquals("OK", redis.set("expiring", "v", SetParams.setParams().px(200)))
        assertEquals("v", redis.get("expiring"))
        val giveUpAt = System.nanoTime() + Duration.ofSeconds(5).toNanos()
        while (redis.get("expiring") != null) {
            assertTrue(System.nanoTime() < giveUpAt, "the TTL had not fired five seconds after PX 200")
        }
        assertNull(redis.get("expiring"))
        assertEquals(-2L, redis.ttl("expiring"))
    }

    /** `INFO` parses: the client reads it as the `field:value` sections Redis writes. */
    private fun introspection(redis: Jedis) {
        val fields = redis.info()
            .lineSequence()
            .filter { it.contains(':') && !it.startsWith("#") }
            .associate { it.substringBefore(':') to it.substringAfter(':') }
        assertEquals("lru", fields["maxmemory_policy"])
        assertTrue(fields.containsKey("dynacache_version"), "INFO named no version: ${fields.keys}")
        assertTrue(fields.getValue("used_memory").toLong() > 0, "INFO reported no memory in use")
        assertTrue(fields.getValue("db0").startsWith("keys="), "INFO's keyspace section was ${fields["db0"]}")
        assertEquals(redis.dbSize(), fields.getValue("db0").removePrefix("keys=").toLong())
    }
}
