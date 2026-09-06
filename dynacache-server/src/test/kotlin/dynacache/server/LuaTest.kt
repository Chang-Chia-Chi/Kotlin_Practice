package dynacache.server

import dynacache.engine.ApEngine
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

/**
 * The two seams this ticket adds: the sandboxed [sandboxedGlobals] a script runs in, and the
 * socket every script arrives and answers on. Nothing here reaches inside the interpreter.
 */
class LuaTest {

    /** The engine under the running server, for a test that has to know where a key lives. */
    private lateinit var running: ApEngine

    private fun withServer(body: (DynaCacheServer) -> Unit) {
        val clock = Clock.fixed(Instant.ofEpochSecond(1_000_000), ZoneOffset.UTC)
        val engine = ApEngine(16, clock)
        running = engine
        val server = DynaCacheServer(port = 0, engine = engine)
        try {
            server.start()
            body(server)
        } finally {
            server.close()
            engine.close()
        }
    }

    /** A key the running engine puts on a partition other than [key]'s. */
    private fun otherPartitionThan(key: String): String =
        (0..99).map { "z$it" }.first { running.partitionOf(Key(it)) != running.partitionOf(Key(key)) }

    // ---- the sandbox, at the globals factory ---------------------------------------------------

    @Test
    fun C11_clock_and_random_unavailable() {
        val globals = sandboxedGlobals()

        // The clock, the random number generator, and every door to the OS and the filesystem.
        listOf("os", "io", "require", "package", "load", "loadstring", "dofile", "loadfile", "debug", "luajava")
            .forEach { assertTrue(globals.get(it).isnil(), "global '$it' must not be reachable") }
        listOf("random", "randomseed").forEach {
            assertTrue(globals.get("math").get(it).isnil(), "math.$it must not be reachable")
        }

        // What a script actually sees: C11's named readings of the clock and the random source
        // either are not there or refuse to be reached.
        listOf("os.time", "os.clock", "os.date", "math.random").forEach { expression ->
            val outcome = runCatching { globals.load("return $expression", "t").call() }
            assertTrue(
                outcome.isFailure || outcome.getOrThrow().isnil(),
                "$expression must be nil or error, was ${outcome.getOrNull()}",
            )
        }

        // What is left is still a working Lua.
        assertEquals("HI", globals.load("return string.upper('hi')", "t").call().tojstring())
    }

    @Test
    fun sandbox_carries_no_state_between_calls() {
        sandboxedGlobals().load("counter = 41", "t").call()

        assertTrue(sandboxedGlobals().get("counter").isnil(), "a fresh globals sees no earlier script's global")
    }

    // ---- EVAL, at the socket -------------------------------------------------------------------

    @Test
    fun lua_keys_argv() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                client.send(
                    "EVAL",
                    "return {#KEYS, KEYS[1], KEYS[2], #ARGV, ARGV[1]}",
                    "2", "{s}.one", "{s}.two", "first", "second",
                )

                assertEquals(
                    Reply.Array(
                        listOf(Reply.Integer(2), bulk("{s}.one"), bulk("{s}.two"), Reply.Integer(2), bulk("first")),
                    ),
                    client.read(),
                    "KEYS and ARGV are 1-indexed and hold exactly what numkeys split",
                )
            }
        }
    }

    @Test
    fun lua_redis_call() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                client.send(
                    "EVAL",
                    "redis.call('SET', KEYS[1], ARGV[1]) return redis.call('GET', KEYS[1])",
                    "1", "counter", "41",
                )
                assertEquals(bulk("41"), client.read(), "the script's write is readable by the script")

                // The spec's own worked example: read, add, write back, all on one partition.
                client.send(
                    "EVAL",
                    "redis.call('SET', KEYS[1], redis.call('GET', KEYS[1]) + 1) return redis.call('GET', KEYS[1])",
                    "1", "counter",
                )
                assertEquals(bulk("42"), client.read())

                // And the write outlived the script, which is the point of it reaching the engine.
                client.send("GET", "counter")
                assertEquals(bulk("42"), client.read())
            }
        }
    }

    @Test
    fun lua_undeclared_key_is_an_error() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                // Same partition as the declared key, so only the declaration itself refuses it.
                client.send("EVAL", "return redis.call('SET', '{t}.b', 'x')", "1", "{t}.a")

                assertEquals(Reply.Error("ERR", "{t}.b was not declared by this batch"), client.read())

                client.send("GET", "{t}.b")
                assertEquals(Reply.Bulk(null), client.read(), "the refused write did not land")
            }
        }
    }

    @Test
    fun lua_refused_command_inside_script_is_an_error() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                client.send("EVAL", "return redis.call('SCAN', '0')", "0")

                assertEquals(
                    Reply.Error("ERR", "this command spans partitions and cannot run inside a batch"),
                    client.read(),
                )
            }
        }
    }

    @Test
    fun lua_pcall_returns_the_error_rather_than_raising() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                client.send("SET", "word", "abc")
                assertEquals(Reply.Simple("OK"), client.read())

                client.send("EVAL", "local r = redis.pcall('INCR', KEYS[1]) return 'caught: ' .. r['err']", "1", "word")

                assertEquals(bulk("caught: ERR value is not an integer or out of range"), client.read())
            }
        }
    }

    @Test
    fun lua_cross_partition_rejected() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                val elsewhere = otherPartitionThan("here")
                client.send("EVAL", "redis.call('SET', KEYS[1], 'ran') return 1", "2", "here", elsewhere)

                assertEquals(Reply.Error("CROSSSLOT", "Keys in request don't hash to the same slot"), client.read())

                // Rejected before execution (C12): not a line of the script ran.
                client.send("GET", "here")
                assertEquals(Reply.Bulk(null), client.read())
            }
        }
    }

    @Test
    fun lua_no_side_effects() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                listOf("os.execute('echo hi')", "io.open('/etc/passwd')", "math.random()", "luajava.bindClass('java.lang.System')")
                    .forEach { attempt ->
                        client.send("EVAL", "return $attempt", "0")
                        val reply = client.read()
                        assertTrue(
                            reply is Reply.Error && reply.message.startsWith("Error running script"),
                            "$attempt must fail the script, was $reply",
                        )
                    }
            }
        }
    }

    /**
     * Spec 5.7's conversion rules, one row per rule. The Lua-to-Redis rows say what a script's
     * return becomes on the wire; the Redis-to-Lua rows make the script report what a
     * `redis.call` result looks like from inside, which is the only place that direction shows.
     * Every row declares the one key `{c}.k` and runs against its own engine.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("conversions")
    fun lua_type_conversion_table(rule: String, script: String, expected: Reply) {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                client.send("EVAL", script, "1", "{c}.k")

                assertEquals(expected, client.read(), rule)
            }
        }
    }

    @Test
    fun lua_deterministic() {
        val script = """
            local total = 0
            for i = 1, 10 do total = total + i end
            redis.call('SET', KEYS[1], total)
            redis.call('RPUSH', KEYS[2], 'a', 'b', 'c')
            return { redis.call('GET', KEYS[1]), redis.call('LRANGE', KEYS[2], 0, -1), string.rep('x', 3), #KEYS }
        """.trimIndent()

        val replies = List(2) { replicaReply(script) }

        assertEquals(replies[0], replies[1], "the same script on the same state answers the same on every node")
        assertEquals(
            Reply.Array(
                listOf(
                    bulk("55"),
                    Reply.Array(listOf(bulk("a"), bulk("b"), bulk("c"))),
                    bulk("xxx"),
                    Reply.Integer(2),
                ),
            ),
            replies[0],
            "and the answer is the one the script's inputs determine, not an empty one",
        )
    }

    /** One replica: a fresh engine, the same seed state, the same script, its reply. */
    private fun replicaReply(script: String): Reply {
        val engine = ApEngine(16, Clock.fixed(Instant.ofEpochSecond(1_000_000), ZoneOffset.UTC))
        val server = DynaCacheServer(port = 0, engine = engine)
        try {
            server.start()
            RespClient(server.boundPort).use { client ->
                client.send("SET", "{d}.a", "seed")
                client.read()
                client.send("EVAL", script, "2", "{d}.a", "{d}.b")
                return client.read()
            }
        } finally {
            server.close()
            engine.close()
        }
    }

    companion object {

        @JvmStatic
        fun conversions(): List<Arguments> = listOf(
            // Lua to Redis (spec 5.7)
            row("number to integer", "return 41", Reply.Integer(41)),
            row("number truncates, it does not round", "return 3.9", Reply.Integer(3)),
            row("a negative number truncates toward zero", "return -3.9", Reply.Integer(-3)),
            row("string to bulk", "return 'hello'", bulk("hello")),
            row("true to :1", "return true", Reply.Integer(1)),
            row("false to nil bulk", "return false", Reply.Bulk(null)),
            row("nil to nil bulk", "return nil", Reply.Bulk(null)),
            row("no return at all is nil", "local unused = 1", Reply.Bulk(null)),
            row(
                "table to array",
                "return {1, 'two', true}",
                Reply.Array(listOf(Reply.Integer(1), bulk("two"), Reply.Integer(1))),
            ),
            row("an array stops at its first hole", "return {1, nil, 3}", Reply.Array(listOf(Reply.Integer(1)))),
            row("a nested table nests", "return {{1}}", Reply.Array(listOf(Reply.Array(listOf(Reply.Integer(1)))))),
            row("ok field to simple string", "return {ok = 'DONE'}", Reply.Simple("DONE")),
            row("err field to error", "return {err = 'MYKIND it went wrong'}", Reply.Error("MYKIND", "it went wrong")),

            // Redis to Lua: what the script sees coming back out of redis.call
            row(
                "integer reply to number",
                "return type(redis.call('INCR', KEYS[1]))",
                bulk("number"),
            ),
            row(
                "bulk reply to string",
                "redis.call('SET', KEYS[1], 'v') return type(redis.call('GET', KEYS[1]))",
                bulk("string"),
            ),
            row(
                "nil bulk reply to false",
                "return tostring(redis.call('GET', KEYS[1]))",
                bulk("false"),
            ),
            row(
                "array reply to table",
                "redis.call('RPUSH', KEYS[1], 'a', 'b') local l = redis.call('LRANGE', KEYS[1], 0, -1) " +
                    "return type(l) .. '/' .. #l .. '/' .. l[2]",
                bulk("table/2/b"),
            ),
            row(
                "simple string reply to an ok table",
                "return redis.call('SET', KEYS[1], 'v')['ok']",
                bulk("OK"),
            ),
            row(
                "error reply to an err table, through pcall",
                "redis.call('SET', KEYS[1], 'abc') return redis.pcall('INCR', KEYS[1])['err']",
                bulk("ERR value is not an integer or out of range"),
            ),
            row(
                "a Lua number argument reaches the command as its string form",
                "redis.call('SET', KEYS[1], 41) return redis.call('GET', KEYS[1])",
                bulk("41"),
            ),
        )

        private fun row(rule: String, script: String, expected: Reply) = Arguments.of(rule, script, expected)
    }
}

private fun bulk(text: String) = Reply.Bulk(text.toByteArray(Charsets.ISO_8859_1))
