package dynacache.server

import dynacache.engine.ApEngine
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

private fun bulk(text: String) = Reply.Bulk(text.toByteArray(Charsets.ISO_8859_1))

/**
 * Bytes in, bytes out over a real socket: the far side of the ticket's second seam. Nothing here
 * reaches inside the server; a test sees only what a Redis client would see.
 */
class DynaCacheServerTest {

    /** The engine under the running server, for a test that has to know where a key lives. */
    private lateinit var running: ApEngine

    /** A key the running engine puts on a partition other than [key]'s. */
    private fun otherPartitionThan(key: String): String =
        (0..99).map { "z$it" }.first { running.partitionOf(Key(it)) != running.partitionOf(Key(key)) }

    /**
     * A server on an ephemeral port with a real engine, torn down whatever the body does. With no
     * [onTick] the server schedules the engine's own tick, which is what production runs.
     */
    private fun withServer(
        partitionCount: Int = 16,
        tickMillis: Long = 1000,
        onTick: (() -> Unit)? = null,
        body: (DynaCacheServer) -> Unit,
    ) {
        val clock = Clock.fixed(Instant.ofEpochSecond(1_000_000), ZoneOffset.UTC)
        val engine = ApEngine(partitionCount, clock, tickMillis = tickMillis)
        running = engine
        // The server reads the same clock as the engine under it, as `main` gives it in production:
        // a deadline the parser works out is otherwise measured from a different "now" than the
        // one the engine compares it against.
        val server =
            if (onTick == null) DynaCacheServer(port = 0, engine = engine, clock = clock)
            else DynaCacheServer(port = 0, engine = engine, clock = clock, tick = onTick)
        try {
            server.start()
            body(server)
        } finally {
            server.close()
            engine.close()
        }
    }

    @Test
    fun server_multi_exec_roundtrip() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                RespClient(server.boundPort).use { onlooker ->
                    client.send("MULTI")
                    assertEquals(Reply.Simple("OK"), client.read())
                    listOf(
                        arrayOf("SET", "{t}.a", "1"),
                        arrayOf("INCR", "{t}.a"),
                        arrayOf("GET", "{t}.a"),
                    ).forEach { words ->
                        client.send(*words)
                        assertEquals(Reply.Simple("QUEUED"), client.read(), words.joinToString(" "))
                    }

                    // Buffered, not executed: another connection cannot see the key yet.
                    onlooker.send("GET", "{t}.a")
                    assertEquals(Reply.Bulk(null), onlooker.read())

                    client.send("EXEC")
                    assertEquals(
                        Reply.Array(listOf(Reply.Simple("OK"), Reply.Integer(2), bulk("2"))),
                        client.read(),
                        "one array, one reply per queued command, in order",
                    )
                    onlooker.send("GET", "{t}.a")
                    assertEquals(bulk("2"), onlooker.read(), "and now the whole batch is visible")
                }
            }
        }
    }

    @Test
    fun multi_exec_cross_partition_rejected() {
        withServer { server ->
            val here = "a"
            val elsewhere = otherPartitionThan(here)
            RespClient(server.boundPort).use { client ->
                client.send("MULTI")
                assertEquals(Reply.Simple("OK"), client.read())
                client.send("SET", here, "1")
                assertEquals(Reply.Simple("QUEUED"), client.read())
                client.send("SET", elsewhere, "2")
                assertEquals(Reply.Simple("QUEUED"), client.read())

                client.send("EXEC")
                assertEquals(
                    Reply.Error("CROSSSLOT", "keys of a batch must share a partition (use a hash tag)"),
                    client.read(),
                )
                client.send("GET", here)
                assertEquals(Reply.Bulk(null), client.read(), "the span was refused before anything ran")
                client.send("GET", elsewhere)
                assertEquals(Reply.Bulk(null), client.read(), "the span was refused before anything ran")
            }
        }
    }

    @Test
    fun discard_clears_buffer() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                client.send("SET", "k", "before")
                assertEquals(Reply.Simple("OK"), client.read())
                client.send("MULTI")
                assertEquals(Reply.Simple("OK"), client.read())
                client.send("SET", "k", "after")
                assertEquals(Reply.Simple("QUEUED"), client.read())

                client.send("DISCARD")
                assertEquals(Reply.Simple("OK"), client.read())
                client.send("GET", "k")
                assertEquals(bulk("before"), client.read(), "the buffered SET never ran")
                client.send("EXEC")
                assertEquals(Reply.Error("ERR", "EXEC without MULTI"), client.read(), "and the buffer is gone")
            }
        }
    }

    @Test
    fun `a parse error while queued makes EXEC abort the whole transaction`() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                client.send("MULTI")
                assertEquals(Reply.Simple("OK"), client.read())
                client.send("SET", "k", "v")
                assertEquals(Reply.Simple("QUEUED"), client.read())

                // Redis names the bad command straight away and refuses the lot at EXEC.
                client.send("NOSUCH", "x")
                assertEquals(
                    Reply.Error("ERR", "unknown command 'nosuch', with args beginning with: 'x', "),
                    client.read(),
                )
                client.send("EXEC")
                assertEquals(
                    Reply.Error("EXECABORT", "Transaction discarded because of previous errors."),
                    client.read(),
                )
                client.send("GET", "k")
                assertEquals(Reply.Bulk(null), client.read(), "the queued SET never ran")
                client.send("MULTI")
                assertEquals(Reply.Simple("OK"), client.read(), "and the connection starts clean")
            }
        }
    }

    @Test
    fun `MULTI does not nest, and EXEC and DISCARD need one`() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                client.send("EXEC")
                assertEquals(Reply.Error("ERR", "EXEC without MULTI"), client.read())
                client.send("DISCARD")
                assertEquals(Reply.Error("ERR", "DISCARD without MULTI"), client.read())

                client.send("MULTI")
                assertEquals(Reply.Simple("OK"), client.read())
                client.send("MULTI")
                assertEquals(Reply.Error("ERR", "MULTI calls can not be nested"), client.read())
                client.send("EXEC")
                assertEquals(Reply.Array(emptyList()), client.read(), "the refused MULTI left the first one alone")
            }
        }
    }

    @Test
    fun server_ping_pong() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                client.send("PING")
                assertEquals(Reply.Simple("PONG"), client.read())

                // The inline form redis-cli uses interactively reaches the same command.
                client.sendRaw("ping\r\n")
                assertEquals(Reply.Simple("PONG"), client.read())
            }
        }
    }

    @Test
    fun server_pipelined_replies_in_order() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                // 100 keys over 16 partitions, so the futures behind them complete out of order.
                val pairs = 100
                repeat(pairs) { i ->
                    client.send("SET", "key:$i", "value:$i")
                    client.send("GET", "key:$i")
                }
                repeat(pairs) { i ->
                    assertEquals(Reply.Simple("OK"), client.read(), "reply ${2 * i}")
                    assertEquals(bulk("value:$i"), client.read(), "reply ${2 * i + 1}")
                }
            }
        }
    }

    @Test
    fun server_unknown_command_error() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                client.send("NOSUCH", "a", "b")
                assertEquals(
                    Reply.Error("ERR", "unknown command 'nosuch', with args beginning with: 'a', 'b', "),
                    client.read(),
                )
                // The connection survives a command error: the next command still answers.
                client.send("PING")
                assertEquals(Reply.Simple("PONG"), client.read())
            }
        }
    }

    @Test
    fun server_arity_error() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                client.send("GET")
                assertEquals(Reply.Error("ERR", "wrong number of arguments for 'get' command"), client.read())
                client.send("SET", "k")
                assertEquals(Reply.Error("ERR", "wrong number of arguments for 'set' command"), client.read())
                client.send("PING")
                assertEquals(Reply.Simple("PONG"), client.read())
            }
        }
    }

    /**
     * C8: an expiry the server cannot honour is the error Redis writes, on a connection that is
     * still there afterwards. The `SET` family refuses a zero or negative span; every family
     * refuses a deadline past what epoch milliseconds can count. What Redis does *not* refuse is
     * a deadline already gone: `EXPIRE k -1` deletes the key, so it stays a command here.
     */
    @Test
    fun C8_invalid_expire_answers_err_not_disconnect() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                client.send("SET", "k", "v")
                assertEquals(Reply.Simple("OK"), client.read())

                val far = "${Long.MAX_VALUE}"
                for (wire in listOf(
                    listOf("EXPIRE", "k", far),
                    listOf("PEXPIRE", "k", far),
                    listOf("EXPIREAT", "k", "99999999999999999"),
                    listOf("PEXPIREAT", "k", "99999999999999999999"),
                    listOf("SET", "k", "v", "EX", "0"),
                    listOf("SET", "k", "v", "EX", "-1"),
                    listOf("SET", "k", "v", "EX", far),
                    listOf("SET", "k", "v", "PX", "0"),
                    listOf("SET", "k", "v", "PX", "-1"),
                    listOf("SET", "k", "v", "PX", far),
                    listOf("SETEX", "k", "0", "v"),
                    listOf("SETEX", "k", "-1", "v"),
                    listOf("SETEX", "k", far, "v"),
                )) {
                    client.send(*wire.toTypedArray())
                    val reply = client.read()
                    assertTrue(reply is Reply.Error, "${wire.joinToString(" ")} answered $reply")
                    // PEXPIREAT cannot overflow -- every Long is a deadline -- so its argument is
                    // refused one step earlier, as the integer it is too big to be.
                    val expected =
                        if (wire[0] == "PEXPIREAT") "value is not an integer or out of range"
                        else "invalid expire time in '${wire[0].lowercase()}' command"
                    assertEquals(Reply.Error("ERR", expected), reply, wire.joinToString(" "))
                }

                // Thirteen bad arguments later, the connection is the same connection.
                client.send("PING")
                assertEquals(Reply.Simple("PONG"), client.read())
                client.send("GET", "k")
                assertEquals(bulk("v"), client.read(), "no invalid expiry touched the key")

                // And the deadline already past is honoured rather than refused, as in Redis.
                client.send("EXPIRE", "k", "-1")
                assertEquals(Reply.Integer(1), client.read())
                client.send("GET", "k")
                assertEquals(Reply.Bulk(null), client.read(), "a past deadline deletes the key")
            }
        }
    }

    @Test
    fun `a protocol error is answered once and closes the connection`() {
        withServer { server ->
            RespClient(server.boundPort).use { client ->
                client.sendRaw("*1\r\n+PING\r\n")
                assertEquals(Reply.Error("ERR", "Protocol error: expected '$', got '+'"), client.read())
                assertTrue(client.serverClosed(), "the server must hang up after a protocol error")
            }
        }
    }

    /**
     * C7 needs the wheel advanced at least once per `tickMillis`, and the server owns the only
     * thread that does it (plan 2.3). Three ticks at a 20 ms period prove it repeats rather than
     * firing once; the latch is a bounded await on a real event, not a sleep.
     */
    @Test
    fun `the scheduler drives the engine's tick while the server is up`() {
        val ticked = CountDownLatch(3)
        withServer(partitionCount = 1, tickMillis = 20, onTick = ticked::countDown) {
            assertTrue(ticked.await(10, TimeUnit.SECONDS), "the scheduler never ran three times")
        }
    }
}
