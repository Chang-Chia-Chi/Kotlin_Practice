package dynacache.server

import dynacache.engine.ApEngine
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
        val server =
            if (onTick == null) DynaCacheServer(port = 0, engine = engine)
            else DynaCacheServer(port = 0, engine = engine, tick = onTick)
        try {
            server.start()
            body(server)
        } finally {
            server.close()
            engine.close()
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
