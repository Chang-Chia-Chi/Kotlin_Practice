package dynacache.cluster

import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Reply
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

class AwaitTest {

    @Test
    fun `a coroutine awaits the engine's reply without blocking on the future`() {
        val engine = ApEngine(partitionCount = 1, clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC))
        try {
            val reply = runBlocking { engine.submit(Command.Ping).await() }
            assertEquals(Reply.Simple("PONG"), reply)
        } finally {
            engine.close()
        }
    }
}
