package dynacache.server

import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

/** The server module reaches the engine through the cluster module (plan 2.2). */
class ModuleGraphTest {

    @Test
    fun `the server module sees the engine's frozen command and reply types`() {
        val engine = ApEngine(partitionCount = 1, clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC))
        try {
            assertEquals(Reply.Simple("PONG"), engine.submit(Command.Ping).get())
        } finally {
            engine.close()
        }
    }
}
