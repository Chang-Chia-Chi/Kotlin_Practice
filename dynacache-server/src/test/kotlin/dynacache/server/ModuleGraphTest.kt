package dynacache.server

import dynacache.engine.Command
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

/** The server module reaches the engine through the cluster module (plan 2.2). */
class ModuleGraphTest {

    @Test
    fun `the server module sees the engine's frozen command and reply types`() {
        assertEquals(Reply.Simple("PONG"), Reply.Simple("PONG"))
        assertThrows(NotImplementedError::class.java) {
            dynacache.engine.ApEngine().submit(Command.Ping)
        }
    }
}
