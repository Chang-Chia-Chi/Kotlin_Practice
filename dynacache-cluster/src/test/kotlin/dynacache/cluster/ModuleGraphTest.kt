package dynacache.cluster

import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/** The cluster module is built on the engine (plan 2.2); Maven enforces the direction. */
class ModuleGraphTest {

    @Test
    fun `the cluster module sees the engine's frozen key and reply types`() {
        assertEquals(Key("{user1}.a").hash, Key("{user1}.b").hash)
        assertEquals(Reply.Bulk("PONG".toByteArray()), Reply.Bulk("PONG".toByteArray()))
    }
}
