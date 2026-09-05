package dynacache.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class CommandEngineTest {

    private val engine: CommandEngine = ApEngine()

    @Test
    fun `submit is a stub until the partition executors arrive`() {
        assertThrows(NotImplementedError::class.java) { engine.submit(Command.Ping) }
    }

    @Test
    fun `atomically is a stub until the partition executors arrive`() {
        assertThrows(NotImplementedError::class.java) {
            engine.atomically(listOf(Key("{user1}.a"), Key("{user1}.b"))) { ctx ->
                ctx.execute(Command.Ping)
            }
        }
    }

    @Test
    fun `a partition is identified by its index`() {
        assertEquals(PartitionId(3), PartitionId(3))
        assertNotEquals(PartitionId(3), PartitionId(4))
    }
}
