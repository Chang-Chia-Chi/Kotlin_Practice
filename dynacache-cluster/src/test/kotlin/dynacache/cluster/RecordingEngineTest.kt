package dynacache.cluster

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/** The test kit's fake at the `CommandEngine` seam (plan 2.3): what the router of T19 talks to. */
class RecordingEngineTest {

    @Test
    fun `the recording engine records every submit and answers with the canned reply`() = runTest {
        val engine = RecordingEngine(reply = Reply.Integer(1))
        val get = Command.Get(Key("orders:4711"))

        assertEquals(Reply.Integer(1), engine.submit(Command.Ping).await())
        assertEquals(Reply.Integer(1), engine.submit(get).await())

        assertEquals(listOf(Command.Ping, get), engine.submitted)
    }
}
