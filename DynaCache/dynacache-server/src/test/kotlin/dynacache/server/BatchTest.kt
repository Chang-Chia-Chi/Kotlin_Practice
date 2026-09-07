package dynacache.server

import dynacache.engine.BatchEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.PartitionContext
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture

/**
 * The batch capability the connection handler holds (plan 2.3, T73). `MULTI`/`EXEC` and `EVAL`
 * are the only two callers, and both reach a [BatchEngine] rather than the engine seam every
 * adapter implements: the AP engine runs a batch, the CP engine has none to run.
 *
 * What the handler adds on top of the capability is the `cp:` refusal C16 used to make in the
 * dispatcher: a batch naming a CP key never runs, because the two engines share no partition.
 */
class BatchTest {

    /** The capability's test adapter: records the declared keys and runs the block with no store. */
    private class Recording : BatchEngine {

        val batches = mutableListOf<List<Key>>()

        override fun <R> atomically(keys: List<Key>, block: (PartitionContext) -> R): CompletableFuture<R> {
            batches += keys
            return CompletableFuture.completedFuture(block(NoStore))
        }
    }

    private object NoStore : PartitionContext {
        override fun execute(command: Command): Reply = Reply.Error("ERR", "no partition in this test")
    }

    private val engine = Recording()

    @Test
    fun C16_a_batch_naming_a_cp_key_never_reaches_the_engine() {
        val answer = engine.runBatch(listOf(Key("plain"), Key("cp:counter:x"))) { Reply.Simple("OK") }
        assertEquals(Reply.Error("ERR", "a batch cannot name a cp: key"), answer.get())
        assertTrue(engine.batches.isEmpty(), "the engine was given ${engine.batches}")
    }

    @Test
    fun I11_a_batch_answers_what_its_block_returned() {
        val keys = listOf(Key("{t}.a"), Key("{t}.b"))
        val replies = Reply.Array(listOf(Reply.Simple("OK"), Reply.Integer(1)))
        assertEquals(replies, engine.runBatch(keys) { replies }.get())
        assertEquals(listOf(keys), engine.batches)
    }
}
