package dynacache.cluster

import dynacache.engine.Command
import dynacache.engine.CommandEngine
import dynacache.engine.Reply
import java.util.concurrent.CompletableFuture

/**
 * The test kit's adapter of the `CommandEngine` seam (plan 2.3): records every submitted
 * command and answers each with one canned [reply], so a router or dispatcher test asserts
 * what reached the engine without running one.
 */
class RecordingEngine(private val reply: Reply = Reply.Simple("OK")) : CommandEngine {

    val submitted = mutableListOf<Command>()

    override fun submit(command: Command): CompletableFuture<Reply> {
        submitted.add(command)
        return CompletableFuture.completedFuture(reply)
    }

    override fun close() = Unit
}
