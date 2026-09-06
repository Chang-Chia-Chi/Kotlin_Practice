package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.CommandEngine
import dynacache.engine.Key
import dynacache.engine.PartitionContext
import dynacache.engine.Reply
import io.microraft.exception.CannotReplicateException
import io.microraft.exception.NotLeaderException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException

/**
 * The CP engine (CONTEXT.md): the command engine backed by the Raft log. [submit] replicates the
 * command and answers with the state machine's reply, so a reply is only ever observed after the
 * entry is committed on a majority of CP members and applied here (C21).
 */
class CpEngine(private val runtime: RaftRuntime) : CommandEngine {

    override fun submit(command: Command): CompletableFuture<Reply> {
        // C16 at the engine's edge: CP work only, and only on the cp: namespace.
        if (command !is Command.Cp) return answer(Reply.Error("NOTCP", "$command is not a CP command"))
        if (!command.key.isCp()) return answer(Reply.Error("NOTCP", "${command.key} is not a cp: key"))
        if (!runtime.isLeader) return answer(notLeader())

        return runtime.replicate(command).handle { committed, failure ->
            when {
                failure == null -> committed.result
                // Leadership moved (or was never here) between the check and the append. The
                // client retries against the hinted leader; forwarding on its behalf is T43.
                failure.cpCause() is NotLeaderException -> notLeader()
                failure.cpCause() is CannotReplicateException -> notLeader()
                else -> throw failure
            }
        }
    }

    override fun <R> atomically(keys: List<Key>, block: (PartitionContext) -> R): CompletableFuture<R> =
        throw NotImplementedError("CP has no batches: the Raft log already serializes every entry")

    override fun close() = runtime.close()

    private fun notLeader(): Reply {
        val leader = runtime.node.term.leaderEndpoint
        return Reply.Error("NOTLEADER", if (leader == null) "no known leader" else "leader is ${leader.id}")
    }

    private fun answer(reply: Reply) = CompletableFuture.completedFuture(reply)

    private fun Throwable.cpCause(): Throwable = if (this is CompletionException) cause ?: this else this

    private fun Key.isCp(): Boolean = toString().startsWith("cp:")
}
