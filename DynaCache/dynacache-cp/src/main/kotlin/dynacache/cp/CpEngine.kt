package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.CommandEngine
import dynacache.engine.Key
import dynacache.engine.PartitionContext
import dynacache.engine.Reply
import io.microraft.exception.CannotReplicateException
import io.microraft.exception.IndeterminateStateException
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
        // CP spec 6.7 asks this member what it can see, which every member can answer.
        if (command is Command.Cp.Introspection) return introspect(command)
        if (!runtime.isLeader) return answer(notLeader())

        return runtime.replicate(command).handle { committed, failure ->
            when {
                failure == null -> committed.result
                // Leadership moved (or was never here) between the check and the append. The
                // client retries against the hinted leader; forwarding on its behalf is T43.
                failure.cpCause() is NotLeaderException -> notLeader()
                failure.cpCause() is CannotReplicateException -> notLeader()
                // A leader that lost quorum mid-append cannot know whether the entry committed. It
                // never answers success: the client retries against the next leader (CP spec 9.1
                // step 7), the same at-least-once contract a NOTLEADER retry already carries.
                failure.cpCause() is IndeterminateStateException -> notLeader()
                else -> throw failure
            }
        }
    }

    override fun <R> atomically(keys: List<Key>, block: (PartitionContext) -> R): CompletableFuture<R> =
        throw NotImplementedError("CP has no batches: the Raft log already serializes every entry")

    override fun close() = runtime.close()

    /** `CP.INFO` and `CP.MEMBERS` off this member's own report; neither becomes a log entry. */
    private fun introspect(command: Command.Cp.Introspection): CompletableFuture<Reply> =
        runtime.node.report.thenApply { ordered ->
            val info = CpWire.info(ordered.result)
            when (command) {
                Command.Cp.Info -> CpWire.infoReply(info)
                Command.Cp.Members -> Reply.Array(info.membersList.map { Reply.Bulk(it.toByteArray()) })
            }
        }

    /**
     * CP spec 6.8's `-NOTLEADER <hint>`: the hint is the leader's member id alone, since a client
     * retries at the first token of the message. A member that knows of no leader has no hint to
     * give and says only `-NOTLEADER`; the client asks the group again.
     */
    private fun notLeader(): Reply =
        Reply.Error("NOTLEADER", runtime.node.term.leaderEndpoint?.id?.toString() ?: "")

    private fun answer(reply: Reply) = CompletableFuture.completedFuture(reply)

    private fun Throwable.cpCause(): Throwable = if (this is CompletionException) cause ?: this else this

    private fun Key.isCp(): Boolean = toString().startsWith("cp:")
}
