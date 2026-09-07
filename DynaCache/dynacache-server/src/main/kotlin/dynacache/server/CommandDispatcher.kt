package dynacache.server

import dynacache.engine.Command
import dynacache.engine.CommandEngine
import dynacache.engine.CpNamespace
import dynacache.engine.CpRouting
import dynacache.engine.Reply
import java.util.concurrent.CompletableFuture

/**
 * The dispatcher (CONTEXT.md): the one place the AP engine and the CP engine meet. Routing is all
 * it does. [CpNamespace] holds the rule -- which primitive owns a `cp:` key and which Redis
 * commands may touch it -- and answers for every command with the CP verb it means, the refusal
 * the namespace gives it, or nothing at all, which is the AP engine's (CP spec 9.5).
 *
 * A compat command and the CP verb it means are one command by the time an engine sees them, so
 * `INCR cp:counter:x` and `CP.LONG.INCR cp:counter:x` are indistinguishable downstream. The
 * dispatcher never rewrites a reply, and never sends the same command to both engines, which is
 * what C16 and C22 are.
 *
 * [cp] is null on a node with no CP subsystem configured; there, every CP-bound command is
 * `-NOTCP`.
 */
class CommandDispatcher(
    private val ap: CommandEngine,
    private val cp: CommandEngine?,
) : CommandEngine {

    override fun submit(command: Command): CompletableFuture<Reply> = when (val routing = CpNamespace.route(command)) {
        is CpRouting.Verb -> cp?.submit(routing.command) ?: done(CpNamespace.notCp("this node has no CP engine"))
        is CpRouting.Refused -> done(routing.error)
        CpRouting.Ap -> ap.submit(command)
    }

    /** Closes the AP engine and, when this node has one, the CP engine. */
    override fun close() {
        ap.close()
        cp?.close()
    }

    private fun done(reply: Reply): CompletableFuture<Reply> = CompletableFuture.completedFuture(reply)
}
