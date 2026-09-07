package dynacache.cluster

import com.google.protobuf.ByteString
import dynacache.cluster.proto.ArrayReply
import dynacache.cluster.proto.BulkReply
import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.ErrorReply
import dynacache.cluster.proto.Forward
import dynacache.cluster.proto.ForwardReply
import dynacache.cluster.proto.ReplyMsg
import dynacache.engine.Command
import dynacache.engine.CommandEngine
import dynacache.engine.Reply
import dynacache.engine.persist.CommandCodec
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

/**
 * One node's request router (spec 5.1 steps 1 to 3, 5.2 step 1). It presents the
 * `CommandEngine` shape, so the node's RESP pipeline submits to it exactly as it submits to an
 * engine, and it decides one thing: whether this node is the key's **coordinator**. If it is,
 * the command runs on [local]; if it is not, the command crosses to the coordinator as a `Forward`
 * carrying the engine codec's bytes, and its answer comes back as a `ForwardReply`, unchanged,
 * errors included. No RESP spelling of a command crosses the cluster seam (T64).
 *
 * It is not the **dispatcher** (CONTEXT.md), which chooses between the AP and the CP engine by
 * namespace; the router sits under that choice and moves one command between nodes.
 *
 * The router does not read the transport: it is one handler of the node's [InboundLoop], which
 * owns the order every handler is offered an envelope in (T68). [receive] takes the two bodies
 * that are the router's, `Forward` and `ForwardReply`, and says no to everything else.
 *
 * @param local the engine that runs a command this node coordinates.
 * @param scope the node's lifecycle scope; a forward and its deadline live on it.
 * @param deadline how long a forward may take before its future answers with an error.
 */
class Router(
    val self: NodeId,
    private val ring: Ring,
    private val n: Int,
    private val local: CommandEngine,
    private val transport: Outbound,
    private val scope: CoroutineScope,
    private val deadline: Duration = 2.seconds,
) : CommandEngine {

    private val pending = ConcurrentHashMap<Long, CompletableFuture<Reply>>()
    private val ids = AtomicLong()

    /**
     * Spec 5.1 steps 2 and 3. Only a single-key command has a coordinator: a keyless command
     * answers for this node, and a multi-key one is [split] into single-key parts first.
     */
    override fun submit(command: Command): CompletableFuture<Reply> {
        if (command is Command.Fanned) return split(command)
        val key = (command as? Command.Keyed)?.key ?: return local.submit(command)
        val coordinator = ring.preferenceList(key, n).first()
        return if (coordinator == self) local.submit(command) else forward(coordinator, command)
    }

    /**
     * ADR 0002 across nodes: each part is routed like the single-key command it is, one after
     * the previous one answered so a repeated key keeps its last value, and the replies join in
     * argument order. Nothing is atomic across parts.
     */
    private fun split(command: Command.Fanned): CompletableFuture<Reply> {
        var parts = CompletableFuture.completedFuture(emptyList<Reply>())
        for (index in command.keys.indices) {
            parts = parts.thenCompose { replies -> submit(command.single(index)).thenApply { replies + it } }
        }
        return parts.thenApply(command::join)
    }

    override fun close() = local.close()

    /** One inbound envelope; true when it was the router's, false when it belongs to someone else. */
    suspend fun receive(envelope: Envelope): Boolean {
        when (envelope.bodyCase) {
            Envelope.BodyCase.FORWARD -> scope.launch { coordinate(NodeId(envelope.from), envelope.forward) }
            Envelope.BodyCase.FORWARD_REPLY ->
                pending.remove(envelope.forwardReply.id)?.complete(ReplyWire.decode(envelope.forwardReply.reply))
            else -> return false
        }
        return true
    }

    /**
     * This node is the coordinator: run what [from] forwarded and answer under the same id.
     * A forward the codec cannot read is an error reply rather than a throw, because the throw
     * would leave the node's [InboundLoop] dead and take its gossip down with its forwarding. A peer of this
     * build cannot write one; a corrupted or older envelope can, and that is what this catches.
     *
     * Each forwarded command runs on its own coroutine on [scope]: the coordinator's answer needs
     * the demux to keep reading, since its quorum's acks and read replies arrive there (T22).
     *
     * ponytail: two forwards from one contact may therefore run out of order at the coordinator;
     * a per-sender queue of forwards is the repair if a pipelining client ever observes it.
     */
    private suspend fun coordinate(from: NodeId, request: Forward) {
        // `single` is not a bet: only a `SET` the log wrote with a decided deadline decodes to two
        // commands, and a forward passes the codec no `now`, so it never carries one. A forward
        // that did would be a bug worth an error reply rather than a silent half.
        val reply = runCatching { CommandCodec.unframe(request.command.toByteArray()).single() }.fold(
            { local.submit(it).await() },
            { Reply.Error("ERR", "unreadable forwarded command: ${it.message}") },
        )
        send(
            from,
            Envelope.newBuilder().setForwardReply(
                ForwardReply.newBuilder().setId(request.id).setReply(ReplyWire.encode(reply))
            ),
        )
    }

    private fun forward(coordinator: NodeId, command: Command): CompletableFuture<Reply> {
        val id = ids.incrementAndGet()
        val answer = CompletableFuture<Reply>()
        pending[id] = answer
        val body = Forward.newBuilder().setId(id).setCommand(ByteString.copyFrom(CommandCodec.frame(command)))
        scope.launch {
            send(coordinator, Envelope.newBuilder().setForward(body))
            // The same coroutine is the deadline and the cleanup: an answer that arrived took
            // its future out of `pending`, so this completes nothing.
            delay(deadline)
            pending.remove(id)?.complete(
                Reply.Error("ERR", "forward timeout after $deadline waiting for $coordinator")
            )
        }
        return answer
    }

    private suspend fun send(to: NodeId, envelope: Envelope.Builder) =
        transport.send(to, envelope.setFrom(self.name).setTo(to.name).build())
}

/**
 * A [Reply] as the cluster's protobuf and back. The five RESP2 shapes map one to one, so
 * nothing about a reply survives the crossing differently than it went in.
 */
object ReplyWire {

    fun encode(reply: Reply): ReplyMsg {
        val message = ReplyMsg.newBuilder()
        when (reply) {
            is Reply.Simple -> message.simple = reply.text
            is Reply.Error -> message.error =
                ErrorReply.newBuilder().setKind(reply.kind).setMessage(reply.message).build()
            is Reply.Integer -> message.integer = reply.value
            is Reply.Bulk -> message.bulk = BulkReply.newBuilder()
                .also { if (reply.bytes == null) it.nil = true else it.value = ByteString.copyFrom(reply.bytes) }
                .build()
            is Reply.Array -> message.array =
                ArrayReply.newBuilder().addAllItem(reply.items.map(::encode)).build()
        }
        return message.build()
    }

    fun decode(message: ReplyMsg): Reply = when (message.kindCase) {
        ReplyMsg.KindCase.SIMPLE -> Reply.Simple(message.simple)
        ReplyMsg.KindCase.ERROR -> Reply.Error(message.error.kind, message.error.message)
        ReplyMsg.KindCase.INTEGER -> Reply.Integer(message.integer)
        ReplyMsg.KindCase.BULK -> Reply.Bulk(if (message.bulk.nil) null else message.bulk.value.toByteArray())
        ReplyMsg.KindCase.ARRAY -> Reply.Array(message.array.itemList.map(::decode))
        ReplyMsg.KindCase.KIND_NOT_SET -> error("a ForwardReply carried no reply")
    }
}
