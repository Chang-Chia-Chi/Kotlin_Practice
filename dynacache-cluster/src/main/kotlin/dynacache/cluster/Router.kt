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
import dynacache.engine.Key
import dynacache.engine.PartitionContext
import dynacache.engine.Reply
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
 * the command runs on [local]; if it is not, the command's tokens cross to the coordinator as a
 * `Forward` and its answer comes back as a `ForwardReply`, unchanged, errors included.
 *
 * It is not the **dispatcher** (CONTEXT.md), which chooses between the AP and the CP engine by
 * namespace; the router sits under that choice and moves one command between nodes.
 *
 * [run] is the node's one inbound loop and owns the demux: [snapshots] sees every envelope
 * first and keeps the Chandy-Lamport markers (T36); `Forward` and `ForwardReply` are the
 * router's, and every other envelope goes to [others]: `Replication.receive` first and then
 * `Swim::deliver`, composed by whoever wires the node.
 *
 * @param local the engine that runs a command this node coordinates.
 * @param tokens the wire form of a command: what a client would have sent for it.
 * @param parse the inverse, applied to the tokens a peer forwarded.
 * @param scope the node's lifecycle scope; a forward and its deadline live on it.
 * @param deadline how long a forward may take before its future answers with an error.
 * @param others where every envelope that is not a forward goes.
 * @param snapshots `DistributedSnapshot.receive`: records in-flight envelopes and consumes markers.
 */
class Router(
    val self: NodeId,
    private val ring: Ring,
    private val n: Int,
    private val local: CommandEngine,
    private val transport: Transport,
    private val tokens: (Command) -> List<ByteArray>,
    private val parse: (List<ByteArray>) -> Command,
    private val scope: CoroutineScope,
    private val deadline: Duration = 2.seconds,
    private val others: suspend (Envelope) -> Unit = {},
    private val snapshots: suspend (Envelope) -> Boolean = { false },
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

    /**
     * A batch runs only on the coordinator of its keys: it is one partition's uninterrupted
     * run (C12), and a forward would have to carry the caller's block, which is code. A batch
     * whose keys this node does not coordinate fails the future rather than answering, since
     * the signature's `R` is the caller's own type and has no error shape.
     */
    override fun <R> atomically(keys: List<Key>, block: (PartitionContext) -> R): CompletableFuture<R> {
        val elsewhere = keys.map { it to ring.preferenceList(it, n).first() }.firstOrNull { it.second != self }
        if (elsewhere != null) {
            return CompletableFuture.failedFuture(
                IllegalStateException("${elsewhere.first} is coordinated by ${elsewhere.second}, not $self")
            )
        }
        return local.atomically(keys, block)
    }

    override fun close() = local.close()

    /** The node's inbound loop: the demux, until the transport closes. */
    suspend fun run() {
        for (envelope in transport.inbound) receive(envelope)
    }

    /** One inbound envelope. Public so a test can hand the router one without a loop. */
    suspend fun receive(envelope: Envelope) {
        if (snapshots(envelope)) return
        when (envelope.bodyCase) {
            Envelope.BodyCase.FORWARD -> scope.launch { coordinate(NodeId(envelope.from), envelope.forward) }
            Envelope.BodyCase.FORWARD_REPLY ->
                pending.remove(envelope.forwardReply.id)?.complete(ReplyWire.decode(envelope.forwardReply.reply))
            else -> others(envelope)
        }
    }

    /**
     * This node is the coordinator: run what [from] forwarded and answer under the same id.
     * Tokens this node cannot read are an error reply rather than a throw, because the throw
     * would leave [run] dead and take the node's gossip down with its forwarding.
     *
     * Each forwarded command runs on its own coroutine on [scope]: the coordinator's answer needs
     * the demux to keep reading, since its quorum's acks and read replies arrive there (T22).
     *
     * ponytail: two forwards from one contact may therefore run out of order at the coordinator;
     * a per-sender queue of forwards is the repair if a pipelining client ever observes it.
     */
    private suspend fun coordinate(from: NodeId, request: Forward) {
        val reply = runCatching { parse(request.tokenList.map(ByteString::toByteArray)) }.fold(
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
        val body = Forward.newBuilder().setId(id).addAllToken(tokens(command).map(ByteString::copyFrom))
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
