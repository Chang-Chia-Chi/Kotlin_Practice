package dynacache.cluster

import com.google.protobuf.ByteString
import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Read
import dynacache.cluster.proto.ReadReply
import dynacache.cluster.proto.Replicate
import dynacache.cluster.proto.ReplicateAck
import dynacache.engine.Command
import dynacache.engine.CommandEngine
import dynacache.engine.Key
import dynacache.engine.PartitionContext
import dynacache.engine.Reply
import java.time.Clock
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.future.future
import kotlinx.coroutines.withTimeoutOrNull

/** Spec 2.4's quorum settings, checked once so C4's arithmetic holds for every request. */
data class ReplicationConfig(val n: Int, val w: Int, val r: Int) {
    init {
        require(w in 1..n && r in 1..n) { "W and R must be within 1..N, got n=$n w=$w r=$r" }
        require(r + w > n) { "C4: R + W must exceed N, got n=$n w=$w r=$r" }
    }
}

/**
 * The **coordinator's** half of a write and a read (spec 5.1 steps 4 to 6 and 8, spec 5.2
 * steps 1 to 4) and the **replica's** half of both, on one node. It presents the `CommandEngine`
 * shape and wraps the node's [engine], so the [Router] hands it every command this node
 * coordinates and nothing downstream learns that replicas exist.
 *
 * Every stored value carries a version: [versions] is this node's `Key -> Dvv` side table, kept
 * next to the engine rather than inside it, so the engine stays cluster-unaware. A write bumps
 * the key's version with the node's [counter] (C2), applies locally, and ships the command's
 * tokens plus that version to the key's other replicas; a read runs locally and on R-1 replicas
 * and answers with the version that dominates (C4). Plan 2.5: every fan-out goes to at most N-1
 * nodes and waits at most [deadline]. Sloppy quorum is T25, read repair T26.
 *
 * @param membership the gossip's view; a replica it holds dead is not asked.
 * @param deadline how long a quorum may take to form. Shorter than the router's forward
 * deadline, so a contact reports the quorum error and not its own timeout.
 */
class Replication(
    val self: NodeId,
    private val ring: Ring,
    private val config: ReplicationConfig,
    private val engine: CommandEngine,
    private val transport: Transport,
    private val membership: Membership,
    private val counter: DotCounter,
    private val clock: Clock,
    private val tokens: (Command) -> List<ByteArray>,
    private val parse: (List<ByteArray>) -> Command,
    private val scope: CoroutineScope,
    private val deadline: Duration = 1.seconds,
) : CommandEngine {

    init {
        require(config.n <= ring.nodes.size) { "N=${config.n} exceeds the ring's ${ring.nodes.size} nodes" }
    }

    private val versions = ConcurrentHashMap<Key, Dvv>()
    private val ids = AtomicLong()
    private val gathers = ConcurrentHashMap<Long, Gather>()

    /** Reads whose R replies did not all carry the winning version: what read repair (T26) pushes on. */
    val divergentReads = AtomicLong()

    /** The version this node holds for [key], null when it never stored one. */
    fun version(key: Key): Dvv? = versions[key]

    override fun submit(command: Command): CompletableFuture<Reply> = when {
        command !is Command.Keyed -> engine.submit(command)
        command.isRead() -> scope.future { read(command) }
        else -> scope.future { write(command) }
    }

    /** A batch is not replicated yet: it runs on this node's engine alone (debt, see progress T22). */
    override fun <R> atomically(keys: List<Key>, block: (PartitionContext) -> R): CompletableFuture<R> =
        engine.atomically(keys, block)

    override fun close() = engine.close()

    /**
     * Spec 5.1 steps 4 to 6 and 8. The version is bumped before the engine runs so two writes
     * racing on one key chain instead of forking; a write the engine refused, or a conditional
     * `SET` that did not apply, replicates nothing and keeps the reply it got.
     */
    private suspend fun write(command: Command.Keyed): Reply {
        val dvv = versions.compute(command.key) { _, held -> held?.bump(counter) ?: Dvv(counter.next(), emptyMap()) }!!
        val reply = engine.submit(command).await()
        if (reply is Reply.Error || (command is Command.Set && command.condition != null && reply == Reply.Bulk(null))) return reply
        val body = Replicate.newBuilder()
            .addAllToken(tokens(decided(command)).map(ByteString::copyFrom))
            .setDvv(ByteString.copyFrom(dvv.encode()))
            .setExpiresAtMillis((command as? Command.Set)?.ttl?.let { clock.instant().plus(it).toEpochMilli() } ?: 0L)
        val acks = gather(command.key, config.w - 1) { id -> Envelope.newBuilder().setReplicate(body.setId(id)) }
        return if (acks.size < config.w - 1) quorumError("write", config.w, acks.size + 1) else reply
    }

    /** The coordinator decided NX/XX and turned the TTL into an instant; a replica applies the plain write. */
    private fun decided(command: Command.Keyed): Command =
        if (command is Command.Set && (command.condition != null || command.ttl != null)) Command.Set(command.key, command.value)
        else command

    /**
     * Spec 5.2 steps 1 to 4: this node's answer and R-1 replicas' answers, each with the version
     * it came from; the dominating version wins, and two concurrent ones fall to spec 2.5's
     * tiebreak ([lastWriter]). Divergence is counted, not repaired (T26).
     */
    private suspend fun read(command: Command.Keyed): Reply {
        val mine = engine.submit(command).await() to versions[command.key]
        if (config.r == 1) return mine.first
        val body = Read.newBuilder().addAllToken(tokens(command).map(ByteString::copyFrom))
        val replies = gather(command.key, config.r - 1) { id -> Envelope.newBuilder().setRead(body.setId(id)) }
        if (replies.size < config.r - 1) return quorumError("read", config.r, replies.size + 1)
        val answers = listOf(mine) + replies.values.map { it.readReply }.map {
            ReplyWire.decode(it.reply) to (if (it.dvv.isEmpty) null else Dvv.decode(it.dvv.toByteArray()))
        }
        val winner = answers.reduce { best, next -> if (newer(next.second, best.second)) next else best }
        if (answers.any { it.second != winner.second }) divergentReads.incrementAndGet()
        return winner.first
    }

    private fun newer(candidate: Dvv?, best: Dvv?): Boolean = when {
        candidate == null -> false
        best == null || candidate.dominates(best) -> true
        candidate.isConcurrent(best) -> lastWriter.compare(candidate, best) > 0
        else -> false
    }

    private fun quorumError(what: String, need: Int, got: Int) =
        Reply.Error("ERR", "quorum not reached: $what needs $need of ${config.n} nodes, $got answered within $deadline")

    /**
     * Plan 2.5's bounded fan-out: one envelope to each live successor of [key]'s preference
     * list (at most N-1), then wait for [need] answers from distinct nodes (C4) or the
     * [deadline], whichever comes first. Fewer live successors than [need] is answered at once.
     */
    private suspend fun gather(key: Key, need: Int, body: (Long) -> Envelope.Builder): Map<NodeId, Envelope> {
        val replicas = ring.preferenceList(key, config.n).drop(1).filter { it !in membership.dead }
        val id = ids.incrementAndGet()
        val gather = Gather(need)
        gathers[id] = gather
        try {
            for (replica in replicas) send(replica, body(id))
            if (need in 1..replicas.size) withTimeoutOrNull(deadline) { gather.done.await() }
            return gather.answers()
        } finally {
            gathers.remove(id)
        }
    }

    /** One request's answers so far, by node, so the same node answering twice counts once. */
    private class Gather(private val need: Int) {
        private val byNode = LinkedHashMap<NodeId, Envelope>()
        val done = CompletableDeferred<Unit>()

        @Synchronized
        fun offer(from: NodeId, envelope: Envelope) {
            if (byNode.putIfAbsent(from, envelope) == null && byNode.size >= need) done.complete(Unit)
        }

        @Synchronized
        fun answers(): Map<NodeId, Envelope> = LinkedHashMap(byNode)
    }

    /** One inbound envelope; true when it was replication's, false when it belongs to someone else. */
    suspend fun receive(envelope: Envelope): Boolean {
        when (envelope.bodyCase) {
            Envelope.BodyCase.REPLICATE -> replicate(NodeId(envelope.from), envelope.replicate)
            Envelope.BodyCase.READ -> answer(NodeId(envelope.from), envelope.read)
            Envelope.BodyCase.REPLICATE_ACK -> gathers[envelope.replicateAck.id]?.offer(NodeId(envelope.from), envelope)
            Envelope.BodyCase.READ_REPLY -> gathers[envelope.readReply.id]?.offer(NodeId(envelope.from), envelope)
            else -> return false
        }
        return true
    }

    /**
     * The replica's half of spec 5.1 step 5 under spec 5.3: a version that dominates what is
     * held is applied and stored; one that is dominated or equal is ignored; a concurrent one is
     * applied under a version descending from both. Tokens or a version this node cannot read
     * are not acked, so the coordinator counts this node as silent rather than as agreeing.
     */
    private suspend fun replicate(from: NodeId, request: Replicate) {
        val command = runCatching { parse(request.tokenList.map(ByteString::toByteArray)) }.getOrNull() as? Command.Keyed ?: return
        val remote = runCatching { Dvv.decode(request.dvv.toByteArray()) }.getOrNull() ?: return
        val held = versions[command.key]
        val next = when {
            held == null || remote.dominates(held) -> remote
            held.isConcurrent(remote) -> held.merge(remote, counter)
            else -> null
        }
        if (next != null) {
            versions[command.key] = next
            engine.submit(command).await()
            if (request.expiresAtMillis != 0L) {
                engine.submit(Command.Expire(command.key, Instant.ofEpochMilli(request.expiresAtMillis))).await()
            }
        }
        send(from, Envelope.newBuilder().setReplicateAck(ReplicateAck.newBuilder().setId(request.id)))
    }

    /** The replica's half of spec 5.2 step 2: run the read here and answer with the version it read. */
    private suspend fun answer(from: NodeId, request: Read) {
        val command = runCatching { parse(request.tokenList.map(ByteString::toByteArray)) }.getOrNull() as? Command.Keyed ?: return
        val reply = ReplyWire.encode(engine.submit(command).await())
        val dvv = versions[command.key]?.let { ByteString.copyFrom(it.encode()) } ?: ByteString.EMPTY
        send(from, Envelope.newBuilder().setReadReply(ReadReply.newBuilder().setId(request.id).setReply(reply).setDvv(dvv)))
    }

    private suspend fun send(to: NodeId, envelope: Envelope.Builder) =
        transport.send(to, envelope.setFrom(self.name).setTo(to.name).build())
}

/**
 * A command that changes nothing about its key goes the read path. The list is the reads the
 * engine knows today; a variant missing here is treated as a write, which costs one needless
 * replication round and never loses data.
 */
private fun Command.Keyed.isRead(): Boolean = when (this) {
    is Command.Get, is Command.Exists, is Command.Type, is Command.Ttl, is Command.StrLen,
    is Command.HGet, is Command.HGetAll, is Command.HMGet, is Command.HExists, is Command.HKeys,
    is Command.HVals, is Command.HScan, is Command.HLen,
    is Command.LRange, is Command.LLen, is Command.LIndex,
    is Command.ZScore, is Command.ZCard, is Command.ZRange, is Command.ZRank, is Command.ZRangeByScore,
    is Command.ZScan -> true
    else -> false
}
