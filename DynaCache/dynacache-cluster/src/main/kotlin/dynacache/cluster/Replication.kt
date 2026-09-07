package dynacache.cluster

import com.google.protobuf.ByteString
import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Read
import dynacache.cluster.proto.ReadReply
import dynacache.cluster.proto.Repair
import dynacache.cluster.proto.Replicate
import dynacache.cluster.proto.ReplicateAck
import dynacache.cluster.proto.Version
import dynacache.engine.Command
import dynacache.engine.CommandEngine
import dynacache.engine.Key
import dynacache.engine.Reply
import dynacache.engine.Stored
import dynacache.engine.persist.CommandCodec
import dynacache.engine.persist.decodeValue
import dynacache.engine.persist.encodeValue
import dynacache.engine.persist.whatChanged
import java.time.Clock
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.Random
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.future.future
import kotlinx.coroutines.launch
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
 * the key's version with the node's [counter] (C2), applies locally, and ships the entry it
 * logged, as the engine codec's bytes (ADR 0003), plus that version to the key's other
 * replicas; a read runs locally and on R-1 replicas
 * and answers with the version that dominates (C4). Plan 2.5: every fan-out goes to at most N-1
 * nodes and waits at most [deadline]. A write whose replica is dead goes to the next healthy
 * node on the ring instead, which keeps it as a hint in [hints] and hands it back when the
 * replica returns (spec 2.4 sloppy quorum, spec 5.1 step 7). A read that finds a replica behind
 * the winning version has the winner push its value there afterwards (spec 5.2 step 5): the
 * value crosses as the engine encodes it, through [view] and [install], and a version
 * concurrent with the winner's is left alone.
 *
 * @param membership the gossip's view; a replica it holds dead is not asked.
 * @param view the engine's live copy of one key, null when absent (`ApEngine.view`).
 * @param install puts such a copy on the engine (`ApEngine.install`, not WAL-logged); a repair
 * that removes a key goes through a `DEL` on [engine] instead.
 * @param deadline how long a quorum may take to form. Shorter than the router's forward
 * deadline, so a contact reports the quorum error and not its own timeout.
 * @param replayBatch how many hints one handoff round sends before waiting for their acks.
 */
class Replication(
    val self: NodeId,
    private val ring: Ring,
    private val config: ReplicationConfig,
    private val engine: CommandEngine,
    private val transport: Outbound,
    private val membership: Membership,
    private val counter: DotCounter,
    private val clock: Clock,
    private val view: (Key) -> CompletableFuture<Stored?>,
    private val install: (Stored) -> CompletableFuture<*>,
    private val scope: CoroutineScope,
    private val deadline: Duration = 1.seconds,
    private val replayBatch: Int = 64,
) : CommandEngine {

    init {
        require(config.n <= ring.nodes.size) { "N=${config.n} exceeds the ring's ${ring.nodes.size} nodes" }
    }

    private val versions = ConcurrentHashMap<Key, Dvv>()
    private val ids = AtomicLong()
    private val gathers = ConcurrentHashMap<Long, Gather>()
    private val hints = HintStore()

    /** How many hints this node holds for others: what `INFO` reports. */
    val hintCount: Int get() = hints.size

    /** Reads whose R replies did not all carry the winning version. */
    val divergentReads = AtomicLong()

    /** Replicas a read on this coordinator found behind and had the winning version sent to (spec 5.2 step 5). */
    val repairsSent = AtomicLong()

    /** The version this node holds for [key], null when it never stored one. */
    fun version(key: Key): Dvv? = versions[key]

    /** Anti-entropy's install (T28): [dvv] is now the version held for [key]; the value itself goes through the engine. */
    fun installVersion(key: Key, dvv: Dvv) { versions[key] = dvv }

    override fun submit(command: Command): CompletableFuture<Reply> = when {
        command !is Command.Keyed -> engine.submit(command)
        command.isRead() -> scope.future { read(command) }
        else -> scope.future { write(command) }
    }

    override fun close() = engine.close()

    /**
     * Spec 5.1 steps 4 to 6 and 8. The version is bumped before the engine runs so two writes
     * racing on one key chain instead of forking. What ships is what the log holds (ADR 0003):
     * [whatChanged] decides it, so a write the engine refused, or a conditional `SET` that did
     * not apply, replicates nothing and keeps the reply it got, and the coordinator's instant
     * settles a `SET`'s TTL into the deadline every replica applies (spec 5.4).
     */
    private suspend fun write(command: Command.Keyed): Reply {
        val dvv = versions.compute(command.key) { _, held -> held?.bump(counter) ?: Dvv(counter.next(), emptyMap()) }!!
        val reply = engine.submit(command).await()
        val changed = whatChanged(command, reply) ?: return reply
        val now = clock.instant()
        val body = Replicate.newBuilder()
            .setCommand(ByteString.copyFrom(CommandCodec.frame(changed, now)))
            .setDvv(ByteString.copyFrom(dvv.encode()))
            .setExpiresAtMillis((changed as? Command.Set)?.ttl?.let { now.plus(it).toEpochMilli() } ?: 0L)
        val acks = gather(command.key, config.w - 1, sloppy = true) { id, hintFor ->
            Envelope.newBuilder().setReplicate(body.setId(id).setHintFor(hintFor?.name ?: ""))
        }
        return if (acks.size < config.w - 1) quorumError("write", config.w, acks.size + 1) else reply
    }

    /**
     * Spec 5.2 steps 1 to 4: this node's answer and R-1 replicas' answers, each with the version
     * it came from; the dominating version wins, and two concurrent ones fall to spec 2.5's
     * tiebreak ([lastWriter]). Step 5: every replica the winner's version dominates is repaired
     * on its own coroutine, so the reply never waits for it; a concurrent sibling is not.
     */
    private suspend fun read(command: Command.Keyed): Reply {
        val mine = engine.submit(command).await() to versions[command.key]
        if (config.r == 1) return mine.first
        val body = Read.newBuilder().setCommand(ByteString.copyFrom(CommandCodec.frame(command)))
        val replies = gather(command.key, config.r - 1, sloppy = false) { id, _ -> Envelope.newBuilder().setRead(body.setId(id)) }
        if (replies.size < config.r - 1) return quorumError("read", config.r, replies.size + 1)
        val answers = mapOf(self to mine) + replies.mapValues { (_, envelope) ->
            val it = envelope.readReply
            ReplyWire.decode(it.reply) to (if (it.dvv.isEmpty) null else Dvv.decode(it.dvv.toByteArray()))
        }
        val (winner, best) = answers.entries.reduce { best, next -> if (newer(next.value.second, best.value.second)) next else best }
        val top = best.second
        if (answers.values.any { it.second != top }) divergentReads.incrementAndGet()
        val behind = answers.filterValues { (_, held) -> top != null && (held == null || top.dominates(held)) }.keys
        if (behind.isNotEmpty()) {
            repairsSent.addAndGet(behind.size.toLong())
            scope.launch { repair(command.key, winner, behind) }
        }
        return best.first
    }

    /** Spec 5.2 step 5: the winner pushes its value to [behind]; asked to, when the winner is another node. */
    private suspend fun repair(key: Key, winner: NodeId, behind: Collection<NodeId>) {
        if (winner == self) push(key, behind)
        else send(winner, Envelope.newBuilder().setRepair(Repair.newBuilder().setKey(ByteString.copyFrom(key.bytes)).addAllTarget(behind.map { it.name })))
    }

    /**
     * Ships this node's value under [key] with its version to [targets]. The version is read
     * before and after the engine hands the value out: a write bumps the version first and
     * applies second, so a version that moved means the two may not match, and that write's
     * own replication carries the fresh pair anyway. No value under a version is a tombstone.
     */
    private suspend fun push(key: Key, targets: Collection<NodeId>) {
        val before = versions[key] ?: return
        val held = view(key).await()
        if (versions[key] != before) return
        val body = Version.newBuilder().setKey(ByteString.copyFrom(key.bytes)).setDvv(ByteString.copyFrom(before.encode()))
            .setValue(held?.let { ByteString.copyFrom(encodeValue(it.value)) } ?: ByteString.EMPTY)
            .setExpiresAtMillis(held?.expiresAt?.toEpochMilli() ?: 0L)
        for (target in targets) send(target, Envelope.newBuilder().setReplicateValue(body))
    }

    /**
     * The receiving end of a repair: taken only when its version dominates what is held (spec
     * 5.3), so a sibling stays. A tombstone, or a value already past its deadline, deletes the
     * key; bytes that do not decode are ignored whole, version included.
     */
    private suspend fun installValue(request: Version) {
        val key = Key(request.key.toByteArray())
        val remote = runCatching { Dvv.decode(request.dvv.toByteArray()) }.getOrNull() ?: return
        val expiresAt = if (request.expiresAtMillis == 0L) null else Instant.ofEpochMilli(request.expiresAtMillis)
        val gone = request.value.isEmpty || (expiresAt != null && clock.instant().isAfter(expiresAt))
        val value = if (gone) null else (runCatching { decodeValue(request.value.toByteArray(), Random(remote.dot.counter)) }.getOrNull() ?: return)
        val held = versions[key]
        if (held != null && !remote.dominates(held)) return
        versions[key] = remote
        if (value == null) engine.submit(Command.Del(key)).await() else install(Stored(key, value, expiresAt)).await()
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
     * [body] is told which dead node, if any, its receiver stands in for.
     */
    private suspend fun gather(key: Key, need: Int, sloppy: Boolean, body: (Long, NodeId?) -> Envelope.Builder): Map<NodeId, Envelope> {
        val targets = successors(key, sloppy)
        val id = ids.incrementAndGet()
        val gather = Gather(need)
        gathers[id] = gather
        try {
            for ((replica, hintFor) in targets) send(replica, body(id, hintFor))
            if (need in 1..targets.size) withTimeoutOrNull(deadline) { gather.done.await() }
            return gather.answers()
        } finally {
            gathers.remove(id)
        }
    }

    /**
     * The live successors of [key]'s preference list, each mapped to null, and when [sloppy]
     * a substitute for each dead one (spec 2.4): the next healthy nodes clockwise past the
     * list, distinct from all of it, each mapped to the dead node it stands in for. A dead
     * node with no healthy node left to stand in for it is simply not written to.
     */
    private fun successors(key: Key, sloppy: Boolean): Map<NodeId, NodeId?> {
        val (live, dead) = ring.preferenceList(key, config.n).drop(1).partition { it !in membership.dead }
        val targets = LinkedHashMap<NodeId, NodeId?>().apply { live.forEach { put(it, null) } }
        if (sloppy && dead.isNotEmpty()) {
            val substitutes = ring.preferenceList(key, ring.nodes.size).drop(config.n).filter { it !in membership.dead }
            dead.zip(substitutes).forEach { (gone, standIn) -> targets[standIn] = gone }
        }
        return targets
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

    /**
     * The node's hint handoff (spec 2.4, plan 2.5): one coroutine that, each time gossip sees a
     * node alive, replays the hints held for it round by round until none is left or a round
     * goes unacked; what stays is retried at the next alive event. Tests call [replayHints].
     */
    suspend fun runHandoff() {
        membership.changes.collect { member ->
            if (member.state == MemberState.ALIVE) while (replayHints(member.node) > 0) continue
        }
    }

    /**
     * One handoff round: sends up to [replayBatch] of [target]'s hints exactly as they were
     * stored (C5), waits for their acks or the [deadline], forgets the acked ones and answers
     * how many that was. An unacked hint is kept for the next round.
     */
    suspend fun replayHints(target: NodeId): Int {
        val batch = hints.pending(target, clock.instant(), replayBatch)
        val rounds = batch.keys.associateWith { id -> Gather(1).also { gathers[id] = it } }
        try {
            for ((id, write) in batch) send(target, Envelope.newBuilder().setReplicate(write.toBuilder().setId(id).clearHintFor()))
            if (rounds.isNotEmpty()) withTimeoutOrNull(deadline) { rounds.values.forEach { it.done.await() } }
            return rounds.count { (id, gather) -> gather.done.isCompleted.also { acked -> if (acked) hints.remove(id) } }
        } finally {
            rounds.keys.forEach(gathers::remove)
        }
    }

    /** One inbound envelope; true when it was replication's, false when it belongs to someone else. */
    suspend fun receive(envelope: Envelope): Boolean {
        when (envelope.bodyCase) {
            Envelope.BodyCase.REPLICATE -> replicate(NodeId(envelope.from), envelope.replicate)
            Envelope.BodyCase.READ -> answer(NodeId(envelope.from), envelope.read)
            Envelope.BodyCase.REPLICATE_ACK -> gathers[envelope.replicateAck.id]?.offer(NodeId(envelope.from), envelope)
            Envelope.BodyCase.READ_REPLY -> gathers[envelope.readReply.id]?.offer(NodeId(envelope.from), envelope)
            Envelope.BodyCase.REPLICATE_VALUE -> installValue(envelope.replicateValue)
            Envelope.BodyCase.REPAIR -> push(Key(envelope.repair.key.toByteArray()), envelope.repair.targetList.map(::NodeId))
            else -> return false
        }
        return true
    }

    /**
     * The replica's half of spec 5.1 step 5 under spec 5.3: a version that dominates what is
     * held is applied and stored; one that is dominated or equal is ignored; a concurrent one is
     * applied under a version descending from both. Bytes or a version this node cannot read
     * are not acked, so the coordinator counts this node as silent rather than as agreeing.
     * A write this node only stands in for (`hint_for` set) is kept whole as a hint instead of
     * applied (C5), and acked toward W all the same.
     *
     * The bytes are the entry the coordinator logged, and the replica submits what they decode
     * to, in order, exactly as its own log would redo them: a `SET` with a settled deadline is
     * the `SET` and then the `EXPIRE` (ADR 0003).
     */
    private suspend fun replicate(from: NodeId, request: Replicate) {
        val commands = runCatching { CommandCodec.unframe(request.command.toByteArray()) }.getOrNull() ?: return
        val key = (commands.firstOrNull() as? Command.Keyed)?.key ?: return
        val remote = runCatching { Dvv.decode(request.dvv.toByteArray()) }.getOrNull() ?: return
        if (request.hintFor.isNotEmpty()) {
            hints.add(ids.incrementAndGet(), NodeId(request.hintFor), request)
            return send(from, Envelope.newBuilder().setReplicateAck(ReplicateAck.newBuilder().setId(request.id)))
        }
        val held = versions[key]
        val next = when {
            held == null || remote.dominates(held) -> remote
            held.isConcurrent(remote) -> held.merge(remote, counter)
            else -> null
        }
        if (next != null) {
            versions[key] = next
            for (command in commands) engine.submit(command).await()
        }
        send(from, Envelope.newBuilder().setReplicateAck(ReplicateAck.newBuilder().setId(request.id)))
    }

    /** The replica's half of spec 5.2 step 2: run the read here and answer with the version it read. */
    private suspend fun answer(from: NodeId, request: Read) {
        val command = runCatching { CommandCodec.unframe(request.command.toByteArray()).single() }.getOrNull() as? Command.Keyed ?: return
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
