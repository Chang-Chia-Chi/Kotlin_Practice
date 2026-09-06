package dynacache.cluster

import com.google.protobuf.ByteString
import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.KeySync
import dynacache.cluster.proto.KeySyncReply
import dynacache.cluster.proto.Leaf
import dynacache.cluster.proto.MerkleRoot
import dynacache.cluster.proto.MerkleRootReply
import dynacache.cluster.proto.Version
import dynacache.engine.ApEngine
import dynacache.engine.Key
import dynacache.engine.Stored
import dynacache.engine.Value
import dynacache.engine.install
import dynacache.engine.persist.decodeValue
import dynacache.engine.persist.encodeValue
import dynacache.engine.view
import java.security.MessageDigest
import java.time.Instant
import java.util.Arrays
import java.util.Random
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull

/**
 * Spec 2.4's anti-entropy on one node: a background process that, one [tick] at a time, takes
 * the next vnode range this node replicates and one live replica of it, compares Merkle roots
 * (T27, C6), and on a mismatch exchanges the divergent keys with their versions so both sides
 * apply spec 5.3: a dominated version is replaced, a dominating one kept, concurrent ones
 * merged by type (T29) under a version descending from both.
 *
 * A leaf is `(key, SHA-256 of the engine's value encoding, version)`; a key the replication
 * layer holds no version for is invisible here, since it has nothing to compare. Nothing is
 * ever deleted: the side that holds a key hands it to the side that lost it.
 *
 * Plan 2.5: one step touches one range, sends at most two requests and waits at most
 * [deadline] for each. [run] is the node's one coroutine, ticking every [interval]; tests
 * call [tick]. [rangesCompared] and [keysSynced] are what `INFO` reports.
 */
class AntiEntropy(
    private val self: NodeId,
    private val ring: Ring,
    n: Int,
    private val engine: ApEngine,
    private val replication: Replication,
    private val transport: Transport,
    private val membership: Membership,
    private val counter: DotCounter,
    private val deadline: Duration = 1.seconds,
    private val interval: Duration = 60.seconds,
) {

    /** The vnode ranges this node replicates, in ring order: what [tick] walks round-robin. */
    val ranges: List<Vnode> = ring.vnodes.filter { self in ring.preferenceList(it, n) }
    private val replicas = ranges.associateWith { ring.preferenceList(it, n) - self }

    val rangesCompared = AtomicLong()
    val keysSynced = AtomicLong()

    private val step = AtomicLong()
    private val ids = AtomicLong()
    private val pending = ConcurrentHashMap<Long, CompletableDeferred<Envelope>>()

    suspend fun run() {
        while (true) {
            tick()
            delay(interval)
        }
    }

    /**
     * One step: the next range against one of its live replicas, rotating through the replicas
     * once per full pass over the ranges. A peer that does not answer within [deadline] ends
     * the step; the range comes round again.
     */
    suspend fun tick() {
        val at = step.getAndIncrement()
        val range = ranges[(at % ranges.size).toInt()]
        val peers = replicas.getValue(range).filter { it !in membership.dead }
        if (peers.isEmpty()) return
        val peer = peers[((at / ranges.size) % peers.size).toInt()]
        val mine = held(range)
        val tree = MerkleTree.of(mine.values.map { it.leaf })
        val root = ByteString.copyFrom(tree.root)
        val vnode = ring.vnodes.indexOf(range)
        val roots = ask(peer) { id -> Envelope.newBuilder().setMerkleRoot(MerkleRoot.newBuilder().setId(id).setVnode(vnode).setRoot(root)) }
            ?.merkleRootReply ?: return
        rangesCompared.incrementAndGet()
        if (roots.root == root) return
        val theirs = runCatching { MerkleTree.of(roots.leafList.map { leaf(it) }) }.getOrNull() ?: return
        val sync = KeySync.newBuilder()
        for (key in tree.diff(theirs).flatMap { it.keys }) {
            sync.addKey(ByteString.copyFrom(key.bytes))
            mine[key]?.let { sync.addVersion(it.wire()) }
        }
        val answer = ask(peer) { id -> Envelope.newBuilder().setKeySync(sync.setId(id)) }?.keySyncReply ?: return
        for (version in answer.versionList) reconcile(mine[Key(version.key.toByteArray())], version)
    }

    /** One inbound envelope; true when it was anti-entropy's, false when it belongs to someone else. */
    suspend fun receive(envelope: Envelope): Boolean {
        when (envelope.bodyCase) {
            Envelope.BodyCase.MERKLE_ROOT -> answerRoot(NodeId(envelope.from), envelope.merkleRoot)
            Envelope.BodyCase.KEY_SYNC -> answerSync(NodeId(envelope.from), envelope.keySync)
            Envelope.BodyCase.MERKLE_ROOT_REPLY -> pending[envelope.merkleRootReply.id]?.complete(envelope)
            Envelope.BodyCase.KEY_SYNC_REPLY -> pending[envelope.keySyncReply.id]?.complete(envelope)
            else -> return false
        }
        return true
    }

    /** The peer's half of the root exchange: this node's root over the range, plus every leaf when the roots differ. */
    private suspend fun answerRoot(from: NodeId, request: MerkleRoot) {
        val range = ring.vnodes.getOrNull(request.vnode) ?: return
        val mine = held(range)
        val tree = MerkleTree.of(mine.values.map { it.leaf })
        val reply = MerkleRootReply.newBuilder().setId(request.id).setRoot(ByteString.copyFrom(tree.root))
        if (reply.root != request.root) {
            for (held in mine.values) {
                reply.addLeaf(Leaf.newBuilder().setKey(ByteString.copyFrom(held.key.bytes)).setValueHash(ByteString.copyFrom(held.leaf.valueHash)).setDvv(ByteString.copyFrom(held.dvv.encode())))
            }
        }
        send(from, Envelope.newBuilder().setMerkleRootReply(reply))
    }

    /**
     * The peer's half of the key exchange: spec 5.3 for every version the sender holds, then
     * this node's own version of every divergent key it still holds, so the sender can apply
     * 5.3 in turn. A version the sender's was installed over verbatim is not sent back.
     */
    private suspend fun answerSync(from: NodeId, request: KeySync) {
        val keys = request.keyList.map { Key(it.toByteArray()) }
        val mine = held(keys)
        val theirs = request.versionList.associateBy { Key(it.key.toByteArray()) }
        val reply = KeySyncReply.newBuilder().setId(request.id)
        for (key in keys) {
            val remote = theirs[key]
            val standing = if (remote == null) mine[key] else reconcile(mine[key], remote)
            standing?.let { reply.addVersion(it.wire()) }
        }
        send(from, Envelope.newBuilder().setKeySyncReply(reply))
    }

    /**
     * Spec 5.3 for one key. Answers what this node holds of its own afterwards: [mine] when it
     * stood, the merged version when the two were concurrent, null when [remote] was installed
     * as it came. Two versions equal by DVV but not by bytes (a value that rotted under its
     * version) have no writer to ask; the greater encoding wins on both sides, so they converge.
     */
    private suspend fun reconcile(mine: Held?, remote: Version): Held? {
        val theirs = runCatching { decode(remote) }.getOrNull() ?: return mine
        val merged = if (mine == null || mine.dvv == theirs.dvv) null else merge(mine.versioned, theirs.versioned, counter)
        val next = when {
            mine == null -> theirs
            merged == null -> if (Arrays.compareUnsigned(theirs.bytes, mine.bytes) > 0) theirs else mine
            merged === mine.versioned -> mine
            merged === theirs.versioned -> theirs
            else -> Held(mine.key, merged.value, encodeValue(merged.value), merged.dvv, later(mine.expiresAt, theirs.expiresAt))
        }
        if (next === mine) return mine
        replication.installVersion(next.key, next.dvv)
        engine.install(Stored(next.key, next.value, next.expiresAt)).await()
        keysSynced.incrementAndGet()
        return if (next === theirs) null else next
    }

    /** A merged value's deadline: the later of the two, and no deadline at all if either side had none. */
    private fun later(a: Instant?, b: Instant?): Instant? = if (a == null || b == null) null else maxOf(a, b)

    /** One key as this node holds it: the value, its encoding (hashed and shipped), its version and its deadline. */
    private class Held(val key: Key, val value: Value, val bytes: ByteArray, val dvv: Dvv, val expiresAt: Instant?) {
        val versioned = Versioned(value, dvv)
        val leaf = MerkleLeaf(key, MessageDigest.getInstance("SHA-256").digest(bytes), dvv)

        fun wire(): Version.Builder = Version.newBuilder()
            .setKey(ByteString.copyFrom(key.bytes)).setValue(ByteString.copyFrom(bytes))
            .setDvv(ByteString.copyFrom(dvv.encode())).setExpiresAtMillis(expiresAt?.toEpochMilli() ?: 0L)
    }

    private suspend fun held(range: Vnode): Map<Key, Held> = held(engine.view { range.holds(ring.positionOf(it)) }.await())
    private suspend fun held(keys: List<Key>): Map<Key, Held> = held(engine.view(keys).await())

    private fun held(stored: List<Stored>): Map<Key, Held> = stored.mapNotNull { held ->
        replication.version(held.key)?.let { Held(held.key, held.value, encodeValue(held.value), it, held.expiresAt) }
    }.associateBy { it.key }

    private fun decode(version: Version): Held {
        val dvv = Dvv.decode(version.dvv.toByteArray())
        val bytes = version.value.toByteArray()
        val expiresAt = if (version.expiresAtMillis == 0L) null else Instant.ofEpochMilli(version.expiresAtMillis)
        return Held(Key(version.key.toByteArray()), decodeValue(bytes, Random(dvv.dot.counter)), bytes, dvv, expiresAt)
    }

    private fun leaf(leaf: Leaf) = MerkleLeaf(Key(leaf.key.toByteArray()), leaf.valueHash.toByteArray(), Dvv.decode(leaf.dvv.toByteArray()))

    /** One request to [peer] and its answer, or null once [deadline] has passed without one. */
    private suspend fun ask(peer: NodeId, body: (Long) -> Envelope.Builder): Envelope? {
        val id = ids.incrementAndGet()
        val answer = CompletableDeferred<Envelope>()
        pending[id] = answer
        try {
            send(peer, body(id))
            return withTimeoutOrNull(deadline) { answer.await() }
        } finally {
            pending.remove(id)
        }
    }

    private suspend fun send(to: NodeId, envelope: Envelope.Builder) =
        transport.send(to, envelope.setFrom(self.name).setTo(to.name).build())
}
