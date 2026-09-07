package dynacache.cluster

import com.google.protobuf.ByteString
import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.KeySync
import dynacache.cluster.proto.KeySyncReply
import dynacache.cluster.proto.Leaf
import dynacache.cluster.proto.MerkleLevel
import dynacache.cluster.proto.MerkleLevelReply
import dynacache.cluster.proto.MerkleRoot
import dynacache.cluster.proto.MerkleRootReply
import dynacache.cluster.proto.Version
import dynacache.engine.Key
import dynacache.engine.Value
import dynacache.engine.persist.decodeValue
import dynacache.engine.persist.encodeValue
import java.security.MessageDigest
import java.time.Instant
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
 * apply spec 5.3, which the [store] decides: a dominated version is replaced, a dominating one
 * kept, concurrent ones merged by type (T29) under a version descending from both.
 *
 * A leaf is `(key, SHA-256 of the engine's value encoding, version)`; a key the store holds no
 * version for is invisible here, since it has nothing to compare. A tombstone (a version whose
 * value is gone) is a leaf over no bytes and crosses as a version with no value, so a `DEL`
 * one replica missed reaches it here rather than being undone (T66).
 *
 * Plan 2.5: one step touches one range, waits at most [deadline] for each request and sends at
 * most [REQUESTS_PER_STEP] of them. A step is the root exchange, then a descent of the peer's
 * tree one level per request (T84), then the leaves under the subtrees that still differ, then
 * the key sync: `tree height + 1` requests, so a fan-out of 16 keeps a range of 16^9 keys
 * inside the budget. A tree deeper than [DESCENT_ROUNDS] ends the step where it stands and the
 * range comes round again on a later tick, rather than this process issuing an unbounded number
 * of requests before its deadline.
 *
 * [run] is the node's one coroutine, ticking every [interval]; tests call [tick].
 * [rangesCompared] and [keysSynced] are what `INFO` reports.
 */
class AntiEntropy(
    private val self: NodeId,
    private val ring: Ring,
    n: Int,
    private val store: VersionedStore,
    private val transport: Outbound,
    private val membership: Membership,
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
        // Both sides state their width and descend only when the two match: a node position
        // means nothing under another fan-out, and a shape neither side can read is worse than
        // a range left for the next tick.
        if (roots.fanout != tree.fanout) return
        val keys = descend(peer, vnode, tree, roots.leafCount) ?: return
        if (keys.isEmpty()) return
        val sync = KeySync.newBuilder()
        for (key in keys) {
            sync.addKey(ByteString.copyFrom(key.bytes))
            mine[key]?.let { sync.addVersion(it.wire()) }
        }
        val answer = ask(peer) { id -> Envelope.newBuilder().setKeySync(sync.setId(id)) }?.keySyncReply ?: return
        for (version in answer.versionList) reconcile(mine[Key(version.key.toByteArray())], version)
    }

    /**
     * The keys the peer's copy of the range differs on, found by descending both trees over the
     * wire: the root has already disagreed, so each round asks only for the children of the
     * nodes that disagreed at the level above, and the last round asks for the leaves under the
     * one subtree that is still different. Null when the peer went silent, answered a level
     * malformed, or the tree is deeper than [DESCENT_ROUNDS] allows; the range comes round again.
     *
     * The peer answers each round over the tree it holds at that moment, so a write landing
     * mid-descent can shift its positions. That costs a key this step at worst: the divergence
     * is decided key by key at the bottom ([MerkleTree.divergentKeys]), never by position, and
     * the range comes round again.
     */
    private suspend fun descend(peer: NodeId, vnode: Int, tree: MerkleTree, theirLeaves: Int): List<Key>? {
        var level = tree.height - 1
        var positions = listOf(0)
        // Trees of different height have no level to line up. Every leaf is suspect then, which
        // costs the range, exactly as the local descent does (MerkleTree.suspectLeaves).
        if (MerkleTree.heightOf(theirLeaves, tree.fanout) != tree.height) {
            level = 0
            positions = (0 until maxOf(tree.size, theirLeaves)).toList()
        }
        var rounds = 0
        while (level > 0) {
            positions = tree.childrenOf(positions)
            if (--level == 0) break
            if (rounds++ >= DESCENT_ROUNDS) return null
            val hashes = askLevel(peer, vnode, level, positions)?.hashList ?: return null
            if (hashes.size != positions.size) return null
            positions = tree.differing(level, positions, hashes.map { if (it.isEmpty) null else it.toByteArray() })
            if (positions.isEmpty()) return emptyList()
        }
        val leaves = askLevel(peer, vnode, 0, positions) ?: return null
        return tree.divergentKeys(positions, leaves.leafList.map { leaf(it) })
    }

    /** One round of the descent: the peer's hashes at [level] for [positions], or its leaves at level 0. */
    private suspend fun askLevel(peer: NodeId, vnode: Int, level: Int, positions: List<Int>) =
        ask(peer) { id ->
            Envelope.newBuilder().setMerkleLevel(
                MerkleLevel.newBuilder().setId(id).setVnode(vnode).setLevel(level).addAllPosition(positions)
            )
        }?.merkleLevelReply

    /** One inbound envelope; true when it was anti-entropy's, false when it belongs to someone else. */
    suspend fun receive(envelope: Envelope): Boolean {
        when (envelope.bodyCase) {
            Envelope.BodyCase.MERKLE_ROOT -> answerRoot(NodeId(envelope.from), envelope.merkleRoot)
            Envelope.BodyCase.MERKLE_LEVEL -> answerLevel(NodeId(envelope.from), envelope.merkleLevel)
            Envelope.BodyCase.KEY_SYNC -> answerSync(NodeId(envelope.from), envelope.keySync)
            Envelope.BodyCase.MERKLE_ROOT_REPLY -> pending[envelope.merkleRootReply.id]?.complete(envelope)
            Envelope.BodyCase.MERKLE_LEVEL_REPLY -> pending[envelope.merkleLevelReply.id]?.complete(envelope)
            Envelope.BodyCase.KEY_SYNC_REPLY -> pending[envelope.keySyncReply.id]?.complete(envelope)
            else -> return false
        }
        return true
    }

    /**
     * The peer's half of the root exchange: this node's root over the range, and the shape of
     * the tree it came from, which is all the sender needs to start descending (T84).
     */
    private suspend fun answerRoot(from: NodeId, request: MerkleRoot) {
        val tree = treeOf(request.vnode) ?: return
        send(
            from,
            Envelope.newBuilder().setMerkleRootReply(
                MerkleRootReply.newBuilder().setId(request.id).setRoot(ByteString.copyFrom(tree.root))
                    .setFanout(tree.fanout).setLeafCount(tree.size)
            ),
        )
    }

    /**
     * The peer's half of one descent round: this node's hashes at the level asked, in the order
     * asked and empty where it has no node there, or, at level 0, the leaves it holds among
     * those positions. The tree is rebuilt per round rather than remembered between them: a
     * session would need a lifetime and an eviction of its own, and the sender is already
     * bounded to [DESCENT_ROUNDS] of them.
     */
    private suspend fun answerLevel(from: NodeId, request: MerkleLevel) {
        val tree = treeOf(request.vnode) ?: return
        val reply = MerkleLevelReply.newBuilder().setId(request.id)
        if (request.level == 0) {
            for (leaf in tree.leavesAt(request.positionList)) {
                reply.addLeaf(
                    Leaf.newBuilder().setKey(ByteString.copyFrom(leaf.key.bytes))
                        .setValueHash(ByteString.copyFrom(leaf.valueHash))
                        .setDvv(ByteString.copyFrom(leaf.dvv.encode()))
                )
            }
        } else {
            for (hash in tree.hashesAt(request.level, request.positionList)) {
                reply.addHash(ByteString.copyFrom(hash ?: NO_VALUE))
            }
        }
        send(from, Envelope.newBuilder().setMerkleLevelReply(reply))
    }

    /** This node's Merkle tree over the range at [vnode], or null when the ring has no such vnode. */
    private suspend fun treeOf(vnode: Int): MerkleTree? {
        val range = ring.vnodes.getOrNull(vnode) ?: return null
        return MerkleTree.of(held(range).values.map { it.leaf })
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
     * Spec 5.3 for one key, decided by the store. Answers what this node holds of its own
     * afterwards: [mine] when it stood, the merged value when the two were concurrent, null
     * when [remote] was installed as it came. Bytes that do not decode leave [mine] standing.
     */
    private suspend fun reconcile(mine: Entry?, remote: Version): Entry? {
        val theirs = runCatching { decode(remote) }.getOrNull() ?: return mine
        val installed = store.install(theirs.key, theirs.value, theirs.expiresAt, theirs.dvv).await()
        if (installed.outcome != Outcome.KEPT) keysSynced.incrementAndGet()
        return if (installed.outcome == Outcome.TAKEN) null else Entry(installed.held)
    }

    /** One key as this node holds it: the value (null for a tombstone), its encoding (hashed and shipped), its version and its deadline. */
    private class Entry(val key: Key, val value: Value?, val bytes: ByteArray, val dvv: Dvv, val expiresAt: Instant?) {
        constructor(held: Held) : this(held.key, held.value, held.value?.let(::encodeValue) ?: NO_VALUE, held.dvv, held.expiresAt)

        val leaf = MerkleLeaf(key, MessageDigest.getInstance("SHA-256").digest(bytes), dvv)

        fun wire(): Version.Builder = Version.newBuilder()
            .setKey(ByteString.copyFrom(key.bytes)).setValue(ByteString.copyFrom(bytes))
            .setDvv(ByteString.copyFrom(dvv.encode())).setExpiresAtMillis(expiresAt?.toEpochMilli() ?: 0L)
    }

    private suspend fun held(range: Vnode): Map<Key, Entry> = store.held { range.holds(ring.positionOf(it)) }.await().map(::Entry).associateBy { it.key }
    private suspend fun held(keys: List<Key>): Map<Key, Entry> = store.held(keys).await().map(::Entry).associateBy { it.key }

    private fun decode(version: Version): Entry {
        val dvv = Dvv.decode(version.dvv.toByteArray())
        val bytes = version.value.toByteArray()
        val expiresAt = if (version.expiresAtMillis == 0L) null else Instant.ofEpochMilli(version.expiresAtMillis)
        val value = if (bytes.isEmpty()) null else decodeValue(bytes, Random(dvv.dot.counter))
        return Entry(Key(version.key.toByteArray()), value, bytes, dvv, expiresAt)
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

    companion object {
        /**
         * How many levels of the peer's tree one step will descend. Nine is every range a
         * fan-out of 16 can hold up to 16^9 keys in; a deeper tree ends the step where it
         * stands and comes round again, so the descent can never run away.
         */
        const val DESCENT_ROUNDS: Int = 9

        /** The step's whole budget: the root, [DESCENT_ROUNDS] levels, the leaves, the key sync. */
        const val REQUESTS_PER_STEP: Int = DESCENT_ROUNDS + 3

        /** A tombstone's encoding on the wire and under its leaf: no bytes at all. */
        private val NO_VALUE = ByteArray(0)
    }
}
