package dynacache.cluster

import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.PartitionId
import dynacache.engine.Reply
import dynacache.engine.StoreAccess
import dynacache.engine.Stored
import dynacache.engine.Value
import dynacache.engine.onEveryPartition
import dynacache.engine.onPartitionOf
import dynacache.engine.persist.KeyVersions
import dynacache.engine.persist.encodeValue
import java.time.Instant
import java.util.Arrays
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap

/**
 * One key as this node holds it, read as one: its value, null for a tombstone (a version whose
 * value is gone), its deadline and its version.
 */
class Held(val key: Key, val value: Value?, val expiresAt: Instant?, val dvv: Dvv)

/** What the store did with a pair that arrived (spec 5.3): kept its own, took the arrival as it came, or merged the two. */
enum class Outcome { KEPT, TAKEN, MERGED }

/** [outcome], and the pair held afterwards: this node's own, the arrival, or the merge. */
class Installed(val outcome: Outcome, val held: Held)

/**
 * The versioned store beside the engine (T66; CONTEXT.md "version"): this node's one owner of
 * the `Key -> Dvv` table and the one caller of the engine's store hooks. A (value, version)
 * pair is read and installed as one task on the key's partition, so a reader never sees a
 * value under another install's version, and spec 5.3 is decided here whether the arriving
 * pair is a command (a replicate, ADR 0003) or a value (a repair's push, an anti-entropy leaf).
 *
 * Spec 5.3 for an arrival under `remote` against the `held` version:
 * - nothing held, or `remote` dominates: taken as it came ([Outcome.TAKEN]);
 * - `remote` dominated or equal: kept out ([Outcome.KEPT]), except a value equal by version
 *   and not by encoding (a value that rotted under its version): the greater encoding wins on
 *   both sides, so they converge;
 * - concurrent: [Outcome.MERGED] under a version descending from both. A value merges by its
 *   type's rule (spec 2.5, [merge]); a command carries no value to merge and is applied over
 *   the local value instead (ADR 0003's consequence), which agrees with the type merge when
 *   the command's side is the last writer and differs otherwise.
 *
 * The table is one map per partition, touched only on that partition's thread next to the
 * value, so it needs no lock of its own. A key the table holds and the engine does not is a
 * tombstone, and it is held like any other pair: anti-entropy carries it (T66, closing T28
 * deviation 3).
 *
 * The table survives a restart (T67): this store is the engine's [KeyVersions], so a snapshot
 * writes every version down beside its value and every tombstone as a row of its own, the log
 * carries each version behind the command that moved it, and recovery hands them all back
 * through [restored] before the node serves anyone. The dot counter takes each restored version
 * as a floor on the way past, so it resumes above every dot this node ever gave a write (C2)
 * and a restarted replica answers a quorum read with the authority it had (I2).
 */
class VersionedStore(private val engine: ApEngine, private val counter: DotCounter) : KeyVersions {

    private val tables = ConcurrentHashMap<PartitionId, ConcurrentHashMap<Key, Dvv>>()

    init {
        // One store per engine, and the engine has nothing else to persist versions for: wiring
        // it here is wiring it everywhere the store is built, rather than at each assembly.
        engine.versions = this
    }

    /** Persistence's view of one partition's table, encoded: what the snapshot writes down. */
    override fun on(partition: PartitionId): Map<Key, ByteArray> =
        tables[partition].orEmpty().mapValues { (_, dvv) -> dvv.encode() }

    /** Persistence's view of one key, encoded: what the log carries behind that key's command. */
    override fun of(key: Key): ByteArray = table(key)[key]?.encode() ?: KeyVersions.NO_VERSION

    /**
     * Recovery hands back what it persisted, snapshot first and then the log after it, so the
     * last version a key was written under is the one it ends held under. A version that does not
     * decode fails the restore rather than starting the key over with none, as the dot ceiling's
     * unreadable file does (T51).
     */
    override fun restored(key: Key, version: ByteArray) {
        val dvv = Dvv.decode(version)
        table(key)[key] = dvv
        counter.saw(dvv)
    }

    private fun table(key: Key): ConcurrentHashMap<Key, Dvv> = tables.computeIfAbsent(engine.partitionOf(key)) { ConcurrentHashMap() }

    /** The version held for [key] on its own, null when none: for a test and for the counter's floor. */
    fun version(key: Key): Dvv? = table(key)[key]

    /** The pair under [key]: null when no version is held, a tombstone when the version's value is gone. */
    fun held(key: Key): CompletableFuture<Held?> = engine.onPartitionOf(key) { access -> heldOn(access, key) }

    /** Every pair among [keys], each partition asked as one task. */
    fun held(keys: Collection<Key>): CompletableFuture<List<Held>> {
        val parts = keys.groupBy(engine::partitionOf).values.map { part -> engine.onPartitionOf(part.first()) { access -> part.mapNotNull { heldOn(access, it) } } }
        return CompletableFuture.allOf(*parts.toTypedArray()).thenApply { parts.flatMap { it.join() } }
    }

    /** Every pair whose key [holds] selects, each partition walked as one task: anti-entropy's leaves. */
    fun held(holds: (Key) -> Boolean): CompletableFuture<List<Held>> = engine.onEveryPartition { id, access ->
        val live = access.view(holds).associateBy { it.key }
        tables[id].orEmpty().filterKeys(holds).map { (key, dvv) -> Held(key, live[key]?.value, live[key]?.expiresAt, dvv) }
    }.thenApply { it.flatten() }

    /**
     * Spec 5.1 step 4 and C2 as one task: the key's version bumped with this node's next dot,
     * then [command] run, so two writes on one key chain and no reader sees one without the
     * other. The version moves whether or not the command changed anything, as it always has.
     * Answers the reply and the version the write carries.
     */
    fun write(command: Command.Keyed): CompletableFuture<Pair<Reply, Dvv>> = engine.onPartitionOf(command.key) { access ->
        val dvv = table(command.key).compute(command.key) { _, held -> held?.bump(counter) ?: Dvv(counter.next(), emptyMap()) }!!
        access.execute(command) to dvv
    }

    /** Spec 5.2 steps 1 and 2 as one task: [command] run, and the version it read under. */
    fun read(command: Command.Keyed): CompletableFuture<Pair<Reply, Dvv?>> =
        engine.onPartitionOf(command.key) { it.execute(command) to table(command.key)[command.key] }

    /**
     * A write arriving as the entry the coordinator logged (spec 5.1 step 5, ADR 0003): the
     * [commands] it decodes to, all on one key, under [remote]. Taken, they run in order, as
     * the replica's own log would redo them; merged, they run over the local value.
     */
    fun apply(commands: List<Command.Keyed>, remote: Dvv): CompletableFuture<Outcome> {
        val key = commands.first().key
        require(commands.all { it.key == key }) { "a replicated entry is one key's: $commands" }
        return engine.onPartitionOf(key) { access ->
            val held = table(key)[key]
            val outcome = decide(held, remote)
            when (outcome) {
                Outcome.KEPT -> Unit
                Outcome.TAKEN -> table(key)[key] = remote
                Outcome.MERGED -> table(key)[key] = held!!.merge(remote, counter)
            }
            if (outcome != Outcome.KEPT) for (command in commands) access.execute(command)
            outcome
        }
    }

    /**
     * A value arriving under [remote]: a repair's push (spec 5.2 step 5) or an anti-entropy
     * leaf (spec 2.4). [value] null is a tombstone, which taken deletes the key through a
     * `DEL`, so the deletion is logged like one. Concurrent with a tombstone on either side,
     * the merge is the last writer's side whole (spec 2.5's tiebreak), there being no value
     * on the other to merge with.
     */
    fun install(key: Key, value: Value?, expiresAt: Instant?, remote: Dvv): CompletableFuture<Installed> = engine.onPartitionOf(key) { access ->
        val held = table(key)[key]
        val mine = access.view(listOf(key)).firstOrNull()
        when (decide(held, remote)) {
            Outcome.TAKEN -> Installed(Outcome.TAKEN, put(access, key, value, expiresAt, remote))
            Outcome.KEPT ->
                if (held == remote && greater(value, mine?.value)) Installed(Outcome.TAKEN, put(access, key, value, expiresAt, remote))
                else Installed(Outcome.KEPT, Held(key, mine?.value, mine?.expiresAt, held!!))
            Outcome.MERGED -> Installed(Outcome.MERGED, merged(access, key, mine, held!!, value, expiresAt, remote))
        }
    }

    /** Spec 5.3's concurrent case for two values: the type merge; with a tombstone on either side, the last writer's side whole. */
    private fun merged(access: StoreAccess, key: Key, mine: Stored?, held: Dvv, value: Value?, expiresAt: Instant?, remote: Dvv): Held {
        if (mine != null && value != null) {
            val merged = merge(Versioned(mine.value, held), Versioned(value, remote), counter)
            return put(access, key, merged.value, later(mine.expiresAt, expiresAt), merged.dvv)
        }
        val theirs = lastWriter.compare(remote, held) > 0
        return put(access, key, if (theirs) value else mine?.value, if (theirs) expiresAt else mine?.expiresAt, held.merge(remote, counter))
    }

    /** Spec 5.3's three branches, the same for a command and a value. */
    private fun decide(held: Dvv?, remote: Dvv): Outcome = when {
        held == null || remote.dominates(held) -> Outcome.TAKEN
        held.isConcurrent(remote) -> Outcome.MERGED
        else -> Outcome.KEPT
    }

    /** The pair put under [key] in this task: the version first, then the value, or the key's deletion for a tombstone. */
    private fun put(access: StoreAccess, key: Key, value: Value?, expiresAt: Instant?, dvv: Dvv): Held {
        table(key)[key] = dvv
        if (value == null) access.execute(Command.Del(key)) else access.install(Stored(key, value, expiresAt))
        return Held(key, value, expiresAt, dvv)
    }

    private fun heldOn(access: StoreAccess, key: Key): Held? {
        val dvv = table(key)[key] ?: return null
        val stored = access.view(listOf(key)).firstOrNull()
        return Held(key, stored?.value, stored?.expiresAt, dvv)
    }

    /** The rot rule's order: the greater encoding, an absent value being the least. */
    private fun greater(candidate: Value?, held: Value?): Boolean =
        candidate != null && (held == null || Arrays.compareUnsigned(encodeValue(candidate), encodeValue(held)) > 0)

    /** A merged value's deadline: the later of the two, and no deadline at all if either side had none. */
    private fun later(a: Instant?, b: Instant?): Instant? = if (a == null || b == null) null else maxOf(a, b)
}
