package dynacache.engine

import dynacache.engine.persist.RdbEntry
import dynacache.engine.persist.RdbSnapshot
import dynacache.engine.persist.WalCodec
import dynacache.engine.persist.WalWriter
import java.time.Clock
import java.time.Instant
import java.util.Random
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CyclicBarrier

/**
 * Runs commands. The engine owns one single-thread executor per partition, so a caller never
 * serializes anything itself and the single-writer rule C1 holds by construction (ADR 0001).
 *
 * Two adapters present this shape: the AP engine and, from T38, the CP engine.
 */
interface CommandEngine {

    /**
     * Runs [command] on its key's partition executor. A multi-key command is fanned out to the
     * partitions involved and joined in argument order; it is not atomic across them (ADR 0002).
     */
    fun submit(command: Command): CompletableFuture<Reply>

    /**
     * Runs [block] on the partition of [keys] with nothing interleaved: the batch behind
     * MULTI/EXEC and EVAL. Keys on different partitions are rejected before [block] runs (C12),
     * and a command inside the block touching an undeclared key answers with an error reply.
     */
    fun <R> atomically(keys: List<Key>, block: (PartitionContext) -> R): CompletableFuture<R>

    /** Shuts the partition executors down. */
    fun close()
}

/**
 * C12: the declared keys did not all live on one partition, so nothing ran. The batch's answer
 * is a [Reply], but `atomically` answers whatever the block returns, so the refusal travels as
 * the failure of the returned future and the caller writes [error] back.
 */
class CrossPartitionBatch(keys: List<Key>, spanned: List<PartitionId>) :
    RuntimeException("keys span ${spanned.size} partitions: $keys") {

    val error: Reply.Error = Reply.Error(
        "CROSSSLOT",
        "Keys in request don't hash to the same slot",
    )
}

/**
 * Which key a partition over its memory share gives up (spec 2.7). The step around the choice is
 * the same either way -- expired keys first, then a bounded number of live ones -- so a policy is
 * one function, and this is the switch between the two implementations of it.
 */
enum class EvictionPolicy {

    /** Redis-style sampling LRU: draw K random keys, evict the one accessed longest ago. */
    LRU,

    /** Caffeine's W-TinyLFU: an admission window, a segmented main space and a frequency sketch. */
    W_TINYLFU;

    /** The name `INFO` reports, in Redis's own lower-case-and-dashes style. */
    val info: String = name.lowercase().replace('_', '-')
}

/** The one partition a batch runs on, for the duration of that batch. */
interface PartitionContext {

    /** Runs [command] here and now, on the partition's thread. */
    fun execute(command: Command): Reply
}

/**
 * The Dynamo-style AP engine: a fixed number of partitions, each with its own executor and
 * store, chosen by `key.hash % partitionCount`. Keyless commands run on partition 0, except the
 * ones that need every partition's answer. [random] is injected so a test can seed it.
 */
class ApEngine(
    partitionCount: Int,
    clock: Clock,
    private val random: Random = Random(),
    /**
     * How wide one wheel tick is, and so how long after its deadline a key may linger before
     * [tick] removes it (C7). The server's scheduler reads this to set its own period.
     */
    val tickMillis: Long = 1000,
    /**
     * The node's memory threshold (spec 2.7), split evenly across the partitions. Each partition
     * evicts its own keys to stay under its share and coordinates with no other (spec 5.5). Null,
     * the default, means the node holds everything it is given.
     */
    maxMemoryBytes: Long? = null,
    /**
     * Which key a partition over its share gives up (spec 2.7). Every partition of a node runs
     * the same policy, so `INFO` reads it off any one of them.
     */
    policy: EvictionPolicy = EvictionPolicy.LRU,
) : CommandEngine {

    /**
     * The write-ahead log every mutation is appended to before its reply completes (C14); null
     * is a node without one. Attached by recovery once the state the log continues is in place,
     * so a replayed entry is never logged a second time.
     */
    @Volatile
    var wal: WalWriter? = null
        internal set

    // Each partition draws from its own stream, seeded from the engine's, so one injected seed
    // makes the whole engine reproducible even though the partitions run on their own threads.
    private val partitions = List(partitionCount) {
        Partition(
            PartitionId(it),
            clock,
            Random(random.nextLong()),
            tickMillis,
            maxMemoryBytes?.let { bytes -> bytes / partitionCount } ?: Long.MAX_VALUE,
            policy,
            ::log,
        )
    }

    /** The partition hook: one entry per command that changed a store, answering when it is durable. */
    private fun log(command: Command, reply: Reply, now: Instant): CompletableFuture<*>? {
        val wal = wal ?: return null
        val (op, payload) = WalCodec.encode(command, reply, now) ?: return null
        return wal.append(op, payload).durable
    }

    /**
     * Advances every partition's timer wheel to the clock's current reading, deleting the keys
     * whose deadlines have fallen due. The engine owns no thread beyond its partition executors:
     * the server drives this once per [tickMillis] and the work runs on each partition's own
     * thread, so an expiring key is deleted with the same exclusion a command has (C1).
     */
    fun tick(): CompletableFuture<Void> =
        CompletableFuture.allOf(*partitions.map { it.tick() }.toTypedArray())

    /** The partition [key] lives on; keys sharing a hash tag share a partition (C12). */
    fun partitionOf(key: Key): PartitionId = PartitionId(key.hash % partitions.size)

    override fun submit(command: Command): CompletableFuture<Reply> = when (command) {
        is Command.Keyed -> partitions[partitionOf(command.key).index].submit(command)
        is Command.Fanned -> fanOut(command)
        is Command.EveryPartition -> everyPartition(command)
        is Command.Ping, is Command.CommandTable -> partitions[0].submit(command)
        is Command.Scan -> scan(command)
        // C16: a cp:* key never belongs here. The dispatcher (T44) routes it away; if one still
        // arrives, the spec's answer is -NOTCP, not a partition write.
        is Command.Cp -> CompletableFuture.completedFuture(
            Reply.Error("NOTCP", "${command.key} is a CP key; the AP engine does not serve it")
        )
    }

    /**
     * One partition per call: the cursor's high 32 bits pick it, the low 32 are its own cursor.
     * A partition that hands back 0 is done, so the next call starts the next partition at 0,
     * and the last partition's 0 is the walk's. A cursor past the last partition is done too.
     */
    private fun scan(command: Command.Scan): CompletableFuture<Reply> {
        val index = (command.cursor ushr 32).toInt()
        if (index !in partitions.indices) return CompletableFuture.completedFuture(Partition.scanReply(0, emptyList()))
        val inner = Command.Scan(command.cursor and 0xFFFF_FFFFL, command.pattern, command.count)
        return partitions[index].scan(inner).thenApply { (next, found) ->
            val cursor = when {
                next != 0L -> (index.toLong() shl 32) or next
                index + 1 < partitions.size -> (index + 1L) shl 32
                else -> 0L
            }
            Partition.scanReply(cursor, found)
        }
    }
    /**
     * A keyless command run on every partition, one after the previous one finished, and joined
     * in partition order. Sequential for the same reason fan-out is: a caller sees the same
     * partition-by-partition view either way, and no partition is asked to know about another.
     */
    private fun everyPartition(command: Command.EveryPartition): CompletableFuture<Reply> {
        var replies = CompletableFuture.completedFuture(emptyList<Reply>())
        for (partition in partitions) {
            replies = replies.thenCompose { soFar -> partition.submit(command).thenApply { soFar + it } }
        }
        return replies.thenApply { command.join(it, random) }
    }

    /**
     * ADR 0002: the command is split by partition and run partition by partition, one part after
     * the previous one finished, then joined in argument order. Nothing is atomic across
     * partitions: a concurrent write lands between two parts, and a test pins that.
     *
     * Sequential on purpose; if fan-out latency ever matters, submit the parts together and
     * gather them with `allOf` instead.
     */
    private fun fanOut(command: Command.Fanned): CompletableFuture<Reply> {
        val joined = arrayOfNulls<Reply>(command.keys.size)
        var parts = CompletableFuture.completedFuture(Unit)
        for ((index, positions) in command.keys.indices.groupBy { partitionOf(command.keys[it]).index }) {
            parts = parts.thenCompose {
                partitions[index].submitAll(positions.map(command::single)).thenApply { replies ->
                    positions.forEachIndexed { at, position -> joined[position] = replies[at] }
                }
            }
        }
        return parts.thenApply { command.join(joined.map { it!! }) }
    }

    /**
     * C12: the span is checked here, before anything is submitted, so a rejected batch leaves
     * the partition untouched. A batch with no key at all runs on partition 0, where every
     * other keyless command runs.
     */
    override fun <R> atomically(keys: List<Key>, block: (PartitionContext) -> R): CompletableFuture<R> {
        val spanned = keys.map(::partitionOf).distinct()
        if (spanned.size > 1) return CompletableFuture.failedFuture(CrossPartitionBatch(keys, spanned))
        val partition = partitions[spanned.singleOrNull()?.index ?: 0]
        val batch = Batch(partition, keys.toSet())
        return partition.inOneTask { block(batch) }
    }

    /**
     * The declared keys' partition, for as long as the block runs on its thread. A command
     * naming a key the batch did not declare is refused rather than run: it would touch a key
     * the C12 span check never saw, and on a batch of two keys that is a key on another
     * partition. The batch continues either way (I11, Redis semantics).
     */
    private class Batch(private val partition: Partition, private val declared: Set<Key>) : PartitionContext {

        override fun execute(command: Command): Reply = when (command) {
            is Command.Keyed ->
                if (command.key in declared) partition.execute(command)
                else Reply.Error("ERR", "${command.key} was not declared by this batch")
            // Keyless and partition-local: they read nothing outside this partition's store.
            is Command.Ping, is Command.CommandTable -> partition.execute(command)
            // Everything else spans partitions by definition -- a fan-out, a keyspace walk, a
            // SCAN cursor, a CP key -- and so cannot run inside one partition's task.
            else -> Reply.Error("ERR", "this command spans partitions and cannot run inside a batch")
        }
    }

    /**
     * Every partition's point-in-time view at [now], each taken as one task on its own executor.
     * The views are one cut: every partition parks at a barrier before copying, and [cut] runs
     * while all of them are parked, so nothing is appended to the log between its answer and
     * any view. That answer is the checkpoint's WAL seq; the default is a node without a log.
     */
    internal fun snapshotView(now: Instant, cut: () -> Long = { 0 }): CompletableFuture<RdbSnapshot> {
        var seq = 0L
        val barrier = CyclicBarrier(partitions.size) { seq = cut() }
        val views = partitions.map { it.snapshotView(now, barrier) }
        return CompletableFuture.allOf(*views.toTypedArray()).thenApply { RdbSnapshot(seq, views.flatMap { it.join() }) }
    }

    /** Writes [entries] into their partitions, each partition on its own executor. */
    internal fun restore(entries: List<RdbEntry>): CompletableFuture<Void> {
        val byPartition = entries.groupBy { partitionOf(it.key).index }
        return CompletableFuture.allOf(*byPartition.map { (index, part) -> partitions[index].restore(part) }.toTypedArray())
    }

    override fun close() = partitions.forEach { it.close() }

}
