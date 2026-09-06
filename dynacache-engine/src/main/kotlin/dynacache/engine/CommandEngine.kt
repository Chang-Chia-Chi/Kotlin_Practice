package dynacache.engine

import java.time.Clock
import java.util.concurrent.CompletableFuture

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

/** The one partition a batch runs on, for the duration of that batch. */
interface PartitionContext {

    /** Runs [command] here and now, on the partition's thread. */
    fun execute(command: Command): Reply
}

/**
 * The Dynamo-style AP engine: a fixed number of partitions, each with its own executor and
 * store, chosen by `key.hash % partitionCount`. Keyless commands run on partition 0.
 */
class ApEngine(partitionCount: Int, clock: Clock) : CommandEngine {

    private val partitions = List(partitionCount) { Partition(PartitionId(it), clock) }

    /** The partition [key] lives on; keys sharing a hash tag share a partition (C12). */
    fun partitionOf(key: Key): PartitionId = PartitionId(key.hash % partitions.size)

    override fun submit(command: Command): CompletableFuture<Reply> = when (command) {
        is Command.Keyed -> partitions[partitionOf(command.key).index].submit(command)
        is Command.Fanned -> fanOut(command)
        is Command.Ping -> partitions[0].submit(command)
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

    override fun <R> atomically(keys: List<Key>, block: (PartitionContext) -> R): CompletableFuture<R> =
        TODO("T14: batches")

    override fun close() = partitions.forEach { it.close() }

}
