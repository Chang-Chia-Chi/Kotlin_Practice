package dynacache.engine

import java.time.Instant
import java.util.concurrent.CompletableFuture

/**
 * One live key as replicas exchange it (T28 anti-entropy): its value, a frozen copy the caller
 * owns, and its deadline or null. The version it carries is the cluster's business, not the
 * engine's (CONTEXT.md "version").
 */
class Stored(val key: Key, val value: Value, val expiresAt: Instant?)

/**
 * One partition's store for the length of one task on its thread (T66): frozen copies of its
 * live keys, a copy put back under its key, and a command run here and now. A caller that
 * keeps something beside a value -- the cluster keeps its version -- moves both inside the one
 * task, so no reader on this thread sees one without the other.
 */
interface StoreAccess {

    /** Frozen copies of the live keys among [keys]. */
    fun view(keys: Collection<Key>): List<Stored>

    /** Frozen copies of every live key [holds] selects: a walk of the whole partition. */
    fun view(holds: (Key) -> Boolean): List<Stored>

    /**
     * Puts [stored] under its key exactly as a restore does, replacing what was held, TTL and
     * all; a copy already past its deadline removes the key instead. Not logged to the WAL:
     * like a restore, an installed value is what a peer already holds durably, and a node that
     * recovers from its own log will be handed it again by the next anti-entropy round.
     */
    fun install(stored: Stored)

    /** Runs [command] here and now, logged as any command is. Its key must live on this partition. */
    fun execute(command: Command): Reply
}

/** [block] as one task on [key]'s partition, with that partition's store at hand. */
fun <R> ApEngine.onPartitionOf(key: Key, block: (StoreAccess) -> R): CompletableFuture<R> =
    partitions[partitionOf(key).index].withStore(block)

/** [block] as one task on every partition, each on its own executor and told which it is, the answers in partition order. */
fun <R> ApEngine.onEveryPartition(block: (PartitionId, StoreAccess) -> R): CompletableFuture<List<R>> {
    val parts = partitions.mapIndexed { index, partition -> partition.withStore { block(PartitionId(index), it) } }
    return CompletableFuture.allOf(*parts.toTypedArray()).thenApply { parts.map { it.join() } }
}
