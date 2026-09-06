package dynacache.engine

import dynacache.engine.persist.RdbEntry
import java.time.Instant
import java.util.concurrent.CompletableFuture

/**
 * One live key as replicas exchange it (T28 anti-entropy): its value, a frozen copy the caller
 * owns, and its deadline or null. The version it carries is the cluster's business, not the
 * engine's (CONTEXT.md "version").
 */
class Stored(val key: Key, val value: Value, val expiresAt: Instant?)

/** Frozen copies of every live key [holds] selects, each partition scanned as one task on its own executor. */
fun ApEngine.view(holds: (Key) -> Boolean): CompletableFuture<List<Stored>> =
    gather(partitions.map { it.view(holds) })

/** Frozen copies of the live keys among [keys], each partition asked as one task on its own executor. */
fun ApEngine.view(keys: Collection<Key>): CompletableFuture<List<Stored>> =
    gather(keys.groupBy { partitionOf(it).index }.map { (index, part) -> partitions[index].view(part) })

/**
 * Puts [stored] under its key exactly as a restore does: through the partition's one write
 * funnel, on its executor, replacing what was held, TTL and all. Not logged to the WAL: like a
 * restore, an installed value is what a peer already holds durably, and a node that recovers
 * from its own log will be handed it again by the next anti-entropy round.
 */
fun ApEngine.install(stored: Stored): CompletableFuture<Void> =
    partitions[partitionOf(stored.key).index].restore(listOf(RdbEntry(stored.key, stored.value, stored.expiresAt, ByteArray(0))))

private fun gather(parts: List<CompletableFuture<List<Stored>>>): CompletableFuture<List<Stored>> =
    CompletableFuture.allOf(*parts.toTypedArray()).thenApply { parts.flatMap { it.join() } }
