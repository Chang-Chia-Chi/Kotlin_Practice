package dynacache.engine

import java.time.Clock
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

/**
 * One partition: a single-thread executor and the store only that thread touches (C1 by
 * construction, ADR 0001). Everything below [submit] runs on the partition executor.
 */
internal class Partition(id: PartitionId, private val clock: Clock) {

    private class Entry(val value: ByteArray, val expiresAt: Instant?)

    private val store = HashMap<Key, Entry>()
    private val executor = Executors.newSingleThreadExecutor { r ->
        Thread(r, "partition-${id.index}").apply { isDaemon = true }
    }

    fun submit(command: Command): CompletableFuture<Reply> =
        CompletableFuture.supplyAsync({ execute(command) }, executor)

    fun close() = executor.shutdown()

    /** The clock is read exactly once per command, so a command sees one instant throughout. */
    private fun execute(command: Command): Reply {
        val now = clock.instant()
        return when (command) {
            is Command.Ping -> Reply.Simple("PONG")
            is Command.Get -> Reply.Bulk(live(command.key, now)?.value)
            is Command.Del -> if (live(command.key, now) == null) ZERO else {
                store.remove(command.key)
                ONE
            }
            is Command.Exists -> if (live(command.key, now) == null) ZERO else ONE
            is Command.Type -> Reply.Simple(if (live(command.key, now) == null) "none" else "string")
            is Command.Set -> {
                val exists = live(command.key, now) != null
                val rejected = when (command.condition) {
                    null -> false
                    Command.Set.Condition.NX -> exists
                    Command.Set.Condition.XX -> !exists
                }
                if (rejected) {
                    NIL
                } else {
                    store[command.key] = Entry(command.value, command.ttl?.let(now::plus))
                    OK
                }
            }
        }
    }

    /**
     * The entry under [key] if it is still alive at [now]; an expired one is deleted on this
     * access (spec 5.4's lazy check). A key is readable through its deadline and gone after it.
     */
    private fun live(key: Key, now: Instant): Entry? {
        val entry = store[key] ?: return null
        if (entry.expiresAt != null && now.isAfter(entry.expiresAt)) {
            store.remove(key)
            return null
        }
        return entry
    }

    private companion object {
        val OK = Reply.Simple("OK")
        val NIL = Reply.Bulk(null)
        val ZERO = Reply.Integer(0)
        val ONE = Reply.Integer(1)
    }
}
