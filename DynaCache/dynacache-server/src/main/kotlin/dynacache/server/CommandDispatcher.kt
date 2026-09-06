package dynacache.server

import dynacache.engine.Command
import dynacache.engine.CommandEngine
import dynacache.engine.Key
import dynacache.engine.PartitionContext
import dynacache.engine.Reply
import java.time.Clock
import java.time.Duration
import java.util.concurrent.CompletableFuture

/**
 * The dispatcher (CONTEXT.md): the one place the AP engine and the CP engine meet. It applies the
 * three routing rules of CP spec 9.5 in order and hands the command on:
 *
 * 1. a [Command.Cp] whose key is not in the `cp:` namespace is `-NOTCP`, and one whose key is
 *    goes to the CP engine;
 * 2. any other command naming a `cp:` key goes to the CP engine when it is in the Redis-compat
 *    set, re-targeted to the CP verb it means (CP spec 6.2, 6.5), and is `-NOTCP` otherwise;
 * 3. everything else goes to the AP engine.
 *
 * Re-targeting is the only thing it does to a command: `INCR cp:counter:x` and
 * `CP.LONG.INCR cp:counter:x` are one command by the time an engine sees them. It never rewrites
 * a reply, and it never sends the same command to both engines, which is what C16 and C22 are.
 *
 * [cp] is null on a node with no CP subsystem configured; there, every CP-bound command is
 * `-NOTCP`. [clock] turns `EXPIRE`'s absolute deadline back into the span the CP verb carries;
 * the CP log then evaluates that span against log time (CP spec 5), not against this clock.
 */
class CommandDispatcher(
    private val ap: CommandEngine,
    private val cp: CommandEngine?,
    private val clock: Clock = Clock.systemUTC(),
) : CommandEngine {

    override fun submit(command: Command): CompletableFuture<Reply> = when {
        command is Command.Cp ->
            if (command.key.isCpKey()) toCp(command) else notCp("${command.key} is not a cp: key")
        // A fanned command counts as CP-bound when any of its keys is, not only the first: half an
        // MGET must not reach the AP engine with a cp: key in it (C16).
        keysOf(command).any(Key::isCpKey) ->
            try {
                toCp(compat(command))
            } catch (rejected: Rejected) {
                done(rejected.error)
            }
        else -> ap.submit(command)
    }

    /**
     * A batch runs on the AP engine: the CP log already serializes every entry, so CP has no
     * batches to run. A `cp:` key among the declared ones is refused before anything runs, the way
     * a cross-partition span is (C12, C16), and reaches the caller as this future's failure.
     */
    override fun <R> atomically(keys: List<Key>, block: (PartitionContext) -> R): CompletableFuture<R> =
        if (keys.any(Key::isCpKey)) {
            CompletableFuture.failedFuture(IllegalArgumentException("a batch cannot name a cp: key"))
        } else {
            ap.atomically(keys, block)
        }

    /** Closes the AP engine and, when this node has one, the CP engine. */
    override fun close() {
        ap.close()
        cp?.close()
    }

    private fun toCp(command: Command.Cp): CompletableFuture<Reply> =
        cp?.submit(command) ?: notCp("this node has no CP engine")

    private fun notCp(message: String): CompletableFuture<Reply> = done(Reply.Error("NOTCP", message))

    /**
     * The CP verb [command] means, or the refusal when the `cp:` namespace does not answer it. The
     * counter's verbs answer a key of any other shape, and `cp:ref:*` is the reference's own,
     * TTL verbs included: CP spec 9.4 gives `EXPIRE`, `TTL` and `PERSIST` to the state machine
     * that owns the key (CP spec 6.2, 6.5).
     *
     * ponytail: the prefix is the only thing consulted, so `GET cp:lock:x` reads an empty counter
     * rather than answering `-WRONGTYPE`, and `EXPIRE cp:lock:x` answers 0 where CP spec 9.4 says
     * it is rejected. Nothing tracks which primitive owns a key yet; the repair is a key-to-kind
     * check in the state machines, where `-WRONGTYPE` would have to come from anyway.
     */
    private fun compat(command: Command): Command.Cp {
        if (command !is Command.Keyed) refuse(command)
        val key = command.key
        val reference = key.toString().startsWith(REFERENCE_PREFIX)
        return when (command) {
            is Command.Get -> if (reference) Command.Cp.RefGet(key) else Command.Cp.LongGet(key)
            // SET NX and SET XX are the compat set's too (CP spec 1, 9.5): they re-target to the
            // conditional form of the kind's SET verb, which applies the condition, the value and
            // the TTL in one committed entry.
            is Command.Set ->
                if (reference) {
                    Command.Cp.RefSet(key, command.value, command.ttl, command.condition)
                } else {
                    Command.Cp.LongSet(key, command.value.asLong() ?: notAnInteger(), command.ttl, command.condition)
                }
            is Command.IncrBy -> when (command.delta) {
                1L -> Command.Cp.LongIncr(key)
                -1L -> Command.Cp.LongDecr(key)
                else -> Command.Cp.LongIncrBy(key, command.delta)
            }
            // EXPIRE, PEXPIRE and EXPIREAT arrive as one absolute deadline; the CP verb carries
            // the span from here, because only the log may say when "here" was.
            is Command.Expire -> Duration.between(clock.instant(), command.deadline).let { ttl ->
                if (reference) Command.Cp.RefExpire(key, ttl) else Command.Cp.LongExpire(key, ttl)
            }
            is Command.Ttl ->
                if (reference) Command.Cp.RefTtl(key, command.precision) else Command.Cp.LongTtl(key, command.precision)
            is Command.Persist -> if (reference) Command.Cp.RefPersist(key) else Command.Cp.LongPersist(key)
            // DEL, EXISTS and TYPE are in the compat set of CP spec 9.5 but no CP primitive
            // answers them yet, so they are a rejection rather than a silent trip to AP.
            else -> refuse(command)
        }
    }

    /** Carries a refusal out of [compat]; never leaves [submit]. */
    private class Rejected(val error: Reply.Error) : RuntimeException(null, null, false, false)

    private fun refuse(command: Command): Nothing {
        val key = keysOf(command).firstOrNull(Key::isCpKey)
        throw Rejected(Reply.Error("NOTCP", "$key is a cp: key and this is not a command it accepts"))
    }

    private fun notAnInteger(): Nothing =
        throw Rejected(Reply.Error("ERR", "value is not an integer or out of range"))

    private companion object {
        const val REFERENCE_PREFIX = "cp:ref:"
    }
}

/** True when [this] belongs to the CP namespace, which the CP engine owns alone (C16). */
internal fun Key.isCpKey(): Boolean = toString().startsWith("cp:")

/** The keys [command] names: what a batch declares, and what the namespace rules are read from. */
internal fun keysOf(command: Command): List<Key> = when (command) {
    is Command.Keyed -> listOf(command.key)
    is Command.Cp -> listOf(command.key)
    is Command.Fanned -> command.keys
    else -> emptyList()
}

/** A counter's value on the wire, or null when it is not a number Redis would accept for one. */
private fun ByteArray.asLong(): Long? = toString(Charsets.ISO_8859_1).toLongOrNull()
