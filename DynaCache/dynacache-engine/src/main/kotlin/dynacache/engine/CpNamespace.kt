package dynacache.engine

import java.time.Duration
import kotlin.reflect.KClass

/**
 * Which CP primitive owns a key. CP spec 2 writes each state machine's keys in its own
 * sub-namespace of `cp:`, so the key alone says which primitive answers for it -- and a `cp:`
 * key in no primitive's sub-namespace is [UNTYPED], which the counter answers, as it always has.
 *
 * [prefix] is read in declaration order, so `cp:` comes last and never hides a longer one.
 */
enum class CpKind(internal val prefix: String) {
    COUNTER("cp:counter:"),
    LOCK("cp:lock:"),
    SEMAPHORE("cp:sem:"),
    LATCH("cp:latch:"),
    REFERENCE("cp:ref:"),
    SESSION("cp:session"),
    UNTYPED("cp:"),
}

/** What the `cp:` namespace makes of one command: see [CpNamespace.route]. */
sealed interface CpRouting {

    /** The command names no `cp:` key, so it is the AP engine's and untouched. */
    data object Ap : CpRouting

    /** The CP verb the command means on its `cp:` key; the only thing a CP engine may be given. */
    data class Verb(val command: Command.Cp) : CpRouting

    /** The namespace does not answer the command; [error] is the reply the client reads. */
    data class Refused(val error: Reply.Error) : CpRouting
}

/**
 * The `cp:` namespace rule (C16, C22): is this a CP key, which primitive kind owns it, and which
 * Redis commands may touch it. It is written once here, beside [Command.Cp], and read by the
 * dispatcher, by both CP engines and by the AP engine's `-NOTCP` reply, so a key's kind is looked
 * up in one place and the compat re-target cannot drift from the routing that carries it.
 *
 * The rule is total: [route] answers for every command, and every refusal is one of the two
 * errors CP spec 6.8 defines for the namespace -- `-NOTCP` for a command it does not answer at
 * all, `-WRONGTYPE` for a verb of one primitive aimed at another primitive's key.
 */
object CpNamespace {

    /**
     * The Redis-compat-for-CP set of CP spec 9.5 (`SET`, `GET`, `DEL`, `EXISTS`, `INCR`, `DECR`,
     * `INCRBY`, `DECRBY`, `SETEX`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `TYPE`), as the
     * commands those fifteen names parse to. A command outside it on a `cp:` key is refused
     * outright; one inside it either re-targets to the verb its key's kind names or is refused
     * for want of a CP verb, which is what `DEL`, `EXISTS` and `TYPE` still are.
     */
    val COMPAT: Set<KClass<out Command>> = setOf(
        Command.Set::class,
        Command.Get::class,
        Command.Del::class,
        Command.DelKeys::class,
        Command.Exists::class,
        Command.ExistsKeys::class,
        Command.IncrBy::class,
        Command.Expire::class,
        Command.Ttl::class,
        Command.Persist::class,
        Command.Type::class,
    )

    /** The primitive [key] belongs to, or null when [key] is not in the `cp:` namespace at all. */
    fun kindOf(key: Key): CpKind? =
        key.toString().let { text -> CpKind.entries.firstOrNull { text.startsWith(it.prefix) } }

    /** True when [key] belongs to the CP namespace, which the CP engine owns alone (C16). */
    fun owns(key: Key): Boolean = kindOf(key) != null

    /** The primitive [command] is a verb of, or null for the verbs that name no key of their own. */
    fun kindOf(command: Command.Cp): CpKind? = when (command) {
        is Command.Cp.AtomicLong -> CpKind.COUNTER
        is Command.Cp.FencedLock -> CpKind.LOCK
        is Command.Cp.Semaphore -> CpKind.SEMAPHORE
        is Command.Cp.CountDownLatch -> CpKind.LATCH
        is Command.Cp.AtomicReference -> CpKind.REFERENCE
        is Command.Cp.Session -> CpKind.SESSION
        is Command.Cp.Introspection -> null
    }

    /**
     * Why a CP engine may not run [command], or null when it may: the edge both CP engines check
     * before they replicate anything. A verb outside the namespace is `-NOTCP` (C16) and a verb
     * aimed at another primitive's key is `-WRONGTYPE` (CP spec 6.8).
     */
    fun refusalFor(command: Command.Cp): Reply.Error? {
        val kind = kindOf(command.key) ?: return notCp("${command.key} is not a cp: key")
        val owner = kindOf(command) ?: return null
        return if (kind == CpKind.UNTYPED || kind == owner) null else wrongType(command.key, kind)
    }

    /**
     * Where [command] goes. A [Command.Cp] is the CP engine's once its key checks out; any other
     * command naming a `cp:` key is the CP verb it means when the key's kind has one, and a
     * refusal otherwise; everything else is the AP engine's (CP spec 9.5's three rules, in order).
     */
    fun route(command: Command): CpRouting {
        if (command is Command.Cp) {
            return refusalFor(command)?.let(CpRouting::Refused) ?: CpRouting.Verb(command)
        }
        if (command is Command.Keyed) {
            return compat(command, kindOf(command.key) ?: return CpRouting.Ap)
        }
        // A fanned command is CP-bound when any of its keys is, not only the first, and no CP
        // primitive answers one: half an MGET must not reach the AP engine with a cp: key in it.
        return if (keysOf(command).any(::owns)) refused(command) else CpRouting.Ap
    }

    /**
     * The CP verb `EXPIRE` and its three other spellings mean on [key], for the parser: what the
     * CP log evaluates is a span against log time (CP spec 5), so the span goes straight to the
     * verb. Turning it into an instant and back again would read a clock twice for one deadline.
     */
    fun expiry(key: Key, ttl: Duration): CpRouting {
        val kind = kindOf(key) ?: return CpRouting.Ap
        return valued(key, kind, { Command.Cp.LongExpire(key, ttl) }, { Command.Cp.RefExpire(key, ttl) })
    }

    /** The one `-NOTCP` (CP spec 6.8): a command the `cp:` namespace does not route. */
    fun notCp(message: String): Reply.Error = Reply.Error("NOTCP", message)

    /** The keys [command] names: what a batch declares, and what the namespace rules are read from. */
    fun keysOf(command: Command): List<Key> = when (command) {
        is Command.Keyed -> listOf(command.key)
        is Command.Cp -> listOf(command.key)
        is Command.Fanned -> command.keys
        else -> emptyList()
    }

    /**
     * CP spec 6.8's `-WRONGTYPE`, "key exists as different primitive": [key] is written in
     * [kind]'s sub-namespace and the command belongs to another primitive.
     */
    private fun wrongType(key: Key, kind: CpKind): Reply.Error =
        Reply.Error("WRONGTYPE", "$key belongs to the ${kind.name.lowercase()}, which does not answer this command")

    private fun compat(command: Command.Keyed, kind: CpKind): CpRouting {
        val key = command.key
        return when (command) {
            is Command.Get -> valued(key, kind, { Command.Cp.LongGet(key) }, { Command.Cp.RefGet(key) })
            is Command.Persist -> valued(key, kind, { Command.Cp.LongPersist(key) }, { Command.Cp.RefPersist(key) })
            is Command.Ttl -> valued(
                key,
                kind,
                { Command.Cp.LongTtl(key, command.precision) },
                { Command.Cp.RefTtl(key, command.precision) },
            )
            is Command.Set -> set(command, kind)
            // A counter's verb, and no other primitive's: a reference holds bytes, not a number.
            is Command.IncrBy -> counterOnly(key, kind) {
                when (command.delta) {
                    1L -> Command.Cp.LongIncr(key)
                    -1L -> Command.Cp.LongDecr(key)
                    else -> Command.Cp.LongIncrBy(key, command.delta)
                }
            }
            // EXPIRE reaches an engine as the verb [expiry] built from the span itself, so the
            // kinds that answer an expiry have nothing left to say here; the others still refuse.
            is Command.Expire -> if (kind.valueBearing()) refused(command) else CpRouting.Refused(wrongType(key, kind))
            // Everything else, DEL and EXISTS and TYPE among it, is a command no CP primitive
            // answers at all rather than one aimed at the wrong primitive, so CP spec 9.5 rule 2
            // applies whatever kind the key is: the cp: namespace does not take it.
            else -> refused(command)
        }
    }

    /** A verb the counter and the reference each answer in their own way; any other kind is a mismatch. */
    private fun valued(
        key: Key,
        kind: CpKind,
        counter: () -> Command.Cp,
        reference: () -> Command.Cp,
    ): CpRouting = when (kind) {
        CpKind.COUNTER, CpKind.UNTYPED -> CpRouting.Verb(counter())
        CpKind.REFERENCE -> CpRouting.Verb(reference())
        else -> CpRouting.Refused(wrongType(key, kind))
    }

    /** A verb only the counter answers. */
    private fun counterOnly(key: Key, kind: CpKind, verb: () -> Command.Cp): CpRouting =
        if (kind == CpKind.COUNTER || kind == CpKind.UNTYPED) {
            CpRouting.Verb(verb())
        } else {
            CpRouting.Refused(wrongType(key, kind))
        }

    private fun set(command: Command.Set, kind: CpKind): CpRouting {
        val key = command.key
        if (kind == CpKind.REFERENCE) {
            return CpRouting.Verb(Command.Cp.RefSet(key, command.value, command.ttl, command.condition))
        }
        if (kind != CpKind.COUNTER && kind != CpKind.UNTYPED) return CpRouting.Refused(wrongType(key, kind))
        // A counter's value is a number; anything else is the error Redis gives for one, and NX
        // does not excuse it, because the value is read before the condition is.
        val value = command.value.toString(Charsets.ISO_8859_1).toLongOrNull()
            ?: return CpRouting.Refused(Reply.Error("ERR", "value is not an integer or out of range"))
        return CpRouting.Verb(Command.Cp.LongSet(key, value, command.ttl, command.condition))
    }

    /** True when the kind holds a value the compat set can read: the counter, or the reference. */
    private fun CpKind.valueBearing(): Boolean =
        this == CpKind.COUNTER || this == CpKind.REFERENCE || this == CpKind.UNTYPED

    private fun refused(command: Command): CpRouting {
        val key = keysOf(command).firstOrNull(::owns)
        return CpRouting.Refused(
            if (command::class in COMPAT) {
                notCp("$key is a cp: key and no CP verb answers this command yet")
            } else {
                notCp("$key is a cp: key and this is not a command it accepts")
            },
        )
    }
}
