package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply

/**
 * The AtomicReference (CP spec 3.5): opaque bytes per `cp:ref:*` key, mutated only by applying
 * committed entries in log order. The compare and the swap of a CAS happen inside the one applied
 * entry, so no reader ever sees a half state (I21). A TTL is measured against the log time passed
 * in with every call, exactly as the counter's is (CP spec 9.4); nothing here reads a clock.
 */
class AtomicReferenceStateMachine : CpPrimitive {

    override val id = CpPrimitive.REFERENCES

    private val references = HashMap<Key, Reference>()

    fun apply(command: Command.Cp.AtomicReference, now: Long): Reply {
        val current = references[command.key]?.takeUnless { it.expired(now) }
        return when (command) {
            is Command.Cp.RefSet ->
                // The Redis lock idiom: NX takes the reference only when nothing live holds it and
                // XX only when something does, with the lease applied in the same entry (I21).
                if (command.condition?.refuses(current != null) == true) {
                    Reply.Bulk(null)
                } else {
                    references[command.key] = Reference(command.value, command.ttl?.let { now + it.toMillis() })
                    Reply.Simple("OK")
                }
            is Command.Cp.RefGet -> Reply.Bulk(current?.value)
            is Command.Cp.RefCas ->
                if (current == null || !current.value.contentEquals(command.expected)) {
                    Reply.Integer(0)
                } else {
                    // As with the counter, a swap keeps the reference's TTL.
                    references[command.key] = Reference(command.new, current.expiresAt)
                    Reply.Integer(1)
                }
            // The counter's TTL verbs, over a reference: CP spec 9.4 gives them to whichever
            // state machine owns the key, and the deadline is log time, never a clock.
            is Command.Cp.RefExpire -> retime(command.key, current, now + command.ttl.toMillis())
            is Command.Cp.RefPersist ->
                if (current?.expiresAt == null) Reply.Integer(0) else retime(command.key, current, null)
            is Command.Cp.RefTtl -> Reply.Integer(
                when {
                    current == null -> -2
                    current.expiresAt == null -> -1
                    command.precision == Command.Ttl.Precision.MILLIS -> current.expiresAt - now
                    else -> (current.expiresAt - now + 500) / 1000
                },
            )
        }
    }

    /** Gives a live reference the expiry [expiresAt]: 1 when there was one to give it to, else 0. */
    private fun retime(key: Key, current: Reference?, expiresAt: Long?): Reply {
        if (current == null) return Reply.Integer(0)
        references[key] = Reference(current.value, expiresAt)
        return Reply.Integer(1)
    }

    /** A tick drops every reference whose TTL has run out. */
    override fun sweep(now: Long) {
        references.values.removeIf { it.expired(now) }
    }

    override fun snapshot(): ByteArray =
        CpWire.encodeTable(references) { writeBlob(it.value); writeLong(it.expiresAt ?: CpWire.NO_TTL) }

    override fun restore(bytes: ByteArray) = CpWire.decodeTable(bytes, references) {
        Reference(readBlob(), readLong().takeIf { it != CpWire.NO_TTL })
    }

    /** A reference's bytes and, when it has a TTL, the log time at which it stops existing. */
    class Reference(val value: ByteArray, val expiresAt: Long?) {
        fun expired(now: Long) = expiresAt != null && expiresAt <= now

        override fun equals(other: Any?): Boolean = this === other ||
            (other is Reference && value.contentEquals(other.value) && expiresAt == other.expiresAt)

        override fun hashCode(): Int = 31 * value.contentHashCode() + expiresAt.hashCode()

        override fun toString(): String = "Reference(${value.toString(Charsets.ISO_8859_1)}, $expiresAt)"
    }
}
