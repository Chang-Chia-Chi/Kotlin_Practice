package dynacache.engine

import java.time.Duration

/**
 * A command a client asked the engine to run. Sealed and frozen as a root (plan 2.3); each
 * ticket adds the variants it implements.
 */
sealed class Command {
    data object Ping : Command()

    data class Get(val key: Key) : Command()

    /**
     * `SET key value [NX|XX] [EX seconds|PX millis]`. The parser reduces `EX` and `PX` to one
     * [ttl]; the engine turns it into an absolute expiry instant from the injected clock.
     * Not a data class: [value] is a byte array, and array equality is by reference.
     */
    class Set(
        val key: Key,
        val value: ByteArray,
        val condition: Condition? = null,
        val ttl: Duration? = null,
    ) : Command() {
        /** `NX`: only when the key is absent. `XX`: only when it exists. */
        enum class Condition { NX, XX }
    }

    /** Single-key `DEL`; the multi-key form fans out in T03 (ADR 0002). */
    data class Del(val key: Key) : Command()

    /** Single-key `EXISTS`; the multi-key form fans out in T03 (ADR 0002). */
    data class Exists(val key: Key) : Command()

    data class Type(val key: Key) : Command()
}
