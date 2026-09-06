package dynacache.engine

import java.time.Duration

/**
 * A command a client asked the engine to run. Sealed and frozen as a root (plan 2.3); each
 * ticket adds the variants it implements.
 */
sealed class Command {

    /**
     * A command that names exactly one key. The engine routes it to that key's partition, and a
     * partition only ever sees commands of this shape (plus [Ping]).
     */
    sealed class Keyed(internal val needs: Value.Kind?) : Command() {
        abstract val key: Key
    }

    /**
     * A command that names several keys. The engine splits it by partition, runs the parts
     * partition by partition and joins their replies in argument order; nothing is atomic
     * across partitions (ADR 0002). Each part is the [single] command for one argument, so a
     * partition still only ever sees single-key work.
     */
    sealed class Fanned(val keys: List<Key>) : Command() {

        /** The single-key command for argument [index]. */
        internal abstract fun single(index: Int): Command

        /** Joins the per-argument [replies], already in argument order. */
        internal abstract fun join(replies: List<Reply>): Reply
    }

    data object Ping : Command()

    /**
     * A CP command: linearizable work on a `cp:*` key, replicated through the Raft log and
     * applied by a CP state machine (CP spec 2.1, 6.2). Never reaches a partition executor;
     * the AP engine answers `-NOTCP` if one ever arrives there (C16).
     */
    sealed class Cp : Command() {
        abstract val key: Key

        /** `CP.LONG.SET K n`. */
        data class LongSet(override val key: Key, val value: Long) : Cp()

        /** `CP.LONG.GET K`: the value, or nil when the counter was never written. */
        data class LongGet(override val key: Key) : Cp()

        /** `CP.LONG.INCR K`: the new value; a missing counter counts as 0. */
        data class LongIncr(override val key: Key) : Cp()

        /** `CP.LONG.DECR K`: the new value; a missing counter counts as 0. */
        data class LongDecr(override val key: Key) : Cp()

        /** `CP.LONG.ADD K d`, the `INCRBY` form: the new value. */
        data class LongIncrBy(override val key: Key, val delta: Long) : Cp()

        /** `CP.LONG.ADD K -d`, the `DECRBY` form: the new value. */
        data class LongDecrBy(override val key: Key, val delta: Long) : Cp()

        /** `CP.LONG.CAS K expected new`: 1 when the swap happened, 0 when it did not. */
        data class LongCas(override val key: Key, val expected: Long, val new: Long) : Cp()
    }


    data class Get(override val key: Key) : Keyed(Value.Kind.STRING)

    /**
     * `SET key value [NX|XX] [EX seconds|PX millis]`. The parser reduces `EX` and `PX` to one
     * [ttl]; the engine turns it into an absolute expiry instant from the injected clock.
     * Not a data class: [value] is a byte array, and array equality is by reference.
     */
    class Set(
        override val key: Key,
        val value: ByteArray,
        val condition: Condition? = null,
        val ttl: Duration? = null,
    ) : Keyed(null) {
        /** `NX`: only when the key is absent. `XX`: only when it exists. */
        enum class Condition { NX, XX }
    }

    /** One key's `DEL`; the variadic form is [DelKeys], which fans out to these. */
    data class Del(override val key: Key) : Keyed(null)

    /** One key's `EXISTS`; the variadic form is [ExistsKeys], which fans out to these. */
    data class Exists(override val key: Key) : Keyed(null)

    data class Type(override val key: Key) : Keyed(null)

    /**
     * `INCR`, `DECR`, `INCRBY` and `DECRBY` in one variant: they differ only in [delta], which
     * the parser signs. A missing key counts as 0; a non-integer value is an error.
     */
    data class IncrBy(override val key: Key, val delta: Long) : Keyed(Value.Kind.STRING)

    /** `APPEND key value`; a missing key starts empty. Replies with the new length. */
    class Append(override val key: Key, val value: ByteArray) : Keyed(Value.Kind.STRING)

    /** `STRLEN key`; a missing key is 0 long. */
    data class StrLen(override val key: Key) : Keyed(Value.Kind.STRING)

    /** `HGET key field`. */
    class HGet(override val key: Key, val field: ByteArray) : Keyed(Value.Kind.HASH)

    /** `HSET key field value [field value ...]`: replies with how many fields were new. */
    class HSet(override val key: Key, val entries: List<Pair<ByteArray, ByteArray>>) : Keyed(Value.Kind.HASH)

    /** `HGETALL key`: one flat array, field then value, field then value. */
    data class HGetAll(override val key: Key) : Keyed(Value.Kind.HASH)

    /** `HDEL key field [field ...]`: how many fields went. The key goes with its last field. */
    class HDel(override val key: Key, val fields: List<ByteArray>) : Keyed(Value.Kind.HASH)

    /** `HMSET key field value [field value ...]`: the same write as [HSet], answering `+OK`. */
    class HMSet(override val key: Key, val entries: List<Pair<ByteArray, ByteArray>>) : Keyed(Value.Kind.HASH)

    /** `HMGET key field [field ...]`: one bulk per field asked for, nil where there is none. */
    class HMGet(override val key: Key, val fields: List<ByteArray>) : Keyed(Value.Kind.HASH)

    /** `HEXISTS key field`. */
    class HExists(override val key: Key, val field: ByteArray) : Keyed(Value.Kind.HASH)

    /** `HKEYS key`: the field names. */
    data class HKeys(override val key: Key) : Keyed(Value.Kind.HASH)

    /** `HVALS key`: the field values. */
    data class HVals(override val key: Key) : Keyed(Value.Kind.HASH)

    /** `HLEN key`: how many fields, 0 when the key is absent. */
    data class HLen(override val key: Key) : Keyed(Value.Kind.HASH)

    /**
     * `MGET key [key ...]`: one array of bulks in argument order, nil for a missing key. A key
     * holding something other than a String is nil too, as in Redis, not a `WRONGTYPE` error.
     */
    class MGet(keys: List<Key>) : Fanned(keys) {
        override fun single(index: Int): Command = Get(keys[index])
        override fun join(replies: List<Reply>): Reply =
            Reply.Array(replies.map { if (it is Reply.Error) Reply.Bulk(null) else it })
    }

    /** `MSET key value [key value ...]`: always `+OK`, and never atomic across partitions. */
    class MSet(private val pairs: List<Pair<Key, ByteArray>>) : Fanned(pairs.map { it.first }) {
        override fun single(index: Int): Command = Set(pairs[index].first, pairs[index].second)
        override fun join(replies: List<Reply>): Reply = Reply.Simple("OK")
    }

    /** `DEL key [key ...]`: the number of keys that were there. */
    class DelKeys(keys: List<Key>) : Fanned(keys) {
        override fun single(index: Int): Command = Del(keys[index])
        override fun join(replies: List<Reply>): Reply = sum(replies)
    }

    /** `EXISTS key [key ...]`: the number of keys that exist, counting a repeated key twice. */
    class ExistsKeys(keys: List<Key>) : Fanned(keys) {
        override fun single(index: Int): Command = Exists(keys[index])
        override fun join(replies: List<Reply>): Reply = sum(replies)
    }

    private companion object {
        fun sum(replies: List<Reply>): Reply =
            Reply.Integer(replies.sumOf { (it as Reply.Integer).value })
    }
}
