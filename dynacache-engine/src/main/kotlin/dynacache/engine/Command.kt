package dynacache.engine

import java.time.Duration
import java.time.Instant
import java.util.Random

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

    /**
     * A command with no key at all: every partition answers for its own share and the engine
     * joins the replies. Nothing is atomic across partitions (ADR 0002), so the answer is a
     * running view of the keyspace, not a snapshot of it.
     */
    sealed class EveryPartition : Command() {

        /**
         * Joins the [replies], one per partition in partition order. [random] is the engine's
         * own source of randomness, for the one command that has to choose.
         */
        internal abstract fun join(replies: List<Reply>, random: Random): Reply
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

    /**
     * `COMMAND`: Redis's command table. Minimal here, an empty array; a client that asks in order
     * to discover arity gets no answer it can act on, which is the ceiling this ticket accepted.
     */
    data object CommandTable : Command()

    /**
     * `INFO`: one bulk string of `field:value` lines in Redis's section layout. Minimal here: the
     * version and the keyspace size, which is what the node's own tests and `redis-cli` look for.
     */
    data object Info : EveryPartition() {
        override fun join(replies: List<Reply>, random: Random): Reply =
            Reply.Bulk(
                listOf(
                    "# Server",
                    "dynacache_version:$VERSION",
                    "",
                    "# Keyspace",
                    "db0:keys=${(sum(replies) as Reply.Integer).value}",
                    "",
                ).joinToString(CRLF).toByteArray(),
            )
    }

    /** `DBSIZE`: how many live keys the node holds. */
    data object DbSize : EveryPartition() {
        override fun join(replies: List<Reply>, random: Random): Reply = sum(replies)
    }

    /**
     * `KEYS pattern`: every live key matching the Redis glob [pattern], in no defined order.
     * O(n) over the keyspace, exactly as Redis's own `KEYS` is.
     */
    class Keys(val pattern: ByteArray) : EveryPartition() {
        override fun join(replies: List<Reply>, random: Random): Reply =
            Reply.Array(replies.flatMap { (it as Reply.Array).items })
    }

    /**
     * `RANDOMKEY`: one live key of the node, nil when there is none. Each partition offers one of
     * its own and the join takes one of those.
     *
     * ponytail: a partition with few keys is over-represented, since the draw is per partition
     * rather than over the keyspace; Redis's own RANDOMKEY is approximate too. Weighting the
     * choice by each partition's key count is the repair if a caller ever needs a uniform draw.
     */
    data object RandomKey : EveryPartition() {
        override fun join(replies: List<Reply>, random: Random): Reply {
            val offered = replies.filter { (it as Reply.Bulk).bytes != null }
            return if (offered.isEmpty()) Reply.Bulk(null) else offered[random.nextInt(offered.size)]
        }
    }

    /** `FLUSHDB`: every partition drops every key. Always `+OK`. */
    data object FlushDb : EveryPartition() {
        override fun join(replies: List<Reply>, random: Random): Reply = Reply.Simple("OK")
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
     * `EXPIRE`, `PEXPIRE` and `EXPIREAT` in one variant. The three differ only in how the wire
     * spells the [deadline] -- seconds from now, milliseconds from now, or an absolute Unix
     * time -- and the parser reduces all three to the instant the engine stores (spec 5.4).
     * Replies 1 when the TTL was set, 0 when the key is not there.
     */
    data class Expire(override val key: Key, val deadline: Instant) : Keyed(null)

    /** `PERSIST key`: drops the TTL. 1 when there was one, 0 when the key had none or is absent. */
    data class Persist(override val key: Key) : Keyed(null)

    /**
     * `TTL` and `PTTL` in one variant, differing only in the [unit] they answer in. Redis's two
     * negative answers are not TTLs: -2 is "no such key" and -1 is "no TTL on this key".
     */
    data class Ttl(override val key: Key, val precision: Precision) : Keyed(null) {
        /** `TTL` answers in [SECONDS], `PTTL` in [MILLIS]. */
        enum class Precision { SECONDS, MILLIS }
    }

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

    /**
     * `SCAN cursor [MATCH pattern] [COUNT n]`: a stateless walk of the keyspace, one partition
     * per call. The cursor carries the partition in its high 32 bits and that partition's own
     * cursor in the low 32; 0 starts the walk and 0 comes back when it is over (C15). The
     * fourth command shape: no key names a partition, and every partition at once is too many.
     */
    class Scan(val cursor: Long, val pattern: ByteArray? = null, val count: Int = 10) : Command()

    /** `HSCAN key cursor [MATCH pattern] [COUNT n]`: the same walk over one hash's fields. */
    class HScan(override val key: Key, val cursor: Long, val pattern: ByteArray? = null, val count: Int = 10) :
        Keyed(Value.Kind.HASH)

    /** `HLEN key`: how many fields, 0 when the key is absent. */
    data class HLen(override val key: Key) : Keyed(Value.Kind.HASH)

    /** Which end of a list a command works on. `LPUSH`/`LPOP` are [HEAD], `RPUSH`/`RPOP` [TAIL]. */
    enum class End { HEAD, TAIL }

    /**
     * `LPUSH`/`RPUSH key value [value ...]`: each value in turn onto [end], so `LPUSH a b c`
     * leaves `c b a`. Creates the list when the key is absent; replies with the new length.
     */
    class Push(override val key: Key, val values: List<ByteArray>, val end: End) :
        Keyed(Value.Kind.LIST)

    /** `LPOP`/`RPOP key`: one value off [end], nil when there is none. An empty list is deleted. */
    data class Pop(override val key: Key, val end: End) : Keyed(Value.Kind.LIST)

    /**
     * `LRANGE key start stop`: the elements from [start] to [stop] inclusive. A negative index
     * counts from the tail, and both ends clamp to the list rather than erroring.
     */
    data class LRange(override val key: Key, val start: Long, val stop: Long) : Keyed(Value.Kind.LIST)

    /** `LLEN key`: how many elements, 0 when the key is absent. */
    data class LLen(override val key: Key) : Keyed(Value.Kind.LIST)

    /** `LINDEX key index`: the element at [index], nil when the index is outside the list. */
    data class LIndex(override val key: Key, val index: Long) : Keyed(Value.Kind.LIST)

    /**
     * `LSET key index value`: replaces the element at [index]. Redis errors here rather than
     * growing the list: `no such key` when the key is absent, `index out of range` beyond it.
     */
    class LSet(override val key: Key, val index: Long, val value: ByteArray) : Keyed(Value.Kind.LIST)

    /**
     * `LREM key count value`: removes elements equal to [value]. A positive [count] takes that
     * many working from the head, a negative one that many from the tail, and zero takes them all.
     */
    class LRem(override val key: Key, val count: Long, val value: ByteArray) : Keyed(Value.Kind.LIST)

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
        /** The node's own version; the pom's, without the snapshot suffix. */
        const val VERSION = "0.1.0"

        /** Every INFO line ends the way every RESP line does. */
        const val CRLF = "\r\n"

        fun sum(replies: List<Reply>): Reply =
            Reply.Integer(replies.sumOf { (it as Reply.Integer).value })
    }
}
