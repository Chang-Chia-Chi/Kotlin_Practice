package dynacache.server

import dynacache.engine.Command

/**
 * The inverse of [CommandParser]: what a client would have sent for a [Command]. This is the
 * form a command crosses the network in when the node a client reached is not the key's
 * coordinator (T19), so the coordinator's own parser reads the forwarded tokens exactly as it
 * would have read the client's frame, and no second encoding of a `Command` exists anywhere.
 *
 * The `when` over `Command.Keyed` is exhaustive on purpose: a variant added to the engine stops
 * the build here rather than failing a forward at runtime.
 *
 * Where the wire has several spellings of one meaning, this writes the one that carries
 * everything the variant holds. `Command.Expire` is an absolute instant however the client
 * spelled it (spec 5.4), so it goes out as `PEXPIREAT`; `Command.Set`'s TTL is a duration that
 * no longer knows whether it arrived as `EX`, `PX` or `SETEX`, so it goes out as `PX`.
 */
fun commandToTokens(command: Command): List<ByteArray> = when (command) {
    is Command.Keyed -> keyedTokens(command)
    // Only a keyed command is ever forwarded or replicated: the router runs everything else on
    // this node (T19), and replication ships only single-key writes and reads (T22).
    else -> throw IllegalArgumentException("no wire form for $command: only a keyed command crosses")
}

private fun keyedTokens(command: Command.Keyed): List<ByteArray> {
    val key = command.key.bytes
    return when (command) {
        // String
        is Command.Get -> words("GET") + key
        is Command.Set -> words("SET") + key + command.value +
            (command.condition?.let { words(it.name) } ?: emptyList()) +
            (command.ttl?.let { words("PX", it.toMillis().toString()) } ?: emptyList())
        is Command.IncrBy -> words("INCRBY") + key + words(command.delta.toString())
        is Command.Append -> words("APPEND") + key + command.value
        is Command.StrLen -> words("STRLEN") + key

        // Keyspace and expiry
        is Command.Del -> words("DEL") + key
        is Command.Exists -> words("EXISTS") + key
        is Command.Type -> words("TYPE") + key
        is Command.Expire -> words("PEXPIREAT") + key + words(command.deadline.toEpochMilli().toString())
        is Command.Persist -> words("PERSIST") + key
        is Command.Ttl -> words(if (command.precision == Command.Ttl.Precision.SECONDS) "TTL" else "PTTL") + key

        // Hash
        is Command.HGet -> words("HGET") + key + command.field
        is Command.HSet -> words("HSET") + key + command.entries.flatten()
        is Command.HMSet -> words("HMSET") + key + command.entries.flatten()
        is Command.HGetAll -> words("HGETALL") + key
        is Command.HDel -> words("HDEL") + key + command.fields
        is Command.HMGet -> words("HMGET") + key + command.fields
        is Command.HExists -> words("HEXISTS") + key + command.field
        is Command.HKeys -> words("HKEYS") + key
        is Command.HVals -> words("HVALS") + key
        is Command.HLen -> words("HLEN") + key
        is Command.HScan -> words("HSCAN") + key + scanTail(command.cursor, command.pattern, command.count)

        // List
        is Command.Push ->
            words(if (command.end == Command.End.HEAD) "LPUSH" else "RPUSH") + key + command.values
        is Command.Pop -> words(if (command.end == Command.End.HEAD) "LPOP" else "RPOP") + key
        is Command.LRange -> words("LRANGE") + key + words(command.start.toString(), command.stop.toString())
        is Command.LLen -> words("LLEN") + key
        is Command.LIndex -> words("LINDEX") + key + words(command.index.toString())
        is Command.LSet -> words("LSET") + key + words(command.index.toString()) + command.value
        is Command.LRem -> words("LREM") + key + words(command.count.toString()) + command.value

        // Sorted Set
        is Command.ZAdd -> words("ZADD") + key +
            (command.condition?.let { words(it.name) } ?: emptyList()) +
            (if (command.changed) words("CH") else emptyList()) +
            command.entries.flatten()
        is Command.ZScore -> words("ZSCORE") + key + command.member
        is Command.ZCard -> words("ZCARD") + key
        is Command.ZRange -> words(if (command.reverse) "ZREVRANGE" else "ZRANGE") + key +
            words(command.start.toString(), command.stop.toString()) +
            (if (command.withScores) words("WITHSCORES") else emptyList())
        is Command.ZRem -> words("ZREM") + key + command.members
        is Command.ZRank -> words(if (command.reverse) "ZREVRANK" else "ZRANK") + key + command.member
        is Command.ZRangeByScore -> words("ZRANGEBYSCORE") + key + command.min + command.max +
            (if (command.withScores) words("WITHSCORES") else emptyList()) +
            // `LIMIT 0 -1` is what the parser leaves behind when a client wrote no LIMIT at all.
            (if (command.offset != 0L || command.count != -1L) {
                words("LIMIT", command.offset.toString(), command.count.toString())
            } else {
                emptyList()
            })
        is Command.ZIncrBy -> words("ZINCRBY") + key + command.delta + command.member
        is Command.ZScan -> words("ZSCAN") + key + scanTail(command.cursor, command.pattern, command.count)
    }
}

/** `cursor [MATCH pattern] COUNT n`: the tail `HSCAN` and `ZSCAN` share. */
private fun scanTail(cursor: Long, pattern: ByteArray?, count: Int): List<ByteArray> =
    words(cursor.toULong().toString()) +
        (pattern?.let { words("MATCH") + it } ?: emptyList()) +
        words("COUNT", count.toString())

/**
 * A command name or a number as the bytes a client would have sent. ISO-8859-1 is a bijection
 * over the 256 byte values, the same one [CommandParser] reads tokens back through.
 */
private fun words(vararg text: String): List<ByteArray> = text.map { it.toByteArray(Charsets.ISO_8859_1) }

private operator fun List<ByteArray>.plus(token: ByteArray): List<ByteArray> = this + listOf(token)

private fun List<Pair<ByteArray, ByteArray>>.flatten(): List<ByteArray> = flatMap { listOf(it.first, it.second) }
