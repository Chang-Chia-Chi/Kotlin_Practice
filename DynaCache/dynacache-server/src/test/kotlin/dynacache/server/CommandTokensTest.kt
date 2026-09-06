package dynacache.server

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

/**
 * The wire form a [dynacache.cluster.Router] forwards a command in, checked the only way it can
 * be: against the parser that reads it back. `tokens(parse(row)) == row` for one canonical row
 * per `Command.Keyed` variant, so a command that crosses to its coordinator is spelled there
 * exactly as the client spelled it here.
 *
 * The rows are canonical, not exhaustive: several names reduce to one variant (`INCR` and
 * `DECRBY` are both [dynacache.engine.Command.IncrBy]), and [commandToTokens] writes the one
 * spelling that carries everything the variant holds -- `PEXPIREAT` for an absolute deadline,
 * `PX` for a TTL. What guarantees every variant has a wire form at all is not this table but
 * the exhaustive `when` in [commandToTokens]: a variant added to `Command.Keyed` stops the
 * build rather than failing a forward at runtime.
 */
class CommandTokensTest {

    @Test
    fun tokens_roundtrip_every_keyed_command() {
        val parser = CommandParser(Clock.fixed(Instant.ofEpochSecond(1_000_000), ZoneOffset.UTC))
        for (row in ROWS) {
            val parsed = parser.parse(row.split(" ").map { it.toByteArray(Charsets.ISO_8859_1) })
            val command = (parsed as? Parsed.Ok)?.command ?: error("the parser refused the row '$row': $parsed")
            val spelled = commandToTokens(command).joinToString(" ") { it.toString(Charsets.ISO_8859_1) }
            assertEquals(row, spelled, "the wire form of '$row' did not come back the same")
        }
    }
}

/** One canonical wire spelling per `Command.Keyed` variant, and per flag that changes one. */
private val ROWS = listOf(
    // String
    "GET k",
    "SET k v",
    "SET k v NX PX 1000",
    "SET k v XX",
    "INCRBY k 5",
    "APPEND k v",
    "STRLEN k",

    // Keyspace and expiry: one absolute deadline, spelled the way it survives the crossing
    "DEL k",
    "EXISTS k",
    "TYPE k",
    "PEXPIREAT k 1700000000000",
    "PERSIST k",
    "TTL k",
    "PTTL k",

    // Hash
    "HGET k f",
    "HSET k f v g w",
    "HGETALL k",
    "HDEL k f g",
    "HMSET k f v",
    "HMGET k f g",
    "HEXISTS k f",
    "HKEYS k",
    "HVALS k",
    "HLEN k",
    "HSCAN k 0 MATCH p* COUNT 20",
    "HSCAN k 0 COUNT 10",

    // List
    "LPUSH k a b",
    "RPUSH k a",
    "LPOP k",
    "RPOP k",
    "LRANGE k 0 -1",
    "LLEN k",
    "LINDEX k 0",
    "LSET k 0 v",
    "LREM k -1 v",

    // Sorted Set
    "ZADD k 1 a",
    "ZADD k NX CH 1 a 2 b",
    "ZADD k XX 1 a",
    "ZSCORE k m",
    "ZCARD k",
    "ZRANGE k 0 -1",
    "ZRANGE k 0 -1 WITHSCORES",
    "ZREVRANGE k 0 -1",
    "ZREM k a b",
    "ZRANK k m",
    "ZREVRANK k m",
    "ZRANGEBYSCORE k -inf +inf",
    "ZRANGEBYSCORE k (1 2 WITHSCORES LIMIT 0 10",
    "ZINCRBY k 1.5 m",
    "ZSCAN k 0 MATCH p* COUNT 20",
)
