package dynacache.cluster

import dynacache.engine.Command
import dynacache.engine.Key

/**
 * The test kit's stand-in for the wire form a [Router] forwards a command in: the few commands
 * `InProcessCluster` and the router tests exercise, in both directions.
 *
 * The real pair belongs beside T13's `CommandParser` in the server module, which is where the
 * wire's spelling of a command is the only place it is still visible; the router takes both
 * directions as functions so the module graph stays as plan 2.2 draws it.
 */
object TokenCodec {

    /** The tokens a client would have sent for [command]. */
    fun tokens(command: Command): List<ByteArray> = when (command) {
        is Command.Get -> listOf(name("GET"), command.key.bytes)
        is Command.Set -> listOf(name("SET"), command.key.bytes, command.value)
        is Command.Del -> listOf(name("DEL"), command.key.bytes)
        is Command.IncrBy -> listOf(name("INCRBY"), command.key.bytes, name(command.delta.toString()))
        is Command.HSet -> listOf(name("HSET"), command.key.bytes) + command.entries.flatMap { listOf(it.first, it.second) }
        is Command.HGetAll -> listOf(name("HGETALL"), command.key.bytes)
        else -> throw IllegalArgumentException("the test kit's codec has no wire form for $command")
    }

    /** What [tokens] mean, the way the server's parser reads them. */
    fun command(tokens: List<ByteArray>): Command =
        when (val verb = tokens[0].decodeToString().uppercase()) {
            "GET" -> Command.Get(Key(tokens[1]))
            "SET" -> Command.Set(Key(tokens[1]), tokens[2])
            "DEL" -> Command.Del(Key(tokens[1]))
            "INCRBY" -> Command.IncrBy(Key(tokens[1]), tokens[2].decodeToString().toLong())
            "HSET" -> Command.HSet(Key(tokens[1]), tokens.drop(2).chunked(2).map { it[0] to it[1] })
            "HGETALL" -> Command.HGetAll(Key(tokens[1]))
            else -> throw IllegalArgumentException("the test kit's codec does not know $verb")
        }

    private fun name(text: String): ByteArray = text.toByteArray()
}
