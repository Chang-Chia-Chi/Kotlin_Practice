package dynacache.server

import dynacache.engine.Reply
import java.io.ByteArrayOutputStream

/** Renders a [Reply] as the RESP2 bytes Redis would write for it (C8). */
fun encodeReply(reply: Reply): ByteArray {
    val out = ByteArrayOutputStream()
    writeReply(out, reply)
    return out.toByteArray()
}

private fun writeReply(out: ByteArrayOutputStream, reply: Reply) {
    when (reply) {
        is Reply.Simple -> out.writeLine("+" + reply.text)
        is Reply.Error -> out.writeLine("-" + reply.kind + " " + reply.message)
        is Reply.Integer -> out.writeLine(":" + reply.value)
        is Reply.Bulk -> {
            val bytes = reply.bytes
            if (bytes == null) {
                out.writeLine("$-1")
            } else {
                out.writeLine("$" + bytes.size)
                out.write(bytes)
                out.writeCrlf()
            }
        }
        is Reply.Array -> {
            out.writeLine("*" + reply.items.size)
            reply.items.forEach { writeReply(out, it) }
        }
    }
}

private fun ByteArrayOutputStream.writeLine(line: String) {
    write(line.toByteArray(Charsets.UTF_8))
    writeCrlf()
}

private fun ByteArrayOutputStream.writeCrlf() {
    write('\r'.code)
    write('\n'.code)
}

/** Malformed bytes on the wire. The connection that produced them cannot be trusted further. */
class RespProtocolException(message: String) : RuntimeException(message)

/** Thrown internally when a frame is not complete yet, so the decoder rewinds and waits. */
private object Underflow : RuntimeException(null, null, false, false)

private const val CR = '\r'.code.toByte()
private const val LF = '\n'.code.toByte()

/**
 * An incremental RESP2 decoder. Bytes arrive in whatever chunks the socket produces; a frame
 * that is not complete yet leaves the decoder untouched and resumes when more bytes arrive.
 *
 * Two ways to read the stream, because Redis reads the two directions differently. A client
 * stream is read with [nextCommand]: a frame starting with `*` is an array of bulk strings,
 * anything else is an inline command. A server stream is read with [nextReply]: every value
 * carries its RESP2 type byte.
 */
class RespDecoder(
    private val maxInlineBytes: Int = 64 * 1024,
    private val maxMultibulkLength: Int = 1024 * 1024,
    private val maxBulkBytes: Int = 512 * 1024 * 1024,
    private val maxNesting: Int = 32,
) {
    private var buffer = ByteArray(0)

    /** First byte of the frame being parsed; only a complete frame moves it. */
    private var start = 0

    /** Scan position inside the frame being parsed. */
    private var cursor = 0

    /** Adds bytes to the stream. Bytes already consumed are dropped first. */
    fun feed(bytes: ByteArray) {
        // ponytail: whole-buffer copy per feed, O(n^2) on a frame fed byte by byte. Ticket 13
        // puts Netty's ByteBuf in front, which is where a ring buffer would go if it matters.
        if (start > 0) {
            buffer = buffer.copyOfRange(start, buffer.size)
            start = 0
        }
        buffer = buffer + bytes
    }

    /**
     * The next complete client command as its tokens, or null while the frame is incomplete.
     * An empty frame (`*0`, `*-1`, a blank inline line) is what Redis skips, so it is skipped
     * here too rather than surfacing as a command with no name.
     */
    fun nextCommand(): List<ByteArray>? {
        while (true) {
            val tokens = attempt { parseCommand() } ?: return null
            if (tokens.isNotEmpty()) return tokens
        }
    }

    /** The next complete RESP2 value, or null while the frame is incomplete. */
    fun nextReply(): Reply? = attempt { parseValue(0) }

    private fun <T> attempt(parse: () -> T): T? {
        cursor = start
        return try {
            val parsed = parse()
            start = cursor
            parsed
        } catch (incomplete: Underflow) {
            null
        } catch (malformed: RespProtocolException) {
            start = buffer.size // the stream is unusable; do not re-raise on the same bytes
            throw malformed
        }
    }

    private fun parseCommand(): List<ByteArray> {
        if (remaining() == 0) throw Underflow
        if (buffer[cursor] != '*'.code.toByte()) return inlineTokens()
        cursor++
        val count = readCount("invalid multibulk length", maxMultibulkLength)
        if (count <= 0) return emptyList()
        val tokens = ArrayList<ByteArray>()
        repeat(count) {
            if (remaining() == 0) throw Underflow
            val type = buffer[cursor]
            if (type != '$'.code.toByte()) {
                protocolError("expected '$', got '${type.toInt().toChar()}'")
            }
            cursor++
            tokens.add(readBulk(allowNil = false)!!)
        }
        return tokens
    }

    /**
     * An inline command's arguments. Latin-1 both ways is a bijection over the 256 byte values,
     * so a key with bytes that are not text survives the split intact.
     */
    private fun inlineTokens(): List<ByteArray> =
        readLine(Charsets.ISO_8859_1).split(' ')
            .filter { it.isNotEmpty() }
            .map { it.toByteArray(Charsets.ISO_8859_1) }

    private fun parseValue(depth: Int): Reply {
        if (depth > maxNesting) protocolError("invalid multibulk length")
        if (remaining() == 0) throw Underflow
        val type = buffer[cursor]
        cursor++
        return when (type.toInt().toChar()) {
            '+' -> Reply.Simple(readLine())
            '-' -> readLine().let { line ->
                val space = line.indexOf(' ')
                if (space < 0) Reply.Error(line, "") else Reply.Error(line.take(space), line.substring(space + 1))
            }
            ':' -> Reply.Integer(readLine().toLongOrNull() ?: protocolError("invalid integer"))
            '$' -> Reply.Bulk(readBulk(allowNil = true))
            '*' -> {
                val count = readCount("invalid multibulk length", maxMultibulkLength)
                if (count < 0) protocolError("invalid multibulk length")
                val items = ArrayList<Reply>() // grown as items arrive, so a bogus count costs nothing
                repeat(count) { items.add(parseValue(depth + 1)) }
                Reply.Array(items)
            }
            else -> {
                cursor--
                Reply.Array(inlineTokens().map { Reply.Bulk(it) })
            }
        }
    }

    /** The body of a bulk string whose `$` has been consumed; null only for `$-1`. */
    private fun readBulk(allowNil: Boolean): ByteArray? {
        val length = readCount("invalid bulk length", maxBulkBytes)
        if (length == -1 && allowNil) return null
        if (length < 0) protocolError("invalid bulk length")
        if (remaining() < length + 2) throw Underflow
        val bytes = buffer.copyOfRange(cursor, cursor + length)
        cursor += length
        if (buffer[cursor] != CR || buffer[cursor + 1] != LF) protocolError("invalid bulk length")
        cursor += 2
        return bytes
    }

    /** A length prefix: `-1` passes through, anything else must be within [max]. */
    private fun readCount(whenInvalid: String, max: Int): Int {
        val value = readLine().toLongOrNull() ?: protocolError(whenInvalid)
        if (value < -1 || value > max) protocolError(whenInvalid)
        return value.toInt()
    }

    private fun readLine(charset: java.nio.charset.Charset = Charsets.UTF_8): String {
        var i = cursor
        while (i + 1 < buffer.size) {
            if (buffer[i] == CR && buffer[i + 1] == LF) {
                val line = String(buffer, cursor, i - cursor, charset)
                cursor = i + 2
                return line
            }
            i++
        }
        if (remaining() > maxInlineBytes) protocolError("too big inline request")
        throw Underflow
    }

    private fun remaining() = buffer.size - cursor

    private fun protocolError(message: String): Nothing = throw RespProtocolException(message)
}
