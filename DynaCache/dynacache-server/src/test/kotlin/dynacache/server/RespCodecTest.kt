package dynacache.server

import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** Bytes as Redis writes them, so a golden literal reads like the wire. */
private fun ByteArray.asWire(): String = toString(Charsets.ISO_8859_1)

private fun bulk(text: String) = Reply.Bulk(text.toByteArray(Charsets.ISO_8859_1))

private const val CR = '\r'.code.toByte()
private const val LF = '\n'.code.toByte()

class RespCodecTest {

    @Test
    fun `a simple string encodes as Redis writes it`() {
        assertEquals("+OK\r\n", encodeReply(Reply.Simple("OK")).asWire())
    }

    /**
     * The golden table, written from the RESP2 specification: one row per reply shape, each the
     * exact bytes a Redis server puts on the wire. `Reply` has no nil-array shape, so `*-1\r\n`
     * is absent by construction.
     */
    @Test
    fun C8_reply_bytes_match_redis() {
        val golden = listOf(
            Reply.Simple("OK") to "+OK\r\n",
            Reply.Simple("PONG") to "+PONG\r\n",
            Reply.Error("ERR", "unknown command 'foo'") to "-ERR unknown command 'foo'\r\n",
            Reply.Error("WRONGTYPE", "Operation against a key holding the wrong kind of value") to
                "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            Reply.Error("EXECABORT", "Transaction discarded because of previous errors.") to
                "-EXECABORT Transaction discarded because of previous errors.\r\n",
            Reply.Integer(0) to ":0\r\n",
            Reply.Integer(1) to ":1\r\n",
            Reply.Integer(-1) to ":-1\r\n",
            Reply.Integer(Long.MAX_VALUE) to ":9223372036854775807\r\n",
            bulk("foo") to "$3\r\nfoo\r\n",
            bulk("") to "$0\r\n\r\n",
            Reply.Bulk(null) to "$-1\r\n",
            Reply.Array(emptyList()) to "*0\r\n",
            Reply.Array(listOf(bulk("foo"), bulk("bar"))) to "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            Reply.Array(listOf(Reply.Integer(1), Reply.Bulk(null))) to "*2\r\n:1\r\n$-1\r\n",
            Reply.Array(listOf(Reply.Array(listOf(Reply.Integer(1), bulk("two"))), Reply.Simple("OK"))) to
                "*2\r\n*2\r\n:1\r\n$3\r\ntwo\r\n+OK\r\n",
        )
        for ((reply, wire) in golden) {
            assertEquals(wire, encodeReply(reply).asWire(), "wire bytes for $reply")
        }
    }

    @Test
    fun `a bulk string carries bytes that are not text`() {
        val raw = byteArrayOf(0, -1, 13, 10, 65)
        val expected = "$5\r\n".toByteArray(Charsets.ISO_8859_1) + raw + "\r\n".toByteArray(Charsets.ISO_8859_1)
        assertArrayEquals(expected, encodeReply(Reply.Bulk(raw)))
    }

    @Test
    fun `a multibulk command decodes to its tokens`() {
        val decoder = RespDecoder()
        decoder.feed("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n".toByteArray(Charsets.ISO_8859_1))
        assertEquals(listOf("GET", "foo"), decoder.nextCommand()?.map { it.asWire() })
        assertNull(decoder.nextCommand(), "the stream holds no second command")
    }

    @Test
    fun resp_encode_decode_roundtrip() {
        val everyShape = listOf(
            Reply.Simple("OK"),
            Reply.Error("ERR", "unknown command 'foo'"),
            Reply.Error("WRONGTYPE", "Operation against a key holding the wrong kind of value"),
            Reply.Integer(0),
            Reply.Integer(-1),
            Reply.Integer(Long.MAX_VALUE),
            bulk("foo"),
            bulk(""),
            Reply.Bulk(byteArrayOf(0, -1, 13, 10, 65)),
            Reply.Bulk(null),
            Reply.Array(emptyList()),
            Reply.Array(listOf(bulk("foo"), Reply.Bulk(null), Reply.Integer(7))),
            Reply.Array(listOf(Reply.Array(listOf(Reply.Simple("OK"), bulk("two"))), Reply.Integer(3))),
        )
        for (reply in everyShape) {
            val decoder = RespDecoder()
            decoder.feed(encodeReply(reply))
            assertEquals(reply, decoder.nextReply(), "round trip of $reply")
            assertNull(decoder.nextReply(), "nothing left after $reply")
        }
    }

    @Test
    fun resp_bulk_string_nil() {
        val decoder = RespDecoder()
        decoder.feed("$-1\r\n".toByteArray(Charsets.ISO_8859_1))
        assertEquals(Reply.Bulk(null), decoder.nextReply())
    }

    @Test
    fun resp_inline_command() {
        val decoder = RespDecoder()
        decoder.feed("PING\r\n".toByteArray(Charsets.ISO_8859_1))
        assertEquals(listOf("PING"), decoder.nextCommand()?.map { it.asWire() })

        decoder.feed("SET  foo bar\r\n".toByteArray(Charsets.ISO_8859_1))
        assertEquals(listOf("SET", "foo", "bar"), decoder.nextCommand()?.map { it.asWire() })

        // "GET " then a key of two bytes that are not text, then CRLF.
        decoder.feed(byteArrayOf(71, 69, 84, 32, 0, -1, CR, LF))
        assertArrayEquals(byteArrayOf(0, -1), decoder.nextCommand()!![1])
    }

    @Test
    fun `a frame split across feeds resumes where it stopped`() {
        val frame = "*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n".toByteArray(Charsets.ISO_8859_1)
        val decoder = RespDecoder()
        for (i in 0 until frame.size - 1) {
            decoder.feed(byteArrayOf(frame[i]))
            assertNull(decoder.nextCommand(), "no command yet after ${i + 1} of ${frame.size} bytes")
        }
        decoder.feed(byteArrayOf(frame[frame.size - 1]))
        assertEquals(listOf("GET", "hello"), decoder.nextCommand()?.map { it.asWire() })
    }

    @Test
    fun `pipelined commands come out one at a time and in order`() {
        val decoder = RespDecoder()
        decoder.feed("*1\r\n$4\r\nPING\r\nECHO hi\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n".toByteArray(Charsets.ISO_8859_1))
        assertEquals(listOf("PING"), decoder.nextCommand()?.map { it.asWire() })
        assertEquals(listOf("ECHO", "hi"), decoder.nextCommand()?.map { it.asWire() })
        assertEquals(listOf("GET", "a"), decoder.nextCommand()?.map { it.asWire() })
        assertNull(decoder.nextCommand())
    }

    @Test
    fun `an empty multibulk frame is skipped, as Redis skips it`() {
        val decoder = RespDecoder()
        decoder.feed("*0\r\n*-1\r\n\r\n*1\r\n$4\r\nPING\r\n".toByteArray(Charsets.ISO_8859_1))
        assertEquals(listOf("PING"), decoder.nextCommand()?.map { it.asWire() })
    }

    @Test
    fun `malformed bytes raise a protocol error in Redis wording`() {
        val cases = mapOf(
            "*1\r\n+OK\r\n" to "expected '$', got '+'",
            "*1\r\n$-1\r\n" to "invalid bulk length",
            "*1\r\n${'$'}x\r\n" to "invalid bulk length",
            "*x\r\n" to "invalid multibulk length",
            "*99999999\r\n" to "invalid multibulk length",
        )
        for ((wire, message) in cases) {
            val decoder = RespDecoder()
            decoder.feed(wire.toByteArray(Charsets.ISO_8859_1))
            val raised = assertThrows(RespProtocolException::class.java) { decoder.nextCommand() }
            assertEquals(message, raised.message, "wording for $wire")
        }
    }

    @Test
    fun `an inline line that never ends is refused rather than buffered forever`() {
        val decoder = RespDecoder()
        decoder.feed(ByteArray(64 * 1024 + 1) { 'a'.code.toByte() })
        assertThrows(RespProtocolException::class.java) { decoder.nextCommand() }
    }

    @Test
    fun resp_error_format() {
        for (kind in listOf("ERR", "WRONGTYPE", "EXECABORT")) {
            val wire = encodeReply(Reply.Error(kind, "something went wrong")).asWire()
            assertTrue(wire.startsWith("-"), "an error reply starts with '-': $wire")
            assertEquals("-$kind something went wrong\r\n", wire)
        }
    }
}
