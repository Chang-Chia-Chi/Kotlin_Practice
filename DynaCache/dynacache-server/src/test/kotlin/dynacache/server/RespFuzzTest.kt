package dynacache.server

import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.random.Random

/** The slot the fuzzer's random expiry argument goes in; matched by identity, never by text. */
private val ARGUMENT = String(charArrayOf('%', 's'))

/** Every command whose argument the parser has to do time arithmetic on (spec 2.1). */
private val EXPIRY_SHAPES = listOf(
    listOf("EXPIRE", "k", ARGUMENT),
    listOf("PEXPIRE", "k", ARGUMENT),
    listOf("EXPIREAT", "k", ARGUMENT),
    listOf("PEXPIREAT", "k", ARGUMENT),
    listOf("SETEX", "k", ARGUMENT, "v"),
    listOf("SET", "k", "v", "EX", ARGUMENT),
    listOf("SET", "k", "v", "PX", ARGUMENT),
)

/**
 * The decoder is the only thing on the server that reads bytes it did not write, so it must
 * answer every byte sequence with either frames or a [RespProtocolException]. Nothing else may
 * escape, including a stack overflow from nested arrays or an allocation from a bogus length.
 */
class RespFuzzTest {

    /** Bytes a fuzzer is most likely to trip the parser with, plus a few ordinary ones. */
    private val interesting = "*$+-:\r\n 0123456789abcx".toByteArray(Charsets.ISO_8859_1)

    private fun Random.junk(): ByteArray = ByteArray(nextInt(1, 24)) {
        if (nextInt(4) == 0) nextInt(256).toByte() else interesting[nextInt(interesting.size)]
    }

    private fun Random.wellFormed(): ByteArray = when (nextInt(5)) {
        0 -> nextInt(1, 6).let { keyLength ->
            "*2\r\n$3\r\nGET\r\n$$keyLength\r\n${"k".repeat(keyLength)}\r\n".toByteArray(Charsets.ISO_8859_1)
        }
        1 -> "PING\r\n".toByteArray(Charsets.ISO_8859_1)
        2 -> encodeReply(Reply.Array(listOf(Reply.Integer(nextLong()), Reply.Bulk(null))))
        3 -> expiring()
        else -> encodeReply(Reply.Bulk(junk()))
    }

    /** One expiry-taking command with an argument no clock is likely to hold (T52, C8). */
    private fun Random.expiring(): ByteArray {
        val words = EXPIRY_SHAPES.random(this).map { if (it === ARGUMENT) nextLong().toString() else it }
        return buildString {
            append("*").append(words.size).append("\r\n")
            words.forEach { append('$').append(it.length).append("\r\n").append(it).append("\r\n") }
        }.toByteArray(Charsets.ISO_8859_1)
    }

    @Test
    fun resp_fuzz_no_crash() {
        val random = Random(20260906)
        val parser = CommandParser(Clock.fixed(Instant.ofEpochSecond(1_000_000), ZoneOffset.UTC))
        var framesDecoded = 0
        var errorsRaised = 0
        var parsed = 0

        repeat(10_000) { iteration ->
            val sequence = ByteArray(0).let { seed ->
                var bytes = seed
                repeat(random.nextInt(1, 5)) {
                    bytes += if (random.nextInt(3) == 0) random.wellFormed() else random.junk()
                }
                bytes
            }
            val asCommands = iteration % 2 == 0
            val decoder = RespDecoder()
            var offset = 0
            try {
                while (offset < sequence.size) {
                    val chunk = minOf(random.nextInt(1, 9), sequence.size - offset)
                    decoder.feed(sequence.copyOfRange(offset, offset + chunk))
                    offset += chunk
                    while (true) {
                        if (!asCommands) {
                            if (decoder.nextReply() == null) break
                            framesDecoded++
                            continue
                        }
                        val tokens = decoder.nextCommand() ?: break
                        framesDecoded++
                        // A decoded frame goes on to the parser, which owes every one of them a
                        // value: a command or the error to write back, never a throwable (C8).
                        parser.parse(tokens)
                        parsed++
                    }
                }
            } catch (expected: RespProtocolException) {
                errorsRaised++
            } catch (unexpected: Throwable) {
                throw AssertionError(
                    "sequence ${sequence.toString(Charsets.ISO_8859_1)} escaped as $unexpected",
                    unexpected,
                )
            }
        }

        assertTrue(framesDecoded > 0, "the fuzzer never produced a parsable frame")
        assertTrue(errorsRaised > 0, "the fuzzer never produced a protocol error")
        assertTrue(parsed > 0, "the fuzzer never reached the command parser")
    }

    @Test
    fun `deeply nested arrays are refused rather than overflowing the stack`() {
        val decoder = RespDecoder()
        decoder.feed("*1\r\n".repeat(10_000).toByteArray(Charsets.ISO_8859_1))
        org.junit.jupiter.api.Assertions.assertThrows(RespProtocolException::class.java) { decoder.nextReply() }
    }
}
