package dynacache.engine.persist

import dynacache.engine.Value
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.util.Random

/**
 * One value on its own, self-describing: the RDB entry's type byte followed by the value bytes
 * exactly as a snapshot writes them. This is how a value crosses between nodes (read repair,
 * anti-entropy) without the cluster learning the engine's types: it carries the bytes it is
 * handed and hands them back.
 */
object ValueCodec {

    fun encode(value: Value): ByteArray = byteArrayOf(CODE_BY_KIND.getValue(value.kind)) + RdbWriter.encode(value)

    /** [seeds] levels a restored sorted set's skip list, the one piece of a value the bytes do not carry. */
    fun decode(bytes: ByteArray, seeds: Random): Value {
        val kind = bytes.firstOrNull()?.let(KIND_BY_CODE::get) ?: throw RdbFormatException(RdbFault.TRUNCATED)
        return RdbReader(seeds).decode(kind, DataInputStream(ByteArrayInputStream(bytes, 1, bytes.size - 1)))
    }
}
