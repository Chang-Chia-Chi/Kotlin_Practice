package dynacache.engine.persist

import dynacache.engine.Value
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.util.Random

/**
 * The engine's one encoding of a value on its own: the RDB type byte followed by the RDB value
 * bytes. It is what replicas hash to compare a key (spec 2.4, C6) and what they ship to each
 * other, so two nodes holding the same value encode the same bytes.
 */
fun encodeValue(value: Value): ByteArray = byteArrayOf(CODE_BY_KIND.getValue(value.kind)) + RdbWriter.encode(value)

/** The inverse of [encodeValue]; [seeds] levels a restored sorted set's skip list, as a restore does. */
fun decodeValue(bytes: ByteArray, seeds: Random): Value {
    val kind = KIND_BY_CODE[bytes[0]] ?: throw RdbFormatException(RdbFault.TRUNCATED)
    return RdbReader(seeds).decode(kind, DataInputStream(ByteArrayInputStream(bytes, 1, bytes.size - 1)))
}
