package dynacache.engine

/**
 * A Redis key: an opaque, binary-safe byte string. Two keys are the same key when their bytes
 * are the same; nothing about text encoding is assumed.
 *
 * [bytes] is held, not copied: a caller must not mutate the array it hands over.
 */
class Key(val bytes: ByteArray) {

    /** Convenience for tests and for command text that is already a Kotlin string. */
    constructor(text: String) : this(text.toByteArray())

    /**
     * The part of the key that decides where it lives: the hash tag when the key has one, the
     * whole key otherwise. Both layers hash these bytes, so keys sharing a tag share a
     * coordinator and a partition and may appear in one batch (C12).
     */
    val hashedBytes: ByteArray get() = bytes.copyOfRange(tagFrom, tagTo)

    private val tagFrom: Int
    private val tagTo: Int

    init {
        // Redis Cluster's rule: the first '{', the first '}' after it, and a non-empty span
        // between them. Anything else and the whole key is hashed.
        val open = indexOf(BRACE_OPEN, 0)
        val close = if (open < 0) -1 else indexOf(BRACE_CLOSE, open + 1)
        if (open >= 0 && close > open + 1) {
            tagFrom = open + 1
            tagTo = close
        } else {
            tagFrom = 0
            tagTo = bytes.size
        }
    }

    /** The non-negative hash of [hashedBytes] the engine buckets a key into a partition by. */
    val hash: Int = run {
        var h = 1
        for (i in tagFrom until tagTo) h = 31 * h + bytes[i]
        h and Int.MAX_VALUE
    }

    override fun equals(other: Any?): Boolean =
        this === other || (other is Key && bytes.contentEquals(other.bytes))

    override fun hashCode(): Int = bytes.contentHashCode()

    override fun toString(): String = bytes.toString(Charsets.ISO_8859_1)

    private fun indexOf(b: Byte, from: Int): Int {
        for (i in from until bytes.size) if (bytes[i] == b) return i
        return -1
    }

    private companion object {
        const val BRACE_OPEN: Byte = '{'.code.toByte()
        const val BRACE_CLOSE: Byte = '}'.code.toByte()
    }
}
