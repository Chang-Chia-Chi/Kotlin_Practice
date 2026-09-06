package dynacache.engine

/**
 * What a command returns, in exactly the five RESP2 shapes Redis uses. There is no separate
 * domain result type: the engine speaks the wire's vocabulary and the server only encodes it.
 *
 * Frozen surface (plan 2.3, C8). Replies compare by content down to the bytes, so a test may
 * write the reply it expects as a literal.
 */
sealed class Reply {

    /** A simple status string, `+OK` on the wire. */
    data class Simple(val text: String) : Reply()

    /** An error whose [kind] is the leading token Redis clients switch on, such as `WRONGTYPE`. */
    data class Error(val kind: String, val message: String) : Reply()

    /** A `:1` style integer reply. */
    data class Integer(val value: Long) : Reply()

    /** A bulk string, or nil when [bytes] is null. Equality and hashing are by byte content. */
    class Bulk(val bytes: ByteArray?) : Reply() {
        override fun equals(other: Any?): Boolean =
            this === other || (other is Bulk && bytes.contentEquals(other.bytes))

        override fun hashCode(): Int = bytes.contentHashCode()

        override fun toString(): String =
            if (bytes == null) "Bulk(nil)" else "Bulk(${bytes.toString(Charsets.ISO_8859_1)})"
    }

    /** A multi-bulk reply. Equality follows [items], so nested [Bulk] bytes compare by content. */
    data class Array(val items: List<Reply>) : Reply()
}
