package dynacache.engine

/**
 * What a key holds. A key is one kind and never another, and a command declares the [Kind]
 * it needs so a wrong-type command is refused before it can touch the entry (C13).
 */
internal sealed class Value(val kind: Kind) {

    /** The word `TYPE` reports, and what a command names when it needs a kind. */
    enum class Kind(val text: String) { STRING("string"), HASH("hash"), LIST("list") }

    class Str(val bytes: ByteArray) : Value(Kind.STRING)

    /**
     * Field names are held as ISO-8859-1 text: that charset maps every byte to one character and
     * back, so a binary-safe field name survives, and the JDK's own map does the hashing.
     */
    class Hash(val fields: LinkedHashMap<String, ByteArray> = LinkedHashMap()) : Value(Kind.HASH)

    /**
     * An ordered sequence. Kotlin's own [ArrayDeque] is a circular buffer, so both ends push and
     * pop in O(1) (spec 2.1) and `LINDEX`, `LSET` and `LRANGE` still index in O(1).
     */
    class List(val items: ArrayDeque<ByteArray> = ArrayDeque()) : Value(Kind.LIST)
}

/** A field name as the store keys it. */
internal fun fieldName(field: ByteArray): String = field.toString(Charsets.ISO_8859_1)

/** The bytes of a field name the store keyed by [fieldName]. */
internal fun fieldBytes(name: String): ByteArray = name.toByteArray(Charsets.ISO_8859_1)
