package infra.simpleetl

import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.Collections

/**
 * One immutable row of canonical values (spec 4.2). Keys are lower case and in source order
 * (spec 4.5), so an Oracle result set and a DuckDB result set over the same columns produce the
 * same keys. Column names are lower-cased on the way in and on every lookup, so a transform
 * cannot introduce an upper-case key by accident.
 *
 * [with] and [without] return copies; nothing mutates a Row in place.
 *
 * @param values the row's values, already lower-cased and in source order.
 * @param step the step name, carried only so that a typed accessor can name it in an error.
 */
class Row internal constructor(
    private val values: LinkedHashMap<String, Any?>,
    private val step: String,
) {

    /** Lower case, in source order. Unmodifiable: a Row never changes once built. */
    val columns: Set<String> get() = Collections.unmodifiableSet(values.keys)

    /** The raw value, or null if the column is absent or held SQL NULL. See [contains]. */
    operator fun get(name: String): Any? = values[name.lowercase()]

    /** True when the column is present, whether or not its value is null. */
    fun contains(name: String): Boolean = values.containsKey(name.lowercase())

    fun string(name: String): String? = typed(name)

    fun long(name: String): Long? = typed(name)

    fun decimal(name: String): BigDecimal? = typed(name)

    fun double(name: String): Double? = typed(name)

    fun bool(name: String): Boolean? = typed(name)

    fun date(name: String): LocalDate? = typed(name)

    fun dateTime(name: String): LocalDateTime? = typed(name)

    fun instant(name: String): Instant? = typed(name)

    fun bytes(name: String): ByteArray? = typed(name)

    /** A copy with [name] added or replaced. */
    fun with(name: String, value: Any?): Row = copy { it[name.lowercase()] = value }

    /** A copy without [name]. Removing an absent column is not an error. */
    fun without(name: String): Row = copy { it.remove(name.lowercase()) }

    /** Names the columns but never their values, so logging a Row cannot export row data. */
    override fun toString(): String = "Row(step=$step, columns=${values.keys})"

    private inline fun copy(edit: (LinkedHashMap<String, Any?>) -> Unit): Row =
        Row(LinkedHashMap(values).also(edit), step)

    /**
     * Never coerces. A value of the wrong type is an error naming step, column, the type the
     * value actually has, and the type that was asked for.
     */
    private inline fun <reified T : Any> typed(name: String): T? {
        val value = this[name] ?: return null
        return value as? T ?: throw IllegalArgumentException(
            "step '$step', column '${name.lowercase()}': value is a ${value.javaClass.simpleName}, " +
                "requested ${T::class.simpleName}",
        )
    }
}
