package infra.simpleetl

import java.sql.Blob
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.time.LocalDateTime
import java.time.OffsetDateTime

/**
 * One column of a result set or of a target table.
 *
 * @param name lower case, per spec 4.5.
 * @param nullable false only when the source states the column is NOT NULL. A source that does
 *   not report nullability at all - DuckDB 1.1.3 reports every column as nullable, including
 *   NOT NULL ones - yields true, which is the safe direction.
 */
class ColumnMeta(val name: String, val type: CanonicalType, val nullable: Boolean)

/**
 * Turns the current row of a [ResultSet] into a [Row], applying the type mapping of spec 4.3 and
 * lower-casing keys per spec 4.5.
 *
 * The metadata is read once, here, not per row. [map] reads the row the result set is already
 * positioned on and never calls [ResultSet.next].
 *
 * @param step the step name, used to make an unsupported column type and a wrong typed accessor
 *   diagnostic rather than generic.
 * @throws IllegalArgumentException if a column's type is outside spec 4.3, naming step and
 *   column.
 */
class RowMapper(metaData: ResultSetMetaData, private val step: String) {

    val columns: List<ColumnMeta> = List(metaData.columnCount) { i ->
        val name = metaData.getColumnLabel(i + 1).lowercase()
        ColumnMeta(
            name = name,
            type = canonicalType(metaData.getColumnType(i + 1), metaData.getColumnTypeName(i + 1), name),
            nullable = metaData.isNullable(i + 1) != ResultSetMetaData.columnNoNulls,
        )
    }

    init {
        // A Row is keyed by name, so two columns of the same name would silently collapse into
        // one slot and leave the Row a column short of what `columns` advertises. Spec 1.3: the
        // framework does not guess.
        val duplicate = columns.map { it.name }.groupingBy { it }.eachCount().entries
            .firstOrNull { it.value > 1 }
        require(duplicate == null) {
            "step '$step', column '${duplicate?.key}': the result set has ${duplicate?.value} " +
                "columns with this name. Alias one of them in the source SQL."
        }
    }

    /** Maps the row the result set is currently positioned on. SQL NULL becomes null. */
    fun map(rs: ResultSet): Row {
        val values = LinkedHashMap<String, Any?>(columns.size * 2)
        columns.forEachIndexed { i, column -> values[column.name] = read(rs, i + 1, column) }
        return Row(values, step)
    }

    private fun canonicalType(sqlType: Int, typeName: String, name: String): CanonicalType =
        try {
            CanonicalType.fromJdbc(sqlType, typeName)
        } catch (e: IllegalArgumentException) {
            throw IllegalArgumentException("step '$step', column '$name': ${e.message}", e)
        }

    /**
     * The primitive getters return 0, false, or NaN for SQL NULL, so every one of them is
     * guarded by [ResultSet.wasNull]. A byte column is read through [ResultSet.getObject] rather
     * than `getBytes` because ojdbc rejects `getBytes` on a BLOB column, and `getObject` is the
     * one call that serves Oracle RAW, Oracle BLOB, and DuckDB BLOB from a single branch.
     * duckdb_jdbc 1.1.3 does implement `getBytes`; it is Oracle that cannot use it.
     */
    private fun read(rs: ResultSet, i: Int, column: ColumnMeta): Any? =
        when (column.type) {
            CanonicalType.STRING -> rs.getString(i)
            CanonicalType.BOOLEAN -> rs.getBoolean(i).takeUnless { rs.wasNull() }
            CanonicalType.LONG -> rs.getLong(i).takeUnless { rs.wasNull() }
            CanonicalType.DECIMAL -> rs.getBigDecimal(i)
            CanonicalType.DOUBLE -> rs.getDouble(i).takeUnless { rs.wasNull() }
            CanonicalType.DATE -> rs.getDate(i)?.toLocalDate()
            CanonicalType.DATETIME -> rs.getObject(i, LocalDateTime::class.java)
            CanonicalType.INSTANT -> rs.getObject(i, OffsetDateTime::class.java)?.toInstant()
            CanonicalType.BYTES -> when (val value = rs.getObject(i)) {
                null -> null
                is ByteArray -> value
                // 7.4: released here, not left to GC. A 2M-row pipe would otherwise hold two
                // million server-side locators open for the length of the step.
                is Blob -> value.getBytes(1, value.length().toInt()).also { value.free() }
                else -> throw IllegalArgumentException(
                    "step '$step', column '${column.name}': ${value.javaClass.name} is not a byte source",
                )
            }
        }
}
