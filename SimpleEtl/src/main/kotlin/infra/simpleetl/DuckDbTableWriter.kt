package infra.simpleetl

import org.duckdb.DuckDBAppender
import org.duckdb.DuckDBConnection
import java.sql.Connection

/** Whether the framework generates the target table's DDL, or requires it to exist (spec 4.4). */
enum class CreateTable { AUTO, REQUIRED }

/**
 * Writes Rows into a DuckDB table through [DuckDBAppender]. Row-by-row and multi-row INSERT are
 * both too slow at this row count, so a DuckDB target is always a table and never a statement
 * (spec 4.6).
 *
 * **Append is positional.** Every column of the target gets exactly one value per row, in
 * catalog ordinal order. The order comes from [catalogColumns] and never from YAML or from the
 * source query, so reordering the target table's columns cannot silently misalign data. Under
 * [CreateTable.AUTO] the table is created and then read back, so both paths reach the appender
 * through the same catalog read and the DDL cannot disagree with the dispatch.
 *
 * **Null.** duckdb_jdbc 1.1.3 has no `appendNull`. Only `append(String)`, `appendBigDecimal` and
 * `appendLocalDateTime` null-check their argument; the primitive overloads cannot express null
 * and there is no `byte[]` overload at all. The writer therefore dispatches on the *target
 * column type* and never on the value:
 *
 * | Target type | Call | Null |
 * |---|---|---|
 * | VARCHAR   | `append(row.string(c))` | yes |
 * | DECIMAL   | `appendBigDecimal(row.decimal(c))` | yes |
 * | TIMESTAMP | `appendLocalDateTime(row.dateTime(c))` | yes |
 * | BIGINT, nullable | `appendBigDecimal(row.long(c)?.toBigDecimal())` | yes |
 * | BIGINT, not null | `append(row.long(c))` | no |
 * | DOUBLE, BOOLEAN  | `append(...)`, not null only | no |
 *
 * Nullable BIGINT is exact **by construction, not by luck** (spike S3, validation rule 15): the
 * value comes from [Row.long], so the BigDecimal has scale 0 and cannot round, and a `Long`
 * always fits INT64 so it cannot overflow. Sourcing it any other way makes S3's silent-rounding
 * case reachable - `appendBigDecimal(42.7)` into BIGINT stores 43 without an error.
 *
 * Rejected at [open], before any Row exists, because every one of these loses data silently:
 * BLOB and RAW (no overload), DATE (`appendLocalDateTime` drops the time component, and seam 1
 * maps every JDBC date-with-time to `LocalDateTime`), TIMESTAMP WITH TIME ZONE (no overload),
 * and any nullable column outside VARCHAR / DECIMAL / TIMESTAMP / BIGINT.
 *
 * A framework-created table carries no PRIMARY KEY, UNIQUE, or index: such a violation fails the
 * whole append batch and inserts nothing (spec 4.6). NOT NULL is emitted, because it is what
 * keeps a NOT NULL source column on the faster primitive path when the table is read back.
 *
 * The [connection] belongs to the caller and is never closed here; the appender is (spec 7.4).
 *
 * @param table the target table, optionally `schema.table`; defaults to DuckDB's `main` schema.
 * @param step the step name, so that every error names it (spec 4.4).
 */
class DuckDbTableWriter(
    private val connection: Connection,
    private val table: String,
    private val createTable: CreateTable,
    private val step: String,
) : RowWriter {

    private val schemaName = table.substringBeforeLast('.', DuckDBConnection.DEFAULT_SCHEMA)
    private val tableName = table.substringAfterLast('.')

    private var targets: List<ColumnMeta> = emptyList()
    private var appender: DuckDBAppender? = null
    private var ordinal = 0L

    override fun open(columns: List<ColumnMeta>) {
        check(appender == null) { "step '$step': the writer for '$table' is already open." }
        if (createTable == CreateTable.AUTO) generateTable(columns)
        targets = catalogColumns(connection, table, step).map { it.toColumnMeta() }
        validate(columns)
        appender = duckDb().createAppender(schemaName, tableName)
    }

    override fun write(chunk: List<Row>): Int {
        val open = checkNotNull(appender) { "step '$step': write called before open on '$table'." }
        chunk.forEach { row ->
            ordinal++
            open.beginRow()
            targets.forEach { append(open, it, row) }
            open.endRow()
        }
        // Not a memory bound - S1 measured the same peak RSS with and without it. It is called
        // once per chunk so that a chunk boundary is an observable event (spec 4.6).
        open.flush()
        return chunk.size
    }

    /** Idempotent, and cleared before the call so a throwing close is never retried. */
    override fun close() {
        val open = appender
        appender = null
        open?.close()
    }

    /**
     * `CREATE TABLE` from the source metadata. Every accepted column keeps its natural
     * [CanonicalType.duckDbType]: the four types that accept null - VARCHAR, DECIMAL, TIMESTAMP,
     * BIGINT - are already the natural mapping of the four canonical types that may be nullable,
     * so one mapping table both creates the table and drives the appender (spec 4.4).
     *
     * DECIMAL is the one type that does not use [CanonicalType.duckDbType] verbatim: see [ddlType].
     */
    private fun generateTable(columns: List<ColumnMeta>) {
        columns.forEach(::rejectUnwritable)
        val definitions = columns.joinToString(", ") {
            "${quoteIdentifier(it.name)} ${ddlType(it)}${if (it.nullable) "" else " not null"}"
        }
        connection.createStatement().use {
            it.execute("create table ${quoteIdentifier(schemaName)}.${quoteIdentifier(tableName)} ($definitions)")
        }
    }

    /**
     * The generated column type. Every type but DECIMAL is its natural [CanonicalType.duckDbType];
     * DECIMAL takes the source's own precision and scale, because the bare keyword resolves to
     * `DECIMAL(18,3)` - three decimal places and fifteen integer digits. That silently rounds a
     * `NUMBER(38,10)`, and an ordinary `NUMBER(18)` key at or above 1e15 fails the append outright,
     * mid-write, after earlier chunks have committed (spec 4.4, 5.4).
     *
     * An unusable pair is rejected here rather than guessed at. A source reports one for an
     * unconstrained `NUMBER` (p=0, s=-127), a `FLOAT` (p=126, s=-127), a negative scale, and every
     * computed expression - `sum`, `avg`, `count`, arithmetic, `nvl`, `round`, a numeric literal.
     * Emitting those verbatim yields `DECIMAL(0,-127)` and a DuckDB parse error naming neither
     * step nor column. A DuckDB source always reports a usable pair.
     */
    private fun ddlType(column: ColumnMeta): String {
        if (column.type != CanonicalType.DECIMAL) return column.type.duckDbType
        require(column.precision in 1..38 && column.scale in 0..column.precision) {
            "step '$step', column '${column.name}': the source declares DECIMAL precision " +
                "${column.precision} and scale ${column.scale}, which is not a DuckDB DECIMAL(p,s) - p " +
                "must be 1 to 38 and s 0 to p. An unconstrained NUMBER, a FLOAT, a negative scale and " +
                "every computed expression report a pair like this. CAST the column to a declared " +
                "NUMBER(p,s) in the source SQL."
        }
        return "DECIMAL(${column.precision},${column.scale})"
    }

    private fun validate(source: List<ColumnMeta>) {
        val bySource = source.associateBy { it.name }
        val unknown = (bySource.keys - targets.mapTo(mutableSetOf()) { it.name }).sorted()
        require(unknown.isEmpty()) {
            "step '$step': the source produces columns $unknown which table '$table' does not have. " +
                "Drop them in the source SQL, or add them to the table."
        }
        targets.forEach { column ->
            rejectUnwritable(column)
            val sourceColumn = bySource[column.name]
            require(column.nullable || sourceColumn != null) {
                "step '$step', column '${column.name}': table '$table' declares it NOT NULL and the " +
                    "source does not produce it. The positional appender must supply every column, so a " +
                    "column default cannot stand in."
            }
            // The dispatch reads the value with the accessor matching the target type, so a mismatch
            // makes Row.string / Row.decimal / Row.dateTime throw on row 1. Detectable without a Row,
            // so it is detected here (spec 4.4).
            require(sourceColumn == null || sourceColumn.type == column.type) {
                "step '$step', column '${column.name}': the source produces ${sourceColumn?.type} and " +
                    "table '$table' declares ${column.type} (${column.type.duckDbType}). CAST the column " +
                    "in the source SQL, or correct the table."
            }
        }
    }

    /** Every type and nullability combination that DuckDB cannot take without losing data. */
    private fun rejectUnwritable(column: ColumnMeta) {
        val reason = unwritableToDuckDb(column) ?: return
        throw IllegalArgumentException("step '$step', column '${column.name}': $reason")
    }

    private fun append(appender: DuckDBAppender, column: ColumnMeta, row: Row) {
        when (column.type) {
            CanonicalType.STRING -> appender.append(present(row.string(column.name), column))
            CanonicalType.DECIMAL -> appender.appendBigDecimal(present(row.decimal(column.name), column))
            CanonicalType.DATETIME -> appender.appendLocalDateTime(present(row.dateTime(column.name), column))
            // Scale 0 and always within INT64, because the value comes from Row.long(). See the
            // class KDoc: the exactness argument is about the accessor, not about BigDecimal.
            CanonicalType.LONG ->
                if (column.nullable) appender.appendBigDecimal(row.long(column.name)?.toBigDecimal())
                else appender.append(row.long(column.name) ?: missing(column))
            // The elvis branches below are defensive: open() rejects a nullable DOUBLE or BOOLEAN,
            // so a null can only mean the Row is missing the column (spec 4.4, 4.6).
            CanonicalType.DOUBLE -> appender.append(row.double(column.name) ?: missing(column))
            CanonicalType.BOOLEAN -> appender.append(row.bool(column.name) ?: missing(column))
            CanonicalType.DATE, CanonicalType.INSTANT, CanonicalType.BYTES -> throw IllegalStateException(
                "step '$step', column '${column.name}': ${column.type} reached the appender",
            )
        }
    }

    private fun <T : Any> present(value: T?, column: ColumnMeta): T? =
        if (value == null && !column.nullable) missing(column) else value

    private fun missing(column: ColumnMeta): Nothing = throw IllegalArgumentException(
        "step '$step', column '${column.name}', row $ordinal: table '$table' declares the column NOT NULL " +
            "and the row has no value for it.",
    )

    /**
     * A pooled or wrapped connection is unwrapped rather than cast, because a datasource hands
     * out a proxy and only the real [DuckDBConnection] can open an appender.
     */
    private fun duckDb(): DuckDBConnection = when {
        connection is DuckDBConnection -> connection
        connection.isWrapperFor(DuckDBConnection::class.java) -> connection.unwrap(DuckDBConnection::class.java)
        else -> throw IllegalArgumentException(
            "step '$step': a DuckDB target needs a DuckDB connection, got ${connection.javaClass.name}.",
        )
    }

    private fun CatalogColumn.toColumnMeta(): ColumnMeta = ColumnMeta(
        name = name,
        type = try {
            CanonicalType.fromJdbc(sqlType, typeName)
        } catch (e: IllegalArgumentException) {
            throw IllegalArgumentException("step '$step', column '$name': ${e.message}", e)
        },
        nullable = nullable,
        precision = precision,
        scale = scale,
    )

}

/** Validation rule 15: the target types with an appender method that accepts null. */
private val NULL_CAPABLE = setOf(
    CanonicalType.STRING,
    CanonicalType.DECIMAL,
    CanonicalType.DATETIME,
    CanonicalType.LONG,
)

/**
 * Validation rule 15 as a predicate: the reason [column] cannot be written to DuckDB, or null when
 * it can. Spec 4.6's table, in one place.
 *
 * Lifted out of [DuckDbTableWriter] in P6 so that startup and writer open decide with the same
 * code, the way P4 lifted `quote` to `quoteIdentifier`. The two reach it with column types from
 * different places and neither can stand in for the other: `TaskFileLoader` applies it to every
 * `transform.addColumns` entry, because a task file states those types outright (rule 15, spec
 * 3.2), while a *table's* declared types live in a catalog the run creates or in result set
 * metadata that exists only once the source query runs, so the writer applies it at open.
 */
internal fun unwritableToDuckDb(column: ColumnMeta): String? = when (column.type) {
    CanonicalType.BYTES ->
        "BLOB and RAW cannot be written to DuckDB. The 1.1.3 appender has no byte[] overload " +
            "at all, null or not. Convert the column in the source SQL, for example to base64 text."
    CanonicalType.DATE ->
        "DATE cannot be written to DuckDB, nullable or not. appendLocalDateTime stores the date " +
            "and drops the time component without an error, and seam 1 maps a date carrying a time " +
            "to LocalDateTime. Cast the column to TIMESTAMP in the source SQL."
    CanonicalType.INSTANT ->
        "TIMESTAMP WITH TIME ZONE cannot be written to DuckDB. The 1.1.3 appender has no method " +
            "that accepts an Instant or an OffsetDateTime. Cast the column to TIMESTAMP in the " +
            "source SQL, after converting it to the zone you want."
    else -> if (column.nullable && column.type !in NULL_CAPABLE) {
        "a nullable ${column.type.duckDbType} column cannot be written to DuckDB. Only VARCHAR, " +
            "DECIMAL, TIMESTAMP and BIGINT have an appender method that accepts null. Declare the " +
            "column NOT NULL, or cast it in the source SQL."
    } else {
        null
    }
}
