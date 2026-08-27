package infra.etl.jdbc

import infra.etl.pipe.CanonicalType
import infra.etl.pipe.ColumnMeta
import infra.etl.pipe.Row
import infra.etl.pipe.RowWriter
import infra.etl.pipe.catalogColumns
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.statement.PreparedBatch
import org.jdbi.v3.core.statement.SqlStatements

/**
 * Writes Rows into a table on a non-DuckDB datasource with one JDBI prepared batch per chunk
 * (spec 4.4, declarative target).
 *
 * The column list comes from the target's catalog, not from YAML and not from the source query,
 * and the generated INSERT names every column it binds. Reordering the target table's columns
 * therefore changes nothing. A Row key the target does not have is an error at [open], never a
 * silently dropped column.
 *
 * A target column the source does not produce is left out of the INSERT, so the database's own
 * default applies and its own NOT NULL constraint reports the violation. The framework does not
 * duplicate that check, because it does not read `COLUMN_DEF` and so cannot tell "no default"
 * from "has a default" - a deviation from spec 4.4's framework error naming the row ordinal.
 *
 * Identifiers are emitted unquoted, so a target created as a quoted lower-case identifier on
 * Oracle - `create table "wip"("lot_id" number)` - is unreachable: Oracle folds the unquoted name
 * to upper case and reports ORA-00904. Such a table needs a `target.sql` step instead.
 *
 * DuckDB is not a valid target here: DuckDB writes go through `DuckDbTableWriter` and its
 * appender (spec 4.6).
 *
 * @param table the target table, optionally `schema.table`.
 * @param step the step name, so that every error names it (spec 4.4).
 */
class JdbcTableWriter(
    private val jdbi: Jdbi,
    private val table: String,
    private val step: String,
) : RowWriter {

    private var handle: Handle? = null
    private var binds: List<ColumnMeta> = emptyList()
    private var sql = ""

    override fun open(columns: List<ColumnMeta>) {
        check(handle == null) { "step '$step': the writer for '$table' is already open." }
        val opened = jdbi.open()
        handle = opened
        // The handle is this writer's own; release it here rather than relying on the caller
        // reaching a `use` block it never entered (spec 7.4).
        try {
            val target = catalogColumns(opened.connection, table, step).map { it.name }
            val bySource = columns.associateBy { it.name }
            val unknown = (bySource.keys - target.toSet()).sorted()
            require(unknown.isEmpty()) {
                "step '$step': the source produces columns $unknown which table '$table' does not have. " +
                    "Drop them in the source SQL, or add them to the table."
            }
            binds = target.mapNotNull { bySource[it] }
            require(binds.isNotEmpty()) {
                "step '$step': the source and table '$table' have no column name in common, so the step " +
                    "would write empty rows. Column names are compared in lower case (spec 4.5)."
            }
            // Identifiers are emitted unquoted so that Oracle folds them to its own storage case,
            // as it already does for the names the author writes in YAML. Quoting them would make
            // the lower-cased names of spec 4.5 case-sensitive and unresolvable on Oracle.
            sql = "insert into $table (${binds.joinToString { it.name }}) " +
                "values (${binds.joinToString { ":${it.name}" }})"
        } catch (e: Throwable) {
            try {
                close()
            } catch (releaseFailure: Throwable) {
                e.addSuppressed(releaseFailure)
            }
            throw e
        }
    }

    override fun write(chunk: List<Row>): Int {
        val opened = checkNotNull(handle) { "step '$step': write called before open on '$table'." }
        if (chunk.isEmpty()) return 0
        return opened.prepareBatch(sql).use { batch ->
            chunk.forEach { row ->
                binds.forEach { batch.bindColumn(it.name, it.type, row) }
                batch.add()
            }
            batch.rowsAffected()
        }
    }

    /** Idempotent, and cleared before the call so a throwing close is never retried. */
    override fun close() {
        val open = handle
        handle = null
        open?.close()
    }
}

/**
 * Runs the author's own statement as a JDBI prepared batch, once per chunk, with Row values bound
 * by name: `:lot_id` binds the Row key `lot_id` (spec 4.4, statement target). This is how MERGE
 * and conditional INSERT are expressed, and it is what makes a step idempotent.
 *
 * Bind names cannot be validated at startup, because the Row key set is only known once the
 * source query runs. They are checked against the first chunk and reported as one error listing
 * every missing name.
 *
 * Only the names the statement actually uses are bound; JDBI rejects an unused binding by
 * default. Their types come from the column list given to [open], so a null binds as a typed
 * null rather than as `Types.OTHER`, which Oracle does not accept on every column. A bind name
 * the column list does not describe - which a transform can produce - binds untyped.
 *
 * DuckDB is not a valid target here: the appender takes a table and not a statement, so a
 * statement target on a DuckDB datasource is rejected at startup (spec 4.4, validation rule 11).
 *
 * @param step the step name, so that every error names it (spec 4.4).
 */
class JdbcStatementWriter(
    private val jdbi: Jdbi,
    private val sql: String,
    private val step: String,
) : RowWriter {

    private var handle: Handle? = null
    private var binds: List<String> = emptyList()
    private var types: Map<String, CanonicalType> = emptyMap()
    private var checked = false

    override fun open(columns: List<ColumnMeta>) {
        check(handle == null) { "step '$step': the statement writer is already open." }
        val opened = jdbi.open()
        handle = opened
        try {
            types = columns.associate { it.name to it.type }
            // JDBI's own parser, so that the names checked here are exactly the names JDBI will
            // look for: it already skips a colon inside a string literal.
            val parser = opened.getConfig(SqlStatements::class.java).sqlParser
            val parameters = opened.createUpdate(sql).use { parser.parse(sql, it.context).parameters }
            require(!parameters.isPositional) {
                "step '$step': target.sql uses positional '?' parameters. Row values bind by name, so " +
                    "write ':column' instead (spec 4.4)."
            }
            binds = parameters.parameterNames
        } catch (e: Throwable) {
            try {
                close()
            } catch (releaseFailure: Throwable) {
                e.addSuppressed(releaseFailure)
            }
            throw e
        }
    }

    override fun write(chunk: List<Row>): Int {
        val opened = checkNotNull(handle) { "step '$step': write called before open." }
        if (chunk.isEmpty()) return 0
        if (!checked) {
            checked = true
            val row = chunk.first()
            val missing = binds.filter { it.lowercase() !in row.columns }.sorted()
            require(missing.isEmpty()) {
                "step '$step': target.sql binds ${missing.map { ":$it" }} which the source row does not " +
                    "provide. The row has ${row.columns.toList()}. Add the columns to the source SQL, or " +
                    "correct the bind names (spec 4.4)."
            }
        }
        return opened.prepareBatch(sql).use { batch ->
            chunk.forEach { row ->
                binds.forEach { batch.bindColumn(it, types[it.lowercase()], row) }
                batch.add()
            }
            batch.rowsAffected()
        }
    }

    /** Idempotent, and cleared before the call so a throwing close is never retried. */
    override fun close() {
        val open = handle
        handle = null
        open?.close()
    }
}

/**
 * Binds one Row value. [type] is the canonical type the source declared for the column, which is
 * what lets a null bind as a typed null; without it JDBI binds an untyped null as `Types.OTHER`
 * and Oracle rejects that on a typed column.
 */
private fun PreparedBatch.bindColumn(name: String, type: CanonicalType?, row: Row) {
    val value = row[name]
    if (type == null) bind(name, value) else bindByType(name, value, type.javaType)
}

/**
 * The rows the batch reported. A driver that answers `SUCCESS_NO_INFO` for a statement it did
 * execute is counted as one row rather than as none.
 */
private fun PreparedBatch.rowsAffected(): Int =
    execute().fold(0) { total, count -> total + if (count >= 0) count else 1 }

/** The Kotlin type a [CanonicalType] value has, which is what JDBI resolves an argument from. */
private val CanonicalType.javaType: Class<*>
    get() = when (this) {
        CanonicalType.STRING -> String::class.java
        CanonicalType.BOOLEAN -> Boolean::class.javaObjectType
        CanonicalType.LONG -> Long::class.javaObjectType
        CanonicalType.DECIMAL -> BigDecimal::class.java
        CanonicalType.DOUBLE -> Double::class.javaObjectType
        CanonicalType.DATE -> LocalDate::class.java
        CanonicalType.DATETIME -> LocalDateTime::class.java
        CanonicalType.INSTANT -> Instant::class.java
        CanonicalType.BYTES -> ByteArray::class.java
    }
