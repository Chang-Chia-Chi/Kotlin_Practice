package infra.etl.jdbc

import infra.etl.pipe.CanonicalType
import infra.etl.pipe.ColumnMeta
import infra.etl.pipe.Row
import infra.etl.pipe.RowWriter
import infra.etl.pipe.catalogColumns
import infra.etl.pipe.parseNamedParameters
import infra.etl.pipe.requireSourceSubset
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
 * silently dropped column - and a key only a Row carries, which is a transform addition no
 * `transform.addColumns` declared, is an error against the first chunk for the same reason.
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
    private var checked = false

    override fun open(columns: List<ColumnMeta>) {
        check(handle == null) { "step '$step': the writer for '$table' is already open." }
        handle = jdbi.openConfigured { opened ->
            val target = catalogColumns(opened.connection, table, step).map { it.name }
            val bySource = columns.associateBy { it.name }
            requireSourceSubset(bySource.keys, target.toSet(), table, step)
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
        }
    }

    override fun write(chunk: List<Row>): Int {
        val opened = checkNotNull(handle) { "step '$step': write called before open on '$table'." }
        if (chunk.isEmpty()) return 0
        // The INSERT is fixed at open from the columns the source declared, so a key that first
        // appears on a Row - a transform's addition that `transform.addColumns` did not declare -
        // is bound nowhere and the column silently takes its database default. Spec 4.4 promises a
        // runtime error for a Row key with no matching column; this is where it is raised. Checked
        // against the first chunk, like JdbcStatementWriter's bind names, because a set difference
        // per row is work in the innermost loop.
        if (!checked) {
            checked = true
            val bound = binds.mapTo(mutableSetOf()) { it.name }
            val unmatched = chunk.first().columns.filterNot { it in bound }.sorted()
            require(unmatched.isEmpty()) {
                "step '$step', row 1: the row carries columns $unmatched which this step does not write " +
                    "to table '$table'. A transform that adds a column must declare it in " +
                    "transform.addColumns, or its value is dropped silently (spec 4.4, 9.1)."
            }
        }
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

    /**
     * Each bind name exactly as the statement writes it, paired with the canonical type the source
     * declared for the matching column, or null when the column list does not describe it - a
     * transform's addition, which binds untyped.
     *
     * Paired once at [open] rather than looked up per bind per row: the name has to be lower-cased
     * to meet the column list's keys (spec 4.5) and the statement's own casing has to be preserved
     * for JDBI, and doing both in the innermost loop was review finding L7. The remaining per-row
     * lowercase is [Row]'s own, on the value lookup, and is load-bearing: this is the caller that
     * passes it raw SQL bind names.
     */
    private var binds: List<Pair<String, CanonicalType?>> = emptyList()
    private var checked = false

    override fun open(columns: List<ColumnMeta>) {
        check(handle == null) { "step '$step': the statement writer is already open." }
        handle = jdbi.openConfigured { opened ->
            val types = columns.associate { it.name to it.type }
            // The handle's own parser, so that the names checked here are exactly the names JDBI
            // will look for; it already skips a colon inside a string literal.
            val parser = opened.getConfig(SqlStatements::class.java).sqlParser
            val parameters = parseNamedParameters(sql, parser).parameters
            require(!parameters.isPositional) {
                "step '$step': target.sql uses positional '?' parameters. Row values bind by name, so " +
                    "write ':column' instead (spec 4.4)."
            }
            binds = parameters.parameterNames.map { it to types[it.lowercase()] }
        }
    }

    override fun write(chunk: List<Row>): Int {
        val opened = checkNotNull(handle) { "step '$step': write called before open." }
        if (chunk.isEmpty()) return 0
        if (!checked) {
            checked = true
            val row = chunk.first()
            val missing = binds.map { it.first }.filter { it.lowercase() !in row.columns }.sorted()
            require(missing.isEmpty()) {
                "step '$step': target.sql binds ${missing.map { ":$it" }} which the source row does not " +
                    "provide. The row has ${row.columns.toList()}. Add the columns to the source SQL, or " +
                    "correct the bind names (spec 4.4)."
            }
        }
        return opened.prepareBatch(sql).use { batch ->
            chunk.forEach { row ->
                binds.forEach { (name, type) -> batch.bindColumn(name, type, row) }
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
 * Opens a handle and prepares it with [configure], releasing it if [configure] throws.
 *
 * Both writers own their handle from [RowWriter.open] onward, and spec 7.4 makes releasing it on
 * that failure path theirs too: a caller whose `open` threw never entered the `use` block that
 * would have closed it. Each had its own copy of the eight-line catch that does this, including
 * the `addSuppressed` that keeps a failing release from hiding the failure that caused it - a
 * protocol two implementations had to get right separately, with no test pinning either (review
 * finding L5).
 *
 * The handle is returned rather than assigned, so a writer whose `open` throws is left with a null
 * handle and a closed connection, and its own idempotent `close` afterwards is a no-op.
 */
private fun Jdbi.openConfigured(configure: (Handle) -> Unit): Handle {
    val opened = open()
    try {
        configure(opened)
    } catch (e: Throwable) {
        try {
            opened.close()
        } catch (releaseFailure: Throwable) {
            e.addSuppressed(releaseFailure)
        }
        throw e
    }
    return opened
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
