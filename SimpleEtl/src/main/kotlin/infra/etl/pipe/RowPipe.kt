package infra.etl.pipe

import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.statement.ColonPrefixSqlParser
import org.jdbi.v3.core.statement.ParsedSql
import org.jdbi.v3.core.statement.SqlParser
import java.sql.ResultSet

/**
 * The source half of a pipe: one query on one JDBC datasource. It comes in two forms,
 * which differ only in **who owns the connection and how far the read transaction reaches**:
 *
 * | Built from | The pipe opens | The pipe closes | Read transaction |
 * |---|---|---|---|
 * | [Jdbi] | a handle, and its connection | both | one pipe |
 * | [Handle] | nothing | nothing | the caller's |
 *
 * The [Jdbi] form is the convenience form and the right default for a single pipe. The [Handle]
 * form exists because **several pipes must be able to read inside one transaction**: N pipes over
 * N fresh connections read N tables at N different points in time, and the union of those tables
 * can show duplicates or gaps that no later inspection can reproduce. A caller loading a group of
 * tables that must agree with each other - the snapshot cache building one generation -
 * opens one handle, begins one transaction, and gives that handle to every pipe:
 *
 * ```kotlin
 * jdbi.open().use { handle ->
 *     handle.begin()
 *     specs.forEach { RowPipe(JdbcSource(handle, it.sql), writerFor(it), it.name).run() }
 *     handle.commit()
 * }
 * ```
 *
 * A borrowed [Handle] belongs to the caller and is never closed here, exactly as
 * `DuckDbTableWriter`'s connection is not. Getting that backwards in either direction is a real
 * defect: closing it would break the caller's transaction mid-group, and failing to close a
 * handle this class opened would leak a connection per pipe. A caller holding only a
 * `java.sql.Connection` wraps it itself with `Jdbi.create(connection).open()`, which releases the
 * connection with a no-op; `Jdbi.open(connection)` does **not** - measured on 3.45.4, it builds a
 * lambda ConnectionFactory that inherits the interface default and closes the caller's
 * connection.
 *
 * @param parameters bound by name through JDBI, so `:lastTs` takes the value of the entry
 *   `lastTs`. Every entry is bound, so a name the SQL does not use is an error rather
 *   than a silent no-op. Identifiers cannot be bound: `select * from :table` is not valid SQL and
 *   the framework offers no substitute.
 *
 *   A null needs no separate type channel, and this map is not the untyped hole it looks like:
 *   JDBI binds an `org.jdbi.v3.core.argument.Argument` value **directly**, so a caller with a type
 *   in hand puts a `NullArgument` in the map and gets `setNull(pos, <type>)` instead of
 *   `Types.OTHER`. Measured on JDBI 3.45.4 through a recording `PreparedStatement` (P5 scratchpad
 *   `P5Probe4`): a plain null records `setNull[1, 1111]`, a `NullArgument(Types.TIMESTAMP)` records
 *   `setNull[1, 93]`. That is how a null task variable from a zero-row `export` reaches Oracle
 *   typed, with this signature unchanged.
 */
class JdbcSource private constructor(
    private val openHandle: () -> Handle,
    private val ownsHandle: Boolean,
    val sql: String,
    val parameters: Map<String, Any?>,
) {

    /** One handle and one connection per pipe, opened and closed by [RowPipe.run]. */
    constructor(jdbi: Jdbi, sql: String, parameters: Map<String, Any?> = emptyMap()) :
        this(jdbi::open, ownsHandle = true, sql = sql, parameters = parameters)

    /** The caller's [handle], borrowed. Left open, in whatever transaction the caller began. */
    constructor(handle: Handle, sql: String, parameters: Map<String, Any?> = emptyMap()) :
        this({ handle }, ownsHandle = false, sql = sql, parameters = parameters)

    internal fun <T> withHandle(block: (Handle) -> T): T {
        val handle = openHandle()
        return if (ownsHandle) handle.use(block) else block(handle)
    }
}

/**
 * The one [SqlParser] every startup check uses, so that the names checked before a run are exactly
 * the names JDBI looks for during it. Cached and thread safe.
 */
private val COLON_PREFIX: SqlParser = ColonPrefixSqlParser()

/**
 * The `:name` parameters [sql] binds, and JDBI's `?`-substituted rewrite of it.
 *
 * One function because there were five copies of this parse - the loader's rule 6, rule 19 and the
 * amended rule 7, the engine's variable resolution and its cacheCopy guard, and
 * `JdbcStatementWriter`'s bind names - and two of them built a throwaway `handle.createUpdate(sql)`
 * solely to obtain a `StatementContext` (review finding L4). Measured on jdbi3-core 3.45.4:
 * `parse(sql, null)` works, because a colon-prefixed parse never touches the context.
 *
 * It lives in this package rather than beside any one caller because `infra.etl.jdbc` may not
 * depend on `infra.etl.task`, and both need it.
 *
 * **The callers keep their own error messages.** Every one of them rejects a positional `?`, but
 * each cites a different rule and offers a different remedy - a task variable, a Row key, or
 * nothing bindable at all - so the sentence is not the duplication; the parse was.
 *
 * Two measured facts the callers rely on. The parser skips a colon inside a string literal, a `::`
 * cast and a `--` comment, all of which appear in real task SQL. It does **not** skip a colon
 * followed by digits: `select site_code[1:3]` yields the name `3` and the rewrite
 * `select site_code[1?]`, and `{'k':1}` yields `1`. Whether that matters depends on whether the
 * text later passes through JDBI - for a `cacheCopy` it does not, and rule 19 ignores such names;
 * everywhere else it does, and they are real if broken bindings.
 *
 * @param parser the handle's own configured parser where a caller has a handle, so run time cannot
 *   parse by one rule while startup parsed by another. Defaults to JDBI's colon-prefix parser,
 *   which is what a handle carries unless a host replaced it.
 */
internal fun parseNamedParameters(sql: String, parser: SqlParser = COLON_PREFIX): ParsedSql =
    parser.parse(sql, null)

/**
 * Caller-supplied per-row code. Returning null drops the row, which is why
 * [PipeResult] reports rows read and rows written separately.
 *
 * Contractually stateless, database-free and side-effect-free; the framework cannot enforce any
 * of the three, and a transform that breaks them makes a retry non-deterministic.
 */
fun interface RowTransform {
    fun apply(row: Row): Row?
}

/** What one [RowPipe.run] moved. The two differ when a transform dropped rows. */
data class PipeResult(val rowsRead: Long, val rowsWritten: Long)

/**
 * Layer 1: rows from a JDBC query into a [RowWriter], chunked and typed. It knows
 * nothing about YAML, phases, retry, or generations, so the snapshot cache can use it without
 * the task model.
 *
 * The loop is fixed: open the source with `fetchSize = chunkSize`, accumulate up to
 * [chunkSize] Rows, apply [transform] to the chunk, write what survives, repeat. The order
 * matters: a chunk boundary falls every [chunkSize] **source** rows, so a
 * selective transform shortens the chunks it writes rather than stretching the span of source
 * rows one commit covers.
 *
 * **Memory is flat in row count.** The driver holds one fetch, the pipe holds one chunk in a
 * buffer it reuses, and nothing accumulates a result list. Oracle's default fetch size is 10,
 * unusable at these row counts, which is the whole reason [chunkSize] is pushed onto the
 * statement.
 *
 * **This class never commits.** A chunk is committed by the act of writing it, and which
 * mechanism does that belongs to the writer, not here - [RowWriter] has no commit method:
 *
 * - `DuckDbTableWriter` flushes the appender once per chunk. Measured on duckdb_jdbc 1.1.3:
 *   appended-but-unflushed rows are invisible even to the appending connection, and after
 *   `flush()` they are immediately visible to a `duplicate()` connection. `autoCommit` is true by
 *   default, so flush *is* the chunk's commit.
 * - `JdbcTableWriter` and `JdbcStatementWriter` execute one prepared batch per chunk on a handle
 *   whose `autoCommit` is left as the datasource supplies it - on, for ojdbc - so each batch
 *   commits itself. Calling `commit()` on such a connection raises ORA-17273 instead, which P1
 *   measured. Measured for the target too, on a real Oracle: `RowPipeOracleTest.each chunk is
 *   committed to an Oracle target at its chunk boundary` watches the target row count from a
 *   second Oracle session and sees the same chunk timeline as the DuckDB case.
 *
 * Nothing therefore spans two chunks, and that is the durability contract: a failure part way
 * through leaves earlier chunks committed, and Layer 2 makes that safe with `idempotent: true`
 * or a work-table swap.
 *
 * **Resources.** [run] closes the source statement and result set always, and the source handle
 * only when [JdbcSource] opened it - a borrowed handle or connection is the caller's and survives
 * the run. It owns [target]'s lifecycle: [RowWriter.open] before the first chunk and
 * [RowWriter.close] on every path including a throw from inside a chunk. The target is
 * closed first, then the source, so a writer that fails while flushing still reports its own
 * failure. Consequently a [RowWriter] is single-use and must not be handed to a second pipe. A
 * connection the caller passed into a writer - the snapshot cache's generation file, for one -
 * stays the caller's, is not closed, and is usable again the moment [run] returns.
 *
 * **A transform that adds a column needs a target that already has it.** [RowWriter.open] is
 * given the source's columns, which is all that exists before the first Row, so under
 * `createTable: AUTO` the generated DDL cannot describe an added column and its value is
 * **silently dropped**. Under `createTable: REQUIRED` it lands. Closing that gap is Layer 2's
 * `transform.addColumns` (validation rule 14), which has no channel into this
 * constructor.
 *
 * A failure propagates unchanged. Retry belongs to Layer 2 and is not in this class.
 *
 * @param step the step name, so that a bad column type or a wrong typed accessor names it. The
 *   writers carry their own for the same reason.
 */
class RowPipe(
    private val source: JdbcSource,
    private val target: RowWriter,
    private val step: String,
    private val chunkSize: Int = 5000,
    private val transform: RowTransform? = null,
) {

    init {
        require(chunkSize > 0) { "step '$step': chunkSize must be at least 1, got $chunkSize." }
    }

    fun run(): PipeResult = source.withHandle { handle ->
        handle.createQuery(source.sql)
            .bindMap(source.parameters)
            // The source is read one chunk at a time. Applied to the PreparedStatement at execution.
            .setFetchSize(chunkSize)
            // Everything happens inside the scanner, because that is the only place the raw
            // ResultSet exists. Both the result set and the statement context are closed here
            // rather than left to whoever unwinds the stack.
            .scanResultSet { rows, context -> context.use { rows.get().use(::pump) } }
    }

    private fun pump(rows: ResultSet): PipeResult = target.use { writer ->
        // Metadata is read once, before any row, so the target can reject a column it cannot
        // write before the first chunk exists. An empty source still opens the target,
        // and so still raises those errors.
        val mapper = RowMapper(rows.metaData, step)
        writer.open(mapper.columns)

        val chunk = ArrayList<Row>(chunkSize)
        var read = 0L
        var written = 0L
        while (rows.next()) {
            read++
            chunk += mapper.map(rows)
            if (chunk.size == chunkSize) {
                written += writeChunk(writer, chunk)
                chunk.clear()
            }
        }
        if (chunk.isNotEmpty()) written += writeChunk(writer, chunk)
        PipeResult(read, written)
    }

    /**
     * The transform runs over an accumulated chunk, not over each row as it is read, so a chunk
     * boundary falls every [chunkSize] **source** rows whatever the transform drops.
     *
     * A chunk the transform empties is not written. `write(emptyList())` is legal on every writer
     * but `DuckDbTableWriter` flushes its appender on every call, so an empty write would add a
     * commit boundary for a chunk with nothing to commit.
     *
     * `mapNotNull` allocates one list per chunk, and only when a transform exists. Memory stays
     * flat in row count - two chunk-sized lists rather than one, and neither grows with the source.
     */
    private fun writeChunk(writer: RowWriter, chunk: List<Row>): Int {
        val surviving = if (transform == null) chunk else chunk.mapNotNull(transform::apply)
        return if (surviving.isEmpty()) 0 else writer.write(surviving)
    }
}
