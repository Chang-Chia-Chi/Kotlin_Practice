package infra.etl.pipe

import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import java.sql.ResultSet

/**
 * The source half of a pipe: one query on one JDBC datasource (spec 11.1). It comes in two forms,
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
 * tables that must agree with each other - the snapshot cache building one generation, spec 9.5 -
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
 *   `lastTs` (spec 6.3). Every entry is bound, so a name the SQL does not use is an error rather
 *   than a silent no-op. Identifiers cannot be bound: `select * from :table` is not valid SQL and
 *   the framework offers no substitute (spec 6.3).
 *
 *   A null needs no separate type channel, and this map is not the untyped hole it looks like:
 *   JDBI binds an `org.jdbi.v3.core.argument.Argument` value **directly**, so a caller with a type
 *   in hand puts a `NullArgument` in the map and gets `setNull(pos, <type>)` instead of
 *   `Types.OTHER`. Measured on JDBI 3.45.4 through a recording `PreparedStatement` (P5 scratchpad
 *   `P5Probe4`): a plain null records `setNull[1, 1111]`, a `NullArgument(Types.TIMESTAMP)` records
 *   `setNull[1, 93]`. That is how a null task variable from a zero-row `export` reaches Oracle
 *   typed (spec 6.3), with this signature unchanged.
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
 * Caller-supplied per-row code (spec 9.1). Returning null drops the row, which is why
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
 * Layer 1 (spec 2.1): rows from a JDBC query into a [RowWriter], chunked and typed. It knows
 * nothing about YAML, phases, retry, or generations, so the snapshot cache can use it without
 * the task model (spec 9.5).
 *
 * The loop is spec 5.2: open the source with `fetchSize = chunkSize`, accumulate up to
 * [chunkSize] Rows, apply [transform] to each, write the chunk, repeat.
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
 *   default, so flush *is* the chunk's commit (spec 4.6).
 * - `JdbcTableWriter` and `JdbcStatementWriter` execute one prepared batch per chunk on a handle
 *   whose `autoCommit` is left as the datasource supplies it - on, for ojdbc - so each batch
 *   commits itself. Calling `commit()` on such a connection raises ORA-17273 instead, which P1
 *   measured. Measured for the target too, on a real Oracle: `RowPipeOracleTest.each chunk is
 *   committed to an Oracle target at its chunk boundary` watches the target row count from a
 *   second Oracle session and sees the same chunk timeline as the DuckDB case.
 *
 * Nothing therefore spans two chunks, which is spec 1.2 and 5.4: a failure part way through
 * leaves earlier chunks committed, and Layer 2 makes that safe with `idempotent: true` or a
 * work-table swap.
 *
 * **Resources.** [run] closes the source statement and result set always, and the source handle
 * only when [JdbcSource] opened it - a borrowed handle or connection is the caller's and survives
 * the run. It owns [target]'s lifecycle: [RowWriter.open] before the first chunk and
 * [RowWriter.close] on every path including a throw from inside a chunk (spec 7.4). The target is
 * closed first, then the source, so a writer that fails while flushing still reports its own
 * failure. Consequently a [RowWriter] is single-use and must not be handed to a second pipe. A
 * connection the caller passed into a writer - the generation file's write connection, in spec
 * 9.5 - stays the caller's, is not closed, and is usable again the moment [run] returns.
 *
 * **A transform that adds a column needs a target that already has it.** [RowWriter.open] is
 * given the source's columns, which is all that exists before the first Row, so under
 * `createTable: AUTO` the generated DDL cannot describe an added column and its value is
 * **silently dropped**. Under `createTable: REQUIRED` it lands. Closing that gap is Layer 2's
 * `transform.addColumns` (spec 9.1, validation rule 14), which has no channel into this
 * constructor.
 *
 * A failure propagates unchanged. Retry belongs to Layer 2 and is not in this class.
 *
 * @param step the step name, so that a bad column type or a wrong typed accessor names it
 *   (spec 4.2, 4.3). The writers carry their own for the same reason.
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
            // Spec 5.2 step 1. Applied to the PreparedStatement at execution.
            .setFetchSize(chunkSize)
            // Everything happens inside the scanner, because that is the only place the raw
            // ResultSet exists. Both the result set and the statement context are closed here
            // rather than left to whoever unwinds the stack.
            .scanResultSet { rows, context -> context.use { rows.get().use(::pump) } }
    }

    private fun pump(rows: ResultSet): PipeResult = target.use { writer ->
        // Metadata is read once, before any row, so the target can reject a column it cannot
        // write before the first chunk exists (spec 4.4). An empty source still opens the target,
        // and so still raises those errors.
        val mapper = RowMapper(rows.metaData, step)
        writer.open(mapper.columns)

        val chunk = ArrayList<Row>(chunkSize)
        var read = 0L
        var written = 0L
        while (rows.next()) {
            read++
            val row = mapper.map(rows)
            chunk += if (transform == null) row else transform.apply(row) ?: continue
            if (chunk.size == chunkSize) {
                written += writer.write(chunk)
                chunk.clear()
            }
        }
        if (chunk.isNotEmpty()) written += writer.write(chunk)
        PipeResult(read, written)
    }
}
