package infra.etl.pipe

import java.sql.Connection
import java.sql.DatabaseMetaData

/**
 * The write seam of spec 4.4. A writer is used exactly once, in this order:
 *
 * 1. [open] - the source column list arrives, the target is resolved and validated, and every
 *    error that can be detected without a Row is raised here. Nothing is written.
 * 2. [write] - zero or more chunks.
 * 3. [close] - always, on the failure path as well. Callers use `use { }`; an implementation
 *    that allocates a resource inside [open] releases it itself if [open] then throws, so a
 *    caller that never reaches its `use` block cannot leak (spec 7.4).
 *
 * A writer holds a connection or a handle and is confined to one thread, like the DuckDB
 * connection it may be built on (spec 7.2).
 */
interface RowWriter : AutoCloseable {

    /**
     * Resolves the target against the columns the source will produce, in source order.
     *
     * @param columns the source result set's columns (spec 4.3), plus any column a transform
     *   declares in `transform.addColumns` (spec 9.1).
     * @throws IllegalArgumentException naming the step and the column, for anything the target
     *   cannot accept: a type DuckDB cannot append, a nullable column outside validation rule
     *   15's set, a Row key the target table does not have, or a NOT NULL target column the
     *   source does not supply.
     */
    fun open(columns: List<ColumnMeta>)

    /** Writes one chunk and returns the number of rows written. */
    fun write(chunk: List<Row>): Int

    override fun close()
}

/**
 * One column of a target table exactly as the catalog reports it, before it is mapped onto a
 * [CanonicalType]. Kept raw because a target table may contain a column of a type this
 * framework cannot map at all; that is only an error when the pipe actually writes to it.
 */
internal class CatalogColumn(
    val name: String,
    val sqlType: Int,
    val typeName: String,
    val nullable: Boolean,
    val precision: Int,
    val scale: Int,
)

/**
 * The target table's columns, in catalog ordinal order, read through [DatabaseMetaData]. This is
 * the only source of target column order and target nullability; YAML never carries either
 * (spec 4.4, 4.6).
 *
 * [table] may be `schema.name`. Identifiers are folded to whatever case the driver says it
 * stores - Oracle stores unquoted names upper case, DuckDB stores them as written - and the
 * returned names are lower-cased per spec 4.5.
 *
 * `getColumns` takes patterns, not names, so `_` in a table name is a single-character wildcard
 * and `wip_stg` also matches `wipXstg` - measured on duckdb_jdbc 1.1.3. Over-matched rows are
 * dropped by comparing `TABLE_NAME` exactly rather than by escaping, because escaping depends on
 * the driver honouring `getSearchStringEscape` and a stray extra column would shift every value
 * of a positional append by one. The **schema** is a pattern for the same reason and is compared
 * the same way whenever [table] states one: without that, `etl_stg.wip` also matches `etl1stg.wip`,
 * which either trips the one-owner check below - telling an already-qualified target to qualify
 * itself - or silently supplies the wrong schema's column list when only that schema has the table.
 *
 * An unqualified [table] also searches every schema, so a same-named table in a second schema
 * contributes its own columns and the two interleave once sorted by ordinal - `main.t1(a,b)` plus
 * `other.t1(zz,yy)` yields `[a, zz, b, yy]`, an over-wide and mis-ordered list that appends
 * shifted from the first mismatch and fails at write time as `Too many appends for chunk!`,
 * naming neither step nor column. Rows are therefore grouped by their owning `(TABLE_CAT,
 * TABLE_SCHEM)` and the lookup fails unless exactly one owner answered.
 *
 * Note that [DatabaseMetaData.getColumns] reports nullability correctly on duckdb_jdbc 1.1.3,
 * unlike `ResultSetMetaData.isNullable`, which reports `columnNullable` for every column
 * including `not null` ones. Never substitute one for the other.
 *
 * @throws IllegalArgumentException if the table has no columns visible to this connection, if two
 *   visible tables of that name make the column list ambiguous, or if one table reports the same
 *   column name twice.
 */
internal fun catalogColumns(connection: Connection, table: String, step: String): List<CatalogColumn> {
    val meta = connection.metaData
    val schema = table.substringBeforeLast('.', "").ifEmpty { null }
    val name = table.substringAfterLast('.')
    val byOwner = LinkedHashMap<String, MutableList<Pair<Int, CatalogColumn>>>()
    meta.getColumns(null, schema?.let { meta.stored(it) }, meta.stored(name), null).use { rs ->
        while (rs.next()) {
            if (!rs.getString("TABLE_NAME").equals(name, ignoreCase = true)) continue
            if (schema != null && !rs.getString("TABLE_SCHEM").orEmpty().equals(schema, ignoreCase = true)) continue
            val owner = listOfNotNull(rs.getString("TABLE_CAT"), rs.getString("TABLE_SCHEM")).joinToString(".")
            byOwner.getOrPut(owner) { mutableListOf() } += rs.getInt("ORDINAL_POSITION") to CatalogColumn(
                name = rs.getString("COLUMN_NAME").lowercase(),
                sqlType = rs.getInt("DATA_TYPE"),
                typeName = rs.getString("TYPE_NAME"),
                nullable = rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls,
                precision = rs.getInt("COLUMN_SIZE"),
                scale = rs.getInt("DECIMAL_DIGITS"),
            )
        }
    }
    require(byOwner.isNotEmpty()) {
        "step '$step': target table '$table' does not exist, or has no column visible to this connection."
    }
    require(byOwner.size == 1) {
        "step '$step': ${byOwner.size} tables named '$table' are visible to this connection, in " +
            "${byOwner.keys}. Column order would be taken from all of them at once. Qualify the target " +
            "with its schema."
    }
    val columns = byOwner.values.first().sortedBy { it.first }.map { it.second }
    val duplicate = columns.groupingBy { it.name }.eachCount().entries.firstOrNull { it.value > 1 }
    require(duplicate == null) {
        "step '$step': the catalog reports ${duplicate?.value} columns named '${duplicate?.key}' for " +
            "table '$table'. A Row is keyed by name, so the positional append cannot tell them apart."
    }
    return columns
}

/** The identifier as the driver stores it, so that a catalog lookup by name finds the table. */
private fun DatabaseMetaData.stored(identifier: String): String = when {
    storesUpperCaseIdentifiers() -> identifier.uppercase()
    storesLowerCaseIdentifiers() -> identifier.lowercase()
    else -> identifier
}
