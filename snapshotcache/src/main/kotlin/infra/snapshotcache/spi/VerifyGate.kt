package infra.snapshotcache.spi

import infra.snapshotcache.api.GenerationCheck
import infra.snapshotcache.api.GenerationInfo
import infra.snapshotcache.api.VerifyConfig
import infra.snapshotcache.api.VerifyResult
import java.sql.Connection

/**
 * What the verify gate concluded about a candidate. [Passed] carries the per-table row
 * counts measured during verification, which become [GenerationInfo.rowCounts].
 */
internal sealed interface GateOutcome {
    data class Passed(val rowCounts: Map<String, Long>) : GateOutcome

    /** [rule] is the `snapshot_verify_failed_total{rule}` label value. */
    data class Failed(val rule: String, val detail: String) : GateOutcome
}

/**
 * The verify gate: the built-in rules as a hardcoded list, followed by the caller's
 * [GenerationCheck]s in list order. The first failure aborts the gate.
 *
 * Lives at the spi boundary for the same reason as [SnapshotHandle]: executing SQL means
 * calling `java.sql` methods, which the package rules forbid in `core` - verified
 * empirically, a `Statement.executeQuery` call from `core` is an ArchUnit violation.
 * `core` decides *when* to verify; this class owns *how*, including the connection's
 * lifecycle: the verify connection is opened here and closed on every path.
 *
 * SQL is deliberately plain and standard (DuckDB 1.1.3 needs nothing newer), and every
 * discovered or configured identifier is quoted - the names come from the data, not from
 * this file, so a reserved word or a mixed-case table must not become a parse error:
 * - tables:            `SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_catalog = current_database()`
 * - row count:         `SELECT COUNT(*) FROM <t>`
 * - key uniqueness:    `SELECT COUNT(id), COUNT(DISTINCT id) FROM <t>` - one scan, not two
 * - required non-null: `SELECT COUNT(*) FROM <t> WHERE <c> IS NULL`
 */
internal class VerifyGate(
    private val config: VerifyConfig,
    private val checks: List<GenerationCheck>,
) {
    /** A rule whose query threw fails that rule - the round ends as a verify failure, not a source error. */
    private class RuleFailed(val rule: String, val detail: String) : RuntimeException(detail)

    /** Runs the whole gate against a fresh connection into [opened]; never throws except [InterruptedException]. */
    fun verify(opened: OpenGeneration, previous: GenerationInfo?): GateOutcome {
        val connection = try {
            opened.connection()
        } catch (interrupted: InterruptedException) {
            throw interrupted
        } catch (failure: Exception) {
            return GateOutcome.Failed(RULE_READABLE, "could not open a connection to the candidate: ${failure.describe()}")
        }
        return try {
            run(connection, previous)
        } catch (failed: RuleFailed) {
            GateOutcome.Failed(failed.rule, failed.detail)
        } finally {
            runCatching { connection.close() }
        }
    }

    private fun run(connection: Connection, previous: GenerationInfo?): GateOutcome {
        // readable (non-disableable): the candidate was reopened by the store;
        // this discovery query proves it can actually be queried.
        val tables = rule(RULE_READABLE, "candidate is not queryable") {
            connection.queryStrings("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_catalog = current_database()")
        }

        // non_empty (non-disableable). Zero tables is the same fault as zero rows.
        if (tables.isEmpty()) return GateOutcome.Failed(RULE_NON_EMPTY, "candidate contains no tables")
        val rowCounts = LinkedHashMap<String, Long>()
        for (table in tables) {
            val count = rule(RULE_NON_EMPTY, "counting rows of table $table") {
                connection.queryLong("SELECT COUNT(*) FROM ${ident(table)}")
            }
            if (count == 0L) return GateOutcome.Failed(RULE_NON_EMPTY, "table $table has 0 rows")
            rowCounts[table] = count
        }

        // key_unique: id unique within its own table, one table at a time. Both counts come
        // from a single scan - the candidate is attached but not yet published, so every
        // table pass widens that window.
        if (config.keyUnique) {
            for (table in tables) {
                val (total, distinct) = rule(RULE_KEY_UNIQUE, "counting ids of table $table") {
                    connection.queryTwoLongs("SELECT COUNT(id), COUNT(DISTINCT id) FROM ${ident(table)}")
                }
                if (total != distinct) {
                    return GateOutcome.Failed(RULE_KEY_UNIQUE, "table $table has $total ids but only $distinct distinct")
                }
            }
        }

        // required_non_null: entries are either `table.column` or a bare column
        // name, in which case the column is checked in every table.
        for (entry in config.requiredNonNull) {
            val tablesToCheck = if ('.' in entry) listOf(entry.substringBefore('.')) else tables
            val column = entry.substringAfter('.')
            for (table in tablesToCheck) {
                val nulls = rule(RULE_REQUIRED_NON_NULL, "checking $table.$column for NULLs") {
                    connection.queryLong("SELECT COUNT(*) FROM ${ident(table)} WHERE ${ident(column)} IS NULL")
                }
                if (nulls > 0L) {
                    return GateOutcome.Failed(RULE_REQUIRED_NON_NULL, "table $table column $column has $nulls NULL values")
                }
            }
        }

        // row_count_delta: off by default; compares against the previous
        // generation's counts, table by table, with separate decrease/increase limits.
        val delta = config.rowCountDelta
        if (delta.enabled && previous != null) {
            for ((table, count) in rowCounts) {
                val before = previous.rowCounts[table] ?: continue
                if (before <= 0L) continue
                val change = (count - before).toDouble() / before
                if (-change > delta.maxDecreaseRatio) {
                    return GateOutcome.Failed(RULE_ROW_COUNT_DELTA, "table $table shrank from $before to $count rows")
                }
                if (change > delta.maxIncreaseRatio) {
                    return GateOutcome.Failed(RULE_ROW_COUNT_DELTA, "table $table grew from $before to $count rows")
                }
            }
        }

        // Caller extensions, in list order; a throwing check is a failing check.
        for (check in checks) {
            val result = rule(RULE_CALLER_CHECK, "${check.javaClass.name} threw") {
                check.verify(connection, previous)
            }
            if (result is VerifyResult.Fail) return GateOutcome.Failed(result.rule, result.detail)
        }

        return GateOutcome.Passed(rowCounts)
    }

    /** Runs one rule's query; an exception fails that rule with the cause in the detail. */
    private inline fun <T> rule(name: String, what: String, query: () -> T): T = try {
        query()
    } catch (interrupted: InterruptedException) {
        throw interrupted
    } catch (failure: Exception) {
        throw RuleFailed(name, "$what: ${failure.describe()}")
    }

    private fun Connection.queryLong(sql: String): Long = createStatement().use { it.queryLong(sql) }

    private fun Connection.queryStrings(sql: String): List<String> = createStatement().use { it.queryStrings(sql) }

    /** The one two-aggregate read of the gate; [Pair] keeps the call site a single line. */
    private fun Connection.queryTwoLongs(sql: String): Pair<Long, Long> = createStatement().use { statement ->
        statement.query(sql) { rs ->
            check(rs.next()) { "query returned no rows: $sql" }
            rs.getLong(1) to rs.getLong(2)
        }
    }

    private companion object {
        // Rule label values, as they appear in `snapshot_verify_failed_total{rule}`.
        const val RULE_READABLE = "readable"
        const val RULE_NON_EMPTY = "non_empty"
        const val RULE_KEY_UNIQUE = "key_unique"
        const val RULE_REQUIRED_NON_NULL = "required_non_null"
        const val RULE_ROW_COUNT_DELTA = "row_count_delta"

        /** Synthetic label for a caller check that threw instead of returning a [VerifyResult]. */
        const val RULE_CALLER_CHECK = "caller_check"
    }
}
