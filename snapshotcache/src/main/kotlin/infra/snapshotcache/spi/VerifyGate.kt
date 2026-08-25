package infra.snapshotcache.spi

import infra.snapshotcache.api.GenerationCheck
import infra.snapshotcache.api.GenerationInfo
import infra.snapshotcache.api.VerifyConfig
import infra.snapshotcache.api.VerifyResult
import java.sql.Connection
import java.sql.ResultSet

/**
 * What the verify gate concluded about a candidate. [Passed] carries the per-table row
 * counts measured during verification, which become [GenerationInfo.rowCounts].
 */
internal sealed interface GateOutcome {
    data class Passed(val rowCounts: Map<String, Long>) : GateOutcome

    /** [rule] is the `snapshot_verify_failed_total{rule}` label value (spec 12.2). */
    data class Failed(val rule: String, val detail: String) : GateOutcome
}

/**
 * The verify gate of spec 8: the built-in rules as a hardcoded list (plan 2.4) followed by
 * the caller's [GenerationCheck]s in list order. The first failure aborts the gate.
 *
 * Lives at the spi boundary for the same reason as [SnapshotHandle] (D28): executing SQL
 * means calling `java.sql` methods, which plan 2.2 rule 4 forbids in `core` - verified
 * empirically, a `Statement.executeQuery` call from `core` is an ArchUnit violation.
 * `core` decides *when* to verify; this class owns *how*, including the connection's
 * lifecycle: the verify connection is opened here and closed on every path.
 *
 * SQL is deliberately plain and standard (DuckDB 1.1.3 needs nothing newer):
 * - tables:            `SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'`
 * - row count:         `SELECT COUNT(*) FROM <t>`
 * - key uniqueness:    `SELECT COUNT(id) FROM <t>` vs `SELECT COUNT(DISTINCT id) FROM <t>`
 * - required non-null: `SELECT COUNT(*) FROM <t> WHERE <c> IS NULL`
 */
internal class VerifyGate(
    private val config: VerifyConfig,
    private val checks: List<GenerationCheck>,
) {
    /** A rule whose query threw fails that rule, not the whole round (spec 9.2: verify failure). */
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
        // readable (spec 8.1, non-disableable): the candidate was reopened by the store;
        // this discovery query proves it can actually be queried.
        val tables = rule(RULE_READABLE, "candidate is not queryable") {
            queryStrings(connection, "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
        }

        // non_empty (spec 8.2, non-disableable). Zero tables is the same fault as zero rows.
        if (tables.isEmpty()) return GateOutcome.Failed(RULE_NON_EMPTY, "candidate contains no tables")
        val rowCounts = LinkedHashMap<String, Long>()
        for (table in tables) {
            val count = rule(RULE_NON_EMPTY, "counting rows of table $table") {
                queryLong(connection, "SELECT COUNT(*) FROM $table")
            }
            if (count == 0L) return GateOutcome.Failed(RULE_NON_EMPTY, "table $table has 0 rows")
            rowCounts[table] = count
        }

        // key_unique (spec 8.1): id unique within its own table, one table at a time (spec 3.3).
        if (config.keyUnique) {
            for (table in tables) {
                val total = rule(RULE_KEY_UNIQUE, "counting ids of table $table") {
                    queryLong(connection, "SELECT COUNT(id) FROM $table")
                }
                val distinct = rule(RULE_KEY_UNIQUE, "counting distinct ids of table $table") {
                    queryLong(connection, "SELECT COUNT(DISTINCT id) FROM $table")
                }
                if (total != distinct) {
                    return GateOutcome.Failed(RULE_KEY_UNIQUE, "table $table has $total ids but only $distinct distinct")
                }
            }
        }

        // required_non_null (spec 8.1): entries are either `table.column` or a bare column
        // name, in which case the column is checked in every table.
        for (entry in config.requiredNonNull) {
            val tablesToCheck = if ('.' in entry) listOf(entry.substringBefore('.')) else tables
            val column = entry.substringAfter('.')
            for (table in tablesToCheck) {
                val nulls = rule(RULE_REQUIRED_NON_NULL, "checking $table.$column for NULLs") {
                    queryLong(connection, "SELECT COUNT(*) FROM $table WHERE $column IS NULL")
                }
                if (nulls > 0L) {
                    return GateOutcome.Failed(RULE_REQUIRED_NON_NULL, "table $table column $column has $nulls NULL values")
                }
            }
        }

        // row_count_delta (spec 8.3, D14): off by default; compares against the previous
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

        // Caller extensions (spec 5.2), in list order; a throwing check is a failing check.
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

    private fun queryLong(connection: Connection, sql: String): Long =
        query(connection, sql) { rs ->
            check(rs.next()) { "query returned no rows: $sql" }
            rs.getLong(1)
        }

    private fun queryStrings(connection: Connection, sql: String): List<String> =
        query(connection, sql) { rs ->
            val values = mutableListOf<String>()
            while (rs.next()) values += rs.getString(1)
            values
        }

    private fun <T> query(connection: Connection, sql: String, read: (ResultSet) -> T): T {
        val statement = connection.createStatement()
        try {
            val resultSet = statement.executeQuery(sql)
            try {
                return read(resultSet)
            } finally {
                runCatching { resultSet.close() }
            }
        } finally {
            runCatching { statement.close() }
        }
    }

    private fun Throwable.describe(): String = message ?: toString()

    private companion object {
        // Rule label values, verbatim from spec 8.1.
        const val RULE_READABLE = "readable"
        const val RULE_NON_EMPTY = "non_empty"
        const val RULE_KEY_UNIQUE = "key_unique"
        const val RULE_REQUIRED_NON_NULL = "required_non_null"
        const val RULE_ROW_COUNT_DELTA = "row_count_delta"

        /** Synthetic label for a caller check that threw instead of returning a [VerifyResult]. */
        const val RULE_CALLER_CHECK = "caller_check"
    }
}
