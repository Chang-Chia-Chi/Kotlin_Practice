package infra.snapshotcache.e2e

import infra.snapshotcache.api.BuildContext
import infra.snapshotcache.api.GenerationSource
import org.duckdb.DuckDBConnection

/**
 * The spec 17.7 synthetic source: a few thousand rows for `t_a` / `t_b` written through
 * the REAL write path - the org.duckdb Appender on the candidate's write connection -
 * plus the spec 3.3 union view (source column, aligned ids). No Oracle involved; the
 * [GenerationSource] seam makes the substitution free (D20).
 *
 * Every value embeds the generation number ("g<gen>-a-<id>"), so "the held handle still
 * queries gen N unchanged" (I8) is provable by content, not just by row count.
 */
class SyntheticSource(
    private val rowsA: Long = 2_000,
    private val rowsB: Long = 3_000,
) : GenerationSource {

    override fun refresh(ctx: BuildContext) {
        ctx.target.createStatement().use { st ->
            st.execute("CREATE TABLE t_a (id BIGINT NOT NULL, name VARCHAR NOT NULL, amount DOUBLE)")
            st.execute("CREATE TABLE t_b (id BIGINT NOT NULL, label VARCHAR NOT NULL)")
            // spec 3.3: two physical tables, one union view. t_b genuinely lacks the
            // `amount` concept, so the view fills a typed NULL; `label` aligns onto `name`.
            st.execute(
                """
                CREATE VIEW t_unified AS
                  SELECT 'A' AS source, id, name, amount FROM t_a
                  UNION ALL
                  SELECT 'B' AS source, id, label AS name, CAST(NULL AS DOUBLE) AS amount FROM t_b
                """.trimIndent(),
            )
        }
        val duck = ctx.target as? DuckDBConnection
            ?: error("SyntheticSource requires the real DuckDB write connection, got ${ctx.target.javaClass.name}")
        duck.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "t_a").use { appender ->
            for (id in 1..rowsA) {
                appender.beginRow()
                appender.append(id)
                appender.append("g${ctx.generation}-a-$id")
                appender.append(id * 0.5)
                appender.endRow()
            }
        }
        duck.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "t_b").use { appender ->
            for (id in 1..rowsB) {
                appender.beginRow()
                appender.append(id)
                appender.append("g${ctx.generation}-b-$id")
                appender.endRow()
            }
        }
    }
}
