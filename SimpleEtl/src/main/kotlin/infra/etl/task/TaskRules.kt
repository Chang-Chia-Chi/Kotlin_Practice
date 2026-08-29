package infra.etl.task

import infra.etl.duckdb.CreateTable
import infra.etl.pipe.parseNamedParameters
import org.jdbi.v3.core.statement.ColonPrefixSqlParser
import org.jdbi.v3.core.statement.SqlParser

/** What a caller with no [org.jdbi.v3.core.Handle] parses `:name` with (spec 10). */
internal val COLON_PREFIX: SqlParser = ColonPrefixSqlParser()

/**
 * One broken rule, for a caller to file where its own report files things.
 *
 * Deliberately **not** [ValidationError]: that carries a non-null `file`, and a [TaskDefinition]
 * built in code did not come from one (spec 2.1). [TaskFileLoader] maps a violation to a
 * `ValidationError` by stamping the file name onto it; [TaskEngine] turns one into the
 * `IllegalArgumentException` it already raised, prefixed with the step name.
 *
 * [step] is nullable to match `ValidationError.step`, so the loader's mapping is a field copy.
 * Every violation [TaskRules.check] produces names a step, because the whole interface is per step.
 */
internal data class RuleViolation(val step: String?, val message: String)

/**
 * The rules of spec 10 that are statements about a **task** rather than about a file, over the one
 * model both sources of a task produce.
 *
 * Rules 7, 8, 11, 12, 13's scratch-only half, 18, 19 and rule 6's positional-`?` half used to
 * exist twice - once over `TaskYaml` in [TaskFileLoader], once over [TaskDefinition] in [TaskEngine] -
 * worded independently. Review findings M10 and M11 each fixed one instance of the drift that
 * produces; this class removes the cause. A rule tightened in the loader alone refuses valid files
 * at boot; one tightened in the engine alone boots clean and dies mid-run, which is the failure
 * spec 10 exists to convert into a boot failure.
 *
 * **File-shaped rules are not here**, and that is the split rather than an omission: rule 1, rule
 * 10 (the sealed [PipeTarget] makes it unrepresentable downstream, so there is nothing left to
 * check), rule 13's `format`-placement half, 16, 17 and rule 6's DuckDB syntax check stay in the
 * loader. The last of those boots an in-memory DuckDB parser, and it stays there so that nothing
 * on the run path ever does.
 *
 * @param parserFor the `:name` parser for a step's datasource. The loader has no `Handle` and takes
 *   the colon-prefix default; the engine hands over the datasource's own configured parser, so run
 *   time cannot parse by one rule while startup parsed by another. A host that reconfigures its
 *   `Jdbi` can still make the two disagree - that is one module called twice with different inputs,
 *   which spec 10 records, and not a rule enforced twice.
 */
internal class TaskRules(private val parserFor: (String?) -> SqlParser = { COLON_PREFIX }) {

    /**
     * Spec 5.3's `retries`, resolved: the stated value, or the datasource-dependent default for a
     * step that did not state one. **This is the only place that resolution happens on the run
     * path**, which is what `Step.retries` being `Int?` buys - a Kotlin constructor default cannot
     * depend on another field, so before E10 every construction site re-derived it.
     *
     * An `export` step has no target, and a `cacheCopy` resolves to 0 rather than to the 3 its
     * scratch output would otherwise earn: no failure a cache copy can produce is transient under
     * spec 5.3, so the knob can never fire, and rule 20 rejects a stated one. Both paths resolve it
     * the same way, which is what retires that rule's old model-versus-YAML asymmetry.
     */
    fun retries(step: Step): Int = step.retries ?: when (step) {
        is PipeStep -> defaultRetries(step.target.datasource)
        is MaterializeStep -> defaultRetries(step.datasource)
        is SqlStep -> defaultRetries(step.datasource)
        is ExportStep, is CacheCopyStep -> 0
    }

    /**
     * Every task-shaped rule this step breaks, accumulated rather than thrown: the loader reports a
     * whole directory at once, and a report naming one error per file makes an author fix a
     * ten-error file ten times.
     *
     * @param defined the variables resolvable at this point in step order - the built-ins, the task
     *   literals, and the exports of every *earlier* step (spec 6.2). A step's own exports are not
     *   in it: they become available to later steps only once this one has succeeded.
     */
    fun check(step: Step, defined: Set<String>): List<RuleViolation> {
        val found = mutableListOf<String>()
        val err: (String) -> Unit = { found += it }

        val stated = step.retries
        if (stated != null && stated < 0) err("retries must not be negative, got $stated (spec 5.3).")
        val retries = retries(step)

        when (step) {
            is PipeStep -> pipe(step, retries, defined, err)
            is MaterializeStep -> materialize(step, retries, defined, err)
            is SqlStep -> {
                step.statements.forEach { sql(it, "statements", step.datasource, defined, err) }
                // Rule 12 as spec 10 amends it for a sql step (review finding H2). Each statement
                // is its own transaction (spec 5.2), so a retry re-runs the whole list.
                if (step.datasource != SCRATCH && retries > 0 && !step.idempotent) {
                    err(
                        "retries $retries on the non-scratch datasource '${step.datasource}' requires " +
                            "idempotent: true (rule 12). Each statement is its own transaction, so a retry " +
                            "re-runs all of them - a failure after the first statement committed would run " +
                            "it a second time (spec 5.2, 5.3).",
                    )
                }
            }

            is ExportStep -> export(step, defined, err)
            is CacheCopyStep -> cacheCopy(step, err)
        }
        return found.map { RuleViolation(step.name, it) }
    }

    private fun pipe(step: PipeStep, retries: Int, defined: Set<String>, err: (String) -> Unit) {
        sql(step.source.sql, "source.sql", step.source.datasource, defined, err)
        val target = step.target
        if (target is StatementTarget) {
            // Rule 11.
            if (target.datasource == SCRATCH) {
                err(
                    "target.sql is not available on '$SCRATCH' (rule 11). DuckDB writes go through the " +
                        "appender, which takes a table and not a statement (spec 4.4).",
                )
            }
            // Rule 7 excepts target.sql: every ':name' there is a Row key, unknown until the source
            // query runs, so the names are checked against the first chunk at run time (spec 4.4,
            // 6.3). Rule 6's positional half still applies.
            positional(target.sql, "target.sql", target.datasource, err)
        }
        // Rule 18. Spec 5.5 is unconditional - every dataset produced inside scratch is written
        // under an attempt-suffixed name - and REQUIRED cannot be, because the author created the
        // table under its stable name. So a retry appends on top of whatever the failed attempt
        // flushed, which spec 12 measures as anything from nothing to one chunk short of the lot,
        // and retries default to 3 for any scratch target, so that duplication would arrive on a
        // default nobody wrote.
        if (target is TableTarget && target.datasource == SCRATCH &&
            target.createTable == CreateTable.REQUIRED && retries > 0
        ) {
            err(
                "a scratch target with createTable REQUIRED cannot be retried (retries $retries, rule " +
                    "18). Its table has no attempt-suffixed name, so a retry appends onto the rows the " +
                    "failed attempt already flushed (spec 5.5). Use createTable AUTO, or retries: 0.",
            )
        }
        // Rule 12.
        if (target.datasource != SCRATCH && retries > 0 && !target.idempotent) {
            err(
                "retries $retries on the non-scratch target '${target.datasource}' requires idempotent: " +
                    "true (rule 12). The framework cannot make a partially written external target safe " +
                    "on its own, so the author states that a rerun converges (spec 5.3).",
            )
        }
    }

    /**
     * Rule 13's scratch-only half, rule 12 as spec 10 amends it for a materialize, and **rule 7 as
     * spec 10 amends it for a non-scratch materialize**: such a step may bind no variable at all.
     *
     * It runs `CREATE TABLE <output> AS <sql>` through `Handle.createUpdate`, which binds every
     * `:name` it parses, and Oracle rejects a bind variable in DDL outright with ORA-01027 - so the
     * unamended rule 7 blessed a step shape that could never run (review finding H3). The identical
     * step on scratch works, which is why it could only surface against a real Oracle.
     */
    private fun materialize(step: MaterializeStep, retries: Int, defined: Set<String>, err: (String) -> Unit) {
        sql(step.sql, "sql", step.datasource, defined, err)
        if (step.datasource == SCRATCH) return
        // Nothing is filtered: unlike a cacheCopy this text does go through JDBI at run time, so
        // whatever JDBI's parser calls a parameter is one here - including the all-digit name it
        // reads out of a DuckDB array slice, which would be rewritten to `?` and broken anyway. A
        // parse failure and a positional `?` are already reported above rather than twice.
        val parsed = names(step.sql, step.datasource)
        val bound = if (parsed == null || parsed.isPositional) emptyList()
        else parsed.parameterNames.distinct().sorted()
        if (bound.isNotEmpty()) {
            err(
                "sql binds ${bound.map { ":$it" }}, and a materialize on the non-scratch datasource " +
                    "'${step.datasource}' can bind nothing (rule 7). It runs as CREATE TABLE " +
                    "${step.output} AS <sql>, and Oracle rejects a bind variable in DDL with ORA-01027 - " +
                    "so this step would fail on every run, not just some. Materialize the wider set here " +
                    "and filter it in a following step, where variables do bind (spec 6.3, 10 rule 7).",
            )
        }
        // Rule 12, refused outright rather than made conditional on an idempotent flag: a retry
        // after the table was created fails on table-already-exists every time, so the flag would be
        // a promise no author could keep. Spec 10 rule 12 records why drop-and-recreate was refused.
        if (retries > 0) {
            err(
                "retries $retries on the non-scratch datasource '${step.datasource}' is rejected for a " +
                    "materialize (rule 12). It runs as CREATE TABLE ${step.output} AS <sql>, so a retry " +
                    "after the table was created fails on table-already-exists every time - there is no " +
                    "idempotent: true that could make it converge. State retries: 0, or build the table " +
                    "with a sql step that can express its own recovery (spec 5.3, 5.4).",
            )
        }
        // Rule 13's scratch-only half. PARQUET is structurally impossible on any other step type,
        // and spec 5.6 puts the file in the scratch directory, so it is scratch-only here too.
        if (step.format == MaterializeFormat.PARQUET) {
            err(
                "format PARQUET writes a file into the scratch directory, so it is available only on the " +
                    "'$SCRATCH' datasource, not '${step.datasource}' (spec 5.6, rule 13).",
            )
        }
    }

    /**
     * Rule 7 over each export query, and rule 8 over the names the step produces.
     *
     * A step's exports resolve for *later* steps and never for its own queries (spec 6.2), so the
     * running set below feeds rule 8 alone. Without it two vars of one name inside a single step
     * would collide silently and the redefinition check would never see the first of them.
     */
    private fun export(step: ExportStep, defined: Set<String>, err: (String) -> Unit) {
        val running = defined.toMutableSet()
        step.vars.forEach { variable ->
            sql(variable.sql, "vars[${variable.name}].sql", step.datasource, defined, err)
            if (!running.add(variable.name)) {
                err(
                    "variable '${variable.name}' is defined more than once (rule 8). A variable may not " +
                        "be redefined once set (spec 6.2). Defined at this point: ${defined.sorted()}.",
                )
            }
        }
    }

    /**
     * Rule 19. `CopyOutSpec.sql` is a plain string with no binding channel (spec 3.6, 7.3), so a
     * `:name` here cannot be bound even where the variable is defined - and interpolating one would
     * be the injection path every other statement in this engine avoids by binding.
     *
     * An all-digit "name" is not one. JDBI's lexer reads a colon followed by digits as a parameter,
     * so DuckDB's `site_code[1:3]` slice and `{'k':1}` struct literal arrive as `:3` and `:1`. This
     * text never passes through JDBI - the cache executes it verbatim - so rejecting them would
     * refuse SQL the cache runs perfectly well.
     */
    private fun cacheCopy(step: CacheCopyStep, err: (String) -> Unit) {
        val parsed = names(step.sql, datasource = null) ?: return
        if (parsed.isPositional) {
            err("sql uses positional '?' parameters, and a cacheCopy binds nothing at all (rule 19).")
            return
        }
        val bound = parsed.parameterNames.filterNot { name -> name.all(Char::isDigit) }.distinct().sorted()
        if (bound.isNotEmpty()) {
            err(
                "sql binds ${bound.map { ":$it" }}, and a cacheCopy takes no variables at all (rule 19). " +
                    "It runs inside the cache's own DuckDB instance through CopyOutSpec.sql, a plain " +
                    "string with no binding channel (spec 3.6, 7.3), so this cannot be bound even if the " +
                    "variable is defined. Copy the wider subset and filter it in the following " +
                    "materialize step.",
            )
        }
    }

    /** Rule 6's positional half and rule 7, for one SQL text. */
    private fun sql(
        text: String,
        where: String,
        datasource: String?,
        defined: Set<String>,
        err: (String) -> Unit,
    ) {
        if (positional(text, where, datasource, err)) return
        val parsed = names(text, datasource) ?: return
        val undefined = parsed.parameterNames.filterNot { it in defined }.distinct().sorted()
        if (undefined.isNotEmpty()) {
            err(
                "$where binds ${undefined.map { ":$it" }}, which no built-in, literal var or earlier " +
                    "export has defined (rule 7). Variables resolve in step order, so an export step must " +
                    "come before its use (spec 6.2). Defined at this point: ${defined.sorted()}.",
            )
        }
    }

    /** Rule 6's positional half alone, for the one text rule 7 excepts. True when it fired. */
    private fun positional(text: String, where: String, datasource: String?, err: (String) -> Unit): Boolean {
        if (names(text, datasource)?.isPositional != true) return false
        err("$where uses positional '?' parameters. Variables bind by name, so write ':name' (spec 6.3).")
        return true
    }

    /**
     * The parsed parameters of one SQL text, or null when it does not parse at all. A parse failure
     * is rule 6's own: the loader reports it against the file it came from, and for a definition
     * built in code JDBI raises it at execution. There is nothing well formed here to judge.
     */
    private fun names(text: String, datasource: String?) =
        runCatching { parseNamedParameters(text, parserFor(datasource)).parameters }.getOrNull()
}
