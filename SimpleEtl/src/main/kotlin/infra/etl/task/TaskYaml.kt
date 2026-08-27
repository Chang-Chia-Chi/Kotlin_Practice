package infra.etl.task

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import infra.etl.duckdb.CreateTable

/**
 * The YAML schema of spec 3, one class per document node. It is deliberately a separate tree from
 * [TaskDefinition] rather than an annotated view of it, for three reasons:
 *
 * - **Validation rule 17 comes free.** Each step type is its own class with its own field set, so
 *   `statements:` on a `pipe` step is an unknown field, which is validation rule 1's error. A
 *   single step class with every field nullable could not tell the two rules apart.
 * - **Defaults that depend on another field cannot be Kotlin defaults.** `createTable` is AUTO
 *   inside scratch and REQUIRED outside it, and `retries` is 3 inside and 0 outside (spec 4.4,
 *   5.3). Both are resolved in `TaskFileLoader`'s conversion, so the YAML form carries null for
 *   "not stated".
 * - **A DTO may hold a value the domain type forbids.** [LiteralVar] throws on a null value
 *   (validation rule 8), and [TaskFileLoader] must report that as an error rather than as an
 *   exception out of the loader - so the null has to survive parsing to be reported.
 *
 * Everything here is `internal`: spec 11.2's public surface is `TaskFileLoader`,
 * `ValidationReport` and `ValidationError`, not the file format's Kotlin shadow.
 *
 * **Nothing in this file expands anything.** Spec 10 keeps task files off the Quarkus config path
 * precisely because config performs property expansion, which would corrupt SQL containing
 * `${...}`. Measured on jackson-dataformat-yaml 2.18.2 (P6 scratchpad `Probe.kt`): a `${env.FOO}`
 * inside a folded or literal block scalar arrives byte for byte, and a `|` block keeps its
 * newlines while a `>` block folds them, which is YAML's own rule and not this loader's.
 */
internal data class TaskYaml(
    val name: String,
    val description: String? = null,
    val enabled: Boolean = true,
    val schedule: ScheduleYaml? = null,
    val logging: Boolean = true,
    val chunkSize: Int = 5000,
    val scratch: ScratchYaml? = null,
    val onSuccess: String? = null,
    val onFailure: String? = null,
    val vars: List<LiteralVarYaml> = emptyList(),
    val phases: List<PhaseYaml> = emptyList(),
)

internal data class ScheduleYaml(val cron: String? = null)

internal data class ScratchYaml(val memoryLimitMb: Int? = null)

/** A task-level literal variable. `value` is nullable so validation rule 8 can report the null. */
internal data class LiteralVarYaml(val name: String, val value: Any? = null)

internal data class PhaseYaml(val name: String, val steps: List<StepYaml> = emptyList())

/**
 * Measured on jackson-databind 2.18.2: `As.PROPERTY` consumes `type` and does not then report it
 * as an unknown field, an unknown value gives `InvalidTypeIdException` listing the known ids, and
 * a missing one says so - all three carrying a line number. `cacheCopy` joined the four in P9,
 * which is when spec 3 gained 3.6 and the step type gained an executor; until then it was
 * deliberately absent and arrived as an unknown type id.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(value = PipeYaml::class, name = "pipe"),
    JsonSubTypes.Type(value = MaterializeYaml::class, name = "materialize"),
    JsonSubTypes.Type(value = SqlYaml::class, name = "sql"),
    JsonSubTypes.Type(value = ExportYaml::class, name = "export"),
    JsonSubTypes.Type(value = CacheCopyYaml::class, name = "cacheCopy"),
)
internal sealed interface StepYaml {
    val name: String

    /** Null means "not stated", so the datasource-dependent default of spec 5.3 can be applied. */
    val retries: Int?
}

internal data class PipeYaml(
    override val name: String,
    val source: PipeSourceYaml,
    val target: PipeTargetYaml,
    val transform: TransformYaml? = null,
    val chunkSize: Int? = null,
    override val retries: Int? = null,
) : StepYaml

internal data class PipeSourceYaml(val datasource: String, val sql: String)

/**
 * Both target forms in one class, because YAML cannot choose a subtype from the presence of a
 * field. Exactly one of [table] and [sql] is present, which is validation rule 10; the sealed
 * [PipeTarget] pair it converts into makes the same rule unrepresentable downstream.
 */
internal data class PipeTargetYaml(
    val datasource: String,
    val table: String? = null,
    val createTable: CreateTable? = null,
    val sql: String? = null,
    val idempotent: Boolean = false,
)

/**
 * [addColumns] is required when the transform adds columns and the target uses `createTable: AUTO`
 * (spec 9.1, validation rule 14), because source metadata cannot describe a column the transform
 * invents.
 */
internal data class TransformYaml(val bean: String, val addColumns: List<AddColumnYaml> = emptyList())

/**
 * [type] is the **DuckDB type keyword** of spec 3.2's example - `VARCHAR`, `BIGINT`, `TIMESTAMP` -
 * matched case-insensitively against [CanonicalType.duckDbType], not the canonical enum constant.
 * AUTO is scratch-only (validation rule 14), so the only target an added column can reach is
 * DuckDB and the author writes the type they would have written in DDL.
 *
 * [nullable] defaults to true because that is the safe direction (spec 1.3): a nullable column is
 * created as a type with a null-accepting append path (spec 4.6). [precision] and [scale] exist
 * for DECIMAL only, where AUTO emits `DECIMAL(p,s)` and rejects an unusable pair at writer open.
 */
internal data class AddColumnYaml(
    val name: String,
    val type: String,
    val nullable: Boolean = true,
    val precision: Int = 0,
    val scale: Int = 0,
)

internal data class MaterializeYaml(
    override val name: String,
    val datasource: String,
    val output: String,
    val sql: String,
    val format: MaterializeFormat = MaterializeFormat.TABLE,
    override val retries: Int? = null,
) : StepYaml

internal data class SqlYaml(
    override val name: String,
    val datasource: String,
    val statements: List<String> = emptyList(),
    override val retries: Int? = null,
) : StepYaml

internal data class ExportYaml(
    override val name: String,
    val datasource: String,
    val vars: List<ExportVarYaml> = emptyList(),
    override val retries: Int? = null,
) : StepYaml

internal data class ExportVarYaml(val name: String, val sql: String)

/**
 * Spec 3.6's `cacheCopy`, added in P9. Four fields and no more: [cache] is a host-bound name and
 * not a datasource, [sql] runs inside the cache's own DuckDB instance, and [output] is an ordinary
 * scratch dataset.
 *
 * [retries] is null for "not stated" like every other step type, but resolves to **0** rather than
 * to the 3 a scratch-targeted step normally gets - and a stated value above 0 is validation rule
 * 20's rejection. Had this inherited the 3, every file that omits `retries` would fail that rule
 * on a value its author never wrote. The asymmetry with `CacheCopyStep.retries`, which is still 3,
 * is deliberate and is recorded in spec 10's rule 20.
 */
internal data class CacheCopyYaml(
    override val name: String,
    val cache: String,
    val sql: String,
    val output: String,
    override val retries: Int? = null,
) : StepYaml
