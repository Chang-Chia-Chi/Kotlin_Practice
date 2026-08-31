package infra.snapshotcache.core

import infra.snapshotcache.api.BuildContext
import infra.snapshotcache.api.CacheEvents
import infra.snapshotcache.api.GenerationCheck
import infra.snapshotcache.api.GenerationSource
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.Hook
import infra.snapshotcache.api.HookRunner
import infra.snapshotcache.api.RefreshPhase
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.api.SnapshotCacheConfig
import infra.snapshotcache.spi.GenerationStore
import infra.snapshotcache.spi.OpenGeneration
import infra.snapshotcache.testkit.AccountingFixture
import infra.snapshotcache.testkit.InMemoryGenerationStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.extension.RegisterExtension
import java.lang.reflect.Proxy
import java.nio.file.Path
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/*
 * P4 shared test support (SDET-owned): the refresh state machine, the verify gate and the
 * failure taxonomy.
 *
 * The P2 fake issues close/isClosed-only connections; the verify gate needs query
 * answers. Per the P2 progress note that stubbing lives here, at the spi boundary:
 * QueryStubGenerationStore delegates every call to the recording fake (so the accounting
 * equations stay honest) and wraps open() so served connections answer queries.
 *
 * Queries are answered by QUEUE, then PATTERN, then shape heuristics over a small
 * dataset model - never by exact SQL string equality: the SQL text is engineer-FREE.
 */

/** One scripted query result: column labels (matched case-insensitively) and rows. */
class StubRows(val labels: List<String>, val rows: List<List<Any?>>) {
    companion object {
        fun single(value: Any?): StubRows = StubRows(listOf("value"), listOf(listOf(value)))
        fun empty(): StubRows = StubRows(listOf("value"), emptyList())
    }
}

/**
 * Answers SQL queries from a tiny dataset model instead of a database.
 *
 * Model: [tables] name -> row count; [duplicateKeys] table -> excess duplicated rows;
 * [nullCounts] column -> NULL count. A healthy model (positive counts, no duplicates,
 * no nulls) makes every built-in verify rule pass; failure tests flip one knob.
 *
 * Resolution order: [enqueue]d results, [onPattern] rules, then shape heuristics
 * (IS NULL check, duplicate-key check, DISTINCT count, per-table COUNT, catalog
 * table listing). Unanswerable SQL throws, naming the statement, so a mismatch with
 * the engineer's formulation surfaces as a readable failure. All SQL is recorded in
 * [executed].
 */
class QueryScript {
    val tables = linkedMapOf<String, Long>()
    val duplicateKeys = mutableMapOf<String, Long>()
    val nullCounts = mutableMapOf<String, Long>()
    val executed = CopyOnWriteArrayList<String>()

    private val queue = ArrayDeque<StubRows>()
    private val patterns = mutableListOf<Pair<Regex, (String) -> StubRows>>()

    /** The next query, whatever its text, is answered with [rows]; one-shot, FIFO. */
    fun enqueue(rows: StubRows) {
        synchronized(queue) { queue += rows }
    }

    /** Queries matching [pattern] (case-insensitive regex, substring match) are answered by [answer]. */
    fun onPattern(pattern: String, answer: (String) -> StubRows) {
        patterns += Regex(pattern, RegexOption.IGNORE_CASE) to answer
    }

    fun answer(sql: String): StubRows {
        executed += sql
        synchronized(queue) { queue.removeFirstOrNull() }?.let { return it }
        patterns.firstOrNull { it.first.containsMatchIn(sql) }?.let { return it.second(sql) }

        val q = sql.lowercase().replace(Regex("\\s+"), " ").trim()
        val table = tables.keys.firstOrNull { Regex("\\b" + Regex.escape(it.lowercase()) + "\\b").containsMatchIn(q) }
        val outerAggregate = q.startsWith("select count")

        return when {
            // required_non_null shape: count of NULLs in a named column.
            "is null" in q ->
                StubRows.single(nullCounts.entries.firstOrNull { it.key.lowercase() in q }?.value ?: 0L)

            // key_unique, grouping shape: either an outer count of duplicated keys, or the duplicate rows themselves.
            "having" in q || "group by" in q -> {
                val dup = table?.let { duplicateKeys[it] } ?: 0L
                when {
                    outerAggregate -> StubRows.single(dup)
                    dup == 0L -> StubRows.empty()
                    else -> StubRows(listOf("id", "cnt"), (1..dup).map { listOf<Any?>(it, 2L) })
                }
            }

            // key_unique, distinct shape: count(distinct id) alone or next to count(*).
            // Order of a two-count row does not matter: healthy means both equal, broken means they differ.
            "distinct" in q && table != null -> {
                val total = tables.getValue(table)
                val distinct = total - (duplicateKeys[table] ?: 0L)
                if (Regex("count\\s*\\(").findAll(q).count() >= 2) {
                    StubRows(listOf("total", "distinct_count"), listOf(listOf(total, distinct)))
                } else {
                    StubRows.single(distinct)
                }
            }

            // non_empty / row counts: plain COUNT over a known table.
            table != null && "count" in q -> StubRows.single(tables.getValue(table))

            // Catalog discovery: information_schema.tables, SHOW TABLES, duckdb_tables(), PRAGMA show_tables.
            "tables" in q || q.startsWith("show") || "pragma" in q -> tableListRows()

            q == "select 1" -> StubRows.single(1L)

            else -> throw IllegalStateException(
                "QueryScript has no answer for SQL: $sql (add an onPattern rule or extend the heuristics)",
            )
        }
    }

    fun tableListRows(): StubRows =
        StubRows(listOf("table_name", "name"), tables.keys.map { listOf<Any?>(it, it) })
}

/**
 * Delegating [GenerationStore]: every call goes through to the recording
 * [InMemoryGenerationStore] (accounting equations stay exact); only [open] wraps the
 * returned [OpenGeneration] so its connections answer queries via [script]. close and
 * isClosed still reach the tracked delegate connection, so the JVM-side leak detector
 * keeps seeing every connection.
 */
class QueryStubGenerationStore(
    private val delegate: InMemoryGenerationStore,
    private val script: QueryScript,
) : GenerationStore by delegate {

    override fun open(gen: Long): OpenGeneration {
        val real = delegate.open(gen)
        return object : OpenGeneration {
            override val generation: Long = real.generation
            override fun connection(): Connection = queryCapableConnection(real.connection(), script)
            override fun fileBytes(): Long = real.fileBytes()
        }
    }
}

private val proxyLoader = QueryScript::class.java.classLoader

/** Wraps a tracked close/isClosed-only connection with query capability driven by [script]. */
fun queryCapableConnection(delegate: Connection, script: QueryScript): Connection =
    Proxy.newProxyInstance(proxyLoader, arrayOf(Connection::class.java)) { proxy, method, args ->
        when (method.name) {
            "createStatement" -> statementProxy(proxy as Connection, script, preparedSql = null)
            "prepareStatement" -> statementProxy(proxy as Connection, script, preparedSql = args!![0] as String)
            "getMetaData" -> metaDataProxy(proxy as Connection, script)
            "close", "isClosed" -> method.invoke(delegate, *(args ?: emptyArray()))
            "isReadOnly" -> true
            "getAutoCommit" -> true
            "setReadOnly", "setAutoCommit", "clearWarnings", "setTransactionIsolation", "setSchema" -> null
            "getWarnings" -> null
            "toString" -> "QueryStub($delegate)"
            "hashCode" -> System.identityHashCode(proxy)
            "equals" -> proxy === args!![0]
            else -> throw UnsupportedOperationException("QueryStub Connection.${method.name} not stubbed")
        }
    } as Connection

private fun statementProxy(owner: Connection, script: QueryScript, preparedSql: String?): PreparedStatement {
    var lastResult: ResultSet? = null
    return Proxy.newProxyInstance(proxyLoader, arrayOf(PreparedStatement::class.java)) { proxy, method, args ->
        fun sqlOf(): String =
            if (args != null && args.isNotEmpty() && args[0] is String) args[0] as String
            else checkNotNull(preparedSql) { "Statement.${method.name} without SQL" }
        when {
            method.name == "executeQuery" -> resultSetProxy(script.answer(sqlOf()))
            method.name == "execute" -> {
                lastResult = resultSetProxy(script.answer(sqlOf()))
                true
            }
            method.name == "getResultSet" -> lastResult
            method.name == "getUpdateCount" -> -1
            method.name == "getMoreResults" -> false
            method.name == "getConnection" -> owner
            method.name == "close" -> null
            method.name == "isClosed" -> false
            method.name.startsWith("set") -> null
            method.name == "toString" -> "QueryStubStatement"
            method.name == "hashCode" -> System.identityHashCode(proxy)
            method.name == "equals" -> proxy === args!![0]
            else -> throw UnsupportedOperationException("QueryStub Statement.${method.name} not stubbed")
        }
    } as PreparedStatement
}

private fun resultSetProxy(data: StubRows): ResultSet {
    var cursor = -1
    var lastWasNull = false

    fun columnIndex(key: Any?): Int = when (key) {
        is Int -> key - 1
        is String -> data.labels.indexOfFirst { it.equals(key, ignoreCase = true) }
            .also { require(it >= 0) { "no column '$key' in ${data.labels}" } }
        else -> throw IllegalArgumentException("unsupported column key $key")
    }

    fun value(args: Array<Any?>): Any? {
        check(cursor in data.rows.indices) { "ResultSet cursor not on a row (call next())" }
        return data.rows[cursor][columnIndex(args[0])].also { lastWasNull = it == null }
    }

    return Proxy.newProxyInstance(proxyLoader, arrayOf(ResultSet::class.java)) { proxy, method, args ->
        when (method.name) {
            "next" -> {
                cursor++
                cursor < data.rows.size
            }
            "getLong" -> (value(args!!) as? Number)?.toLong() ?: 0L
            "getInt" -> (value(args!!) as? Number)?.toInt() ?: 0
            "getShort" -> (value(args!!) as? Number)?.toShort() ?: 0.toShort()
            "getDouble" -> (value(args!!) as? Number)?.toDouble() ?: 0.0
            "getFloat" -> (value(args!!) as? Number)?.toFloat() ?: 0.0f
            "getString" -> value(args!!)?.toString()
            "getObject" -> value(args!!)
            "getBoolean" -> {
                val v = value(args!!)
                (v as? Boolean) ?: (((v as? Number)?.toInt() ?: 0) != 0)
            }
            "wasNull" -> lastWasNull
            "findColumn" -> columnIndex(args!![0]) + 1
            "getMetaData" -> resultSetMetaDataProxy(data)
            "close" -> null
            "isClosed" -> false
            "toString" -> "QueryStubResultSet"
            "hashCode" -> System.identityHashCode(proxy)
            "equals" -> proxy === args!![0]
            else -> throw UnsupportedOperationException("QueryStub ResultSet.${method.name} not stubbed")
        }
    } as ResultSet
}

private fun resultSetMetaDataProxy(data: StubRows): ResultSetMetaData =
    Proxy.newProxyInstance(proxyLoader, arrayOf(ResultSetMetaData::class.java)) { proxy, method, args ->
        when (method.name) {
            "getColumnCount" -> data.labels.size
            "getColumnLabel", "getColumnName" -> data.labels[(args!![0] as Int) - 1]
            "toString" -> "QueryStubResultSetMetaData"
            "hashCode" -> System.identityHashCode(proxy)
            "equals" -> proxy === args!![0]
            else -> throw UnsupportedOperationException("QueryStub ResultSetMetaData.${method.name} not stubbed")
        }
    } as ResultSetMetaData

private fun metaDataProxy(owner: Connection, script: QueryScript): DatabaseMetaData =
    Proxy.newProxyInstance(proxyLoader, arrayOf(DatabaseMetaData::class.java)) { proxy, method, args ->
        when {
            method.name == "getTables" -> resultSetProxy(script.tableListRows())
            method.name == "getConnection" -> owner
            method.name == "getDatabaseProductName" || method.name == "getDriverName" -> "QueryStubDB"
            method.name.startsWith("supports") -> false
            method.name == "toString" -> "QueryStubDatabaseMetaData"
            method.name == "hashCode" -> System.identityHashCode(proxy)
            method.name == "equals" -> proxy === args!![0]
            else -> throw UnsupportedOperationException("QueryStub DatabaseMetaData.${method.name} not stubbed")
        }
    } as DatabaseMetaData

// ---------------------------------------------------------------------- source / recorders

/** [GenerationSource] whose behavior is swapped per round; records every [BuildContext]. */
class ScriptedSource : GenerationSource {
    val contexts = CopyOnWriteArrayList<BuildContext>()

    @Volatile
    var behavior: (BuildContext) -> Unit = {}

    override fun refresh(ctx: BuildContext) {
        contexts += ctx
        behavior(ctx)
    }
}

/** Records the P4-relevant [CacheEvents]. */
class RecordingRefreshEvents : CacheEvents {
    val finished = CopyOnWriteArrayList<Pair<RefreshResult, Long?>>()
    val phases = CopyOnWriteArrayList<Pair<RefreshPhase, Duration>>()
    val verifyFailures = CopyOnWriteArrayList<Pair<String, String>>()
    val escalations = CopyOnWriteArrayList<Int>()
    val reclaimedGens = CopyOnWriteArrayList<Long>()

    override fun refreshFinished(group: GroupId, result: RefreshResult, generation: Long?) {
        finished += result to generation
    }

    override fun refreshPhase(group: GroupId, phase: RefreshPhase, elapsed: Duration) {
        phases += phase to elapsed
    }

    override fun verifyFailed(group: GroupId, rule: String, detail: String) {
        verifyFailures += rule to detail
    }

    override fun verifyFailureEscalated(group: GroupId, consecutiveFailures: Int) {
        escalations += consecutiveFailures
    }

    override fun generationReclaimed(group: GroupId, generation: Long) {
        reclaimedGens += generation
    }
}

/** Records every hook firing, across the registry and the cycle, in order. */
class RecordingHooks : HookRunner {
    val fired = CopyOnWriteArrayList<Hook>()

    override fun at(hook: Hook) {
        fired += hook
    }
}

/** Deterministic advancing clock; no real waiting. */
class MutableTestClock(@Volatile private var now: Instant) : Clock() {
    override fun getZone(): ZoneId = ZoneOffset.UTC
    override fun withZone(zone: ZoneId): Clock = this
    override fun instant(): Instant = now

    fun advance(by: Duration) {
        now = now.plus(by)
    }
}

// ---------------------------------------------------------------------- shared suite base

/**
 * Shared P4 fixture: healthy two-table dataset, recording fake store wrapped with the
 * query stub, recording events and hooks, [AccountingFixture] registered with its
 * suppliers wired to the registry, so every subclass gets the accounting equations for free
 * (P2 checklist item).
 */
internal abstract class RefreshCycleTestBase {

    protected val t0: Instant = Instant.parse("2026-01-01T00:00:00Z")
    protected val group = GroupId("orders")
    protected val store = InMemoryGenerationStore()
    protected val script = QueryScript().apply {
        tables["t_a"] = 10L
        tables["t_b"] = 20L
    }
    protected val stubStore = QueryStubGenerationStore(store, script)
    protected val clock = MutableTestClock(t0)
    protected val hooks = RecordingHooks()
    protected val registry = GenerationRegistry(3, Duration.ofMinutes(5), clock, hooks)
    protected val events = RecordingRefreshEvents()
    protected val source = ScriptedSource()
    protected val config = SnapshotCacheConfig(
        storagePath = Path.of("unused-storage"),
        tempDirectory = Path.of("unused-temp"),
    )

    /** Tests that build their own registry (e.g. K=1) point the fixture at it. */
    protected var trackedRegistry: GenerationRegistry? = null

    @RegisterExtension
    @JvmField
    val accounting = AccountingFixture(store).apply {
        currentGeneration = { (trackedRegistry ?: registry).current() }
        refCounts = { (trackedRegistry ?: registry).liveGenerations().associate { it.generation to it.refCount } }
    }

    /** The pinned P4 construction surface (lead decision; code against it exactly). */
    protected fun cycle(
        cfg: SnapshotCacheConfig = config,
        reg: GenerationRegistry = registry,
        checks: List<GenerationCheck> = emptyList(),
    ): RefreshCycle = RefreshCycle(
        group = group,
        registry = reg,
        store = stubStore,
        source = source,
        config = cfg,
        events = events,
        checks = checks,
        clock = clock,
        hooks = hooks,
    )

    protected fun runSuccess(c: RefreshCycle): Long {
        val out = c.runOnce()
        assertThat(out.result)
            .describedAs("expected SUCCESS, got %s (detail=%s)", out.result, out.detail)
            .isEqualTo(RefreshResult.SUCCESS)
        return checkNotNull(out.generation) { "SUCCESS outcome must carry its generation" }
    }

    /** Bounded latch wait: a precondition check, never sequencing by sleep. */
    protected fun await(latch: CountDownLatch) {
        assertThat(latch.await(10, TimeUnit.SECONDS)).describedAs("latch must open").isTrue()
    }

    /** Bounded join: a bound on broken implementations, never sequencing. */
    protected fun joinOrFail(thread: Thread) {
        thread.join(10_000)
        assertThat(thread.isAlive).describedAs("thread %s must have finished", thread.name).isFalse()
    }
}
