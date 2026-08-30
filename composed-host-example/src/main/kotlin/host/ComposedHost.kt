package host

import infra.etl.task.CacheBinding
import infra.etl.task.CronScheduler
import infra.etl.task.EtlWiring
import infra.etl.task.TaskAdmin
import infra.etl.task.TaskEvent
import infra.etl.task.TaskRunListener
import infra.etl.task.TriggerResult
import infra.etl.task.WiringResult
import infra.snapshotcache.api.AcquireUnavailableReason
import infra.snapshotcache.api.BuildContext
import infra.snapshotcache.api.CacheEvents
import infra.snapshotcache.api.GenerationSource
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.LeaseInfo
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.api.SnapshotCacheConfig
import infra.snapshotcache.api.VerifyConfig
import infra.snapshotcache.bootstrap.ManagedSnapshotCache
import infra.snapshotcache.bootstrap.openSnapshotCache
import org.jdbi.v3.core.Jdbi
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch

const val GROUP = "wip"
const val CACHE_NAME = "wip_cache"

/** The host obligation spec 8.6 row 5 names: a scheduler that really parses, and hands off. */
class ManualCron : CronScheduler {
    private val registered = ConcurrentHashMap<String, () -> Unit>()

    override fun schedule(taskName: String, cron: String, run: () -> Unit): AutoCloseable {
        // 8.6: "make CronScheduler.schedule throw on an unparseable cron".
        require(cron.trim().split(Regex("\\s+")).size in 5..6) { "unparseable cron: " + cron }
        registered[taskName] = run
        return AutoCloseable { registered.remove(taskName) }
    }

    fun names(): Set<String> = registered.keys.toSet()
    fun fire(name: String) = requireNotNull(registered[name]) { "nothing registered for " + name }.invoke()
}

/** Everything the cache told us, kept so a test can read `LeaseInfo.owner` verbatim. */
class RecordingCacheEvents : CacheEvents {
    val leases = CopyOnWriteArrayList<LeaseInfo>()
    val refreshes = CopyOnWriteArrayList<Pair<RefreshResult, Long?>>()
    val unavailable = CopyOnWriteArrayList<AcquireUnavailableReason>()
    val reclaimed = CopyOnWriteArrayList<Long>()
    val verifyFailures = CopyOnWriteArrayList<Pair<String, String>>()

    override fun leaseReleased(group: GroupId, lease: LeaseInfo, heldFor: Duration) { leases += lease }
    override fun leaseExpired(group: GroupId, lease: LeaseInfo, heldFor: Duration) { leases += lease }
    override fun refreshFinished(group: GroupId, result: RefreshResult, generation: Long?) { refreshes += result to generation }
    override fun acquireUnavailable(group: GroupId, reason: AcquireUnavailableReason) { unavailable += reason }
    override fun generationReclaimed(group: GroupId, generation: Long) { reclaimed += generation }
    override fun verifyFailed(group: GroupId, rule: String, detail: String) { verifyFailures += rule to detail }
}

/** Records the thread each step actually ran on - the owner/threading measurement. */
class ThreadRecordingListener : TaskRunListener {
    val stepThreads = CopyOnWriteArrayList<Pair<String, String>>()
    val stepErrors = CopyOnWriteArrayList<TaskEvent.StepError>()
    @Volatile var ended = CountDownLatch(1)

    override fun on(event: TaskEvent) {
        when (event) {
            is TaskEvent.StepStart -> stepThreads += event.step.step to Thread.currentThread().name
            is TaskEvent.StepError -> stepErrors += event
            is TaskEvent.TaskEnd -> ended.countDown()
            else -> Unit
        }
    }
}

/**
 * **M3 / scenario 11: how a host tells "busy" from "dying".**
 *
 * `TriggerResult.AlreadyRunning` is deliberately reused for both (SimpleEtl spec 11.2, and
 * `TaskRunner`'s KDoc): after `WiringResult.Wired.close()` the three words the case is *named* for
 * are untrue, but the three it is *defined* by - rejected, not queued, will not run later - are
 * exactly true, so no fifth sealed case was added. That deferral rests on a claim: **the host does
 * not need the framework to tell the two apart, because the host is the one that called `close()`.**
 *
 * This class is that claim, in nine lines of host code. The flag is raised *before* `close()`,
 * never after, which is the only ordering that cannot lie: in between, a probe that flipped
 * afterwards would answer "busy, retry later" to a caller that will never be served.
 *
 * A real host also serves [shuttingDown] from its readiness endpoint, so the load balancer stops
 * sending work before the 503s start - which is the whole reason the distinction is worth making.
 */
class ReadinessProbe {

    @Volatile
    var shuttingDown = false
        private set

    /** Called before `WiringResult.Wired.close()`, and only by the host that is closing it. */
    fun beginShutdown() {
        shuttingDown = true
    }

    /** What the host's `AdminResource` answers. The framework decides [result]; the rest is ours. */
    fun classify(result: TriggerResult): String = when {
        result !is TriggerResult.AlreadyRunning -> "202 accepted"
        shuttingDown -> "503 gone - this instance is shutting down, retry elsewhere"
        else -> "409 busy - that task is already running, retry later"
    }
}

/**
 * A real GenerationSource: writes real rows into the candidate through BuildContext.target.
 *
 * `id` is not decoration. `VerifyConfig.keyUnique` defaults to true and VerifyGate runs
 * `SELECT COUNT(id), COUNT(DISTINCT id)` against EVERY base table in the candidate, so a
 * table without an `id` column fails the gate and the generation is never published.
 */
class LotSource(private val rowsPerRound: Int = 500, private val withIdColumn: Boolean = true) : GenerationSource {
    @Volatile var round = 0
    @Volatile var beforeWrite: (() -> Unit)? = null

    override fun refresh(ctx: BuildContext) {
        round++
        beforeWrite?.invoke()
        ctx.target.createStatement().use { st ->
            if (withIdColumn) {
                st.execute("CREATE TABLE lot (id BIGINT, lot_id VARCHAR, qty DECIMAL(18,3), site VARCHAR)")
                st.execute(
                    "INSERT INTO lot SELECT i, 'L' || i, i * 1.5, CASE WHEN i % 2 = 0 THEN 'F12' ELSE 'F11' END " +
                        "FROM range(1, " + (rowsPerRound + 1) + ") t(i)",
                )
            } else {
                st.execute("CREATE TABLE lot (lot_id VARCHAR, qty DECIMAL(18,3), site VARCHAR)")
                st.execute(
                    "INSERT INTO lot SELECT 'L' || i, i * 1.5, 'F12' FROM range(1, " + (rowsPerRound + 1) + ") t(i)",
                )
            }
        }
    }
}

/** The composition root: one snapshot cache group feeding one SimpleEtl task directory. */
class ComposedHost(
    val root: Path,
    val source: LotSource = LotSource(),
    val cacheEvents: RecordingCacheEvents = RecordingCacheEvents(),
    val listener: ThreadRecordingListener = ThreadRecordingListener(),
    waitBudget: Duration = Duration.ofMillis(200),
    leaseDrainTimeout: Duration = Duration.ofSeconds(5),
) : AutoCloseable {

    val group = GroupId(GROUP)
    val cron = ManualCron()
    val taskDirectory: Path = root.resolve("tasks").also { Files.createDirectories(it) }

    val managed: ManagedSnapshotCache = openSnapshotCache(
        config = SnapshotCacheConfig(
            storagePath = root.resolve("cache").also { Files.createDirectories(it) },
            tempDirectory = root.resolve("tmp").also { Files.createDirectories(it) },
            defaultWaitBudget = waitBudget,
            leaseDrainTimeout = leaseDrainTimeout,
            verify = VerifyConfig(),
        ),
        sources = mapOf(group to source),
        events = cacheEvents,
    )

    /** The host's second, consumer-side DuckDB - spec 5.4 says consumerMemoryLimit is the host's. */
    val reportConnection: Connection = DriverManager.getConnection("jdbc:duckdb:" + root.resolve("report.db")).also {
        it.createStatement().use { st ->
            st.execute("SET memory_limit='1GB'")
            st.execute("CREATE TABLE wip_summary (site VARCHAR, lots BIGINT, total_qty DECIMAL(38,3))")
        }
    }

    private val wiring = EtlWiring(
        scratchDirectory = root.resolve("scratch").also { Files.createDirectories(it) },
        cron = cron,
        datasources = mapOf("report" to Jdbi.create(reportConnection)),
        caches = mapOf(CACHE_NAME to CacheBinding(managed.cache, group)),
        listener = listener,
    )

    lateinit var admin: TaskAdmin
        private set

    private var wired: WiringResult.Wired? = null

    /** Scenario 11's host state. Public: a readiness endpoint is the host's job, not the framework's. */
    val readiness = ReadinessProbe()

    fun start(): WiringResult = wiring.start(taskDirectory).also {
        if (it is WiringResult.Wired) {
            admin = it.admin
            wired = it
        }
    }

    /**
     * Spec 10.2 steps 2-3 - stop scheduling, stop starting new work - with the readiness flag
     * raised **first**. Raised after `close()` instead, there is a window in which a trigger is
     * refused by an already-cancelled runner while the probe still says "busy, retry later", which
     * is the one wrong answer of the four.
     *
     * Not folded into [close]: the existing scenarios assert on a host whose ETL side is still
     * live at teardown, and scenario 11 is the one that needs the two halves separable.
     */
    fun shutdownEtl() {
        readiness.beginShutdown()
        wired?.close()
    }

    fun reportRows(): List<Triple<String, Long, String>> =
        reportConnection.createStatement().use { st ->
            st.executeQuery("SELECT site, lots, total_qty FROM wip_summary ORDER BY site").use { rs ->
                buildList { while (rs.next()) add(Triple(rs.getString(1), rs.getLong(2), rs.getString(3))) }
            }
        }

    override fun close() {
        runCatching { managed.close() }
        runCatching { reportConnection.close() }
    }
}

/** Spec 2.4 shape D, as a task file. */
fun writeShapeD(
    directory: Path,
    name: String = "wip-summary",
    cron: String? = "0 */10 * * * ?",
    copySql: String = "select id, lot_id, qty, site from lot",
) {
    val schedule = if (cron == null) "" else "schedule:\n  cron: \"" + cron + "\"\n"
    val yaml = "name: " + name + "\n" +
        schedule +
        "phases:\n" +
        "  - name: load\n" +
        "    steps:\n" +
        "      - name: copy-wip\n" +
        "        type: cacheCopy\n" +
        "        cache: " + CACHE_NAME + "\n" +
        "        sql: " + copySql + "\n" +
        "        output: wip_cache\n" +
        "      - name: summarise\n" +
        "        type: materialize\n" +
        "        datasource: scratch\n" +
        "        output: summary\n" +
        "        sql: select site, count(*) as lots, sum(qty) as total_qty from wip_cache group by site\n" +
        "      - name: publish\n" +
        "        type: pipe\n" +
        "        source:\n" +
        "          datasource: scratch\n" +
        "          sql: select site, lots, total_qty from summary\n" +
        "        target:\n" +
        "          datasource: report\n" +
        "          table: wip_summary\n"
    Files.writeString(directory.resolve(name + ".yaml"), yaml)
}
