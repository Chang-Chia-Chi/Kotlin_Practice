package etlhost

import infra.etl.task.EtlWiring
import infra.etl.task.TaskAdmin
import infra.etl.task.TriggerResult
import infra.etl.task.ValidationReport
import infra.etl.task.WiringResult
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.bootstrap.ManagedSnapshotCache
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.event.Observes
import jakarta.inject.Singleton
import java.nio.file.Path
import org.jboss.logging.Logger

/**
 * The host's own lifecycle: snapshotcache spec 10.1 steps 3-5 on the way up, spec 10.2 steps 2-3
 * plus the readiness flag on the way down, and the two things nothing else can hold in between -
 * the live [WiringResult.Wired] and the readiness state.
 *
 * ### Startup, in the order spec 10.1 fixes
 *
 * 1-2. The stale-file wipe and the serving instance's limits happen inside `openSnapshotCache`,
 *      which the container has already run by the time this observer fires.
 * 3.   `ready = false`, which is this class's initial state and not an assignment.
 * 4.   **The first refresh runs immediately, not on the schedule.** The tick's first firing is one
 *      whole interval away, and spec 10.1's reason for not waiting is that consumers waiting is far
 *      better than consumers reading an empty table.
 * 5.   `ready = true` once a generation has published.
 *
 * The ETL side is wired between 4 and 5, and that ordering is deliberate in both directions. After
 * the first refresh, because a task fired before the cache's first publish spends the whole
 * `defaultWaitBudget` in `cacheCopy` and then fails with `TIMEOUT` - SimpleEtl spec 8.6's note on
 * 3.6 - and every task whose cron lands inside the startup window is that task. Before readiness,
 * because readiness is a promise that this instance can serve, and an instance whose admin endpoint
 * has no task list cannot.
 *
 * A failed first refresh is not a failed startup. The app boots, the admin endpoint answers, and
 * readiness stays false until a later tick publishes - which is exactly what a readiness probe is
 * for, and is why this observer does not throw.
 *
 * ### Shutdown, and the one ordering that cannot lie
 *
 * `shuttingDown = true` **before** `wired.close()`, then `managed.close()`. `composed-host-example`
 * M3 is where that order was settled: `TriggerResult.AlreadyRunning` answers both "busy" and "this
 * instance is gone", and the host can tell them apart only because the host is the one that called
 * `close()`. Raise the flag afterwards and there is a window in which a cancelled runner refuses a
 * trigger while the probe still says "409, retry later" - the one wrong answer of the four.
 *
 * `wired.close()` before `managed.close()` is `composed-host-example`'s recipe step 3 and the
 * cache's spec 10.2 steps 2 and 3: stop scheduling and stop starting new work, *then* drain the
 * leases. Reversed, a `cacheCopy` step can take a lease on a cache that is already draining and be
 * answered `ShuttingDownException` mid-run.
 */
@Singleton
class EtlHost(
    private val config: HostConfig,
    private val managed: ManagedSnapshotCache,
    private val wiring: EtlWiring,
    private val cacheMetrics: CacheMetrics,
) {

    val groups: List<GroupId> = config.groupSql.keys.map(::GroupId)

    /** Raised before `close()`, and only by the shutdown sequence below. */
    // `final` because the all-open compiler plugin opens every @Singleton class for CDI, and
    // Kotlin forbids a private setter on an open property. Nothing subclasses this.
    @Volatile
    final var shuttingDown: Boolean = false
        private set

    /** Raised once [onStart] has finished, so readiness cannot be true mid-boot. */
    @Volatile
    private var started: Boolean = false

    /**
     * **The one readiness answer, computed rather than stored, and the only source there is.**
     *
     * The JAX-RS `/health/ready` resource renders this string; [CacheReadinessCheck] serves the
     * same value at SmallRye's conventional `/q/health/ready`. Two probes cannot disagree because
     * there is nothing for them to disagree about.
     *
     * It is computed on every read because the alternative was a flag written once, at the end of
     * startup, and never again - which made this class's own KDoc and README.md ("readiness stays
     * false until a later tick publishes") describe behaviour no code had. A host whose startup
     * refresh failed stayed not-ready **forever**, through every later tick that published
     * perfectly well, and the only recovery was a restart. Asking the cache costs one in-memory
     * read per group per probe.
     *
     * Three states, and the middle one is `composed-host-example`'s M3: shutting down is reported
     * before `close()` runs, so a load balancer stops sending work before the 409s and 503s start.
     */
    val readinessState: String
        get() = when {
            shuttingDown -> "shutting-down"
            started && groups.isNotEmpty() && groups.all { managed.cache.currentInfo(it) != null } -> READY
            else -> "awaiting-first-generation"
        }

    private lateinit var wired: WiringResult.Wired

    val admin: TaskAdmin get() = wired.admin

    fun onStart(@Observes event: StartupEvent) {
        Producers.mkdirs(config.taskDirectory)
        cacheMetrics.bind(groups, managed.cache, managed.admin)

        groups.forEach { group ->
            val outcome = managed.admin.triggerRefresh(group)
            // WARN, not INFO. This is the line that carries the verify gate's failing rule and its
            // detail - "Referenced column \"id\" not found" - and it is the whole answer to "why is
            // this pod not ready". At INFO it sits below the level a production deployment keeps,
            // and the first ERROR anything logs is spec 8.5's escalation at the *third* consecutive
            // failure: two refresh intervals, twenty minutes at the shipped default, after the fact.
            if (outcome.result == RefreshResult.SUCCESS) {
                log.infov("startup refresh of group {0}: SUCCESS", group)
            } else {
                log.warnv(
                    "startup refresh of group {0}: {1} {2}",
                    group, outcome.result, outcome.detail ?: "",
                )
            }
        }

        // Fail fast and loudly. A host whose task files do not validate has nothing to serve, and
        // booting anyway would leave `GET /admin/etl/tasks` answering an empty list as though that
        // were the deployment's intent.
        when (val result = wiring.start(config.taskDirectory)) {
            is WiringResult.Wired -> wired = result
            is WiringResult.Invalid -> throw IllegalStateException(
                "task directory ${config.taskDirectory} did not validate: " + render(result.report),
            )
        }

        // Spec 10.1 step 5 asks whether a generation has PUBLISHED, and that is the question asked
        // here - of the cache, not of the refresh round this method happened to start. The two are
        // not the same answer: a round that comes back SKIPPED_OVERLAP published nothing itself
        // while another round published everything, and reading the result instead of the cache
        // reported "not ready" over a live, serving generation. `delayed` on CacheTick removed the
        // overlap that produced it at boot; this removes the class of wrong answer.
        started = true
        log.infov("startup complete, readiness = {0}", readinessState)
    }

    fun onStop(@Observes event: ShutdownEvent) {
        shuttingDown = true
        if (this::wired.isInitialized) runCatching { wired.close() }.onFailure { log.warn("wired.close failed", it) }
        runCatching { managed.close() }.onFailure { log.warn("cache close failed", it) }
    }

    /**
     * `POST /admin/etl/reload`.
     *
     * Nothing else happens here, and the emptiness is the point. Until 2026-08-30 spec 8.6 made
     * re-seeding the metrics the host's job at exactly this call - a pairing every host
     * re-implements for a moment `TaskAdmin` already owns. `EtlWiring` now takes `onTasksLoaded`
     * and the framework invokes it at both load moments, so a host that writes this method as a
     * one-line delegate has not forgotten anything.
     */
    fun reload(directory: Path = config.taskDirectory): ValidationReport? = admin.reload(directory)

    /**
     * SimpleEtl spec 8.2's status codes, given only the sealed result and this host's own flag.
     *
     * An exhaustive `when` over the sealed type rather than a chain of `if`s, which is what the
     * type is sealed *for*: a fifth `TriggerResult` becomes a compile error here rather than a
     * silent 202. Written the other way once, and `Unknown` and `Disabled` both answered 202
     * because they are "not AlreadyRunning" - a host that accepts a trigger for a task it does not
     * have, with a run id it never allocated.
     *
     * `AlreadyRunning` is the only line with a condition, and it is `composed-host-example`'s M3:
     * the framework reuses that case for "busy" and for "closed", spec 11.2 declined a fifth case
     * on the argument that the host can tell them apart because the host raised [shuttingDown]
     * itself, and this is that argument executed.
     */
    fun classify(result: TriggerResult): Int = when (result) {
        is TriggerResult.Accepted -> 202
        TriggerResult.Unknown -> 404
        TriggerResult.Disabled -> 400
        TriggerResult.AlreadyRunning -> if (shuttingDown) 503 else 409
    }

    companion object {

        /** The one state string that means "serving". Everything else is a 503. */
        const val READY: String = "ready"

        private val log: Logger = Logger.getLogger(EtlHost::class.java)

        private fun render(report: ValidationReport) = report.errors.joinToString("; ") {
            "${it.file}${it.step?.let { s -> "/$s" } ?: ""}: ${it.message}"
        }
    }
}
