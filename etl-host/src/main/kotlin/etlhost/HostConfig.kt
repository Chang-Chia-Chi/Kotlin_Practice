package etlhost

import jakarta.inject.Singleton
import java.nio.file.Path
import java.time.Duration
import org.eclipse.microprofile.config.inject.ConfigProperty

/**
 * Every knob this host reads, in one place, so that "what is configuration and what is code" is
 * answerable by reading one file.
 *
 * **None of these values is a recommendation.** README.md says why at length; the short version is
 * that the operating point - how many tasks overlap, at what memory limits, in what pod - is a
 * statement about a memory request and a real schedule, and no test in this reactor is evidence
 * about it. What is fixed here is the *shape*: which settings exist, which two are load-bearing
 * anywhere (`quarkus.scheduler.start-mode` and `-Dkotlinx.coroutines.debug`), and which framework
 * parameter each one reaches.
 *
 * `@ConfigProperty` rather than `@ConfigMapping`: the mapping interface is nicer, and it would put
 * the defaults in a Kotlin interface where a deployment cannot see them. A host's config surface is
 * read by whoever writes the deployment, and `application.properties` beside a commented default is
 * where they look.
 */
@Singleton
class HostConfig {

    // ---- snapshot cache (spec 13) ----

    @ConfigProperty(name = "etl-host.cache.storage-path")
    lateinit var storagePath: Path

    @ConfigProperty(name = "etl-host.cache.temp-directory")
    lateinit var tempDirectory: Path

    /**
     * One entry per group: the group name, and the SQL that produces its single table.
     *
     * The map's keys are the groups this host serves - there is no second "which groups" setting
     * to disagree with it. The table inside a generation takes the group's name, so a `cacheCopy`
     * step reads `select ... from <group>`.
     *
     * **Each statement must project an `id` column.** `VerifyConfig.keyUnique` defaults to true and
     * the gate runs `COUNT(id), COUNT(DISTINCT id)` over every table of a candidate, so a group
     * without one fails its first refresh and the symptom surfaces two systems away as a failed
     * `cacheCopy`. Named here because this is the property whose value decides it.
     */
    @ConfigProperty(name = "etl-host.cache.sql")
    lateinit var groupSql: Map<String, String>

    @ConfigProperty(name = "etl-host.cache.serving-memory-limit")
    lateinit var servingMemoryLimit: String

    @ConfigProperty(name = "etl-host.cache.wait-budget")
    lateinit var waitBudget: Duration

    @ConfigProperty(name = "etl-host.cache.lease-deadline")
    lateinit var leaseDeadline: Duration

    /** Keep `terminationGracePeriodSeconds` above this plus headroom (snapshotcache spec 11.3). */
    @ConfigProperty(name = "etl-host.cache.lease-drain-timeout")
    lateinit var leaseDrainTimeout: Duration

    // ---- the source behind every group ----

    @ConfigProperty(name = "etl-host.source.url")
    lateinit var sourceUrl: String

    @ConfigProperty(name = "etl-host.source.username")
    lateinit var sourceUsername: java.util.Optional<String>

    @ConfigProperty(name = "etl-host.source.password")
    lateinit var sourcePassword: java.util.Optional<String>

    /**
     * Spec 8.6's timeout row, which is the host's and has no framework equivalent: `TaskEngine.run`
     * is a blocking call with no `Statement` handle to cancel, so a wedged driver parks a task's
     * dispatcher forever and every later firing is skipped in silence. Applied to every statement
     * this host's `Jdbi` instances issue.
     */
    @ConfigProperty(name = "etl-host.source.query-timeout-seconds")
    var queryTimeoutSeconds: Int = 0

    @ConfigProperty(name = "etl-host.source.fetch-size")
    var fetchSize: Int = 0

    // ---- SimpleEtl (spec 11.2) ----

    @ConfigProperty(name = "etl-host.etl.task-directory")
    lateinit var taskDirectory: Path

    @ConfigProperty(name = "etl-host.etl.scratch-directory")
    lateinit var scratchDirectory: Path

    @ConfigProperty(name = "etl-host.etl.scratch-memory-limit-mb")
    var scratchMemoryLimitMb: Int = 0

    /** The one non-scratch datasource the shipped task files name as a pipe target. */
    @ConfigProperty(name = "etl-host.etl.target-url")
    lateinit var targetUrl: String

    @ConfigProperty(name = "etl-host.etl.target-name")
    lateinit var targetName: String
}
