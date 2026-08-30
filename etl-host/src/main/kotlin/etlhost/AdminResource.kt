package etlhost

import infra.etl.task.TriggerResult
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.bootstrap.ManagedSnapshotCache
import jakarta.annotation.security.PermitAll
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs.GET
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.core.SecurityContext

/**
 * SimpleEtl spec 8.2's four endpoints, plus snapshotcache spec 12.7's, with the mapping the
 * frameworks deliberately do not own.
 *
 * **`@RolesAllowed("etl-admin")` is on the class, so it is on every method.** Spec 8.6 states the
 * obligation per endpoint and the symptom is blunt - an unauthenticated caller can trigger any
 * task. `TaskAdmin` authorises nothing; it *records* the identity it is handed, which is what makes
 * an API-triggered run distinguishable from a scheduled one in the listing and in the logs.
 * Declaring it once at class level is what makes "on every endpoint" true by construction rather
 * than by review, and it is why a fifth endpoint added below cannot be added unprotected.
 *
 * ### The status codes, and why the mapping lives here
 *
 * `TriggerResult` is sealed precisely so this `when` is exhaustive: `Accepted` 202, `Unknown` 404,
 * `Disabled` 400, `AlreadyRunning` 409. The fourth is the interesting one. It is also the answer
 * after `WiringResult.Wired.close()`, when nothing is running and nothing ever will be again -
 * SimpleEtl spec 11.2 declined a fifth sealed case on the argument that *the host can tell the two
 * apart, because the host is the one that called close()*. [EtlHost.classify] is that argument in
 * code: the same framework answer becomes 409 while serving and 503 while shutting down, decided by
 * a flag this host raised itself.
 *
 * **202 is returned as soon as the run is submitted, never when it finishes.** A 30-minute request
 * held open is spec 8.2's explicit non-goal.
 */
@Path("/admin/etl")
@RolesAllowed("etl-admin")
@Produces(MediaType.APPLICATION_JSON)
class AdminResource(
    private val host: EtlHost,
    private val managed: ManagedSnapshotCache,
) {

    @GET
    @Path("/tasks")
    fun tasks(): List<Map<String, Any?>> = host.admin.list().map {
        mapOf(
            "name" to it.name,
            "enabled" to it.enabled,
            "cron" to it.cron,
            // False beside a non-null cron has two causes and they render identically: the host
            // forgot TaskScheduler.apply, or shutdown has run. Both mean it will never fire.
            "scheduled" to it.scheduled,
            "running" to it.running,
            "lastRun" to it.lastRun?.let { run ->
                mapOf(
                    "runId" to run.runId,
                    "trigger" to run.trigger,
                    "triggeredBy" to run.triggeredBy,
                    "startedAt" to run.startedAt.toString(),
                    "finishedAt" to run.finishedAt?.toString(),
                    "outcome" to run.outcome,
                )
            },
        )
    }

    @POST
    @Path("/tasks/{name}/runs")
    fun trigger(@PathParam("name") name: String, security: SecurityContext): Response {
        val result = host.admin.trigger(name, security.userPrincipal?.name)
        val status = host.classify(result)
        val body: Map<String, Any?> = when (result) {
            is TriggerResult.Accepted -> mapOf("runId" to result.runId)
            TriggerResult.AlreadyRunning ->
                mapOf("message" to if (status == 503) "this instance is shutting down" else "$name is already running")
            TriggerResult.Unknown -> mapOf("message" to "no task named $name")
            TriggerResult.Disabled -> mapOf("message" to "$name is disabled")
        }
        return Response.status(status).entity(body).build()
    }

    /**
     * 404 covers both "no such run" and "that run has not finished", which is what `TaskAdmin.run`
     * returns null for. `GET /admin/etl/tasks` distinguishes them through `running`, and duplicating
     * that distinction here would mean two places to keep in step.
     */
    @GET
    @Path("/tasks/{name}/runs/{id}")
    fun run(@PathParam("name") name: String, @PathParam("id") id: String): Response {
        val outcome = host.admin.run(name, id)
            ?: return Response.status(404).entity(mapOf("message" to "no finished run $id of $name")).build()
        return Response.ok(
            mapOf(
                "runId" to outcome.runId,
                "outcome" to outcome.outcome,
                "failure" to outcome.failure?.toString(),
            ),
        ).build()
    }

    /**
     * Atomic in both halves: an invalid file or a rejected cron changes nothing (spec 8.5).
     *
     * **400 is not the answer to every rejected reload**, and the exception is the one a naive
     * mapping gets wrong in the worst place. `WiringResult.Wired.close()` is terminal, so a reload
     * after shutdown has begun comes back as a `ValidationReport` too. Rendered as 400 that tells an
     * operator, in the middle of a shutdown, that their task files are badly authored: they go and
     * look at YAML that is fine while the pod goes away underneath them. It is the same shape as
     * M3's busy-versus-dying, one layer up - the framework answers the state and the *host* is the
     * one that knows why - so it gets the same answer 503 and the same readiness the probe serves.
     *
     * **The discriminator is [EtlHost.shuttingDown], not the report's contents**, and that is this
     * host's answer to the reopen trigger spec 11.2 left on `TriggerResult.ShuttingDown`. Until now
     * this branch matched `error.file == "<wiring>"` - an untyped framework sentinel, copied into a
     * host constant, in a field otherwise holding file names. Nothing would have failed to compile
     * had the framework renamed it; the branch would simply have stopped matching, and an operator
     * mid-shutdown would have been handed 400 "your YAML is bad" - the precise wrong answer this
     * KDoc exists to prevent. The flag is the same state the trigger mapping and the probe already
     * read, it is owned by the host because the host is the one that calls `close()`, and using it
     * here makes all three answers come from one place. Host state suffices; nothing needed a
     * fifth sealed case, and one string match fewer is the evidence.
     */
    @POST
    @Path("/reload")
    fun reload(): Response {
        val report = host.reload()
            ?: return Response.ok(mapOf("tasks" to host.admin.list().size)).build()
        if (host.shuttingDown) {
            return Response.status(503).entity(
                mapOf(
                    "state" to "shutting-down",
                    "message" to "this wiring is closed; reload cannot re-register a cancelled runner",
                ),
            ).build()
        }
        return Response.status(400).entity(
            mapOf(
                "errors" to report.errors.map {
                    mapOf("file" to it.file, "step" to it.step, "line" to it.line, "message" to it.message)
                },
            ),
        ).build()
    }

    /** snapshotcache spec 12.7: full live state for manual investigation, generation detail and all. */
    @GET
    @Path("/snapshot/{group}")
    fun snapshot(@PathParam("group") group: String): Response {
        val id = GroupId(group)
        if (host.groups.none { it == id }) {
            return Response.status(404).entity(mapOf("message" to "no group named $group")).build()
        }
        return Response.ok(
            mapOf(
                "current" to managed.cache.currentInfo(id)?.let {
                    mapOf(
                        "generation" to it.generation,
                        "dataAsOf" to it.dataAsOf.toString(),
                        "publishedAt" to it.publishedAt.toString(),
                        "rowCounts" to it.rowCounts,
                    )
                },
                "liveGenerations" to managed.admin.liveGenerations(id).map { state ->
                    mapOf(
                        "generation" to state.generation,
                        "isCurrent" to state.isCurrent,
                        "refCount" to state.refCount,
                        "fileBytes" to state.fileBytes,
                        "leases" to state.leases.map {
                            mapOf(
                                "owner" to it.owner,
                                "acquiredAt" to it.acquiredAt.toString(),
                                "deadline" to it.deadline.toString(),
                            )
                        },
                    )
                },
            ),
        ).build()
    }
}

/**
 * The readiness probe, and the only unauthenticated endpoint this host has.
 *
 * Kubernetes does not carry a bearer token, so a readiness endpoint behind `@RolesAllowed` never
 * answers 200 and the pod never joins the service. `@PermitAll` is stated rather than left implicit
 * so that the exception to the class-level rule next door is a decision someone made.
 *
 * It reports **two** states as not-ready, and the difference is `composed-host-example`'s M3: not
 * yet published (spec 10.1 step 5 - consumers waiting beats consumers reading an empty table), and
 * shutting down (the flag raised before `close()`, so the load balancer stops sending work before
 * the 409s and 503s start).
 */
@Path("/health/ready")
@Produces(MediaType.APPLICATION_JSON)
class ReadinessResource(private val host: EtlHost) {

    @GET
    @PermitAll
    fun ready(): Response {
        val state = when {
            host.shuttingDown -> "shutting-down"
            host.ready -> "ready"
            else -> "awaiting-first-generation"
        }
        return Response.status(if (state == "ready") 200 else 503).entity(mapOf("state" to state)).build()
    }
}
