package com.mapreduce.deadletter.api

import com.mapreduce.deadletter.DeadLetterService
import com.mapreduce.deadletter.api.dto.BulkReplayRequest
import com.mapreduce.deadletter.api.dto.ReplayResponse
import com.mapreduce.deadletter.api.dto.ReplaySingleRequest
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.DefaultValue
import jakarta.ws.rs.GET
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.QueryParam
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import java.time.Instant

/**
 * Dead Letter Processor REST API.
 *
 * Stateless — any pod can serve these endpoints. No leader requirement.
 *
 * Endpoints:
 * - GET  /api/dead-letters          — paginated list with filters (§3.1)
 * - GET  /api/dead-letters/{id}     — single task detail (§3.2)
 * - GET  /api/dead-letters/summary  — aggregation by handler/group (§3.3)
 * - GET  /api/dead-letters/errors   — error pattern grouping (§3.4)
 * - POST /api/dead-letters/{id}/replay     — replay single (§4.1)
 * - POST /api/dead-letters/replay          — bulk replay by filter (§4.2)
 * - POST /api/dead-letters/replay-job/{id} — replay MR job (§4.4)
 */
@Path("/api/dead-letters")
@Produces(MediaType.APPLICATION_JSON)
class DeadLetterResource(private val service: DeadLetterService) {

    // ── Inspection Endpoints ──────────────────────────────────────

    @GET
    fun list(
        @QueryParam("handler") handler: String?,
        @QueryParam("groupId") groupId: String?,
        @QueryParam("since") since: String?,
        @QueryParam("until") until: String?,
        @QueryParam("errorPattern") errorPattern: String?,
        @QueryParam("limit") @DefaultValue("50") limit: Int,
        @QueryParam("offset") @DefaultValue("0") offset: Int,
    ): Response {
        val items = service.list(
            handler = handler,
            groupId = groupId,
            since = since?.let { Instant.parse(it) },
            until = until?.let { Instant.parse(it) },
            errorPattern = errorPattern,
            limit = limit,
            offset = offset,
        )
        return Response.ok(items).build()
    }

    @GET
    @Path("/summary")
    fun summary(@QueryParam("since") since: String?): Response {
        val result = service.summary(since?.let { Instant.parse(it) })
        return Response.ok(result).build()
    }

    @GET
    @Path("/errors")
    fun errors(
        @QueryParam("handler") handler: String?,
        @QueryParam("since") since: String?,
    ): Response {
        val result = service.errorPatterns(handler, since?.let { Instant.parse(it) })
        return Response.ok(result).build()
    }

    @GET
    @Path("/{taskId}")
    fun detail(@PathParam("taskId") taskId: String): Response {
        val detail = service.getDetail(taskId)
            ?: return Response.status(Response.Status.NOT_FOUND)
                .entity(mapOf("error" to "Dead-lettered task not found: $taskId"))
                .build()
        return Response.ok(detail).build()
    }

    // ── Replay Endpoints ──────────────────────────────────────────

    @POST
    @Path("/{taskId}/replay")
    @Consumes(MediaType.APPLICATION_JSON)
    fun replaySingle(
        @PathParam("taskId") taskId: String,
        request: ReplaySingleRequest?,
    ): Response {
        val replayed = service.replaySingle(
            taskId,
            request?.maxRetries,
            request?.scheduledAt,
        )
        return if (replayed != null) {
            Response.ok(ReplayResponse(replayed, "Task $taskId replayed")).build()
        } else {
            Response.status(Response.Status.CONFLICT)
                .entity(mapOf("error" to "Task $taskId not in DEAD_LETTER status or already replayed"))
                .build()
        }
    }

    @POST
    @Path("/replay")
    @Consumes(MediaType.APPLICATION_JSON)
    fun bulkReplay(request: BulkReplayRequest): Response {
        val count = service.replayByFilter(request.filter, request.maxRetries, request.scheduledAt)
        return Response.ok(ReplayResponse(count, "Replayed $count task(s)")).build()
    }

    @POST
    @Path("/replay-job/{jobId}")
    @Consumes(MediaType.APPLICATION_JSON)
    fun replayJob(
        @PathParam("jobId") jobId: String,
        @QueryParam("force") @DefaultValue("false") force: Boolean,
    ): Response {
        val result = service.replayJob(jobId, force)
        return when {
            result == -1 -> Response.status(Response.Status.CONFLICT)
                .entity(mapOf(
                    "error" to "Job $jobId is COMPLETED. Replaying would produce stale reduce output. Use ?force=true to override.",
                ))
                .build()
            result == 0 -> Response.ok(ReplayResponse(0, "No dead-lettered tasks for job $jobId")).build()
            else -> Response.ok(ReplayResponse(result, "Replayed $result task(s) for job $jobId. Job transitioned to RUNNING.")).build()
        }
    }
}
