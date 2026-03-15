package com.mapreduce.schedule.api

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.schedule.ScheduleMetrics
import com.mapreduce.schedule.api.dto.CreateScheduleRequest
import com.mapreduce.schedule.api.dto.ScheduleResponse
import com.mapreduce.schedule.api.dto.TriggerResponse
import com.mapreduce.schedule.api.dto.UpdateScheduleRequest
import com.mapreduce.schedule.cron.CronExpressionParser
import com.mapreduce.schedule.model.CronSchedule
import com.mapreduce.schedule.model.ScheduleTaskMetadata
import com.mapreduce.schedule.repository.CronScheduleRepository
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.DELETE
import jakarta.ws.rs.GET
import jakarta.ws.rs.PATCH
import jakarta.ws.rs.POST
import jakarta.ws.rs.PUT
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import java.time.Instant
import java.util.UUID

/**
 * CRUD + operational endpoints for cron schedule management.
 *
 * Stateless — any pod can serve these endpoints.
 *
 * ```
 * GET    /api/schedules                  — list all
 * GET    /api/schedules/{id}             — detail
 * POST   /api/schedules                  — create
 * PUT    /api/schedules/{id}             — update
 * PATCH  /api/schedules/{id}/enable      — enable
 * PATCH  /api/schedules/{id}/disable     — disable
 * DELETE /api/schedules/{id}             — delete
 * POST   /api/schedules/{id}/trigger     — fire immediately
 * ```
 */
@Path("/api/schedules")
@Produces(MediaType.APPLICATION_JSON)
class ScheduleResource(
    private val scheduleRepository: CronScheduleRepository,
    private val taskRepository: TaskRepository,
    private val objectMapper: ObjectMapper,
    private val scheduleMetrics: ScheduleMetrics,
) {

    @GET
    fun list(): Response {
        val schedules = scheduleRepository.findAll().map { it.toResponse() }
        return Response.ok(schedules).build()
    }

    @GET
    @Path("/{id}")
    fun detail(@PathParam("id") id: String): Response {
        val schedule = scheduleRepository.findById(id)
            ?: return notFound(id)
        return Response.ok(schedule.toResponse()).build()
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    fun create(request: CreateScheduleRequest): Response {
        // Validate cron expression
        try {
            CronExpressionParser.validate(request.cronExpression)
        } catch (e: IllegalArgumentException) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "Invalid cron expression: ${e.message}"))
                .build()
        }

        // Check name uniqueness
        if (scheduleRepository.findByName(request.name) != null) {
            return Response.status(Response.Status.CONFLICT)
                .entity(mapOf("error" to "Schedule with name '${request.name}' already exists"))
                .build()
        }

        val now = Instant.now()
        val nextFireAt = if (request.enabled) {
            CronExpressionParser.nextFireTime(request.cronExpression, now)
        } else null

        val schedule = CronSchedule(
            scheduleId = UUID.randomUUID().toString(),
            name = request.name,
            handler = request.handler,
            cronExpression = request.cronExpression,
            payload = request.payload,
            queue = request.queue,
            maxRetries = request.maxRetries,
            overlapPolicy = request.overlapPolicy,
            enabled = request.enabled,
            nextFireAt = nextFireAt,
        )

        val id = scheduleRepository.insert(schedule)
        val created = scheduleRepository.findById(id)!!
        return Response.status(Response.Status.CREATED)
            .entity(created.toResponse())
            .build()
    }

    @PUT
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    fun update(@PathParam("id") id: String, request: UpdateScheduleRequest): Response {
        val existing = scheduleRepository.findById(id) ?: return notFound(id)

        // Validate cron expression if provided
        request.cronExpression?.let {
            try {
                CronExpressionParser.validate(it)
            } catch (e: IllegalArgumentException) {
                return Response.status(Response.Status.BAD_REQUEST)
                    .entity(mapOf("error" to "Invalid cron expression: ${e.message}"))
                    .build()
            }
        }

        // Recompute next fire time if cron expression changed
        val newCron = request.cronExpression ?: existing.cronExpression
        val nextFireAt = if (existing.enabled) {
            CronExpressionParser.nextFireTime(newCron, Instant.now())
        } else null

        val updated = scheduleRepository.update(
            scheduleId = id,
            version = existing.version,
            handler = request.handler,
            cronExpression = request.cronExpression,
            payload = request.payload,
            queue = request.queue,
            maxRetries = request.maxRetries,
            overlapPolicy = request.overlapPolicy,
            nextFireAt = nextFireAt,
        )

        if (!updated) {
            return Response.status(Response.Status.CONFLICT)
                .entity(mapOf("error" to "Concurrent modification — retry the request"))
                .build()
        }

        val result = scheduleRepository.findById(id)!!
        return Response.ok(result.toResponse()).build()
    }

    @PATCH
    @Path("/{id}/enable")
    fun enable(@PathParam("id") id: String): Response {
        val existing = scheduleRepository.findById(id) ?: return notFound(id)

        // Compute next fire time when enabling
        val nextFireAt = CronExpressionParser.nextFireTime(existing.cronExpression, Instant.now())
        scheduleRepository.setEnabled(id, true)
        scheduleRepository.update(
            scheduleId = id,
            version = existing.version,
            handler = null, cronExpression = null, payload = null,
            queue = null, maxRetries = null, overlapPolicy = null,
            nextFireAt = nextFireAt,
        )

        val result = scheduleRepository.findById(id)!!
        return Response.ok(result.toResponse()).build()
    }

    @PATCH
    @Path("/{id}/disable")
    fun disable(@PathParam("id") id: String): Response {
        scheduleRepository.findById(id) ?: return notFound(id)
        scheduleRepository.setEnabled(id, false)
        val result = scheduleRepository.findById(id)!!
        return Response.ok(result.toResponse()).build()
    }

    @DELETE
    @Path("/{id}")
    fun delete(@PathParam("id") id: String): Response {
        if (!scheduleRepository.delete(id)) return notFound(id)
        return Response.noContent().build()
    }

    /**
     * Fire a schedule immediately, bypassing the cron expression.
     * Enqueues a task now regardless of `next_fire_at` or overlap policy.
     */
    @POST
    @Path("/{id}/trigger")
    fun trigger(@PathParam("id") id: String): Response {
        val schedule = scheduleRepository.findById(id) ?: return notFound(id)

        val metadata = ScheduleTaskMetadata(
            scheduleName = schedule.name,
            scheduleId = schedule.scheduleId,
            fireTime = Instant.now(),
            sequenceNumber = 0, // manual trigger
        )

        val taskId = taskRepository.enqueue(
            EnqueueRequest(
                handler = schedule.handler,
                payload = schedule.payload,
                queue = schedule.queue,
                maxRetries = schedule.maxRetries,
                metadata = objectMapper.writeValueAsString(metadata),
            ),
        )

        scheduleMetrics.recordFired(schedule.name)

        return Response.ok(
            TriggerResponse(
                taskId = taskId,
                scheduleName = schedule.name,
                message = "Schedule '${schedule.name}' triggered manually — task $taskId enqueued",
            ),
        ).build()
    }

    private fun notFound(id: String): Response =
        Response.status(Response.Status.NOT_FOUND)
            .entity(mapOf("error" to "Schedule not found: $id"))
            .build()

    private fun CronSchedule.toResponse() = ScheduleResponse(
        scheduleId = scheduleId,
        name = name,
        handler = handler,
        cronExpression = cronExpression,
        payload = payload,
        queue = queue,
        maxRetries = maxRetries,
        overlapPolicy = overlapPolicy,
        enabled = enabled,
        lastFiredAt = lastFiredAt,
        lastCompletedAt = lastCompletedAt,
        lastTaskId = lastTaskId,
        lastStatus = lastStatus,
        nextFireAt = nextFireAt,
        createdAt = createdAt,
        updatedAt = updatedAt,
    )
}
