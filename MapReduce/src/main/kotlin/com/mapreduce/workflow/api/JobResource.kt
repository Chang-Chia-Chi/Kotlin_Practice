package com.mapreduce.workflow.api

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.StepStatus
import com.mapreduce.queue.model.WorkflowStep
import com.mapreduce.queue.repository.WorkflowStepRepository
import com.mapreduce.workflow.api.dto.JobResponse
import com.mapreduce.workflow.api.dto.SubmitJobRequest
import com.mapreduce.workflow.registry.WorkflowRegistry
import com.mapreduce.workflow.spi.WorkflowDefinition
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.GET
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.QueryParam
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import org.jboss.logging.Logger
import java.time.Duration
import java.time.Instant
import java.util.UUID

@Path("/api/jobs")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class JobResource(
    private val workflowStepRepository: WorkflowStepRepository,
    private val registry: WorkflowRegistry,
    private val config: FrameworkConfig,
) {

    private val log = Logger.getLogger(JobResource::class.java)

    @POST
    @Path("/submit")
    @Suppress("UNCHECKED_CAST")
    suspend fun submitJob(request: SubmitJobRequest): Response {
        val definition = registry.getDefinition(request.jobType)
            ?: return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "Unknown workflow: ${request.jobType}"))
                .build()

        val def = definition as WorkflowDefinition<Any>
        val pipeline = definition.pipeline()
        val firstStep = pipeline[0]

        val params = def.deserializeParams(request.params)
        val taskPayloads = def.initialTasks(params)
        if (taskPayloads.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "Initial tasks produced zero tasks"))
                .build()
        }

        val runId = UUID.randomUUID().toString()
        val stepId = UUID.randomUUID().toString()
        val resolvedDeadline = resolveDeadline(firstStep.deadline)

        val step = WorkflowStep(
            stepId = stepId,
            workflowName = request.jobType,
            runId = runId,
            status = StepStatus.ACTIVE,
            params = request.params,
            queue = firstStep.queue,
            stepLabel = firstStep.name,
            stepTotal = taskPayloads.size,
            onCompleteHandler = "${request.jobType}.__step_transition",
            failurePolicy = firstStep.failurePolicy.name,
            failureThreshold = firstStep.failureThreshold,
            deadlineAt = Instant.now().plus(resolvedDeadline),
        )
        val tasks = taskPayloads.map { payload ->
            EnqueueRequest(
                handler = firstStep.handler,
                payload = payload.payload,
                queue = firstStep.queue,
                stepId = stepId,
                metadata = payload.metadata,
                maxRetries = firstStep.maxRetries,
            )
        }
        workflowStepRepository.submitStep(step, tasks)

        log.infof("Submitted workflow run %s (type=%s, step='%s', tasks=%d)",
            runId, request.jobType, firstStep.name, taskPayloads.size)

        return Response.status(Response.Status.CREATED)
            .entity(mapOf("runId" to runId, "stepId" to stepId, "totalTasks" to taskPayloads.size))
            .build()
    }

    @GET
    @Path("/{runId}")
    suspend fun getJob(@PathParam("runId") runId: String): Response {
        val steps = workflowStepRepository.findStepsByRunId(runId)
        if (steps.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND).build()
        }
        return Response.ok(steps.map { JobResponse.from(it) }).build()
    }

    @GET
    suspend fun listJobs(@QueryParam("status") status: String?): Response {
        if (status != null) {
            val stepStatus = try {
                StepStatus.valueOf(status.uppercase())
            } catch (_: IllegalArgumentException) {
                return Response.status(Response.Status.BAD_REQUEST)
                    .entity(mapOf("error" to "Invalid status: $status"))
                    .build()
            }
            val steps = workflowStepRepository.findStepsByStatus(stepStatus)
            return Response.ok(steps.map { JobResponse.from(it) }).build()
        }

        val steps = workflowStepRepository.findAllSteps()
        return Response.ok(steps.map { JobResponse.from(it) }).build()
    }

    private fun resolveDeadline(specDeadline: Duration): Duration {
        val hardcodedDefault = Duration.ofHours(1)
        return if (specDeadline == hardcodedDefault) {
            config.workflow().defaultStepDeadline()
        } else {
            specDeadline
        }
    }
}
