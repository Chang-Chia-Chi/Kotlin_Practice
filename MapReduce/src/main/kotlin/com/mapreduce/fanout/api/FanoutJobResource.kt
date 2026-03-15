package com.mapreduce.fanout.api

import com.mapreduce.fanout.api.dto.FanoutJobResponse
import com.mapreduce.fanout.api.dto.SubmitFanoutJobRequest
import com.mapreduce.fanout.model.FanoutJobStatus
import com.mapreduce.fanout.registry.FanoutRegistrar
import com.mapreduce.fanout.repository.FanoutJobRepository
import com.mapreduce.fanout.spi.unsafeCast
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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jboss.logging.Logger
import java.util.UUID

@Path("/api/fanout-jobs")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class FanoutJobResource(
    private val fanoutJobRepository: FanoutJobRepository,
    private val registrar: FanoutRegistrar,
) {

    private val log = Logger.getLogger(FanoutJobResource::class.java)

    @POST
    @Path("/submit")
    suspend fun submitJob(request: SubmitFanoutJobRequest): Response {
        val definition = registrar.getDefinition(request.jobType)
            ?: return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "Unknown fanout job type: ${request.jobType}"))
                .build()

        val def = definition.unsafeCast()
        val jobId = UUID.randomUUID().toString()

        val taskCount = withContext(Dispatchers.IO) {
            val params = def.deserializeParams(request.params)
            val taskInputs = def.split(params)

            if (taskInputs.isEmpty()) {
                return@withContext 0
            }

            val serializedInputs = taskInputs.map { def.serializeInput(it) }

            fanoutJobRepository.submitJob(
                jobId = jobId,
                jobType = request.jobType,
                jobParams = request.params,
                taskInputs = serializedInputs,
                maxRetries = definition.maxRetries,
                failurePolicy = definition.failurePolicy,
                failureThreshold = definition.failureThreshold,
                queue = definition.queue,
            )
            taskInputs.size
        }

        if (taskCount == 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "Split produced zero tasks"))
                .build()
        }

        log.infof("Submitted fanout job %s (type=%s, tasks=%d)", jobId, request.jobType, taskCount)

        return Response.status(Response.Status.CREATED)
            .entity(mapOf("jobId" to jobId, "totalTasks" to taskCount))
            .build()
    }

    @GET
    @Path("/{jobId}")
    suspend fun getJob(@PathParam("jobId") jobId: String): Response {
        val job = withContext(Dispatchers.IO) { fanoutJobRepository.findJobById(jobId) }
            ?: return Response.status(Response.Status.NOT_FOUND).build()
        return Response.ok(FanoutJobResponse.from(job)).build()
    }

    @GET
    suspend fun listJobs(@QueryParam("status") status: String?): Response {
        if (status != null) {
            val jobStatus = try {
                FanoutJobStatus.valueOf(status.uppercase())
            } catch (_: IllegalArgumentException) {
                return Response.status(Response.Status.BAD_REQUEST)
                    .entity(mapOf("error" to "Invalid status: $status"))
                    .build()
            }
            val jobs = withContext(Dispatchers.IO) { fanoutJobRepository.findJobsByStatus(jobStatus) }
            return Response.ok(jobs.map { FanoutJobResponse.from(it) }).build()
        }

        val jobs = withContext(Dispatchers.IO) { fanoutJobRepository.findAllJobs() }
        return Response.ok(jobs.map { FanoutJobResponse.from(it) }).build()
    }
}
