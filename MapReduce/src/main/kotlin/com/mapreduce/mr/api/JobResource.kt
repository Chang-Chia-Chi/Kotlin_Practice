package com.mapreduce.mr.api

import com.mapreduce.mr.api.dto.JobResponse
import com.mapreduce.mr.api.dto.SubmitJobRequest
import com.mapreduce.mr.model.JobStatus
import com.mapreduce.mr.registry.MapReduceRegistrar
import com.mapreduce.mr.repository.JobRepository
import com.mapreduce.mr.spi.PartitionedMapReduceDefinition
import com.mapreduce.mr.spi.unsafeCast
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

@Path("/api/jobs")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class JobResource(
    private val jobRepository: JobRepository,
    private val registrar: MapReduceRegistrar,
) {

    private val log = Logger.getLogger(JobResource::class.java)

    @POST
    @Path("/submit")
    suspend fun submitJob(request: SubmitJobRequest): Response {
        val definition = registrar.getDefinition(request.jobType)
            ?: return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "Unknown job type: ${request.jobType}"))
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

            val totalPartitions = if (definition is PartitionedMapReduceDefinition<*, *, *, *>) {
                definition.totalPartitions
            } else {
                1
            }

            jobRepository.submitJob(
                jobId = jobId,
                jobType = request.jobType,
                jobParams = request.params,
                taskInputs = serializedInputs,
                maxRetries = definition.maxRetries,
                failurePolicy = definition.failurePolicy,
                failureThreshold = definition.failureThreshold,
                queue = definition.queue,
                totalPartitions = totalPartitions,
            )
            taskInputs.size
        }

        if (taskCount == 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "Split produced zero tasks"))
                .build()
        }

        log.infof("Submitted job %s (type=%s, tasks=%d)", jobId, request.jobType, taskCount)

        return Response.status(Response.Status.CREATED)
            .entity(mapOf("jobId" to jobId, "totalTasks" to taskCount))
            .build()
    }

    @GET
    @Path("/{jobId}")
    suspend fun getJob(@PathParam("jobId") jobId: String): Response {
        val job = withContext(Dispatchers.IO) { jobRepository.findJobById(jobId) }
            ?: return Response.status(Response.Status.NOT_FOUND).build()
        return Response.ok(JobResponse.from(job)).build()
    }

    @GET
    suspend fun listJobs(@QueryParam("status") status: String?): Response {
        if (status != null) {
            val jobStatus = try {
                JobStatus.valueOf(status.uppercase())
            } catch (_: IllegalArgumentException) {
                return Response.status(Response.Status.BAD_REQUEST)
                    .entity(mapOf("error" to "Invalid status: $status"))
                    .build()
            }
            val jobs = withContext(Dispatchers.IO) { jobRepository.findJobsByStatus(jobStatus) }
            return Response.ok(jobs.map { JobResponse.from(it) }).build()
        }

        val jobs = withContext(Dispatchers.IO) { jobRepository.findAllJobs() }
        return Response.ok(jobs.map { JobResponse.from(it) }).build()
    }
}
