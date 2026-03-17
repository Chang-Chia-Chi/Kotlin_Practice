package com.mapreduce.mr.api

import com.mapreduce.mr.api.dto.JobResponse
import com.mapreduce.mr.api.dto.SubmitJobRequest
import com.mapreduce.mr.registry.MapReduceRegistrar
import com.mapreduce.mr.spi.unsafeCast
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.GroupStatus
import com.mapreduce.queue.model.TaskGroup
import com.mapreduce.queue.repository.TaskGroupRepository
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
    private val taskGroupRepository: TaskGroupRepository,
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
            if (taskInputs.isEmpty()) return@withContext 0

            val group = TaskGroup(
                groupId = jobId,
                groupType = request.jobType,
                status = GroupStatus.ACTIVE,
                params = request.params,
                queue = definition.queue,
                phase = "map",
                phaseTotal = taskInputs.size,
                onCompleteHandler = "${request.jobType}.__phase_complete",
                failurePolicy = definition.failurePolicy.name,
                failureThreshold = definition.failureThreshold,
            )
            val tasks = taskInputs.mapIndexed { i, input ->
                EnqueueRequest(
                    handler = "${request.jobType}.map",
                    payload = def.serializeInput(input),
                    queue = definition.queue,
                    groupId = jobId,
                    metadata = """{"task_index":$i,"phase":"MAP"}""",
                    maxRetries = definition.maxRetries,
                )
            }
            taskGroupRepository.submitGroup(group, tasks)
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
        val group = withContext(Dispatchers.IO) { taskGroupRepository.findGroup(jobId) }
            ?: return Response.status(Response.Status.NOT_FOUND).build()
        return Response.ok(JobResponse.from(group)).build()
    }

    @GET
    suspend fun listJobs(@QueryParam("status") status: String?): Response {
        if (status != null) {
            val groupStatus = try {
                GroupStatus.valueOf(status.uppercase())
            } catch (_: IllegalArgumentException) {
                return Response.status(Response.Status.BAD_REQUEST)
                    .entity(mapOf("error" to "Invalid status: $status"))
                    .build()
            }
            val groups = withContext(Dispatchers.IO) { taskGroupRepository.findGroupsByStatus(groupStatus) }
            return Response.ok(groups.map { JobResponse.from(it) }).build()
        }

        val groups = withContext(Dispatchers.IO) { taskGroupRepository.findAllGroups() }
        return Response.ok(groups.map { JobResponse.from(it) }).build()
    }
}
