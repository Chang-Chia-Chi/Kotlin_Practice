package com.mapreduce.mr.api

import com.mapreduce.mr.api.dto.JobResponse
import com.mapreduce.mr.api.dto.SubmitJobRequest
import com.mapreduce.mr.service.MapReduceService
import com.mapreduce.queue.model.GroupStatus
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

@Path("/api/jobs")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class JobResource(
    private val mapReduceService: MapReduceService,
    private val taskGroupRepository: TaskGroupRepository,
) {

    private val log = Logger.getLogger(JobResource::class.java)

    @POST
    @Path("/submit")
    suspend fun submitJob(request: SubmitJobRequest): Response {
        val result = try {
            withContext(Dispatchers.IO) {
                mapReduceService.submitJob(request.jobType, request.params)
            }
        } catch (e: IllegalArgumentException) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to e.message))
                .build()
        }

        log.infof("Submitted job %s (type=%s, tasks=%d)", result.jobId, request.jobType, result.totalTasks)

        return Response.status(Response.Status.CREATED)
            .entity(mapOf("jobId" to result.jobId, "totalTasks" to result.totalTasks))
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
