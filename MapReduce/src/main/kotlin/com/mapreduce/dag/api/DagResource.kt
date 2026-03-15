package com.mapreduce.dag.api

import com.mapreduce.dag.api.dto.DagRunResponse
import com.mapreduce.dag.api.dto.SubmitDagRequest
import com.mapreduce.dag.model.DagRunStatus
import com.mapreduce.dag.registry.DagRegistrar
import com.mapreduce.dag.repository.DagRepository
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

@Path("/api/dags")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class DagResource(
    private val dagRepository: DagRepository,
    private val registrar: DagRegistrar,
) {

    private val log = Logger.getLogger(DagResource::class.java)

    @POST
    @Path("/submit")
    suspend fun submitRun(request: SubmitDagRequest): Response {
        val blueprint = registrar.getBlueprint(request.dagId)
            ?: return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "Unknown DAG: ${request.dagId}"))
                .build()

        val nodes = blueprint.nodes()
        if (nodes.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "DAG has no nodes"))
                .build()
        }

        val taskKeys = nodes.map { it.taskKey }.toSet()
        if (taskKeys.size != nodes.size) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "DAG contains duplicate task_keys"))
                .build()
        }

        val invalidDeps = nodes.flatMap { it.dependencies }.filter { it !in taskKeys }
        if (invalidDeps.isNotEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "DAG references unknown dependencies: $invalidDeps"))
                .build()
        }

        val runId = UUID.randomUUID().toString()
        val defaults = blueprint.defaults()

        withContext(Dispatchers.IO) {
            dagRepository.submitRun(
                runId = runId,
                dagId = request.dagId,
                globalContext = request.globalContext,
                nodes = nodes,
                defaultMaxAttempts = defaults.maxAttempts,
            )
        }

        log.infof("Submitted DAG run %s (dag=%s, nodes=%d)", runId, request.dagId, nodes.size)

        return Response.status(Response.Status.CREATED)
            .entity(mapOf("runId" to runId, "totalNodes" to nodes.size))
            .build()
    }

    @GET
    @Path("/{runId}")
    suspend fun getRun(@PathParam("runId") runId: String): Response {
        val run = withContext(Dispatchers.IO) { dagRepository.findRunById(runId) }
            ?: return Response.status(Response.Status.NOT_FOUND).build()
        val instances = withContext(Dispatchers.IO) { dagRepository.findInstancesByRunId(runId) }
        return Response.ok(DagRunResponse.from(run, instances)).build()
    }

    @GET
    suspend fun listRuns(
        @QueryParam("status") status: String?,
        @QueryParam("dagId") dagId: String?,
    ): Response {
        if (status != null) {
            val dagStatus = try {
                DagRunStatus.valueOf(status.uppercase())
            } catch (_: IllegalArgumentException) {
                return Response.status(Response.Status.BAD_REQUEST)
                    .entity(mapOf("error" to "Invalid status: $status"))
                    .build()
            }
            val runs = withContext(Dispatchers.IO) { dagRepository.findRunsByStatus(dagStatus) }
            return Response.ok(runs.map { mapOf("runId" to it.runId, "dagId" to it.dagId, "status" to it.status.name) }).build()
        }

        val runs = withContext(Dispatchers.IO) { dagRepository.findAllRuns() }
        return Response.ok(runs.map { mapOf("runId" to it.runId, "dagId" to it.dagId, "status" to it.status.name) }).build()
    }

    /** Request cancellation of a Run (best-effort). */
    @POST
    @Path("/{runId}/cancel")
    suspend fun cancelRun(@PathParam("runId") runId: String): Response {
        val success = withContext(Dispatchers.IO) { dagRepository.cancelRun(runId) }
        return if (success) {
            log.infof("Cancelled DAG run %s", runId)
            Response.ok(mapOf("status" to "CANCELLED", "runId" to runId)).build()
        } else {
            Response.status(Response.Status.CONFLICT)
                .entity(mapOf("error" to "Run $runId is not in a cancellable state"))
                .build()
        }
    }

    /** Manual retry of a failed node. */
    @POST
    @Path("/{runId}/nodes/{taskKey}/retry")
    suspend fun retryNode(
        @PathParam("runId") runId: String,
        @PathParam("taskKey") taskKey: String,
    ): Response {
        val success = withContext(Dispatchers.IO) { dagRepository.manualRetryInstance(runId, taskKey) }
        return if (success) {
            log.infof("Manual retry: %s in run %s", taskKey, runId)
            Response.ok(mapOf("status" to "RETRYING", "taskKey" to taskKey)).build()
        } else {
            Response.status(Response.Status.CONFLICT)
                .entity(mapOf("error" to "Node $taskKey is not in a retryable state"))
                .build()
        }
    }

    /** Manual skip of a blocked/failed node. */
    @POST
    @Path("/{runId}/nodes/{taskKey}/skip")
    suspend fun skipNode(
        @PathParam("runId") runId: String,
        @PathParam("taskKey") taskKey: String,
    ): Response {
        val success = withContext(Dispatchers.IO) { dagRepository.manualSkipInstance(runId, taskKey) }
        return if (success) {
            log.infof("Manual skip: %s in run %s", taskKey, runId)
            Response.ok(mapOf("status" to "SKIPPED", "taskKey" to taskKey)).build()
        } else {
            Response.status(Response.Status.CONFLICT)
                .entity(mapOf("error" to "Node $taskKey is not in a skippable state"))
                .build()
        }
    }

    /** Retrieve node output data (XCom). */
    @GET
    @Path("/{runId}/xcom/{taskKey}")
    suspend fun getXcom(
        @PathParam("runId") runId: String,
        @PathParam("taskKey") taskKey: String,
    ): Response {
        val instances = withContext(Dispatchers.IO) { dagRepository.findInstancesByRunId(runId) }
        val instance = instances.find { it.taskKey == taskKey }
            ?: return Response.status(Response.Status.NOT_FOUND)
                .entity(mapOf("error" to "Node $taskKey not found in run $runId"))
                .build()

        return Response.ok(
            mapOf(
                "taskKey" to taskKey,
                "status" to instance.status.name,
                "outputData" to instance.outputData,
            ),
        ).build()
    }

    /** List registered DAG blueprints. */
    @GET
    @Path("/blueprints")
    suspend fun listBlueprints(): Response {
        val dags = registrar.supportedDags().map { dagId ->
            val bp = registrar.getBlueprint(dagId)
            mapOf(
                "dagId" to dagId,
                "namespace" to (bp?.namespace ?: ""),
                "description" to (bp?.description ?: ""),
                "nodeCount" to (bp?.nodes()?.size ?: 0),
            )
        }
        return Response.ok(dags).build()
    }
}
