package com.mapreduce.chain.api

import com.mapreduce.chain.api.dto.ChainResponse
import com.mapreduce.chain.api.dto.SubmitChainRequest
import com.mapreduce.chain.model.ChainStatus
import com.mapreduce.chain.registry.ChainRegistrar
import com.mapreduce.chain.repository.ChainRepository
import io.micrometer.core.instrument.MeterRegistry
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

@Path("/api/chains")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class ChainResource(
    private val chainRepository: ChainRepository,
    private val registrar: ChainRegistrar,
    private val meterRegistry: MeterRegistry,
) {

    private val log = Logger.getLogger(ChainResource::class.java)

    @POST
    @Path("/submit")
    suspend fun submitChain(request: SubmitChainRequest): Response {
        val definition = registrar.getDefinition(request.chainType)
            ?: return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "Unknown chain type: ${request.chainType}"))
                .build()

        if (definition.steps.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "Chain has no steps"))
                .build()
        }

        val firstStep = definition.steps.first()
        val chainId = UUID.randomUUID().toString()

        withContext(Dispatchers.IO) {
            chainRepository.startChain(
                chainId = chainId,
                chainType = request.chainType,
                chainParams = request.params,
                totalSteps = definition.steps.size,
                failurePolicy = definition.failurePolicy,
                firstStep = firstStep,
            )
        }

        meterRegistry.counter("taskqueue.chain.started", "chain_type", request.chainType).increment()
        log.infof("Started chain %s (type=%s, steps=%d)", chainId, request.chainType, definition.steps.size)

        return Response.status(Response.Status.CREATED)
            .entity(mapOf("chainId" to chainId, "totalSteps" to definition.steps.size))
            .build()
    }

    @GET
    @Path("/{chainId}")
    suspend fun getChain(@PathParam("chainId") chainId: String): Response {
        val chain = withContext(Dispatchers.IO) { chainRepository.findById(chainId) }
            ?: return Response.status(Response.Status.NOT_FOUND).build()
        return Response.ok(ChainResponse.from(chain)).build()
    }

    @GET
    suspend fun listChains(@QueryParam("status") status: String?): Response {
        if (status != null) {
            val chainStatus = try {
                ChainStatus.valueOf(status.uppercase())
            } catch (_: IllegalArgumentException) {
                return Response.status(Response.Status.BAD_REQUEST)
                    .entity(mapOf("error" to "Invalid status: $status"))
                    .build()
            }
            val chains = withContext(Dispatchers.IO) { chainRepository.findByStatus(chainStatus) }
            return Response.ok(chains.map { ChainResponse.from(it) }).build()
        }

        val chains = withContext(Dispatchers.IO) { chainRepository.findAll() }
        return Response.ok(chains.map { ChainResponse.from(it) }).build()
    }
}
