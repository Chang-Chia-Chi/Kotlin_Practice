package infra.shuttle.quarkus

import infra.shuttle.core.DeliveryId
import infra.shuttle.core.TransferId
import infra.shuttle.core.TransferState
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs.DefaultValue
import jakarta.ws.rs.GET
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.QueryParam
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import kotlinx.coroutines.runBlocking

/**
 * Spec 14.1's seven endpoints, every one under the admin role by the class-level annotation, so an eighth cannot
 * be added unprotected. Each changes exactly what the table says; none triggers a poll. 503 before the host is up.
 */
@Path("/admin/shuttle")
@RolesAllowed("shuttle-admin")
@Produces(MediaType.APPLICATION_JSON)
class AdminResource(private val lifecycle: ShuttleLifecycle) {

    private fun <T> withHost(block: suspend (ShuttleHost) -> T): T? = lifecycle.host?.let { host -> runBlocking { block(host) } }

    private fun <T> respond(block: suspend (ShuttleHost) -> T?): Response = withHost(block)
        ?.let { Response.ok(it).build() }
        ?: Response.status(if (lifecycle.host == null) 503 else 404).entity(mapOf("message" to if (lifecycle.host == null) "the host is not running" else "not found")).build()

    private fun outcome(outcome: ShuttleHost.Outcome?): Response = when (outcome) {
        ShuttleHost.Outcome.DONE -> Response.ok(mapOf("state" to "done")).build()
        ShuttleHost.Outcome.NOT_FOUND -> Response.status(404).entity(mapOf("message" to "not found")).build()
        ShuttleHost.Outcome.WRONG_STATE -> Response.status(409).entity(mapOf("message" to "not in a state this operation applies to")).build()
        null -> Response.status(503).entity(mapOf("message" to "the host is not running")).build()
    }

    @GET
    @Path("/routes")
    fun routes(): Response = respond { it.routes() }

    @GET
    @Path("/transfers")
    fun transfers(@QueryParam("route") route: String?, @QueryParam("state") state: String?, @QueryParam("limit") @DefaultValue("100") limit: Int): Response {
        val wanted = state?.let { s -> TransferState.entries.firstOrNull { it.name.equals(s, ignoreCase = true) } ?: return Response.status(400).entity(mapOf("message" to "no state named $s")).build() }
        return respond { it.transfers(route, wanted, limit) }
    }

    @GET
    @Path("/transfers/{id}/deliveries")
    fun deliveries(@PathParam("id") id: Long): Response = respond { it.deliveries(TransferId(id)) }

    @POST
    @Path("/transfers/{id}/redrive")
    fun redrive(@PathParam("id") id: Long): Response = outcome(withHost { it.redrive(TransferId(id)) })

    @POST
    @Path("/transfers/{id}/ack")
    fun ack(@PathParam("id") id: Long): Response = outcome(withHost { it.ack(TransferId(id)) })

    @POST
    @Path("/deliveries/{id}/redrive")
    fun redriveDelivery(@PathParam("id") id: Long): Response = outcome(withHost { it.redriveDelivery(DeliveryId(id)) })

    @POST
    @Path("/routes/{name}/restart")
    fun restart(@PathParam("name") name: String): Response =
        outcome(withHost { if (it.restart(name)) ShuttleHost.Outcome.DONE else ShuttleHost.Outcome.NOT_FOUND })
}
