package com.example.gauntlet.adapter

import arrow.core.Either
import com.example.gauntlet.application.BuildDailySummaryUseCase
import com.example.gauntlet.application.NewOrderCommand
import com.example.gauntlet.application.ProcessOrderUseCase
import com.example.gauntlet.domain.DailySummary
import com.example.gauntlet.domain.DomainError
import com.example.gauntlet.domain.Order
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.GET
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import java.time.LocalDate

@Path("/orders")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class OrderResource(
    private val processOrder: ProcessOrderUseCase,
    private val buildDailySummary: BuildDailySummaryUseCase,
) {

    @POST
    fun create(request: NewOrderRequest): Response =
        processOrder.execute(
            NewOrderCommand(
                id = request.id,
                customerId = request.customerId,
                amountCents = request.amountCents,
                orderDate = request.orderDate?.let { runCatching { LocalDate.parse(it) }.getOrNull() },
            ),
        ).toResponse(Response.Status.CREATED) { it.toResponseBody() }

    @GET
    @Path("/{id}")
    fun findById(@PathParam("id") id: String): Response =
        processOrder.findById(id).toResponse(Response.Status.OK) { it.toResponseBody() }

    @POST
    @Path("/summaries/{date}")
    fun buildSummary(@PathParam("date") date: String): Response {
        val parsed = runCatching { LocalDate.parse(date) }.getOrNull()
            ?: return problem(DomainError.InvalidDate("bad date: $date"))
        return buildDailySummary.execute(parsed)
            .toResponse(Response.Status.OK) { it.toResponseBody() }
    }

    private fun <T> Either<DomainError, T>.toResponse(
        success: Response.Status,
        body: (T) -> Any,
    ): Response = fold(
        ifLeft = { problem(it) },
        ifRight = { Response.status(success).entity(body(it)).build() },
    )

    private fun problem(error: DomainError): Response {
        val status = statusOf(error)
        return Response.status(status)
            .entity(
                ProblemResponse(
                    type = "https://example.com/problems/${error::class.simpleName}",
                    title = error::class.simpleName ?: "DomainError",
                    status = status.statusCode,
                    detail = error.message,
                ),
            )
            .type(MediaType.APPLICATION_JSON)
            .build()
    }

    private fun statusOf(error: DomainError): Response.Status = when (error) {
        is DomainError.OrderNotFound, is DomainError.NoDataForDate -> Response.Status.NOT_FOUND
        is DomainError.DuplicateOrder -> Response.Status.CONFLICT
        is DomainError.StorageFailure -> Response.Status.INTERNAL_SERVER_ERROR
        is DomainError.InvalidOrderId,
        is DomainError.InvalidCustomer,
        is DomainError.InvalidAmount,
        is DomainError.InvalidDate,
        -> Response.Status.BAD_REQUEST
    }

    private fun Order.toResponseBody() = OrderResponse(
        id = id,
        customerId = customerId,
        amountCents = amountCents,
        orderDate = orderDate.toString(),
    )

    private fun DailySummary.toResponseBody() = DailySummaryResponse(
        date = date.toString(),
        orderCount = orderCount,
        totalAmountCents = totalAmountCents,
        maxAmountCents = maxAmountCents,
        averageAmountCents = averageAmountCents,
    )
}
