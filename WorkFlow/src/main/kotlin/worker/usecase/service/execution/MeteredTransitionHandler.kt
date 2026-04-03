package com.workflow.worker.usecase.service.execution

import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerOutput
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer

class MeteredTransitionHandler(
    private val delegate: TransitionHandler,
    private val handlerKey: String,
    private val meterRegistry: MeterRegistry,
) : TransitionHandler {

    override fun key(): String = delegate.key()

    override suspend fun execute(input: HandlerInput): HandlerOutput {
        val sample = Timer.start(meterRegistry)
        try {
            val output = delegate.execute(input)
            sample.stop(timer("success"))
            return output
        } catch (e: Exception) {
            sample.stop(timer("failure"))
            throw e
        }
    }

    private fun timer(status: String): Timer =
        Timer.builder("taskqueue_handler_duration_seconds")
            .tag("handler", handlerKey)
            .tag("status", status)
            .publishPercentileHistogram()
            .register(meterRegistry)
}
