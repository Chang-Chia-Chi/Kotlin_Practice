package com.workflow.worker.usecase.service.execution

import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer

class MeteredTransitionHandler(
    private val delegate: TransitionHandler,
    private val handlerKey: String,
    private val meterRegistry: MeterRegistry,
) : TransitionHandler {

    private val successTimer: Timer = Timer.builder("taskqueue_handler_duration_seconds")
        .tag("handler", handlerKey)
        .tag("status", "success")
        .publishPercentileHistogram()
        .register(meterRegistry)

    private val failureTimer: Timer = Timer.builder("taskqueue_handler_duration_seconds")
        .tag("handler", handlerKey)
        .tag("status", "failure")
        .publishPercentileHistogram()
        .register(meterRegistry)

    override fun key(): String = delegate.key()

    override suspend fun execute(input: HandlerInput): HandlerResult {
        val sample = Timer.start(meterRegistry)
        try {
            val output = delegate.execute(input)
            sample.stop(successTimer)
            return output
        } catch (e: Exception) {
            sample.stop(failureTimer)
            throw e
        }
    }
}
