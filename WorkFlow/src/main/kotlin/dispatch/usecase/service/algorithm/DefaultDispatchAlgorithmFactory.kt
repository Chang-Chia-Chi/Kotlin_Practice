package com.workflow.dispatch.usecase.service.algorithm

import com.workflow.dispatch.dsl.dispatchAlgorithm
import com.workflow.dispatch.model.DispatchMode
import com.workflow.dispatch.usecase.port.inbound.algorithm.DispatchAlgorithm
import com.workflow.dispatch.usecase.port.inbound.algorithm.DispatchAlgorithmFactory
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class DefaultDispatchAlgorithmFactory : DispatchAlgorithmFactory {
    override fun create(mode: DispatchMode, algorithmId: String): DispatchAlgorithm =
        when (algorithmId) {
            "default" -> dispatchAlgorithm(mode)
            else -> throw IllegalArgumentException("Unknown algorithm: $algorithmId")
        }
}
