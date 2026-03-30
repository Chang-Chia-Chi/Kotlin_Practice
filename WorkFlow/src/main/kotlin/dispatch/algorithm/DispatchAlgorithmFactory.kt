package com.workflow.dispatch.algorithm

import com.workflow.dispatch.model.DispatchMode
import jakarta.enterprise.context.ApplicationScoped

interface DispatchAlgorithmFactory {
    fun create(mode: DispatchMode, algorithmId: String): DispatchAlgorithm
}

@ApplicationScoped
class DefaultDispatchAlgorithmFactory : DispatchAlgorithmFactory {
    override fun create(mode: DispatchMode, algorithmId: String): DispatchAlgorithm =
        when (algorithmId) {
            "default" -> dispatchAlgorithm(mode)
            else -> throw IllegalArgumentException("Unknown algorithm: $algorithmId")
        }
}
