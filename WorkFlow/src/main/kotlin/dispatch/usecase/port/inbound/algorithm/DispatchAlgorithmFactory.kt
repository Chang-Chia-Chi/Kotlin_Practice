package com.workflow.dispatch.usecase.port.inbound.algorithm

import com.workflow.dispatch.model.DispatchMode

interface DispatchAlgorithmFactory {
    fun create(mode: DispatchMode, algorithmId: String): DispatchAlgorithm
}
