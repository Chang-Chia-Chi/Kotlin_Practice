package com.workflow.dispatch.dsl

import com.workflow.dispatch.model.DispatchMode
import com.workflow.dispatch.usecase.port.inbound.algorithm.CandidateMatcher
import com.workflow.dispatch.usecase.port.inbound.algorithm.DispatchAlgorithm
import com.workflow.dispatch.usecase.port.inbound.algorithm.GapComputer
import com.workflow.dispatch.usecase.port.inbound.algorithm.TerminationStrategy
import com.workflow.dispatch.usecase.service.algorithm.FirstFitCandidateMatcher
import com.workflow.dispatch.usecase.service.algorithm.GapBasedDispatchAlgorithm
import com.workflow.dispatch.usecase.service.algorithm.FailFastTermination
import com.workflow.dispatch.usecase.service.algorithm.QtyCandidateMatcher
import com.workflow.dispatch.usecase.service.algorithm.QtyGapComputer
import com.workflow.dispatch.usecase.service.algorithm.RatioGapComputer

class AlgorithmBuilder(mode: DispatchMode) {
    var gapComputer: GapComputer = when (mode) {
        DispatchMode.QTY -> QtyGapComputer()
        DispatchMode.RATIO -> RatioGapComputer()
    }
    var candidateMatcher: CandidateMatcher = when (mode) {
        DispatchMode.QTY -> QtyCandidateMatcher()
        DispatchMode.RATIO -> FirstFitCandidateMatcher()
    }
    var terminationStrategy: TerminationStrategy = FailFastTermination()
}

fun dispatchAlgorithm(
    mode: DispatchMode,
    block: AlgorithmBuilder.() -> Unit = {},
): DispatchAlgorithm {
    val builder = AlgorithmBuilder(mode).apply(block)
    return GapBasedDispatchAlgorithm(
        gapComputer = builder.gapComputer,
        candidateMatcher = builder.candidateMatcher,
        terminationStrategy = builder.terminationStrategy,
    )
}
