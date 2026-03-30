package com.workflow.dispatch.algorithm

import com.workflow.dispatch.model.DispatchMode

class AlgorithmBuilder(mode: DispatchMode) {
    var gapComputer: GapComputer = when (mode) {
        DispatchMode.QTY -> QtyGapComputer()
        DispatchMode.RATIO -> RatioGapComputer()
    }
    var candidateMatcher: CandidateMatcher = when (mode) {
        DispatchMode.QTY -> QtyCandidateMatcher()
        DispatchMode.RATIO -> DefaultCandidateMatcher()
    }
    var terminationStrategy: TerminationStrategy = FailFastTermination()
}

fun dispatchAlgorithm(
    mode: DispatchMode,
    block: AlgorithmBuilder.() -> Unit = {},
): DispatchAlgorithm {
    val builder = AlgorithmBuilder(mode).apply(block)
    return DefaultDispatchAlgorithm(
        gapComputer = builder.gapComputer,
        candidateMatcher = builder.candidateMatcher,
        terminationStrategy = builder.terminationStrategy,
    )
}
