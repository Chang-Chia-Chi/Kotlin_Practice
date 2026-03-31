package com.workflow.workflow.model

sealed interface AdvancementDecision {
    data class Advance(val nextSequence: Int) : AdvancementDecision
    data object Complete : AdvancementDecision
    data class Abort(val reason: String) : AdvancementDecision
}
