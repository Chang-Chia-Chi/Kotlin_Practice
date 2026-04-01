package com.workflow.dispatch.dsl

import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.dsl.workflow
import java.time.Duration

val dispatchWorkflow: WorkflowDefinition = workflow {
    deadline(Duration.ofHours(2))

    activity("scatter") {
        transition("DispatchScatterHandler")
        fanOut("simulate")
    }

    activity("simulate") {
        transition("DispatchSimulationHandler")
        retries(2)
        deadline(Duration.ofMinutes(30))
        joinPolicy(JoinPolicy.All)
    }

    activity("join") {
        transition("DispatchJoinHandler")
        deadline(Duration.ofMinutes(10))
        inputs {
            "batchToken" from "simulate.batchToken"
        }
    }
}
