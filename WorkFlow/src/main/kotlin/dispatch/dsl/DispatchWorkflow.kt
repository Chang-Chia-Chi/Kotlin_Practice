package com.workflow.dispatch.dsl

import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.dsl.workflow
import java.time.Duration

val dispatchWorkflow: WorkflowDefinition = workflow {
    deadline(Duration.ofHours(2))

    activity("scatter") {
        transition("DispatchScatterHandler")
        fanOut {
            transition("DispatchSimulationHandler")
            retries(2)
            deadline(Duration.ofMinutes(30))
        }
        next("join")
    }

    activity("join") {
        transition("DispatchJoinHandler")
        deadline(Duration.ofMinutes(10))
        inputs {
            "batchToken" from "scatter.batchToken"
        }
    }
}
