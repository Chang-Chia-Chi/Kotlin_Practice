package com.workflow.dispatch.handler

import com.workflow.dsl.JoinPolicy
import com.workflow.dsl.WorkflowDefinition
import com.workflow.dsl.workflow
import java.time.Duration

val dispatchWorkflow: WorkflowDefinition = workflow {
    deadline(Duration.ofHours(2))

    activity("scatter") {
        transition("dispatch.scatter")
        fanOut("simulate")
    }

    activity("simulate") {
        transition("dispatch.simulate")
        retries(2)
        deadline(Duration.ofMinutes(30))
        joinPolicy(JoinPolicy.All)
    }

    activity("join") {
        transition("dispatch.join")
        deadline(Duration.ofMinutes(10))
        inputs {
            "batchToken" from "simulate.batchToken"
        }
    }
}
