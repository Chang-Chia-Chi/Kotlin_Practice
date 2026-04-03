package com.workflow.workflow.dsl

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.DEFAULT_BRANCH
import com.workflow.workflow.model.Edge
import com.workflow.workflow.model.FanOutDefinition
import com.workflow.workflow.model.WorkflowDefinition
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals

class WorkflowDslTest {

    private val mapper = jacksonObjectMapper().registerModule(JavaTimeModule())

    private inline fun <reified T> roundTrip(value: T): T {
        val json = mapper.writeValueAsString(value)
        return mapper.readValue(json)
    }

    @Test
    fun `linear workflow serialization round-trip`() {
        val def = workflow {
            activity("step-1") {
                transition("process.step1")
                retries(2)
                next("step-2")
            }
            activity("step-2") { transition("process.step2") }
        }
        assertEquals(def, roundTrip(def))
    }

    @Test
    fun `conditional workflow round-trip`() {
        val def = workflow {
            activity("validate") {
                transition("v.h")
                on("OK") { next("charge") }
                on("INVALID") { next("reject") }
            }
            activity("charge") { transition("c.h") }
            activity("reject") { transition("r.h") }
        }
        assertEquals(def, roundTrip(def))
    }

    @Test
    fun `fan-out workflow round-trip preserves FanOutDefinition`() {
        val def = workflow {
            activity("scatter") {
                transition("sc.h")
                fanOut { transition("par.h"); retries(2) }
                next("join")
            }
            activity("join") { transition("j.h") }
        }
        val restored = roundTrip(def)
        assertEquals(def, restored)
        assertEquals("par.h", restored.activities["scatter"]!!.fanOut!!.transition)
    }
}
