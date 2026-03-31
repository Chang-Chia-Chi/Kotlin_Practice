package com.workflow.workflow.dsl

import com.workflow.workflow.dsl.workflow
import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.WorkflowDefinition
import java.time.Duration
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class WorkflowDslBuildersTest {

    @Test
    fun `linear workflow with two activities`() {
        val definition = workflow {
            activity("step-1") {
                transition("process.step1")
                retries(2)
                failurePolicy(FailurePolicy.ABORT)
                deadline(Duration.ofMinutes(10))
            }
            activity("step-2") {
                transition("process.step2")
                retries(0)
                failurePolicy(FailurePolicy.BEST_EFFORT)
                deadline(Duration.ofMinutes(5))
            }
        }

        assertEquals(2, definition.activities.size)

        val first = definition.activities[0]
        assertEquals("step-1", first.name)
        assertEquals("process.step1", first.transition)
        assertEquals(2, first.retries)
        assertEquals(FailurePolicy.ABORT, first.failurePolicy)
        assertEquals(Duration.ofMinutes(10), first.deadline)
        assertNull(first.fanOut)

        val second = definition.activities[1]
        assertEquals("step-2", second.name)
        assertEquals("process.step2", second.transition)
        assertEquals(0, second.retries)
        assertEquals(FailurePolicy.BEST_EFFORT, second.failurePolicy)
        assertEquals(Duration.ofMinutes(5), second.deadline)
        assertNull(second.fanOut)
    }

    @Test
    fun `fan-out with Percentage join policy`() {
        val definition = workflow {
            activity("scatter") {
                transition("scatter.dispatch")
                fanOut("parallel")
            }
            activity("parallel") {
                transition("scatter.process")
                retries(3)
                failurePolicy(FailurePolicy.BEST_EFFORT)
                deadline(Duration.ofMinutes(15))
                joinPolicy(JoinPolicy.Percentage(95))
            }
        }

        assertEquals(2, definition.activities.size)
        val scatter = definition.activities[0]
        assertEquals("scatter", scatter.name)
        assertEquals("scatter.dispatch", scatter.transition)
        assertEquals("parallel", scatter.fanOut)

        val parallel = definition.activities[1]
        assertEquals("scatter.process", parallel.transition)
        assertEquals(3, parallel.retries)
        assertEquals(FailurePolicy.BEST_EFFORT, parallel.failurePolicy)
        assertEquals(Duration.ofMinutes(15), parallel.deadline)
        assertEquals(JoinPolicy.Percentage(95), parallel.joinPolicy)
    }

    @Test
    fun `fan-out with default joinPolicy when omitted`() {
        val definition = workflow {
            activity("scatter") {
                transition("scatter.dispatch")
                fanOut("parallel")
            }
            activity("parallel") {
                transition("parallel.process")
            }
        }

        assertEquals(JoinPolicy.All, definition.activities[1].joinPolicy)
    }

    @Test
    fun `missing activity transition throws IllegalArgumentException`() {
        assertFailsWith<IllegalArgumentException> {
            workflow {
                activity("no-transition") {
                    retries(1)
                }
            }
        }
    }

    @Test
    fun `empty workflow throws IllegalArgumentException`() {
        assertFailsWith<IllegalArgumentException> {
            workflow { }
        }
    }

    @Test
    fun `fanOut target must reference existing activity`() {
        assertThrows<IllegalArgumentException> {
            workflow {
                activity("scatter") {
                    transition("scatter.handler")
                    fanOut("nonexistent")
                }
            }
        }
    }

    @Test
    fun `fanOut target must be next activity`() {
        assertThrows<IllegalArgumentException> {
            workflow {
                activity("scatter") {
                    transition("scatter.handler")
                    fanOut("join")
                }
                activity("parallel") { transition("parallel.handler") }
                activity("join") { transition("join.handler") }
            }
        }
    }

    @Test
    fun `chained fanOut not allowed`() {
        assertThrows<IllegalArgumentException> {
            workflow {
                activity("scatter1") {
                    transition("s1.handler")
                    fanOut("scatter2")
                }
                activity("scatter2") {
                    transition("s2.handler")
                    fanOut("parallel")
                }
                activity("parallel") { transition("p.handler") }
            }
        }
    }

    @Test
    fun `workflow deadline defaults to 1 hour`() {
        val def = workflow {
            activity("step1") { transition("handler1") }
        }
        assertEquals(Duration.ofHours(1), def.deadline)
    }

    @Test
    fun `workflow deadline can be customized`() {
        val def = workflow {
            deadline(Duration.ofMinutes(30))
            activity("step1") { transition("handler1") }
        }
        assertEquals(Duration.ofMinutes(30), def.deadline)
    }

    @Test
    fun `workflow deadline must be positive`() {
        assertThrows<IllegalArgumentException> {
            workflow {
                deadline(Duration.ZERO)
                activity("step1") { transition("handler1") }
            }
        }
    }

    @Test
    fun `workflow deadline negative throws`() {
        assertThrows<IllegalArgumentException> {
            workflow {
                deadline(Duration.ofMinutes(-1))
                activity("step1") { transition("handler1") }
            }
        }
    }

    // ── Inputs DSL ──────────────────────────────────────────────────────

    @Test
    fun `activity with no inputs has empty inputs map`() {
        val def = workflow {
            activity("step1") {
                transition("step1.handler")
            }
        }
        assertTrue(def.activities[0].inputs.isEmpty())
    }

    @Test
    fun `activity with field-level inputs`() {
        val def = workflow {
            activity("notify") {
                transition("notify.handler")
                inputs {
                    "chunks" from "split.uri"
                    "count" from "split.total"
                }
            }
        }
        val inputs = def.activities[0].inputs
        assertEquals(2, inputs.size)
        assertEquals("split.uri", inputs["chunks"])
        assertEquals("split.total", inputs["count"])
    }

    @Test
    fun `activity with whole-result input`() {
        val def = workflow {
            activity("aggregate") {
                transition("agg.handler")
                inputs {
                    "data" from "split"
                }
            }
        }
        assertEquals("split", def.activities[0].inputs["data"])
    }

    @Test
    fun `inputs from multiple activities`() {
        val def = workflow {
            activity("final") {
                transition("final.handler")
                inputs {
                    "a" from "step1.field"
                    "b" from "step2"
                }
            }
        }
        val inputs = def.activities[0].inputs
        assertEquals("step1.field", inputs["a"])
        assertEquals("step2", inputs["b"])
    }

    @Test
    fun `inputs serializes correctly via Jackson`() {
        val objectMapper = com.fasterxml.jackson.databind.ObjectMapper()
            .registerModule(com.fasterxml.jackson.module.kotlin.KotlinModule.Builder().build())
            .registerModule(com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
        val def = workflow {
            activity("step") {
                transition("s.handler")
                inputs {
                    "x" from "prev.field"
                }
            }
        }
        val json = objectMapper.writeValueAsString(def)
        val restored = objectMapper.readValue(json, WorkflowDefinition::class.java)
        assertEquals("prev.field", restored.activities[0].inputs["x"])
    }

    @Test
    fun `duplicate activity names throws`() {
        assertThrows<IllegalArgumentException> {
            workflow {
                activity("step1") { transition("a.handler") }
                activity("step1") { transition("b.handler") }
            }
        }
    }
}
