package com.workflow.workflow.dsl

import com.workflow.workflow.model.DEFAULT_BRANCH
import com.workflow.workflow.model.Edge
import com.workflow.workflow.model.FanOutDefinition
import java.time.Duration
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class WorkflowDslBuildersTest {

    // ── Spec item 31: Linear workflow ────────────────────────────────────

    @Test
    fun `linear workflow builds correctly`() {
        val def = workflow {
            activity("step-1") {
                transition("process.step1")
                retries(2)
                deadline(Duration.ofMinutes(10))
                next("step-2")
            }
            activity("step-2") {
                transition("process.step2")
            }
        }

        assertEquals("step-1", def.start)
        assertEquals(2, def.activities.size)

        val first = def.activities["step-1"]!!
        assertEquals("process.step1", first.transition)
        assertEquals(2, first.retries)
        assertEquals(Duration.ofMinutes(10), first.deadline)
        assertNull(first.fanOut)
        assertEquals(listOf(Edge("step-2", DEFAULT_BRANCH)), first.successors)

        val second = def.activities["step-2"]!!
        assertEquals("process.step2", second.transition)
        assertTrue(second.successors.isEmpty())
    }

    // ── Spec item 32: Conditional workflow ───────────────────────────────

    @Test
    fun `conditional workflow builds with correct edge labels`() {
        val def = workflow {
            activity("validate") {
                transition("v.h")
                on("OK") { next("charge") }
                on("INVALID") { next("reject") }
            }
            activity("charge") { transition("c.h") }
            activity("reject") { transition("r.h") }
        }

        val validate = def.activities["validate"]!!
        assertEquals(2, validate.successors.size)
        val okEdge = validate.successors.first { it.label == "OK" }
        val invalidEdge = validate.successors.first { it.label == "INVALID" }
        assertEquals("charge", okEdge.target)
        assertEquals("reject", invalidEdge.target)
    }

    // ── Spec item 33: Unconditional fork ─────────────────────────────────

    @Test
    fun `fork builds with multiple DEFAULT_BRANCH edges`() {
        val def = workflow {
            activity("prepare") {
                transition("p.h")
                next("send-email")
                next("update-crm")
            }
            activity("send-email") { transition("e.h") }
            activity("update-crm") { transition("c.h") }
        }

        val prepare = def.activities["prepare"]!!
        assertEquals(2, prepare.successors.size)
        assertTrue(prepare.successors.all { it.label == DEFAULT_BRANCH })
        assertEquals(setOf("send-email", "update-crm"), prepare.successors.map { it.target }.toSet())
    }

    // ── Spec item 34: Fan-out with FanOutDefinition ───────────────────────

    @Test
    fun `fan-out builds with FanOutDefinition embedded and next() as successor`() {
        val def = workflow {
            activity("scatter") {
                transition("DispatchScatterHandler")
                fanOut {
                    transition("DispatchSimulationHandler")
                    retries(2)
                }
                next("join")
            }
            activity("join") { transition("DispatchJoinHandler") }
        }

        val scatter = def.activities["scatter"]!!
        assertNotNull(scatter.fanOut)
        assertEquals("DispatchSimulationHandler", scatter.fanOut!!.transition)
        assertEquals(2, scatter.fanOut!!.retries)
        assertEquals(listOf(Edge("join", DEFAULT_BRANCH)), scatter.successors)
    }

    // ── Spec item 35: Migrated dispatchWorkflow builds ────────────────────

    @Test
    fun `migrated dispatchWorkflow builds and scatter batchToken resolves from scatter`() {
        val def = workflow {
            start("scatter")
            activity("scatter") {
                transition("DispatchScatterHandler")
                fanOut {
                    transition("DispatchSimulationHandler")
                    retries(2)
                }
                next("join")
            }
            activity("join") {
                transition("DispatchJoinHandler")
                deadline(Duration.ofMinutes(10))
                inputs { "batchToken" from "scatter.batchToken" }
            }
        }

        assertEquals("scatter", def.start)
        assertEquals("scatter.batchToken", def.activities["join"]!!.inputs["batchToken"])
    }

    // ── Spec item 36: Mixed on() + next() is rejected ────────────────────

    @Test
    fun `mixing on() and next() on same activity is rejected at build time`() {
        assertFailsWith<IllegalArgumentException> {
            workflow {
                activity("a") {
                    transition("a.h")
                    next("b")
                    on("OK") { next("c") }
                }
                activity("b") { transition("b.h") }
                activity("c") { transition("c.h") }
            }
        }
    }

    // ── Additional DSL tests ──────────────────────────────────────────────

    @Test
    fun `missing transition throws`() {
        assertFailsWith<IllegalArgumentException> {
            workflow {
                activity("step") { retries(1) }
            }
        }
    }

    @Test
    fun `empty workflow throws`() {
        assertFailsWith<IllegalArgumentException> {
            workflow { }
        }
    }

    @Test
    fun `workflow deadline defaults to 1 hour`() {
        val def = workflow {
            activity("step") { transition("h") }
        }
        assertEquals(Duration.ofHours(1), def.deadline)
    }

    @Test
    fun `workflow deadline customizable`() {
        val def = workflow {
            deadline(Duration.ofMinutes(30))
            activity("step") { transition("h") }
        }
        assertEquals(Duration.ofMinutes(30), def.deadline)
    }

    @Test
    fun `inputs DSL works on new builder`() {
        val def = workflow {
            activity("step") {
                transition("h")
                inputs {
                    "x" from "prev.field"
                    "y" from "prev"
                }
            }
        }
        val inputs = def.activities["step"]!!.inputs
        assertEquals("prev.field", inputs["x"])
        assertEquals("prev", inputs["y"])
    }

    @Test
    fun `BranchBuilder supports multiple next() calls for fork on label`() {
        val def = workflow {
            activity("charge") {
                transition("c.h")
                on("SUCCESS") { next("notify"); next("audit") }
                on("FAILED") { next("reject") }
            }
            activity("notify") { transition("n.h") }
            activity("audit")  { transition("a.h") }
            activity("reject") { transition("r.h") }
        }
        val charge = def.activities["charge"]!!
        val successEdges = charge.successors.filter { it.label == "SUCCESS" }
        assertEquals(2, successEdges.size)
        assertEquals(setOf("notify", "audit"), successEdges.map { it.target }.toSet())
    }
}
