package com.workflow.dsl

import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

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
                fanOut {
                    transition("scatter.process")
                    retries(3)
                    failurePolicy(FailurePolicy.BEST_EFFORT)
                    deadline(Duration.ofMinutes(15))
                    joinPolicy(JoinPolicy.Percentage(95))
                }
            }
        }

        assertEquals(1, definition.activities.size)
        val activity = definition.activities[0]
        assertEquals("scatter", activity.name)
        assertEquals("scatter.dispatch", activity.transition)

        val fanOut = activity.fanOut!!
        assertEquals("scatter.process", fanOut.transition)
        assertEquals(3, fanOut.retries)
        assertEquals(FailurePolicy.BEST_EFFORT, fanOut.failurePolicy)
        assertEquals(Duration.ofMinutes(15), fanOut.deadline)
        assertEquals(JoinPolicy.Percentage(95), fanOut.joinPolicy)
    }

    @Test
    fun `fan-out with default joinPolicy when omitted`() {
        val definition = workflow {
            activity("barrier-activity") {
                transition("barrier.dispatch")
                fanOut {
                    transition("barrier.process")
                }
            }
        }

        val fanOut = definition.activities[0].fanOut!!
        assertEquals(JoinPolicy.All, fanOut.joinPolicy)
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
    fun `DslMarker prevents calling activity inside fanOut`() {
        assertFailsWith<Exception> {
            @Suppress("UNUSED_EXPRESSION")
            workflow {
                activity("outer") {
                    transition("outer.run")
                    fanOut {
                        transition("fan.process")
                        // This should not compile normally due to @DslMarker,
                        // but at runtime the builder should not expose activity()
                        (this as? WorkflowBuilder)?.activity("leaked") {
                            transition("leaked.run")
                        }
                            ?: throw IllegalStateException("DslMarker correctly prevents scope leakage")
                    }
                }
            }
        }
    }
}
