package com.workflow.dsl

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class WorkflowDslTest {

    private val mapper = jacksonObjectMapper().registerModule(JavaTimeModule())

    // -- Round-trip helpers --

    private inline fun <reified T> roundTrip(value: T): T {
        val json = mapper.writeValueAsString(value)
        return mapper.readValue(json)
    }

    // -- Linear workflow round-trip --

    @Test
    fun `linear workflow serialization round-trip`() {
        val definition = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(
                    name = "step-1",
                    transition = "process.step1",
                    retries = 2,
                    failurePolicy = FailurePolicy.ABORT,
                    deadline = Duration.ofMinutes(10),
                    fanOut = null,
                ),
                ActivityDefinition(
                    name = "step-2",
                    transition = "process.step2",
                    retries = 0,
                    failurePolicy = FailurePolicy.BEST_EFFORT,
                    deadline = Duration.ofMinutes(30),
                    fanOut = null,
                ),
            ),
        )

        val result = roundTrip(definition)
        assertEquals(definition, result)
    }

    // -- Fan-out with JoinPolicy.All --

    @Test
    fun `fan-out with JoinPolicy All round-trip`() {
        val definition = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(
                    name = "scatter-all",
                    transition = "fan.scatter",
                    retries = 1,
                    failurePolicy = FailurePolicy.ABORT,
                    deadline = Duration.ofMinutes(15),
                    fanOut = FanOutDefinition(
                        transition = "fan.process",
                        retries = 3,
                        failurePolicy = FailurePolicy.ABORT,
                        deadline = Duration.ofMinutes(5),
                        joinPolicy = JoinPolicy.All,
                    ),
                ),
            ),
        )

        val result = roundTrip(definition)
        assertEquals(definition, result)
    }

    // -- Fan-out with JoinPolicy.Percentage --

    @Test
    fun `fan-out with JoinPolicy Percentage round-trip`() {
        val definition = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(
                    name = "scatter-pct",
                    transition = "pct.scatter",
                    retries = 0,
                    failurePolicy = FailurePolicy.BEST_EFFORT,
                    deadline = Duration.ofMinutes(20),
                    fanOut = FanOutDefinition(
                        transition = "pct.process",
                        retries = 2,
                        failurePolicy = FailurePolicy.BEST_EFFORT,
                        deadline = Duration.ofMinutes(10),
                        joinPolicy = JoinPolicy.Percentage(95),
                    ),
                ),
            ),
        )

        val result = roundTrip(definition)
        assertEquals(definition, result)
    }

    // -- Fan-out with JoinPolicy.Threshold --

    @Test
    fun `fan-out with JoinPolicy Threshold round-trip`() {
        val definition = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(
                    name = "scatter-thr",
                    transition = "thr.scatter",
                    retries = 0,
                    failurePolicy = FailurePolicy.ABORT,
                    deadline = Duration.ofHours(1),
                    fanOut = FanOutDefinition(
                        transition = "thr.process",
                        retries = 1,
                        failurePolicy = FailurePolicy.ABORT,
                        deadline = Duration.ofMinutes(30),
                        joinPolicy = JoinPolicy.Threshold(40),
                    ),
                ),
            ),
        )

        val result = roundTrip(definition)
        assertEquals(definition, result)
    }

    // -- JoinPolicy.Threshold validation --

    @Test
    fun `Threshold with zero throws IllegalArgumentException`() {
        assertFailsWith<IllegalArgumentException> {
            JoinPolicy.Threshold(0)
        }
    }

    @Test
    fun `Threshold with negative throws IllegalArgumentException`() {
        assertFailsWith<IllegalArgumentException> {
            JoinPolicy.Threshold(-1)
        }
    }

    // -- JoinPolicy.Percentage validation --

    @Test
    fun `Percentage with zero throws IllegalArgumentException`() {
        assertFailsWith<IllegalArgumentException> {
            JoinPolicy.Percentage(0)
        }
    }

    @Test
    fun `Percentage with 101 throws IllegalArgumentException`() {
        assertFailsWith<IllegalArgumentException> {
            JoinPolicy.Percentage(101)
        }
    }

    // -- WorkflowDefinition validation --

    @Test
    fun `WorkflowDefinition with empty activities throws IllegalArgumentException`() {
        assertFailsWith<IllegalArgumentException> {
            WorkflowDefinition(activities = emptyList())
        }
    }
}
