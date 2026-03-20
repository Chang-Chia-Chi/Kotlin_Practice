package com.mapreduce.workflow.registry

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.WorkflowStepRepository
import com.mapreduce.workflow.spi.WorkflowDefinition
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.flow.Flow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration

class WorkflowRegistryTest {

    private lateinit var handlerRegistry: HandlerRegistry
    private lateinit var stepRepo: WorkflowStepRepository
    private lateinit var config: FrameworkConfig
    private lateinit var definitions: Instance<WorkflowDefinition<*>>

    @BeforeEach
    fun setUp() {
        handlerRegistry = mock()
        stepRepo = mock()
        config = mock()
        definitions = mock()
        val workflowConfig = mock<FrameworkConfig.WorkflowConfig>()
        whenever(config.workflow()).thenReturn(workflowConfig)
        whenever(workflowConfig.defaultStepDeadline()).thenReturn(Duration.ofHours(1))
    }

    private fun fakeDefinition(
        name: String = "wc",
        pipeline: List<WorkflowDefinition.StepSpec> = listOf(
            WorkflowDefinition.StepSpec(name = "map", handler = "wc.map"),
            WorkflowDefinition.StepSpec(name = "reduce", handler = "wc.reduce"),
        ),
    ): WorkflowDefinition<Any> = object : WorkflowDefinition<Any> {
        override val workflowName = name
        override fun serializeParams(params: Any) = "{}"
        override fun deserializeParams(json: String) = "{}" as Any
        override fun pipeline() = pipeline
        override suspend fun initialTasks(params: Any) = emptyList<WorkflowDefinition.TaskPayload>()
        override suspend fun transitionTasks(
            stepIndex: Int, previousStepParams: String, previousOutputs: Flow<WorkflowDefinition.TaskOutput>,
        ) = WorkflowDefinition.StepTransition(emptyList())
        override suspend fun onCompleted(lastStepParams: String, finalOutputs: Flow<WorkflowDefinition.TaskOutput>) {}
    }

    private fun registryWith(vararg defs: WorkflowDefinition<*>): WorkflowRegistry {
        @Suppress("UNCHECKED_CAST")
        val iter = defs.toList().iterator() as MutableIterator<WorkflowDefinition<*>>
        whenever(definitions.iterator()).thenReturn(iter)
        val registry = WorkflowRegistry(definitions, handlerRegistry, stepRepo, config)
        registry.onStart(StartupEvent())
        return registry
    }

    // ── Discovery ───────────────────────────────────────────────

    @Test
    fun `onStart registers step transition handler for each definition`() {
        registryWith(fakeDefinition("wc"), fakeDefinition("etl"))

        verify(handlerRegistry, org.mockito.kotlin.times(2)).register(any())
    }

    @Test
    fun `getDefinition returns registered definition`() {
        val registry = registryWith(fakeDefinition("wc"))

        assertNotNull(registry.getDefinition("wc"))
        assertEquals("wc", registry.getDefinition("wc")!!.workflowName)
    }

    @Test
    fun `getDefinition returns null for unknown workflow`() {
        val registry = registryWith(fakeDefinition("wc"))

        assertNull(registry.getDefinition("unknown"))
    }

    @Test
    fun `supportedWorkflows returns all registered names`() {
        val registry = registryWith(fakeDefinition("wc"), fakeDefinition("etl"))

        val supported = registry.supportedWorkflows()
        assertEquals(2, supported.size)
        assertTrue(supported.containsAll(listOf("wc", "etl")))
    }

    // ── Validation ──────────────────────────────────────────────

    @Test
    fun `rejects workflow with empty pipeline`() {
        assertThrows<IllegalArgumentException> {
            registryWith(fakeDefinition("empty", pipeline = emptyList()))
        }
    }

    @Test
    fun `rejects workflow with duplicate step names`() {
        val pipeline = listOf(
            WorkflowDefinition.StepSpec(name = "map", handler = "wc.map"),
            WorkflowDefinition.StepSpec(name = "map", handler = "wc.map2"),
        )

        assertThrows<IllegalArgumentException> {
            registryWith(fakeDefinition("dup", pipeline = pipeline))
        }
    }

    @Test
    fun `rejects workflow with blank handler name`() {
        val pipeline = listOf(
            WorkflowDefinition.StepSpec(name = "map", handler = ""),
        )

        assertThrows<IllegalArgumentException> {
            registryWith(fakeDefinition("blank", pipeline = pipeline))
        }
    }

    private fun assertTrue(condition: Boolean) {
        org.junit.jupiter.api.Assertions.assertTrue(condition)
    }
}
