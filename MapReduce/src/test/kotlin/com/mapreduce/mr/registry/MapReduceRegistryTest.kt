package com.mapreduce.mr.registry

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.mr.model.FailurePolicy
import com.mapreduce.mr.shuffle.BlobStore
import com.mapreduce.mr.spi.MapReduceDefinition
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.TaskGroupRepository
import com.mapreduce.queue.spi.TaskHandler
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.inject.Instance
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class MapReduceRegistryTest {

    private lateinit var definitions: Instance<MapReduceDefinition<*, *, *, *>>
    private lateinit var handlerRegistry: HandlerRegistry
    private lateinit var taskGroupRepository: TaskGroupRepository
    private lateinit var blobStore: BlobStore
    private lateinit var objectMapper: ObjectMapper

    private lateinit var registry: MapReduceRegistry

    @BeforeEach
    fun setUp() {
        definitions = mock()
        handlerRegistry = mock()
        taskGroupRepository = mock()
        blobStore = mock()
        objectMapper = mock()
    }

    private fun createRegistry(): MapReduceRegistry =
        MapReduceRegistry(definitions, handlerRegistry, taskGroupRepository, blobStore, objectMapper)

    private fun mockDefinition(
        jobType: String,
        maxRetries: Int = 3,
        queue: String = "mr",
        failurePolicy: FailurePolicy = FailurePolicy.FAIL_GROUP,
        failureThreshold: Double = 0.0,
    ): MapReduceDefinition<Any, Any, Any, Any> {
        val def = mock<MapReduceDefinition<Any, Any, Any, Any>>()
        whenever(def.jobType).thenReturn(jobType)
        whenever(def.maxRetries).thenReturn(maxRetries)
        whenever(def.queue).thenReturn(queue)
        whenever(def.failurePolicy).thenReturn(failurePolicy)
        whenever(def.failureThreshold).thenReturn(failureThreshold)
        return def
    }

    private fun stubDefinitions(vararg defs: MapReduceDefinition<*, *, *, *>) {
        whenever(definitions.iterator()).thenAnswer { defs.toList().iterator() }
    }

    @Nested
    inner class `no definitions` {

        @Test
        fun `supportedJobTypes returns empty list when no definitions registered`() {
            stubDefinitions()
            registry = createRegistry()

            registry.onStart(StartupEvent())

            assertTrue(registry.supportedJobTypes().isEmpty())
        }

        @Test
        fun `getDefinition returns null when no definitions registered`() {
            stubDefinitions()
            registry = createRegistry()

            registry.onStart(StartupEvent())

            assertNull(registry.getDefinition("anything"))
        }

        @Test
        fun `register is never called on handlerRegistry when no definitions exist`() {
            stubDefinitions()
            registry = createRegistry()

            registry.onStart(StartupEvent())

            verify(handlerRegistry, never()).register(any())
        }
    }

    @Nested
    inner class `single definition` {

        private lateinit var mockDef: MapReduceDefinition<Any, Any, Any, Any>

        @BeforeEach
        fun setUpDefinition() {
            mockDef = mockDefinition("wordcount")
            stubDefinitions(mockDef)
            registry = createRegistry()
        }

        @Test
        fun `registers exactly 3 handlers -- map, reduce, phase_complete`() {
            registry.onStart(StartupEvent())

            verify(handlerRegistry, times(3)).register(any())
        }

        @Test
        fun `registers a map handler with correct handlerName`() {
            registry.onStart(StartupEvent())

            verify(handlerRegistry).register(argThat<TaskHandler> {
                handlerName == "wordcount.map"
            })
        }

        @Test
        fun `registers a reduce handler with correct handlerName`() {
            registry.onStart(StartupEvent())

            verify(handlerRegistry).register(argThat<TaskHandler> {
                handlerName == "wordcount.reduce"
            })
        }

        @Test
        fun `registers a phase transition handler with correct handlerName`() {
            registry.onStart(StartupEvent())

            verify(handlerRegistry).register(argThat<TaskHandler> {
                handlerName == "wordcount.__phase_complete"
            })
        }

        @Test
        fun `getDefinition returns the registered definition`() {
            registry.onStart(StartupEvent())

            assertSame(mockDef, registry.getDefinition("wordcount"))
        }

        @Test
        fun `supportedJobTypes contains the registered job type`() {
            registry.onStart(StartupEvent())

            assertEquals(listOf("wordcount"), registry.supportedJobTypes())
        }

        @Test
        fun `getDefinition returns null for unknown job type`() {
            registry.onStart(StartupEvent())

            assertNull(registry.getDefinition("unknown"))
        }
    }

    @Nested
    inner class `multiple definitions` {

        private lateinit var defA: MapReduceDefinition<Any, Any, Any, Any>
        private lateinit var defB: MapReduceDefinition<Any, Any, Any, Any>
        private lateinit var defC: MapReduceDefinition<Any, Any, Any, Any>

        @BeforeEach
        fun setUpDefinitions() {
            defA = mockDefinition("wordcount")
            defB = mockDefinition("pagerank", maxRetries = 5, queue = "heavy")
            defC = mockDefinition("inverted-index")
            stubDefinitions(defA, defB, defC)
            registry = createRegistry()
        }

        @Test
        fun `registers 3 handlers per definition -- 9 total`() {
            registry.onStart(StartupEvent())

            verify(handlerRegistry, times(9)).register(any())
        }

        @Test
        fun `registers map handler for each definition`() {
            registry.onStart(StartupEvent())

            verify(handlerRegistry).register(argThat<TaskHandler> { handlerName == "wordcount.map" })
            verify(handlerRegistry).register(argThat<TaskHandler> { handlerName == "pagerank.map" })
            verify(handlerRegistry).register(argThat<TaskHandler> { handlerName == "inverted-index.map" })
        }

        @Test
        fun `registers reduce handler for each definition`() {
            registry.onStart(StartupEvent())

            verify(handlerRegistry).register(argThat<TaskHandler> { handlerName == "wordcount.reduce" })
            verify(handlerRegistry).register(argThat<TaskHandler> { handlerName == "pagerank.reduce" })
            verify(handlerRegistry).register(argThat<TaskHandler> { handlerName == "inverted-index.reduce" })
        }

        @Test
        fun `registers phase transition handler for each definition`() {
            registry.onStart(StartupEvent())

            verify(handlerRegistry).register(argThat<TaskHandler> { handlerName == "wordcount.__phase_complete" })
            verify(handlerRegistry).register(argThat<TaskHandler> { handlerName == "pagerank.__phase_complete" })
            verify(handlerRegistry).register(argThat<TaskHandler> { handlerName == "inverted-index.__phase_complete" })
        }

        @Test
        fun `supportedJobTypes returns all registered types`() {
            registry.onStart(StartupEvent())

            val types = registry.supportedJobTypes().sorted()
            assertEquals(listOf("inverted-index", "pagerank", "wordcount"), types)
        }

        @Test
        fun `getDefinition returns correct definition for each type`() {
            registry.onStart(StartupEvent())

            assertSame(defA, registry.getDefinition("wordcount"))
            assertSame(defB, registry.getDefinition("pagerank"))
            assertSame(defC, registry.getDefinition("inverted-index"))
        }

        @Test
        fun `getDefinition returns null for type not in registry`() {
            registry.onStart(StartupEvent())

            assertNull(registry.getDefinition("nonexistent"))
        }
    }

    @Nested
    inner class `pre-initialization` {

        @Test
        fun `supportedJobTypes returns empty list before onStart`() {
            stubDefinitions(mockDefinition("wordcount"))
            registry = createRegistry()

            assertTrue(registry.supportedJobTypes().isEmpty())
        }

        @Test
        fun `getDefinition returns null before onStart`() {
            stubDefinitions(mockDefinition("wordcount"))
            registry = createRegistry()

            assertNull(registry.getDefinition("wordcount"))
        }
    }
}
