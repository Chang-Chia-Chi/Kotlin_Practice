package com.mapreduce.queue.metrics

import com.mapreduce.queue.repository.TaskRepository
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class QueueDepthExporterTest {

    private lateinit var taskRepository: TaskRepository
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var exporter: QueueDepthExporter

    @BeforeEach
    fun setUp() {
        taskRepository = mock()
        meterRegistry = SimpleMeterRegistry()

        exporter = QueueDepthExporter(taskRepository, meterRegistry)
    }

    @Test
    fun `polls queue depth from repository`() {
        whenever(taskRepository.countPendingByQueue()).thenReturn(mapOf("default" to 5))

        exporter.pollQueueDepth()

        verify(taskRepository).countPendingByQueue()
    }

    @Test
    fun `registers queue depth gauge per queue name`() {
        whenever(taskRepository.countPendingByQueue()).thenReturn(mapOf("mr" to 3, "default" to 7))

        exporter.pollQueueDepth()

        val mrGauge = meterRegistry.find("framework.queue.depth").tag("queue_name", "mr").gauge()
        assertNotNull(mrGauge)
        assertEquals(3.0, mrGauge!!.value())

        val defaultGauge = meterRegistry.find("framework.queue.depth").tag("queue_name", "default").gauge()
        assertNotNull(defaultGauge)
        assertEquals(7.0, defaultGauge!!.value())
    }

    @Test
    fun `resets gauge to zero when queue disappears`() {
        whenever(taskRepository.countPendingByQueue()).thenReturn(mapOf("mr" to 5))
        exporter.pollQueueDepth()

        // Second poll — "mr" queue no longer has pending tasks
        whenever(taskRepository.countPendingByQueue()).thenReturn(emptyMap())
        exporter.pollQueueDepth()

        val mrGauge = meterRegistry.find("framework.queue.depth").tag("queue_name", "mr").gauge()
        assertNotNull(mrGauge)
        assertEquals(0.0, mrGauge!!.value())
    }
}
