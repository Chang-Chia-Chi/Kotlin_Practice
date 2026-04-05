package com.workflow.worker.adapter.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.port.inbound.trigger.TriggerTypes
import io.fabric8.kubernetes.api.model.ConfigMap
import io.fabric8.kubernetes.api.model.ConfigMapBuilder
import io.fabric8.kubernetes.api.model.ConfigMapList
import io.fabric8.kubernetes.api.model.batch.v1.Job
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobConditionBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobList
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.Watch
import io.fabric8.kubernetes.client.Watcher
import io.fabric8.kubernetes.client.WatcherException
import io.fabric8.kubernetes.api.model.DeletionPropagation
import io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL
import io.fabric8.kubernetes.client.dsl.MixedOperation
import io.fabric8.kubernetes.client.dsl.Resource
import io.fabric8.kubernetes.client.dsl.ScalableResource
import io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class K8sJobTriggerDriverTest {

    private val objectMapper: ObjectMapper = jacksonObjectMapper()
    private val kubernetesClient: KubernetesClient = mock()
    private lateinit var driver: K8sJobTriggerDriver

    // Shared mocks for the Fabric8 batch().v1().jobs() chain
    private val batchApi: BatchAPIGroupDSL = mock()
    private val v1Api: V1BatchAPIGroupDSL = mock()

    @BeforeEach
    fun setUp() {
        driver = K8sJobTriggerDriver(kubernetesClient, objectMapper)
        whenever(kubernetesClient.batch()).thenReturn(batchApi)
        whenever(batchApi.v1()).thenReturn(v1Api)
    }

    // ── Factories ───────────────────────────────────────────────────────

    private fun makeMeta(jobName: String = "training-t1", namespace: String = "default"): String =
        objectMapper.writeValueAsString(K8sJobMeta(jobName, namespace))

    private fun makeRef(
        taskId: String = "t-1",
        jobName: String = "training-t1",
        namespace: String = "default",
    ) = DeferredTaskRef(
        taskId = taskId,
        workflowId = "wf-1",
        sequenceNumber = 1,
        triggerType = TriggerTypes.K8S_JOB,
        triggerMeta = makeMeta(jobName, namespace),
        deadlineAt = Instant.now().plusSeconds(3600),
        retryCount = 0,
        maxRetries = 3,
    )

    private fun completedJob(jobName: String = "training-t1", namespace: String = "default"): Job =
        JobBuilder()
            .withNewMetadata().withName(jobName).withNamespace(namespace).endMetadata()
            .withNewStatus()
            .withConditions(JobConditionBuilder().withType("Complete").withStatus("True").build())
            .endStatus()
            .build()

    private fun failedJob(
        jobName: String = "training-t1",
        namespace: String = "default",
        reason: String = "BackoffLimitExceeded",
    ): Job =
        JobBuilder()
            .withNewMetadata().withName(jobName).withNamespace(namespace).endMetadata()
            .withNewStatus()
            .withConditions(
                JobConditionBuilder().withType("Failed").withStatus("True").withReason(reason).build(),
            )
            .endStatus()
            .build()

    // ── Fabric8 mock wiring helpers ─────────────────────────────────────

    /**
     * Wires the batch().v1().jobs().inNamespace(ns).withName(name).watch(watcher) chain.
     * Returns the [Watch] mock and an [argumentCaptor] that captures the [Watcher] passed to `.watch()`.
     */
    @Suppress("UNCHECKED_CAST")
    private fun mockJobWatchChain(
        jobName: String = "training-t1",
        namespace: String = "default",
    ): Pair<Watch, () -> Watcher<Job>> {
        val mockWatch: Watch = mock()
        val watcherCaptor = argumentCaptor<Watcher<Job>>()

        val jobResource = mock<ScalableResource<Job>>()
        val jobsOp = mock<MixedOperation<Job, JobList, ScalableResource<Job>>>()

        whenever(v1Api.jobs()).thenReturn(jobsOp as MixedOperation<Job, JobList, ScalableResource<Job>>)
        whenever(jobsOp.inNamespace(namespace)).thenReturn(jobsOp)
        whenever(jobsOp.withName(jobName)).thenReturn(jobResource)
        whenever(jobResource.watch(watcherCaptor.capture())).thenReturn(mockWatch)

        return mockWatch to { watcherCaptor.firstValue }
    }

    /**
     * Variant that supports multiple captures (for re-watch scenarios).
     */
    @Suppress("UNCHECKED_CAST")
    private fun mockJobWatchChainMulti(
        jobName: String = "training-t1",
        namespace: String = "default",
    ): Pair<List<Watch>, () -> List<Watcher<Job>>> {
        val watches = mutableListOf<Watch>()
        val watcherCaptor = argumentCaptor<Watcher<Job>>()

        val jobResource = mock<ScalableResource<Job>>()
        val jobsOp = mock<MixedOperation<Job, JobList, ScalableResource<Job>>>()

        whenever(v1Api.jobs()).thenReturn(jobsOp as MixedOperation<Job, JobList, ScalableResource<Job>>)
        whenever(jobsOp.inNamespace(namespace)).thenReturn(jobsOp)
        whenever(jobsOp.withName(jobName)).thenReturn(jobResource)

        // Each .watch() call returns a fresh Watch mock
        whenever(jobResource.watch(watcherCaptor.capture())).thenAnswer {
            val w: Watch = mock()
            watches.add(w)
            w
        }

        return watches to { watcherCaptor.allValues }
    }

    @Suppress("UNCHECKED_CAST")
    private fun mockConfigMap(
        jobName: String = "training-t1",
        namespace: String = "default",
        data: Map<String, String>? = null,
    ) {
        val cmResource = mock<Resource<ConfigMap>>()
        val cmOp = mock<MixedOperation<ConfigMap, ConfigMapList, Resource<ConfigMap>>>()

        whenever(kubernetesClient.configMaps())
            .thenReturn(cmOp as MixedOperation<ConfigMap, ConfigMapList, Resource<ConfigMap>>)
        whenever(cmOp.inNamespace(namespace)).thenReturn(cmOp)
        whenever(cmOp.withName("$jobName-output")).thenReturn(cmResource)

        if (data != null) {
            val builder = ConfigMapBuilder()
                .withNewMetadata().withName("$jobName-output").withNamespace(namespace).endMetadata()
            data.forEach { (k, v) -> builder.addToData(k, v) }
            whenever(cmResource.get()).thenReturn(builder.build())
        } else {
            whenever(cmResource.get()).thenReturn(null)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun mockJobDeleteChain(
        jobName: String = "training-t1",
        namespace: String = "default",
    ): ScalableResource<Job> {
        val jobResource = mock<ScalableResource<Job>>()
        val deletableResource = mock<ScalableResource<Job>>()
        val jobsOp = mock<MixedOperation<Job, JobList, ScalableResource<Job>>>()

        whenever(v1Api.jobs()).thenReturn(jobsOp as MixedOperation<Job, JobList, ScalableResource<Job>>)
        whenever(jobsOp.inNamespace(namespace)).thenReturn(jobsOp)
        whenever(jobsOp.withName(jobName)).thenReturn(jobResource)
        whenever(jobResource.withPropagationPolicy(eq(DeletionPropagation.BACKGROUND))).thenReturn(deletableResource)

        return deletableResource
    }

    // ── 1. type() ───────────────────────────────────────────────────────

    @Test
    fun `type returns k8s-job`() {
        assertEquals(TriggerTypes.K8S_JOB, driver.type())
    }

    // ── 2. start() with new tasks ───────────────────────────────────────

    @Test
    fun `start with new task registers Watch and increments trackedCount`() = runTest {
        mockJobWatchChain()
        driver.start(listOf(makeRef()))
        assertEquals(1, driver.trackedCount())
    }

    // ── 3. start() idempotent for already-tracked ───────────────────────

    @Test
    fun `start with already-tracked task does not create duplicate Watch`() = runTest {
        val (_, getWatchers) = mockJobWatchChainMulti()
        val ref = makeRef()

        driver.start(listOf(ref))
        driver.start(listOf(ref))

        assertEquals(1, driver.trackedCount())
        assertEquals(1, getWatchers().size, "Only one Watcher should have been registered")
    }

    // ── 4. Job Complete -> poll() returns Succeeded with ConfigMap result ─

    @Test
    fun `Job Complete condition produces Succeeded with ConfigMap result`() = runTest {
        val (_, getWatcher) = mockJobWatchChain()
        mockConfigMap(data = mapOf("result" to """{"accuracy":"0.95"}"""))

        driver.start(listOf(makeRef()))
        getWatcher().eventReceived(Watcher.Action.MODIFIED, completedJob())

        val results = driver.poll()
        assertEquals(1, results.size)
        val s = results[0] as TriggerResult.Succeeded
        assertEquals("t-1", s.taskId)
        assertEquals("""{"accuracy":"0.95"}""", s.result)
    }

    // ── 5. Job Failed condition -> poll() returns Failed with reason ─────

    @Test
    fun `Job Failed condition produces Failed with reason`() = runTest {
        val (_, getWatcher) = mockJobWatchChain()

        driver.start(listOf(makeRef()))
        getWatcher().eventReceived(Watcher.Action.MODIFIED, failedJob())

        val results = driver.poll()
        assertEquals(1, results.size)
        val f = results[0] as TriggerResult.Failed
        assertEquals("t-1", f.taskId)
        assertTrue(f.reason.contains("BackoffLimitExceeded"))
    }

    // ── 6. cancel() -> Job deleted, Watch closed, trackedCount == 0 ─────

    @Test
    fun `cancel deletes Job and closes Watch`() = runTest {
        val (mockWatch, _) = mockJobWatchChain()

        driver.start(listOf(makeRef()))
        assertEquals(1, driver.trackedCount())

        // Re-wire for the delete chain (cancel calls batch().v1().jobs() again)
        val deletable = mockJobDeleteChain()

        driver.cancel("t-1")

        verify(mockWatch).close()
        verify(deletable).delete()
        assertEquals(0, driver.trackedCount())
    }

    // ── 7. close() -> all Watches closed, trackedCount == 0 ─────────────

    @Test
    fun `close closes all Watches and clears tracked`() = runTest {
        val (mockWatch, _) = mockJobWatchChain()

        driver.start(listOf(makeRef()))
        assertEquals(1, driver.trackedCount())

        driver.close()

        verify(mockWatch).close()
        assertEquals(0, driver.trackedCount())
    }

    @Test
    fun `close is idempotent`() = runTest {
        val (mockWatch, _) = mockJobWatchChain()
        driver.start(listOf(makeRef()))

        driver.close()
        driver.close()

        assertEquals(0, driver.trackedCount())
    }

    // ── 8. ConfigMap absent -> Succeeded with result = null ──────────────

    @Test
    fun `ConfigMap absent produces Succeeded with null result`() = runTest {
        val (_, getWatcher) = mockJobWatchChain()
        mockConfigMap(data = null)

        driver.start(listOf(makeRef()))
        getWatcher().eventReceived(Watcher.Action.MODIFIED, completedJob())

        val results = driver.poll()
        assertEquals(1, results.size)
        val s = results[0] as TriggerResult.Succeeded
        assertNull(s.result)
    }

    // ── 9. Watch onClose does NOT produce Failed result ──────────────────

    @Test
    fun `Watch onClose with exception does not produce Failed result`() = runTest {
        val (_, getWatcher) = mockJobWatchChain()
        driver.start(listOf(makeRef()))

        getWatcher().onClose(WatcherException("Connection reset"))

        val results = driver.poll()
        assertTrue(results.isEmpty(), "Transient watch close should not produce a result")
    }

    @Test
    fun `Watch onClose with null cause does not produce Failed result`() = runTest {
        val (_, getWatcher) = mockJobWatchChain()
        driver.start(listOf(makeRef()))

        getWatcher().onClose(null)

        val results = driver.poll()
        assertTrue(results.isEmpty())
    }

    // ── 10. start() recreates Watch for task with closed Watch ───────────

    @Test
    fun `start recreates Watch for task whose Watch was closed`() = runTest {
        val (watches, getWatchers) = mockJobWatchChainMulti()
        mockConfigMap(data = null)

        val ref = makeRef()
        driver.start(listOf(ref))
        assertEquals(1, getWatchers().size)

        // Simulate watch close (triggers closedTaskIds)
        getWatchers()[0].onClose(WatcherException("Connection reset"))

        // Next sweep should detect closed watch and re-register
        driver.start(listOf(ref))
        assertEquals(2, getWatchers().size, "A new Watcher should have been registered after close")
        assertEquals(1, driver.trackedCount())

        // Verify the new watcher can deliver events
        getWatchers()[1].eventReceived(Watcher.Action.MODIFIED, completedJob())
        val results = driver.poll()
        assertEquals(1, results.size)
        assertTrue(results[0] is TriggerResult.Succeeded)
    }

    // ── 11. start() removes tracked tasks no longer in task list ─────────

    @Test
    fun `start removes tracked tasks no longer in the deferred list`() = runTest {
        val (mockWatch, _) = mockJobWatchChain()
        driver.start(listOf(makeRef()))
        assertEquals(1, driver.trackedCount())

        // Next sweep with empty list -> should remove the tracked task
        driver.start(emptyList())

        assertEquals(0, driver.trackedCount())
        verify(mockWatch).close()
    }

    // ── Additional edge cases ───────────────────────────────────────────

    @Test
    fun `poll with no events returns empty list`() = runTest {
        val results = driver.poll()
        assertTrue(results.isEmpty())
    }

    @Test
    fun `poll drains all queued events`() = runTest {
        val (_, getWatcher) = mockJobWatchChain()
        mockConfigMap(data = mapOf("result" to "ok"))

        driver.start(listOf(makeRef()))

        // Enqueue two events rapidly (complete then a second event is impossible since
        // the first removes tracking, but we test that poll drains everything queued)
        getWatcher().eventReceived(Watcher.Action.MODIFIED, completedJob())

        val results = driver.poll()
        assertEquals(1, results.size)

        // Second poll should be empty
        val results2 = driver.poll()
        assertTrue(results2.isEmpty())
    }

    @Test
    fun `duplicate terminal event on same watcher produces only one Succeeded`() = runTest {
        val (_, getWatcher) = mockJobWatchChain()
        mockConfigMap(data = null)

        driver.start(listOf(makeRef()))
        val watcher = getWatcher()
        watcher.eventReceived(Watcher.Action.MODIFIED, completedJob())
        watcher.eventReceived(Watcher.Action.MODIFIED, completedJob())

        val results = driver.poll()
        assertEquals(1, results.size)
        assertTrue(results[0] is TriggerResult.Succeeded)
    }

    @Test
    fun `Job with no status conditions does not produce result`() = runTest {
        val (_, getWatcher) = mockJobWatchChain()
        driver.start(listOf(makeRef()))

        val jobNoConditions = JobBuilder()
            .withNewMetadata().withName("training-t1").withNamespace("default").endMetadata()
            .withNewStatus()
            .endStatus()
            .build()

        getWatcher().eventReceived(Watcher.Action.MODIFIED, jobNoConditions)

        val results = driver.poll()
        assertTrue(results.isEmpty())
    }

    @Test
    fun `Job with null status does not produce result`() = runTest {
        val (_, getWatcher) = mockJobWatchChain()
        driver.start(listOf(makeRef()))

        val jobNullStatus = JobBuilder()
            .withNewMetadata().withName("training-t1").withNamespace("default").endMetadata()
            .build()

        getWatcher().eventReceived(Watcher.Action.MODIFIED, jobNullStatus)

        val results = driver.poll()
        assertTrue(results.isEmpty())
    }

    @Test
    fun `Job condition with status False is ignored`() = runTest {
        val (_, getWatcher) = mockJobWatchChain()
        driver.start(listOf(makeRef()))

        val jobNotReady = JobBuilder()
            .withNewMetadata().withName("training-t1").withNamespace("default").endMetadata()
            .withNewStatus()
            .withConditions(JobConditionBuilder().withType("Complete").withStatus("False").build())
            .endStatus()
            .build()

        getWatcher().eventReceived(Watcher.Action.MODIFIED, jobNotReady)

        val results = driver.poll()
        assertTrue(results.isEmpty())
    }

    @Test
    fun `cancel on unknown taskId is a no-op`() = runTest {
        driver.cancel("nonexistent")
        assertEquals(0, driver.trackedCount())
    }

    @Test
    fun `cancel swallows exception from Job delete`() = runTest {
        val (_, _) = mockJobWatchChain()
        driver.start(listOf(makeRef()))

        // Wire delete chain to throw
        val deletable = mockJobDeleteChain()
        whenever(deletable.delete()).thenThrow(RuntimeException("API timeout"))

        // Should not throw
        driver.cancel("t-1")
        assertEquals(0, driver.trackedCount())
    }

    @Test
    fun `readConfigMapOutput returns result key from ConfigMap`() {
        mockConfigMap(data = mapOf("result" to """{"output":"done"}"""))
        val result = driver.readConfigMapOutput("training-t1", "default")
        assertEquals("""{"output":"done"}""", result)
    }

    @Test
    fun `readConfigMapOutput returns null when ConfigMap is absent`() {
        mockConfigMap(data = null)
        val result = driver.readConfigMapOutput("training-t1", "default")
        assertNull(result)
    }

    @Test
    fun `readConfigMapOutput returns null when ConfigMap has no result key`() {
        mockConfigMap(data = mapOf("other-key" to "value"))
        val result = driver.readConfigMapOutput("training-t1", "default")
        assertNull(result)
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `readConfigMapOutput returns null on exception`() {
        val cmOp = mock<MixedOperation<ConfigMap, ConfigMapList, Resource<ConfigMap>>>()
        whenever(kubernetesClient.configMaps())
            .thenReturn(cmOp as MixedOperation<ConfigMap, ConfigMapList, Resource<ConfigMap>>)
        whenever(cmOp.inNamespace(any())).thenThrow(RuntimeException("API error"))

        val result = driver.readConfigMapOutput("training-t1", "default")
        assertNull(result)
    }

    @Test
    fun `Failed condition with null reason uses fallback`() = runTest {
        val (_, getWatcher) = mockJobWatchChain()
        driver.start(listOf(makeRef()))

        val failedNoReason = JobBuilder()
            .withNewMetadata().withName("training-t1").withNamespace("default").endMetadata()
            .withNewStatus()
            .withConditions(
                JobConditionBuilder().withType("Failed").withStatus("True").withReason(null).build(),
            )
            .endStatus()
            .build()

        getWatcher().eventReceived(Watcher.Action.MODIFIED, failedNoReason)

        val results = driver.poll()
        assertEquals(1, results.size)
        val f = results[0] as TriggerResult.Failed
        assertTrue(f.reason.isNotEmpty(), "Reason should have a fallback value")
    }

    @Test
    fun `close swallows exception from Watch close`() = runTest {
        val (mockWatch, _) = mockJobWatchChain()
        whenever(mockWatch.close()).thenThrow(RuntimeException("Already closed"))

        driver.start(listOf(makeRef()))

        // Should not throw
        driver.close()
        assertEquals(0, driver.trackedCount())
    }

    @Test
    fun `start with multiple tasks tracks all of them`() = runTest {
        // Need separate chains for two different job names
        val jobResource1 = mock<ScalableResource<Job>>()
        val jobResource2 = mock<ScalableResource<Job>>()
        @Suppress("UNCHECKED_CAST")
        val jobsOp = mock<MixedOperation<Job, JobList, ScalableResource<Job>>>()

        whenever(v1Api.jobs()).thenReturn(jobsOp as MixedOperation<Job, JobList, ScalableResource<Job>>)
        whenever(jobsOp.inNamespace("default")).thenReturn(jobsOp)
        whenever(jobsOp.withName("job-a")).thenReturn(jobResource1)
        whenever(jobsOp.withName("job-b")).thenReturn(jobResource2)
        whenever(jobResource1.watch(any<Watcher<Job>>())).thenReturn(mock<Watch>())
        whenever(jobResource2.watch(any<Watcher<Job>>())).thenReturn(mock<Watch>())

        val refs = listOf(
            makeRef(taskId = "t-1", jobName = "job-a"),
            makeRef(taskId = "t-2", jobName = "job-b"),
        )

        driver.start(refs)
        assertEquals(2, driver.trackedCount())
    }

    @Test
    fun `start removes stale task and adds new task in same sweep`() = runTest {
        val jobResource1 = mock<ScalableResource<Job>>()
        val jobResource2 = mock<ScalableResource<Job>>()
        val watch1: Watch = mock()
        @Suppress("UNCHECKED_CAST")
        val jobsOp = mock<MixedOperation<Job, JobList, ScalableResource<Job>>>()

        whenever(v1Api.jobs()).thenReturn(jobsOp as MixedOperation<Job, JobList, ScalableResource<Job>>)
        whenever(jobsOp.inNamespace("default")).thenReturn(jobsOp)
        whenever(jobsOp.withName("job-a")).thenReturn(jobResource1)
        whenever(jobsOp.withName("job-b")).thenReturn(jobResource2)
        whenever(jobResource1.watch(any<Watcher<Job>>())).thenReturn(watch1)
        whenever(jobResource2.watch(any<Watcher<Job>>())).thenReturn(mock<Watch>())

        driver.start(listOf(makeRef(taskId = "t-1", jobName = "job-a")))
        assertEquals(1, driver.trackedCount())

        // New sweep with different task
        driver.start(listOf(makeRef(taskId = "t-2", jobName = "job-b")))
        assertEquals(1, driver.trackedCount())
        verify(watch1).close()
    }

    @Test
    fun `ADDED action with Complete condition also triggers Succeeded`() = runTest {
        val (_, getWatcher) = mockJobWatchChain()
        mockConfigMap(data = mapOf("result" to "done"))

        driver.start(listOf(makeRef()))
        getWatcher().eventReceived(Watcher.Action.ADDED, completedJob())

        val results = driver.poll()
        assertEquals(1, results.size)
        assertTrue(results[0] is TriggerResult.Succeeded)
    }
}
