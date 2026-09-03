package infra.shuttle.testkit

import infra.shuttle.core.HookPoint.afterFetch
import infra.shuttle.core.HookPoint.afterStore
import infra.shuttle.core.TransferId
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class HookDriverTest {
    private val id = TransferId(7)

    @Test
    fun a_paused_point_suspends_the_arriving_coroutine_until_resumed() = runTest {
        val driver = HookDriver()
        driver.pauseAt(afterStore)
        var passed = false
        val job = launch {
            driver.at(afterFetch, id) // not paused: a no-op
            driver.at(afterStore, id)
            passed = true
        }
        assertEquals(id, driver.awaitArrival(afterStore))
        advanceUntilIdle()
        assertFalse(passed)
        assertTrue(job.isActive)

        driver.resume(afterStore)
        driver.resume(afterStore) // single release: the second is a no-op
        job.join()
        assertTrue(passed)
    }

    @Test
    fun cancelAt_cancels_the_arrived_coroutine_and_code_after_the_point_never_runs() = runTest {
        val driver = HookDriver()
        driver.pauseAt(afterStore)
        var after = false
        var caught: Throwable? = null
        val job = launch {
            try {
                driver.at(afterStore, id)
                after = true
            } catch (e: CancellationException) {
                caught = e
                throw e
            }
        }
        driver.awaitArrival(afterStore)
        driver.cancelAt(afterStore)
        job.join()
        assertTrue(job.isCancelled)
        assertNotNull(caught)
        assertFalse(after)
    }

    @Test
    fun crash_throws_a_CancellationException_inside_the_paused_coroutine_and_disarms_the_point() = runTest {
        val driver = HookDriver()
        driver.pauseAt(afterStore)
        var after = false
        val job = launch {
            driver.at(afterStore, id)
            after = true
        }
        driver.awaitArrival(afterStore)
        driver.crash(afterStore)
        job.join()
        assertTrue(job.isCancelled)
        assertFalse(after)

        // The next run passes the point: a pause is one-shot.
        var second = false
        launch { driver.at(afterStore, id); second = true }.join()
        assertTrue(second)
    }
}
