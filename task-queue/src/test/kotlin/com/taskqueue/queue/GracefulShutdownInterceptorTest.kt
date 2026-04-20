package com.taskqueue.queue

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.quarkus.runtime.ShutdownEvent
import jakarta.interceptor.InvocationContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class GracefulShutdownInterceptorTest {

    private lateinit var interceptor: GracefulShutdownInterceptor

    @BeforeEach
    fun setUp() {
        interceptor = GracefulShutdownInterceptor()
        interceptor.setTimeoutSeconds(1)
    }

    private fun mockContext(proceedResult: Any? = null, onProceed: (() -> Unit)? = null): InvocationContext {
        val ctx = mockk<InvocationContext>(relaxed = true)
        every { ctx.method } returns GracefulShutdownInterceptorTest::class.java
            .getDeclaredMethod("setUp") // any method, just needs to be non-null
        every { ctx.proceed() } answers {
            onProceed?.invoke()
            proceedResult
        }
        return ctx
    }

    @Test
    fun `intercept proceeds when not shutting down`() {
        val ctx = mockContext(proceedResult = "result")

        val result = interceptor.intercept(ctx)

        assertThat(result).isEqualTo("result")
        verify(exactly = 1) { ctx.proceed() }
    }

    @Test
    fun `intercept skips when shutting down`() {
        interceptor.onShutdown(ShutdownEvent())

        val ctx = mockContext(proceedResult = "result")
        val result = interceptor.intercept(ctx)

        assertThat(result).isNull()
        verify(exactly = 0) { ctx.proceed() }
    }

    @Test
    fun `in-flight count is tracked during invocation`() {
        val insideMethod = CountDownLatch(1)
        val canFinish = CountDownLatch(1)

        val ctx = mockContext(onProceed = {
            insideMethod.countDown()
            canFinish.await()
        })

        val t = thread {
            interceptor.intercept(ctx)
        }

        insideMethod.await()
        assertThat(interceptor.getInFlightCount()).isEqualTo(1)

        canFinish.countDown()
        t.join()
        assertThat(interceptor.getInFlightCount()).isEqualTo(0)
    }

    @Test
    fun `in-flight count decrements even when proceed throws`() {
        val ctx = mockContext()
        every { ctx.proceed() } throws RuntimeException("boom")

        try {
            interceptor.intercept(ctx)
        } catch (_: RuntimeException) {
        }

        assertThat(interceptor.getInFlightCount()).isEqualTo(0)
    }

    @Test
    fun `onShutdown waits for in-flight invocations to drain`() {
        val insideMethod = CountDownLatch(1)
        val canFinish = CountDownLatch(1)

        val ctx = mockContext(onProceed = {
            insideMethod.countDown()
            canFinish.await()
        })

        // Start an in-flight invocation
        val worker = thread {
            interceptor.intercept(ctx)
        }

        insideMethod.await()
        assertThat(interceptor.getInFlightCount()).isEqualTo(1)

        // Start shutdown in another thread
        val shutdownDone = AtomicBoolean(false)
        val shutdownThread = thread {
            interceptor.onShutdown(ShutdownEvent())
            shutdownDone.set(true)
        }

        // Give shutdown time to start waiting
        Thread.sleep(100)
        assertThat(shutdownDone.get()).isFalse()

        // Let the in-flight invocation finish
        canFinish.countDown()
        worker.join()

        shutdownThread.join(2000)
        assertThat(shutdownDone.get()).isTrue()
        assertThat(interceptor.getInFlightCount()).isEqualTo(0)
    }

    @Test
    fun `onShutdown times out if invocations do not drain`() {
        val insideMethod = CountDownLatch(1)
        val canFinish = CountDownLatch(1)

        val ctx = mockContext(onProceed = {
            insideMethod.countDown()
            canFinish.await()
        })

        // Start a long-running invocation
        val worker = thread {
            interceptor.intercept(ctx)
        }

        insideMethod.await()

        // Shutdown with 1s timeout — should return before the invocation finishes
        val start = System.currentTimeMillis()
        interceptor.onShutdown(ShutdownEvent())
        val elapsed = System.currentTimeMillis() - start

        assertThat(interceptor.isShuttingDown()).isTrue()
        assertThat(interceptor.getInFlightCount()).isEqualTo(1)
        assertThat(elapsed).isGreaterThanOrEqualTo(900) // waited ~1s
        assertThat(elapsed).isLessThan(3000)             // didn't wait forever

        // Cleanup
        canFinish.countDown()
        worker.join()
    }

    @Test
    fun `onShutdown returns immediately when no invocations are in-flight`() {
        val start = System.currentTimeMillis()
        interceptor.onShutdown(ShutdownEvent())
        val elapsed = System.currentTimeMillis() - start

        assertThat(interceptor.isShuttingDown()).isTrue()
        assertThat(elapsed).isLessThan(500)
    }
}
