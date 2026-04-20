# Peer Notification Resilience Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make peer HTTP broadcast fire-and-forget with sender-side signal coalescing, and add HTTP timeouts so dead peers fail fast.

**Architecture:** The existing per-queue `SharedFlow` in `HttpWorkerNotifier` gains a background broadcast collector as an additional subscriber. `signal()` becomes a single `tryEmit()` that returns immediately. The broadcast collector picks up emissions, coalesces rapid signals naturally via `DROP_OLDEST`, and fans out HTTP POSTs to peers in parallel. A 2s HTTP timeout caps dead-peer cost.

**Tech Stack:** Kotlin Coroutines (SharedFlow, SupervisorJob, CoroutineScope), Ktor HttpClient (HttpTimeout plugin)

**Spec:** `docs/superpowers/specs/2026-04-04-peer-notification-resilience-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/main/kotlin/worker/adapter/http/HttpWorkerNotifier.kt` | Modify | Replace `supervisorScope` fan-out with detached broadcast collector per queue |
| `src/main/kotlin/infrastructure/http/HttpClientProducer.kt` | Modify | Add `HttpTimeout` plugin (2s connect + 2s request) |
| `src/test/kotlin/worker/adapter/http/WorkerNotifierTest.kt` | Modify | Adjust peer broadcast tests for async collection, add 3 new test classes |
| `src/test/kotlin/stress/StressTestBase.kt` | Modify | Adapt `HttpWorkerNotifier` construction (no functional change) |

No new files. No new dependencies (HttpTimeout is in `ktor-client-core-jvm` already on classpath).

---

### Task 1: Add HTTP timeout to Ktor client

**Files:**
- Modify: `src/main/kotlin/infrastructure/http/HttpClientProducer.kt`

- [ ] **Step 1: Write the updated HttpClientProducer**

Replace the current producer with one that installs `HttpTimeout`:

```kotlin
package com.workflow.infrastructure.http

import io.ktor.client.HttpClient
import io.ktor.client.engine.java.Java
import io.ktor.client.plugins.timeout.HttpTimeout
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Disposes
import jakarta.enterprise.inject.Produces

class HttpClientProducer {
    @Produces
    @ApplicationScoped
    fun httpClient(): HttpClient = HttpClient(Java) {
        install(HttpTimeout) {
            connectTimeoutMillis = 2_000
            requestTimeoutMillis = 2_000
        }
    }

    fun close(@Disposes client: HttpClient) = client.close()
}
```

- [ ] **Step 2: Verify build compiles**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -f WorkFlow/pom.xml -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/infrastructure/http/HttpClientProducer.kt
git commit -m "feat(http): add 2s connect/request timeout to Ktor HttpClient"
```

---

### Task 2: Refactor HttpWorkerNotifier to fire-and-forget with broadcast collector

**Files:**
- Modify: `src/main/kotlin/worker/adapter/http/HttpWorkerNotifier.kt`

- [ ] **Step 1: Write the updated HttpWorkerNotifier**

Replace the entire file content:

```kotlin
package com.workflow.worker.adapter.http

import com.workflow.worker.usecase.port.outbound.notification.WorkerNotifier
import com.workflow.worker.usecase.port.outbound.peer.PeerDiscovery
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import jakarta.annotation.PreDestroy
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.net.URLEncoder
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class HttpWorkerNotifier(
    private val peerDiscovery: PeerDiscovery,
    private val httpClient: HttpClient,
) : WorkerNotifier {
    private val log = LoggerFactory.getLogger(HttpWorkerNotifier::class.java)

    private val broadcastScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val flows = ConcurrentHashMap<String, MutableSharedFlow<Unit>>()

    private fun flowFor(queue: String) =
        flows.getOrPut(queue) {
            MutableSharedFlow<Unit>(
                replay = 0,
                extraBufferCapacity = 1,
                onBufferOverflow = BufferOverflow.DROP_OLDEST,
            ).also { flow -> launchBroadcastCollector(queue, flow) }
        }

    private fun launchBroadcastCollector(queue: String, flow: MutableSharedFlow<Unit>) {
        broadcastScope.launch {
            val encodedQueue = URLEncoder.encode(queue, Charsets.UTF_8)
            flow.collect {
                val peers = peerDiscovery.peers()
                for (peer in peers) {
                    launch {
                        try {
                            httpClient.post("http://$peer:8080/internal/dispatch-notify?queue=$encodedQueue")
                        } catch (e: Exception) {
                            log.debug("Peer notify failed for {}: {}", peer, e.message)
                        }
                    }
                }
            }
        }
    }

    override suspend fun signal(queueName: String) {
        flowFor(queueName).tryEmit(Unit)
    }

    override fun onRemoteSignal(queueName: String) {
        flowFor(queueName).tryEmit(Unit)
    }

    override suspend fun awaitWork(
        queueName: String,
        timeout: Duration,
    ): Boolean =
        withTimeoutOrNull(timeout.toMillis()) {
            flowFor(queueName).first()
        } != null

    @PreDestroy
    fun shutdown() {
        broadcastScope.cancel()
    }
}
```

- [ ] **Step 2: Verify build compiles**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -f WorkFlow/pom.xml -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/worker/adapter/http/HttpWorkerNotifier.kt
git commit -m "feat(notification): fire-and-forget broadcast with sender-side coalescing"
```

---

### Task 3: Update existing tests for async broadcast collector

The broadcast collector now runs in a background scope. Tests that assert HTTP request counts after `signal()` need to give the collector time to execute. All tests that create `HttpWorkerNotifier` locally need to call `shutdown()` to clean up the broadcast scope.

**Files:**
- Modify: `src/test/kotlin/worker/adapter/http/WorkerNotifierTest.kt`

- [ ] **Step 1: Update test setup and teardown**

Add an `@AfterEach` that calls `notifier.shutdown()` to cancel the broadcast scope. This prevents leaked coroutines between tests.

In the class-level fields and setup, add:

```kotlin
import org.junit.jupiter.api.AfterEach

// Add after the existing @BeforeEach setup() method:
@AfterEach
fun teardown() {
    notifier.shutdown()
}
```

- [ ] **Step 2: Update SignalWithPeers tests to await async broadcast**

The `SignalWithPeers` tests create local `HttpWorkerNotifier` instances. Each needs:
1. A call to `yield()` or a short `delay(50)` after `signal()` to let the broadcast collector execute
2. A `shutdown()` call at the end to clean up

Update every test in the `SignalWithPeers` nested class. Replace the entire nested class:

```kotlin
@Nested
inner class SignalWithPeers {

    @Test
    fun `signal with peers calls HTTP post once per peer`() = runTest(UnconfinedTestDispatcher()) {
        whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.2", "10.0.0.3"))
        val engine = MockEngine { respond("") }
        val localNotifier = HttpWorkerNotifier(peerRegistry, HttpClient(engine))

        localNotifier.signal("default")
        yield()

        assertEquals(2, engine.requestHistory.size)
        val hosts = engine.requestHistory.map { it.url.host }.toSet()
        assertEquals(setOf("10.0.0.2", "10.0.0.3"), hosts)
        engine.requestHistory.forEach { req ->
            assertEquals(HttpMethod.Post, req.method)
            assertEquals(8080, req.url.port)
            assertEquals("/internal/dispatch-notify", req.url.encodedPath)
            assertEquals("default", req.url.parameters["queue"])
        }

        localNotifier.shutdown()
    }

    @Test
    fun `signal with no peers does not make HTTP calls`() = runTest(UnconfinedTestDispatcher()) {
        whenever(peerRegistry.peers()).thenReturn(emptyList())
        val engine = MockEngine { respond("") }
        val localNotifier = HttpWorkerNotifier(peerRegistry, HttpClient(engine))

        localNotifier.signal("default")
        yield()

        assertTrue(engine.requestHistory.isEmpty())

        localNotifier.shutdown()
    }

    @Test
    fun `signal propagates queue name in HTTP path`() = runTest(UnconfinedTestDispatcher()) {
        whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.5"))
        val engine = MockEngine { respond("") }
        val localNotifier = HttpWorkerNotifier(peerRegistry, HttpClient(engine))

        localNotifier.signal("priority-queue")
        yield()

        assertEquals(1, engine.requestHistory.size)
        assertEquals("priority-queue", engine.requestHistory[0].url.parameters["queue"])

        localNotifier.shutdown()
    }

    @Test
    fun `signal URL-encodes queue name with special characters`() = runTest(UnconfinedTestDispatcher()) {
        whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.7"))
        val engine = MockEngine { respond("") }
        val localNotifier = HttpWorkerNotifier(peerRegistry, HttpClient(engine))

        localNotifier.signal("queue with spaces")
        yield()

        assertEquals(1, engine.requestHistory.size)
        assertEquals("queue with spaces", engine.requestHistory[0].url.parameters["queue"])

        localNotifier.shutdown()
    }

    @Test
    fun `signal with HTTP failure does not throw`() = runTest(UnconfinedTestDispatcher()) {
        whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.9"))
        val engine = MockEngine { respond("", HttpStatusCode.InternalServerError) }
        val localNotifier = HttpWorkerNotifier(peerRegistry, HttpClient(engine))

        localNotifier.signal("default") // should not throw
        yield()

        localNotifier.shutdown()
    }
}
```

Key changes vs. old tests:
- All use `runTest(UnconfinedTestDispatcher())` so the broadcast collector executes eagerly
- `yield()` after `signal()` gives the collector a chance to run
- `localNotifier.shutdown()` at the end cleans up the broadcast scope

- [ ] **Step 3: Run existing tests to verify they still pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -f WorkFlow/pom.xml -Dtest="WorkerNotifierTest" -pl WorkFlow`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add src/test/kotlin/worker/adapter/http/WorkerNotifierTest.kt
git commit -m "test(notification): adapt existing tests for async broadcast collector"
```

---

### Task 4: Add new test — signal returns before HTTP completes (fire-and-forget)

**Files:**
- Modify: `src/test/kotlin/worker/adapter/http/WorkerNotifierTest.kt`

- [ ] **Step 1: Write the test**

Add a new `@Nested` class inside `WorkerNotifierTest`. The key insight: since the broadcast runs in a detached scope, `signal()` returns before the HTTP call is even initiated. We verify by checking that `engine.requestHistory` is empty immediately after `signal()` returns.

```kotlin
@Nested
inner class FireAndForget {

    @Test
    fun `signal returns before HTTP executes`() = runTest {
        whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.2"))
        val engine = MockEngine { respond("") }
        val localNotifier = HttpWorkerNotifier(peerRegistry, HttpClient(engine))

        localNotifier.signal("default")

        // Broadcast hasn't executed yet -- it's in a detached scope
        assertEquals(0, engine.requestHistory.size, "signal() should return before HTTP executes")

        localNotifier.shutdown()
    }
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -f WorkFlow/pom.xml -Dtest="WorkerNotifierTest\$FireAndForget" -pl WorkFlow`
Expected: PASS (signal returns immediately because broadcast runs in detached scope)

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/worker/adapter/http/WorkerNotifierTest.kt
git commit -m "test(notification): verify signal returns before HTTP completes"
```

---

### Task 5: Add new test — rapid signals coalesce HTTP broadcasts

**Files:**
- Modify: `src/test/kotlin/worker/adapter/http/WorkerNotifierTest.kt`

- [ ] **Step 1: Write the failing test**

Add a new `@Nested` class inside `WorkerNotifierTest`:

```kotlin
@Nested
inner class BroadcastCoalescing {

    @Test
    fun `rapid signals coalesce into fewer HTTP broadcasts`() = runTest(UnconfinedTestDispatcher()) {
        whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.2"))
        val engine = MockEngine { respond("") }
        val localNotifier = HttpWorkerNotifier(peerRegistry, HttpClient(engine))

        repeat(10) { localNotifier.signal("default") }
        yield()

        assertTrue(
            engine.requestHistory.size < 10,
            "10 rapid signals should coalesce, but got ${engine.requestHistory.size} HTTP requests"
        )

        localNotifier.shutdown()
    }
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -f WorkFlow/pom.xml -Dtest="WorkerNotifierTest\$BroadcastCoalescing" -pl WorkFlow`
Expected: PASS (SharedFlow DROP_OLDEST coalesces rapid emissions)

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/worker/adapter/http/WorkerNotifierTest.kt
git commit -m "test(notification): verify rapid signals coalesce HTTP broadcasts"
```

---

### Task 6: Add new test — shutdown cancels broadcast collectors

**Files:**
- Modify: `src/test/kotlin/worker/adapter/http/WorkerNotifierTest.kt`

- [ ] **Step 1: Write the failing test**

Add a new `@Nested` class inside `WorkerNotifierTest`:

```kotlin
@Nested
inner class ShutdownBehavior {

    @Test
    fun `shutdown cancels broadcast collectors`() = runTest(UnconfinedTestDispatcher()) {
        whenever(peerRegistry.peers()).thenReturn(listOf("10.0.0.2"))
        val engine = MockEngine { respond("") }
        val localNotifier = HttpWorkerNotifier(peerRegistry, HttpClient(engine))

        localNotifier.signal("default")
        yield()
        val countBefore = engine.requestHistory.size

        localNotifier.shutdown()

        localNotifier.signal("default")
        yield()

        assertEquals(
            countBefore,
            engine.requestHistory.size,
            "No HTTP calls should be made after shutdown"
        )
    }
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -f WorkFlow/pom.xml -Dtest="WorkerNotifierTest\$ShutdownBehavior" -pl WorkFlow`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/worker/adapter/http/WorkerNotifierTest.kt
git commit -m "test(notification): verify shutdown stops broadcast collectors"
```

---

### Task 7: Update StressTestBase notifier construction

**Files:**
- Modify: `src/test/kotlin/stress/StressTestBase.kt`

The `StressTestBase` creates `HttpWorkerNotifier` directly. Since the constructor signature is unchanged, it will still compile. However, the new `HttpWorkerNotifier` launches a broadcast collector per queue, so the stress test's notifier needs to be shut down to avoid leaked coroutines. If the base class already has teardown logic, add `shutdown()` there; otherwise verify the tests still pass as-is (with no peers, the broadcast collector does nothing on emission).

- [ ] **Step 1: Verify stress tests compile and pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -f WorkFlow/pom.xml -Dtest="ResilienceStressTest" -pl WorkFlow`
Expected: PASS (no peers configured, broadcast collector is a no-op)

- [ ] **Step 2: Commit (only if changes were needed)**

If no changes needed, skip this commit.

---

### Task 8: Run full test suite

- [ ] **Step 1: Run all tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -f WorkFlow/pom.xml -pl WorkFlow`
Expected: All tests PASS

- [ ] **Step 2: Run coverage check**

Run: `python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70`
Expected: Coverage thresholds met
