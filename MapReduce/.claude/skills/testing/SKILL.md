This is a fantastic foundation. Your focus on generation fencing and crash-only design is exactly what is needed for a highly concurrent, distributed orchestrator.

However, if you hand your original document to an AI right now, it will likely hallucinate a bunch of generic `Thread.sleep()` calls, flaky assertions, and generic mocking that doesn't actually test the database transaction boundaries. To force an AI to write robust, production-mimicking tests in Quarkus and JUnit5, we need to tighten the terminology, explicitly name the chaos testing tools (like Toxiproxy and Awaitility), and strictly define the K8s and Coroutine boundaries.

Here is the refined strategy document, optimized to be used as a strict prompt for an AI.

---

### Distributed Framework Testing & Chaos Strategy

**Goal:** Verify the exactly-once guarantees, zombie worker fencing, and leader failover mechanisms of the orchestrator under simulated catastrophic infrastructure failures.

#### 1. Testing Pyramid for the Orchestrator

* **Domain Unit Tests (In-Memory):** Test the Layer 2 state machines (DAG trigger rules, Map-Reduce barrier math) purely in memory. Use `kotlinx-coroutines-test` (`runTest`) to instantly advance virtual time for timeout and delay evaluations. These must run in milliseconds and require no database or CDI container overhead.
* **Concurrency Integration Tests (Testcontainers):** Verify the Layer 1 `SKIP LOCKED` query and worker loop against an Oracle Testcontainer instance. Use `Toxiproxy` alongside Testcontainers to inject deterministic network faults (e.g., latency, dropped packets) directly at the TCP level to guarantee real database-level lock contention behavior.
* **Chaos Simulations (System Level):** Programmatic simulations of K8s pod deaths and leader lease expirations using `@QuarkusTestProfile` and injected mocks of the Kubernetes client.

#### 2. Simulating the Zombie Worker (Split-Brain Fencing)

**Objective:** Prove that a worker waking up from a massive network partition cannot corrupt intermediate outputs if its task was reassigned.

**Test Execution Steps:**

1. Submit a generic task to the Oracle Testcontainer.
2. **Worker A** claims the task, generating `execution_generation_1`.
3. Use `Toxiproxy` to silently drop all TCP packets between Worker A and the Oracle container, simulating a hard network partition without closing the connection.
4. Advance the system clock. The Leader's stale-task reaper detects the timeout and transitions the task back to PENDING.
5. **Worker B** claims the exact same task, generating `execution_generation_2`. Worker B processes it, commits the output with `generation_2`, and marks the task COMPLETED.
6. Use `Toxiproxy` to restore Worker A's network connection. Worker A attempts to commit its output using `generation_1`.
7. **Assertion:** The database transaction for Worker A must throw a constraint violation. The framework must catch this, silently discard Worker A's output, and assert that no external blob storage commit API was invoked by Worker A.

#### 3. Simulating Leader Failover (The K8s Lease Drop)

**Objective:** Prove that if the orchestrator pod dies mid-evaluation, a new pod assumes leadership and resumes the DAG or Map-Reduce job exactly where it left off.

**Test Execution Steps:**

1. Boot identical worker replicas connected to the same Oracle Testcontainer.
2. Inject a mocked `Fabric8` Kubernetes client using `@InjectMock`. Replica 1 acquires the mocked Leader Lease.
3. Submit a complex DAG with multiple branches. Use `Awaitility` to wait until the DAG reaches a state where exactly half the tasks are COMPLETED, and the Leader has just transitioned several downstream nodes to READY.
4. Programmatically evict Replica 1's Leader Lease and trigger a simulated pod crash (bypassing the graceful `@Observes ShutdownEvent`).
5. Grant the mocked K8s Lease to Replica 2 so it starts its Leader Loop.
6. **Assertion:** Replica 2 must successfully read the DAG state from Oracle, recognize the READY nodes, and dispatch them to Layer 1. Use `Awaitility` to assert the entire DAG eventually reaches a COMPLETED state without repeating any previously completed nodes.

#### 4. Simulating Speculative Execution (The Straggler Race)

**Objective:** Prove that duplicate Map tasks spawned to mitigate slow nodes do not result in duplicate downstream data.

**Test Execution Steps:**

1. Submit a Map-Reduce job with 5 tasks.
2. Inject a programmatic delay into Task 3's handler so it suspends for an extended duration.
3. Tasks 1, 2, 4, and 5 complete normally. The Leader's monitoring loop calculates the median time and detects Task 3 is a straggler.
4. The Leader enqueues a speculative duplicate of Task 3.
5. A healthy worker claims the duplicate and processes it at normal speed.
6. **Assertion:** The healthy worker writes its output and increments the MR barrier counter. When the slow worker finally resumes and finishes, its write must be rejected due to the generation fencing. Assert that the final MR barrier counter equals exactly 5, not 6.

#### 5. Simulating the Pod-Level Circuit Breaker

**Objective:** Ensure a defective pod removes itself from the active pool rather than infinitely failing tasks.

**Test Execution Steps:**

1. Configure the worker's SmallRye Fault Tolerance circuit breaker threshold to 5 consecutive failures.
2. Submit a batch of 20 tasks containing a "poison pill" payload designed to throw an unhandled exception in the handler.
3. **Worker A** claims the tasks. It fails the first 5 tasks consecutively.
4. **Assertion:** Upon the 5th failure, assert that the SmallRye Circuit Breaker transitions to the `OPEN` state. Worker A must immediately cease polling Oracle for new tasks. Check the injected Quarkus Health endpoint to ensure the readiness probe transitions from UP to DOWN. Assert the remaining 15 tasks remain safely in the queue for healthy pods to claim.

---

#### AI Prompt Directives for Test Generation

**Constraint Directive:** Ensure the AI strictly adheres to the following when generating the test suite:

* **Constraint 1:** Do not use `Thread.sleep()`. All asynchronous assertions must use `Awaitility.await().untilAsserted(...)`.
* **Constraint 2:** All pure unit tests must use `runTest` from `kotlinx-coroutines-test` for deterministic time control.
* **Constraint 3:** Mock Kubernetes interactions strictly using `@InjectMock` on the Fabric8 `KubernetesClient`. Do not attempt to spin up a real Kubernetes cluster via Testcontainers.
* **Constraint 4:** Use `ToxiproxyContainer` for all network fault injection scenarios at the database layer.