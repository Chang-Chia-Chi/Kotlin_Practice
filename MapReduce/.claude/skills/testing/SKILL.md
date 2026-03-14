# Distributed Framework Testing & Chaos Strategy

**Goal:** Verify the exactly-once guarantees, zombie worker fencing, and leader failover mechanisms of the orchestrator under simulated catastrophic infrastructure failures.



## 1. Testing Pyramid for the Orchestrator

* **Domain Unit Tests (In-Memory):** Test the Layer 2 state machines (DAG trigger rules, Map-Reduce barrier math) purely in memory using standard mocking frameworks. These must run in milliseconds and require no database or container overhead.
* **Concurrency Integration Tests (Testcontainers):** Verify the Layer 1 `SKIP LOCKED` query and worker loop against a real Oracle Testcontainer instance to guarantee database-level row visibility and lock contention behavior.
* **Chaos Simulations (System Level):** Programmatic simulations of network partitions, GC pauses, and pod deaths using deterministic fault injection.

## 2. Simulating the Zombie Worker (Split-Brain Fencing)

**Objective:** Prove that a worker waking up from a massive network partition cannot corrupt intermediate outputs if its task was reassigned.

**Test Execution Steps:**
1. Submit a generic task to the Oracle Testcontainer.
2. **Worker A** claims the task, generating `execution_generation_1`.
3. Intercept Worker A's execution thread and force it to sleep (simulating a severe GC pause or K8s CPU throttling).
4. Fast-forward the system clock or trigger the Leader's stale-task reaper. The Leader transitions the task back to PENDING.
5. **Worker B** claims the exact same task, generating `execution_generation_2`. Worker B processes it, commits the output with `generation_2`, and marks the task COMPLETED.
6. Wake up **Worker A**. Worker A attempts to commit its output using `generation_1`.
7. **Assertion:** The database transaction for Worker A must throw an optimistic locking or constraint violation exception. The framework must catch this, silently discard Worker A's output, and prevent any external blob storage commits.

## 3. Simulating Leader Failover (The K8s Lease Drop)

**Objective:** Prove that if the orchestrator pod dies mid-evaluation, a new pod assumes leadership and resumes the DAG or Map-Reduce job exactly where it left off.

**Test Execution Steps:**
1. Boot three identical worker replicas connected to the same Oracle Testcontainer.
2. Use a mocked K8s Fabric8 client. Replica 1 acquires the Leader Lease.
3. Submit a complex DAG with multiple branches. Wait for the DAG to reach a state where exactly half the tasks are COMPLETED, and the Leader has just transitioned several downstream nodes to READY.
4. **Kill Replica 1** instantly (simulate `SIGKILL`, bypassing graceful shutdown). The Leader Lease expires.
5. Replica 2 acquires the Lease and starts its Leader Loop.
6. **Assertion:** Replica 2 must successfully read the DAG state from Oracle, recognize the READY nodes, dispatch them to Layer 1, and eventually drive the entire DAG to a COMPLETED state without repeating any previously completed nodes.



## 4. Simulating Speculative Execution (The Straggler Race)

**Objective:** Prove that duplicate Map tasks spawned to mitigate slow nodes do not result in duplicate downstream data.

**Test Execution Steps:**
1. Submit a Map-Reduce job with 5 tasks.
2. Inject a fault into Task 3's handler so it executes at 10% speed.
3. Tasks 1, 2, 4, and 5 complete normally. The Leader's monitoring loop calculates the median time and detects Task 3 is a straggler.
4. The Leader enqueues a speculative duplicate of Task 3.
5. A healthy worker claims the duplicate and processes it at normal speed.
6. **Assertion:** The healthy worker writes its output and increments the MR barrier counter. When the slow worker finally finishes, its write is rejected due to the generation fencing. The final MR barrier counter must equal exactly 5, not 6.

## 5. Simulating the Pod-Level Circuit Breaker

**Objective:** Ensure a defective pod removes itself from the active pool rather than infinitely failing tasks.

**Test Execution Steps:**
1. Configure the worker's circuit breaker threshold to 5 consecutive failures.
2. Submit a batch of 20 tasks containing a "poison pill" payload designed to throw an unhandled exception in the handler.
3. **Worker A** claims the tasks. It fails the first, second, third, fourth, and fifth tasks.
4. **Assertion:** Upon the 5th failure, Worker A must flip its internal circuit breaker state. It must immediately cease polling Oracle for new tasks. Its exposed health check endpoint must transition from UP to DOWN, which would normally trigger Kubernetes to restart the pod. The remaining 15 tasks must remain safely in the queue for other, healthy pods to claim.