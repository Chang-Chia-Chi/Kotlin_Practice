# Distributed Compute Fault Tolerance Specification

**Pattern Goal:** Ensure absolute resilience, exact-once processing semantics, and zero job/task loss across a homogeneous fleet of distributed workers processing massive datasets.

## 1. Zombie Worker Fencing (Execution Generations)
To prevent split-brain scenarios where a severely delayed worker attempts to commit data after the orchestrator has already reclaimed and reassigned its task.

* **Mechanism:** When a pod claims a task from Layer 1, the framework generates a unique `execution_generation` UUID.
* **Enforcement:** All database state updates or intermediate output registrations performed by the worker MUST include this UUID.
* **Outcome:** If the Leader timed out the original task and gave it to a new pod, the task's generation ID changes. When the zombie worker finally wakes up and attempts to commit, its generation ID mismatches, the transaction is rejected, and the worker is fenced off.



## 2. External Shuffle Architecture
Writing massive intermediate datasets to the relational database will cause I/O collapse and memory exhaustion. The framework must implement a true "Shuffle" phase.

* **Map Phase:** Workers stream their intermediate computational outputs to an external, immutable blob store (e.g., S3 or compatible object storage).
* **State Tracking:** The database `mr_output` table never stores the data. It only stores the routing partition hash and the `blob_uri` pointing to the external storage.
* **Reduce Phase:** Workers stream the inputs directly from the external object storage using the URIs, bypassing the database entirely for data movement.

## 3. Speculative Execution (Straggler Mitigation)
In large clusters, tasks rarely fail outright; they hang due to hardware degradation, noisy neighbors, or GC pauses.

* **Detection:** The Leader's monitoring loop continuously calculates the median execution time of all successfully completed map tasks within a specific job.
* **Action:** If an active map task exceeds a configurable multiple of the median (e.g., 3x), the Leader proactively enqueues a duplicate map task into the Layer 1 queue.
* **Resolution:** Both the slow worker and the speculative worker race. Because of the Zombie Worker Fencing mechanism, the first one to successfully write its output and increment the job counter wins. The latecomer's write is rejected or safely ignored.

## 4. Sharded Reduce Phase
A massive fan-out cannot funnel into a single Reduce task without creating a catastrophic single point of failure and memory bottleneck.

* **Partitioning:** During the Split phase, the orchestrator defines a set of partition keys. Map handlers tag their external `blob_uri` outputs with a specific partition hash.
* **Parallel Reduce:** When the Map barrier is met, the Leader enqueues *multiple* parallel Reduce tasks—one for each partition hash.
* **Isolation:** If a specific Reduce partition fails, only that slice of the data is retried, ensuring localized fault tolerance identical to Spark's lineage recovery.

## 5. Pod-Level Circuit Breaking
A malfunctioning worker pod (e.g., corrupted local disk, broken network interface) can rapidly claim and fail tasks, exhausting job retry limits and failing healthy distributed jobs.

* **Tracking:** The generic worker loop tracks its own consecutive failure rate.
* **Tripping:** If a worker fails a high threshold of consecutive tasks across different generic handlers, it voluntarily trips its internal circuit breaker.
* **Quarantine:** The tripped worker immediately stops polling the database for new tasks and deliberately fails its Kubernetes readiness probe, signaling the orchestrator to restart or terminate the defective pod.