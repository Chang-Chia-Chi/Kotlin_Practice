# Metrics & Kubernetes Autoscaling Specification

**Goal:** Define the exact Micrometer telemetry required to feed the Kubernetes Horizontal Pod Autoscaler (HPA) using custom metrics, enabling dynamic scaling based on queue backlog and worker utilization without degrading database performance.



## 1. The Telemetry Pipeline
The framework relies on a standard Kubernetes custom metrics pipeline. The workers do not interact directly with the K8s API.

1. **Quarkus Micrometer:** Exposes a `/q/metrics` endpoint on every pod in Prometheus format.
2. **Prometheus Scraper:** Aggregates metrics from all active pod endpoints at regular intervals (e.g., 15s).
3. **Prometheus Adapter:** Translates the aggregated Prometheus metrics into the Kubernetes Custom Metrics API format.
4. **HPA Controller:** Evaluates the Custom Metrics API and mathematically scales the Quarkus Deployment replicas up or down.

## 2. Core Scaling Metrics (The Golden Signals)

These are the primary gauges the K8s HPA will use to make scaling decisions.

### A. The "Scale Up" Signal: Queue Depth
* **Metric Name:** `framework.queue.depth` (Gauge)
* **Labels:** `queue_name` (e.g., "default", "heavy_compute")
* **Mechanism:** To prevent database connection exhaustion, **only the pod holding the Leader Lease** is permitted to query the database for the total number of `PENDING` tasks.
* **Behavior:** The Leader pod exposes the actual count. All non-leader pods either expose `0` or omit the metric. Prometheus aggregates this into a single, accurate cluster-wide metric.

### B. The "Scale Down" Signal: Bulkhead Utilization
* **Metric Name:** `framework.worker.bulkhead.utilization` (Gauge)
* **Labels:** `pod_id`
* **Mechanism:** Every pod tracks its own internal concurrent execution state. If a pod has a bulkhead limit of 20, and 15 threads are currently processing tasks, it reports `0.75` (75%).
* **Behavior:** The HPA calculates the average utilization across all active pods to determine if the cluster is over-provisioned.

## 3. Operational Health Metrics (Dashboards & Alerts)

While not strictly used for HPA scaling, these metrics are vital for monitoring the health of the DAG and Map-Reduce orchestrations.

* **Task Processing Latency:** * `framework.task.duration.seconds` (Timer)
    * **Labels:** `handler`, `status` (Success/Retry/DeadLetter)
    * **Purpose:** Detects slow handlers and database lock contention. If scaling up pods causes this latency to spike, the database is the bottleneck, not the compute layer.
* **Error Rate:** * `framework.task.errors.total` (Counter)
    * **Labels:** `handler`, `error_type`
    * **Purpose:** Tracks task failures before they hit the dead-letter threshold.
* **Job/DAG Completion Time:** * `framework.orchestration.duration.seconds` (Timer)
    * **Labels:** `orchestration_type` (MapReduce, DAG), `identifier`
    * **Purpose:** Measures the macro-level performance of Layer 2 workflows from submission to terminal state.

## 4. HPA Configuration Strategies

The framework supports two distinct scaling formulas depending on the workload profile.

### Strategy 1: Target External Metric (Queue-Centric)
Best for bursty Map-Reduce workloads with massive sudden fan-outs.
* **Rule:** The HPA looks at the global `framework.queue.depth` divided by the current number of replicas.
* **Target:** Maintain a ratio of *X* pending tasks per replica (e.g., 50).
* **Result:** If a DAG triggers a Map-Reduce job that enqueues 10,000 tasks, the HPA detects the massive ratio imbalance instantly and aggressively scales up to the maximum allowed replicas before the workers even begin processing.

### Strategy 2: Target Average Value (Utilization-Centric)
Best for steady-state DAG execution and standalone background tasks.
* **Rule:** The HPA looks at the average of `framework.worker.bulkhead.utilization` across all pods.
* **Target:** Maintain an average utilization of 70%.
* **Result:** Replicas are added smoothly as the current workers reach their concurrency limits, and scaled down gently as workers sit idle, providing highly efficient resource usage.

## 5. Safeguarding the Scale-Down (Graceful Termination)
Because the HPA can terminate pods when the queue drains, the application must intercept the Kubernetes `SIGTERM` signal.
1. The Quarkus application receives `SIGTERM`.
2. It immediately stops the `SKIP LOCKED` polling loop.
3. The pod's readiness probe is marked as `DOWN`.
4. The application waits for the active bulkhead threads to finish their current handlers, commit their outputs, and transition the Layer 1 tasks to `COMPLETED`.
5. The K8s `terminationGracePeriodSeconds` must be configured to be longer than the maximum expected duration of any single task handler.