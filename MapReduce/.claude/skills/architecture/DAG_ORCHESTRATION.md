# DAG Orchestration Specification (Layer 2)

**Pattern Goal:** Task Parallelism. Orchestrate heterogeneous, multi-step business workflows with complex dependency graphs, dynamic branching, and state passing — all layered on top of the generic distributed task queue (Layer 1).

---

## 1. Core Concepts

| Concept | Description |
|---|---|
| **DAG Blueprint** | The static, declarative definition of nodes, edges, and execution semantics. Stored as a versioned YAML document. |
| **Run** | A specific execution instance of a DAG Blueprint at a pinned version. Acts as the correlation boundary (`group_id`) for underlying Layer 1 tasks. |
| **Task Instance** | A single node within a Run. Backed 1:1 by a generic Layer 1 task. |
| **Task Type** | A pluggable handler category (analogous to Airflow Operators / Kestra task types). Determines _how_ a node executes. |
| **XCom (State Passing)** | The mechanism for passing output data from upstream nodes to downstream nodes. Heavy payloads (> configurable threshold, default 64 KB) must be written to MinIO/S3, with only URI references passed through the state machine. |

---

## 2. DAG Blueprint Schema (YAML-First Definition)

A declarative YAML format is the primary authoring surface. The goal is Kestra's readability with Airflow's expressive power. Blueprints are stored in a registry table (or Git-backed config) and are **immutable once versioned** — in-flight Runs always execute against their pinned snapshot.

### 2.1 Blueprint Structure

```yaml
dag_id: semiconductor-lot-disposition
version: 3                          # monotonic, immutable once published
namespace: fab.disposition           # hierarchical namespace for organization
description: "End-to-end lot disposition after inline metrology"
labels:
  team: yield-engineering
  domain: metrology

# --- Typed Inputs (validated at Run creation) ---
inputs:
  - name: lot_id
    type: STRING
    required: true
    description: "Manufacturing lot identifier"
  - name: process_step
    type: STRING
    required: true
    enum: [LITHO, ETCH, CMP, DEPO]
  - name: force_review
    type: BOOLEAN
    default: false
  - name: measurement_uri
    type: URI
    required: true
    description: "MinIO path to raw metrology payload"

# --- Execution Defaults (inheritable, overridable per-node) ---
defaults:
  timeout: PT30M                    # ISO-8601 duration
  retry:
    max_attempts: 3
    backoff: EXPONENTIAL
    initial_delay: PT10S
    max_delay: PT5M
    retryable_errors: [TRANSIENT]   # error classification from handler
  task_type: GENERIC_HANDLER        # default if node omits task_type

# --- Concurrency Controls ---
concurrency:
  max_parallel_runs: 5              # per dag_id, prevents resource starvation
  max_parallel_nodes: 10            # per run, caps fan-out width

# --- Node Definitions ---
nodes:
  - key: validate_input
    task_type: SQL_QUERY
    description: "Verify lot exists and is in correct state"
    config:
      datasource: oracle-fab
      sql: "SELECT status FROM lot WHERE lot_id = :lot_id"
      bind: { lot_id: "{{ inputs.lot_id }}" }

  - key: fetch_metrology
    task_type: OBJECT_FETCH
    depends_on: [validate_input]
    config:
      source_uri: "{{ inputs.measurement_uri }}"

  - key: run_spc_model
    task_type: GENERIC_HANDLER
    depends_on: [fetch_metrology]
    handler: spc-analysis-handler
    timeout: PT15M                   # overrides default
    config:
      model: western-electric-rules
      data_ref: "{{ xcom.fetch_metrology.output_uri }}"

  - key: disposition_router
    task_type: GENERIC_HANDLER
    depends_on: [run_spc_model]
    handler: disposition-router
    # This node uses __dag_route__ to dynamically select the next branch

  # --- Conditional Branches ---
  - key: auto_release
    depends_on: [disposition_router]
    trigger_rule: ALL_SUCCESS
    handler: lot-release-handler

  - key: engineering_hold
    depends_on: [disposition_router]
    trigger_rule: ALL_SUCCESS
    handler: hold-handler
    config:
      hold_code: ENGR_REVIEW

  - key: scrap_review
    depends_on: [disposition_router]
    trigger_rule: ALL_SUCCESS
    handler: scrap-handler

  # --- Convergence / Join ---
  - key: notify_disposition
    depends_on: [auto_release, engineering_hold, scrap_review]
    trigger_rule: ONE_SUCCESS        # fires when any single branch completes
    task_type: NOTIFICATION
    config:
      channel: fab-disposition-alerts
      template: "Lot {{ inputs.lot_id }} dispositioned: {{ xcom.*.status }}"

  # --- Error Handler (on-failure hook) ---
  - key: on_failure_alert
    trigger_rule: ON_FAILURE         # only dispatched if the Run fails
    task_type: NOTIFICATION
    config:
      channel: fab-ops-escalation
      severity: P2

# --- Triggers (how Runs are created) ---
triggers:
  - type: EVENT
    source: nats
    subject: "metrology.completed"
    filter: "payload.process_step IN ('LITHO','ETCH')"
  - type: MANUAL                     # always available via API
```

### 2.2 Input Types

| Type | Validation | Example |
|---|---|---|
| `STRING` | Non-blank, optional `enum`, optional `pattern` (regex) | Lot ID, step name |
| `INTEGER` | Range bounds (`min`, `max`) | Retry count, threshold |
| `BOOLEAN` | — | Feature flag |
| `FLOAT` | Range bounds | Measurement tolerance |
| `URI` | Must be a valid URI scheme (s3://, minio://, file://) | Payload reference |
| `JSON` | Optional JSON Schema reference for deep validation | Arbitrary config blob |
| `DATETIME` | ISO-8601 | Scheduled time, deadline |

### 2.3 Template Expressions

Templates use `{{ }}` Mustache-style interpolation, resolved at dispatch time by the Leader. Available namespaces:

| Namespace | Resolves To |
|---|---|
| `{{ inputs.<name> }}` | Run input parameters (from `global_context`) |
| `{{ xcom.<task_key>.<field> }}` | Upstream node's `output_data` field |
| `{{ xcom.*.<field> }}` | Merged output from all completed upstream parents |
| `{{ run.run_id }}` | Current Run UUID |
| `{{ run.dag_id }}` | Blueprint identifier |
| `{{ run.created_at }}` | Run creation timestamp |
| `{{ env.<key> }}` | Environment variable (allowlisted via config) |

Resolution rules: templates are evaluated lazily at dispatch time, never at blueprint parse time. Unresolvable references cause the node to transition to `FAILED` with a clear diagnostic.

---

## 3. Task Type System

Task Types are the pluggable execution abstraction — analogous to Airflow Operators or Kestra task plugins. Each type defines config schema validation, payload assembly, and result extraction.

### 3.1 Built-in Types

| Type | Purpose | Config Shape |
|---|---|---|
| `GENERIC_HANDLER` | Invokes a named handler via Layer 1's SPI | `{ handler: string, ... }` |
| `SQL_QUERY` | Executes parameterized SQL, returns result set | `{ datasource, sql, bind }` |
| `SQL_EXECUTE` | Executes DML/DDL, returns affected row count | `{ datasource, sql, bind }` |
| `OBJECT_FETCH` | Retrieves object from MinIO/S3 into task workspace | `{ source_uri, target_key? }` |
| `OBJECT_PUT` | Writes task output to MinIO/S3 | `{ target_uri, source_ref }` |
| `NOTIFICATION` | Sends alert via configured channel (NATS, Slack, email) | `{ channel, template, severity? }` |
| `SUB_DAG` | Triggers a child DAG Run and waits for completion | `{ dag_id, inputs_mapping }` |
| `NOOP` | Passthrough — useful for join/sync points | `{}` |

### 3.2 Custom Task Type SPI

```kotlin
interface TaskTypeHandler {
    /** Unique type identifier, matches YAML task_type field */
    val typeId: String

    /** JSON Schema for config validation at blueprint registration time */
    fun configSchema(): JsonSchema

    /**
     * Assemble the Layer 1 task payload from resolved config + xcom.
     * Called by the Leader at dispatch time.
     */
    fun assemblePayload(
        resolvedConfig: JsonObject,
        xcomContext: Map<String, JsonObject>,
        globalContext: JsonObject
    ): TaskPayload

    /**
     * Extract structured output from the Layer 1 task result.
     * Called by the Leader during Reconcile.
     */
    fun extractOutput(rawResult: JsonObject): TaskOutput
}
```

### 3.3 SUB_DAG Semantics

Sub-DAGs provide composability (Airflow's SubDagOperator / Kestra's subflows). The parent node transitions to `RUNNING` and creates a child Run. The Leader monitors the child Run's terminal state:

- Child `COMPLETED` → parent node `COMPLETED`, child's outputs merged into parent xcom.
- Child `FAILED` → parent node `FAILED`, normal retry/error-handling applies.
- Depth limit: configurable (default 3) to prevent runaway recursion.

---

## 4. Logical Data Model

### 4.1 Table: `dag_blueprint`

| Column | Type | Description |
|---|---|---|
| `dag_id` | VARCHAR PK | Stable identifier across versions |
| `version` | INTEGER PK | Monotonic version number, composite PK with `dag_id` |
| `namespace` | VARCHAR | Hierarchical grouping (dot-separated) |
| `definition` | CLOB (JSON) | Serialized YAML→JSON blueprint |
| `input_schema` | CLOB (JSON) | Extracted and compiled input validation schema |
| `status` | VARCHAR | `DRAFT`, `PUBLISHED`, `DEPRECATED` |
| `checksum` | VARCHAR | SHA-256 of `definition` for integrity verification |
| `created_at` | TIMESTAMP | Publication timestamp |
| `created_by` | VARCHAR | Author identity |

**Versioning contract:** `PUBLISHED` blueprints are immutable. A new version is required for any change. `DEPRECATED` blueprints reject new Run creation but allow in-flight Runs to complete.

### 4.2 Table: `dag_run`

| Column | Type | Description |
|---|---|---|
| `run_id` | UUID PK | Unique execution instance, also the Layer 1 `group_id` |
| `dag_id` | VARCHAR FK | Blueprint identifier |
| `dag_version` | INTEGER FK | Pinned blueprint version for this Run |
| `status` | VARCHAR | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED` |
| `global_context` | CLOB (JSON) | Validated input parameters |
| `trigger_type` | VARCHAR | `MANUAL`, `EVENT`, `SCHEDULED`, `SUB_DAG` |
| `trigger_metadata` | CLOB (JSON) | Source event ID, schedule expression, parent run reference |
| `parent_run_id` | UUID nullable FK | For SUB_DAG: the parent Run that spawned this |
| `started_at` | TIMESTAMP | First node dispatched |
| `completed_at` | TIMESTAMP nullable | Terminal state reached |
| `deadline_at` | TIMESTAMP nullable | Run-level SLA deadline |
| `created_at` | TIMESTAMP | Row creation |

**Indexes:** `(dag_id, status)` for Leader polling; `(parent_run_id)` for SUB_DAG correlation; `(status, created_at)` for housekeeping scans.

### 4.3 Table: `dag_task_instance`

| Column | Type | Description |
|---|---|---|
| `instance_id` | UUID PK | Unique node execution |
| `run_id` | UUID FK | Parent Run |
| `task_key` | VARCHAR | Logical node identifier within the blueprint |
| `task_type` | VARCHAR | Resolved task type (from node or defaults) |
| `dependencies` | JSON | Array of `task_key` strings that must resolve before dispatch |
| `status` | VARCHAR | `BLOCKED`, `READY`, `QUEUED`, `RUNNING`, `COMPLETED`, `SKIPPED`, `FAILED`, `TIMED_OUT` |
| `trigger_rule` | VARCHAR | `ALL_SUCCESS`, `ALL_DONE`, `ONE_SUCCESS`, `NONE_FAILED`, `ON_FAILURE` |
| `attempt` | INTEGER | Current attempt number (1-indexed) |
| `max_attempts` | INTEGER | Resolved from node config or defaults |
| `resolved_config` | CLOB (JSON) | Template-resolved configuration snapshot |
| `output_data` | CLOB (JSON) | Result metadata or external storage URIs |
| `error` | CLOB (JSON) nullable | Error classification, message, stacktrace reference |
| `layer1_task_id` | UUID nullable FK | Corresponding Layer 1 task for the current attempt |
| `timeout_at` | TIMESTAMP nullable | Absolute deadline for current attempt |
| `dispatched_at` | TIMESTAMP nullable | When transitioned to QUEUED |
| `completed_at` | TIMESTAMP nullable | When reached terminal state |

**Indexes:** `(run_id, status)` for Leader graph traversal; `(layer1_task_id)` for reconciliation join; `(status, timeout_at)` for timeout reaping.

---

## 5. The Orchestration State Machine (Leader Loop)

Workers remain unaware of the DAG. The Leader pod holding the Kubernetes Lease drives the graph forward by monitoring Layer 1 tasks and evaluating the dependency graph.

### 5.1 Leader Loop Phases

The Leader executes the following phases in a continuous loop (configurable interval, default 2s):

**Phase 1 — Reconcile**

The Leader polls for Layer 1 tasks tied to active `dag_run` records that have reached a terminal state (`COMPLETED` or `DEAD_LETTER`). For each:

1. Update the corresponding `dag_task_instance` status and persist `output_data` (or `error`).
2. Intercept dynamic routing directives (see §6).
3. If the task has a `COMPLETED` output with error classification `TRANSIENT` and `attempt < max_attempts`, transition to `READY` for retry instead of `FAILED`.

**Phase 2 — Timeout Reaping**

Query `dag_task_instance` where `status IN (QUEUED, RUNNING)` and `timeout_at < NOW()`. For each:

1. Transition to `TIMED_OUT`.
2. Attempt cancellation of the Layer 1 task (best-effort).
3. If retryable (`attempt < max_attempts`), transition to `READY`.

**Phase 3 — Identify Dependents**

For every node that transitioned to a terminal state during Phase 1 or 2, query the blueprint topology to find all `BLOCKED` nodes that list the resolved node in their `dependencies`.

**Phase 4 — Evaluate Trigger Rules**

For each candidate dependent node, inspect all upstream parents' statuses. Apply the trigger rule to decide the transition:

| Trigger Rule | Fires When | Use Case |
|---|---|---|
| `ALL_SUCCESS` | Every parent is `COMPLETED` | Default, strict pipeline |
| `ALL_DONE` | Every parent is terminal (any state) | Cleanup / finally blocks |
| `ONE_SUCCESS` | At least one parent is `COMPLETED`, rest terminal | Branch convergence / join |
| `NONE_FAILED` | Every parent is `COMPLETED` or `SKIPPED`, none `FAILED` | Lenient pipeline |
| `ON_FAILURE` | The Run has been marked `FAILED` | Error-handler hooks |

If the rule is not satisfied and all parents are terminal, the node transitions to `SKIPPED`.

**Cascade Protocol:** When a node evaluates to `SKIPPED`, the Leader immediately and recursively evaluates that node's dependents to propagate the skip down the branch. This is a synchronous, depth-first traversal within the same Leader loop iteration — it must not require a subsequent polling cycle.

**Phase 5 — Dispatch**

For every node now in `READY` state:

1. Resolve template expressions against `global_context` + upstream `output_data`.
2. Invoke the `TaskTypeHandler.assemblePayload()` to build the Layer 1 task payload.
3. Enforce concurrency limits (`max_parallel_nodes`). If the limit is reached, leave excess nodes in `READY` — they will be picked up in the next loop iteration.
4. Transition to `QUEUED`, set `timeout_at`, and enqueue into the Layer 1 task table via `INSERT`.

**Phase 6 — Run Completion Check**

If all `dag_task_instance` rows for a Run are terminal, determine the Run's final status:
- All nodes `COMPLETED` or `SKIPPED` → Run `COMPLETED`.
- Any node `FAILED` or `TIMED_OUT` (after exhausting retries) → Run `FAILED`.
- On `FAILED`, dispatch any `ON_FAILURE` trigger-rule nodes before finalizing.

### 5.2 Leader Consistency Guarantees

- All Phase 1–6 updates for a single Run are executed within a single Oracle transaction (serializable isolation on the Run's rows via `SELECT FOR UPDATE` on `dag_run`).
- Fencing token from the K8s Lease is validated before each transaction commit.
- The Leader is idempotent: re-processing the same terminal Layer 1 task is a no-op if the `dag_task_instance` is already terminal.

---

## 6. Dynamic Branch Routing

To support runtime execution paths without pre-declaring every condition in the blueprint, handlers can inject routing directives.

### 6.1 Route-by-Key (Explicit Pruning)

If a completed task's `output_data` contains the reserved key `__dag_route__` with an array of target node keys, the Leader intercepts this during Reconcile:

1. Identify all immediate downstream nodes of the completed task.
2. Forcefully transition any downstream nodes **not** in the `__dag_route__` array to `SKIPPED`.
3. Trigger the Cascade Protocol for skipped nodes.

```json
{
  "disposition": "HOLD",
  "__dag_route__": ["engineering_hold"]
}
```

### 6.2 Conditional Expressions (Declarative Branching)

For simpler cases where a full handler is overkill, nodes can declare inline conditions evaluated by the Leader:

```yaml
- key: auto_release
  depends_on: [disposition_router]
  condition: "{{ xcom.disposition_router.disposition }} == 'PASS'"

- key: engineering_hold
  depends_on: [disposition_router]
  condition: "{{ xcom.disposition_router.disposition }} == 'HOLD'"
```

The Leader evaluates `condition` expressions using a minimal, sandboxed expression engine supporting equality, comparison, boolean operators, `IN` lists, and null checks. If `condition` evaluates to `false`, the node is `SKIPPED` (with cascade). Nodes without a `condition` field default to `true`.

**Precedence:** `__dag_route__` (from output) takes priority over `condition` (from blueprint). If a routing directive is present, conditions on affected downstream nodes are not evaluated.

---

## 7. XCom (Cross-Communication) Protocol

### 7.1 Payload Size Policy

| Size | Storage | XCom Value |
|---|---|---|
| ≤ 64 KB (configurable) | Inline in `output_data` column | The JSON payload itself |
| > 64 KB | Written to MinIO by the handler | `{ "__xcom_uri__": "minio://bucket/run_id/task_key/output.json" }` |

### 7.2 Resolution at Dispatch

When the Leader resolves `{{ xcom.fetch_metrology.output_uri }}`:

1. Load `output_data` from `dag_task_instance` where `task_key = fetch_metrology`.
2. If the value is a `__xcom_uri__` reference, the Leader does **not** dereference it — the reference is passed as-is to the downstream handler, which is responsible for fetching from object storage.
3. If the value is inline JSON, extract the requested field and inject it into the resolved config.

### 7.3 Wildcard Merge

`{{ xcom.* }}` merges `output_data` from all completed upstream parents into a single JSON object, keyed by `task_key`. Conflicts (duplicate keys across parents) are resolved by the merge order defined in the `depends_on` array.

---

## 8. Retry and Error Handling

### 8.1 Error Classification

Handlers return a structured error with a classification that drives retry behavior:

```kotlin
enum class ErrorClass {
    TRANSIENT,    // Network timeout, DB lock, temporary unavailability → retryable
    DATA_ERROR,   // Bad input, validation failure → not retryable, may skip branch
    FATAL         // Infrastructure failure, config error → not retryable, fail Run
}
```

### 8.2 Retry Semantics

Retries are managed at the DAG layer, not Layer 1. When a node fails with `TRANSIENT` and has remaining attempts:

1. The `dag_task_instance.attempt` is incremented.
2. A **new** Layer 1 task is created (new `layer1_task_id`), preserving the same `instance_id`.
3. Backoff delay is enforced by setting `dispatched_at` in the future; the Leader skips nodes whose dispatch time hasn't arrived.

This separation ensures Layer 1's `DEAD_LETTER` semantics remain clean (one attempt = one Layer 1 task) while the DAG layer manages higher-order retry policy.

### 8.3 Node-Level Error Hooks

```yaml
- key: critical_step
  handler: critical-handler
  on_failure:
    handler: alert-handler
    config:
      severity: P1
      channel: ops-pager
```

The `on_failure` block defines an inline error handler dispatched when the node exhausts all retries. It runs as a synthetic child node with `trigger_rule: ALL_DONE` on the failed parent. This is distinct from the Run-level `ON_FAILURE` trigger rule.

---

## 9. Concurrency and Resource Controls

### 9.1 Run-Level Concurrency

`concurrency.max_parallel_runs` limits how many `RUNNING` Runs can exist for a given `dag_id`. The Leader checks this before transitioning a `PENDING` Run to `RUNNING`. Excess Runs remain `PENDING` in FIFO order.

### 9.2 Node-Level Parallelism

`concurrency.max_parallel_nodes` caps the number of `QUEUED` + `RUNNING` nodes within a single Run. The Leader dispatches `READY` nodes in topological order, respecting this limit.

### 9.3 Resource Pools (Optional Extension)

For cross-DAG resource management, nodes can declare a `pool` and `pool_slots`:

```yaml
- key: heavy_query
  pool: oracle-fab-pool
  pool_slots: 2   # consumes 2 of the pool's capacity
```

A `dag_pool` table tracks pool capacity. The Leader checks pool availability before dispatch. This prevents, for example, 50 concurrent oracle-heavy queries across all active Runs.

---

## 10. Triggers and Run Creation

### 10.1 Trigger Types

| Type | Source | Behavior |
|---|---|---|
| `MANUAL` | REST API / UI | Always available. Caller provides `global_context` matching input schema. |
| `EVENT` | Message broker (NATS, JMS) | Leader subscribes to configured subjects. Filter expressions gate Run creation. |
| `SCHEDULED` | Cron expression | Leader evaluates cron schedules. Only one active Run per schedule tick (skip-if-running policy). |
| `SUB_DAG` | Parent Run | Created programmatically by the Leader when a `SUB_DAG` node dispatches. |

### 10.2 Run Creation Flow

1. Validate `global_context` against `input_schema`.
2. Insert `dag_run` with `PENDING` status and pinned `dag_version` (latest `PUBLISHED`).
3. Hydrate `dag_task_instance` rows from the blueprint — all nodes start as `BLOCKED`, except root nodes (no dependencies), which start as `READY`.
4. The Leader loop picks up `PENDING` Runs (respecting `max_parallel_runs`) and transitions them to `RUNNING`.

---

## 11. Observability

### 11.1 Metrics (Prometheus via Micrometer)

| Metric | Type | Labels |
|---|---|---|
| `dag_run_duration_seconds` | Histogram | `dag_id`, `status` |
| `dag_run_active_count` | Gauge | `dag_id`, `status` |
| `dag_node_duration_seconds` | Histogram | `dag_id`, `task_key`, `task_type` |
| `dag_node_retry_total` | Counter | `dag_id`, `task_key` |
| `dag_node_timeout_total` | Counter | `dag_id`, `task_key` |
| `dag_dispatch_lag_seconds` | Histogram | `dag_id` |
| `dag_leader_loop_duration_seconds` | Histogram | — |

### 11.2 Structured Event Log

Every state transition emits a structured JSON event to a dedicated log stream (NATS subject or DB table):

```json
{
  "event": "NODE_STATE_CHANGE",
  "run_id": "...",
  "task_key": "run_spc_model",
  "from_status": "RUNNING",
  "to_status": "COMPLETED",
  "attempt": 1,
  "duration_ms": 4523,
  "timestamp": "2025-07-01T08:15:30Z"
}
```

### 11.3 Run-Level SLA

`dag_run.deadline_at` is computed from the blueprint's `sla` field (e.g., `sla: PT2H`). The Leader emits an alert event when a `RUNNING` Run exceeds its deadline. The Run is **not** auto-cancelled — the alert allows operators to decide.

---

## 12. Versioning and Migration

### 12.1 Blueprint Versioning Contract

- Each `(dag_id, version)` pair is immutable once `PUBLISHED`.
- In-flight Runs always execute against their pinned `dag_version`.
- New Runs always use the latest `PUBLISHED` version.
- `DEPRECATED` versions reject new Run creation but honor existing Runs.

### 12.2 Schema Migration

When a new blueprint version changes the node graph:

- **Added nodes:** Not present in existing Runs — no impact.
- **Removed nodes:** Existing Runs still reference the old version — no impact.
- **Changed dependencies/config:** Only affects new Runs.

There is no automatic migration of in-flight Runs. If a critical fix must be applied, operators can `CANCEL` the Run and re-trigger with the new version.

---

## 13. Task Group (Logical Grouping)

Task Groups provide visual and organizational structure without creating sub-DAG execution boundaries (analogous to Airflow's TaskGroup). They are purely a blueprint-level construct — the Leader treats grouped nodes identically to ungrouped nodes.

```yaml
task_groups:
  - group_key: metrology_pipeline
    description: "All metrology data acquisition and validation"
    nodes: [validate_input, fetch_metrology, run_spc_model]

  - group_key: disposition_actions
    description: "Disposition branch handlers"
    nodes: [auto_release, engineering_hold, scrap_review]
```

Task groups enable: UI rendering of collapsible sections, bulk application of labels/pool assignments, and documentation of logical boundaries.

---

## 14. API Surface

### 14.1 Run Management

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/dags` | GET | List blueprints with optional namespace filter |
| `/api/v1/dags/{dag_id}` | GET | Get latest published blueprint |
| `/api/v1/dags/{dag_id}/versions` | GET | List all versions |
| `/api/v1/dags/{dag_id}` | POST | Publish new version (validates schema + input types) |
| `/api/v1/runs` | POST | Create a Run (validates inputs against schema) |
| `/api/v1/runs/{run_id}` | GET | Run status with all node statuses |
| `/api/v1/runs/{run_id}/cancel` | POST | Request cancellation (best-effort) |
| `/api/v1/runs/{run_id}/nodes/{task_key}/retry` | POST | Manual retry of a failed node |
| `/api/v1/runs/{run_id}/nodes/{task_key}/skip` | POST | Manual skip of a blocked/failed node |
| `/api/v1/runs/{run_id}/xcom/{task_key}` | GET | Retrieve node output data |

### 14.2 Operational

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/pools` | GET/POST | Manage resource pools |
| `/api/v1/runs?dag_id=X&status=FAILED` | GET | Query Runs by filters |

---

## 15. Security Considerations

- **Input Sanitization:** Template expressions are evaluated in a sandboxed interpreter. No arbitrary code execution — only field access, comparison, and boolean logic.
- **Namespace RBAC:** Integrate with the Zanzibar-Lite permission engine. `dag:<namespace>` as the object, with `publish`, `trigger`, `cancel`, `view` relations.
- **Secret Injection:** Sensitive config values (credentials, API keys) are referenced by vault key, resolved at dispatch time by the Leader, and never persisted in `resolved_config`. Marker: `{{ secret.<key> }}`.
- **XCom Isolation:** `output_data` is scoped to a Run. Cross-Run xcom access is prohibited.

---

## Appendix A: Design Decision Log

| Decision | Rationale |
|---|---|
| YAML-first blueprints over code-first (Airflow) | Kestra proves YAML lowers the authoring barrier for ops/DE teams. Code-level extensibility is preserved via the TaskType SPI. |
| Leader-driven orchestration over worker-aware DAGs | Workers remain generic. This avoids coupling Layer 1 workers to DAG semantics, preserving horizontal scalability. |
| Retry at DAG layer, not Layer 1 | DAG-level retry can factor in dependency state, skip cascading, and backoff policy. Layer 1 retries are too coarse. |
| Immutable versioned blueprints | Prevents mid-flight mutation of execution semantics. Matches Kestra's flow versioning model. |
| `__dag_route__` + `condition` dual routing | Route-by-key handles complex business logic in handlers. Conditions handle simple cases declaratively. Both needed in practice. |
| No automatic in-flight migration | The risk of corrupting Run state mid-execution outweighs the convenience. Cancel-and-retrigger is the safe path. |