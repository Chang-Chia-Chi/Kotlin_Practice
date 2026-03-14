# DAG Orchestration Specification (Layer 2)

**Pattern Goal:** Task Parallelism. Orchestrate heterogeneous, multi-step business workflows with complex dependency graphs, dynamic branching, and state passing.

## 1. Core Concepts
* **DAG Blueprint:** The static, declarative definition of nodes and directed edges.
* **Run:** A specific execution instance of a DAG Blueprint. Acts as the correlation boundary (`group_id`) for underlying Layer 1 tasks.
* **Task Instance:** A single node within a Run. Backed 1:1 by a generic Layer 1 task.
* **XCom (State Passing):** The mechanism for passing output data from upstream nodes to downstream nodes. Heavy payloads must be written to external object storage, with only URI references passed through the state machine.



## 2. Logical Data Model
The DAG orchestrator requires two state tables.

**Table: dag_run**
* `run_id`: Primary Key (UUID). Correlates all child tasks.
* `dag_id`: Identifier linking back to the static DAG Blueprint.
* `status`: Current state (RUNNING, COMPLETED, FAILED).
* `global_context`: JSON payload containing initial trigger parameters.

**Table: dag_task_instance**
* `instance_id`: Primary Key (UUID).
* `run_id`: Foreign key to `dag_run`.
* `task_key`: Logical identifier for the node within the blueprint.
* `dependencies`: JSON array of `task_key` strings that must complete before this node.
* `status`: Current node state (BLOCKED, READY, RUNNING, COMPLETED, SKIPPED, FAILED).
* `trigger_rule`: Defines failure tolerance (e.g., ALL_SUCCESS, ONE_SUCCESS, ALL_DONE).
* `output_data`: JSON payload containing the result metadata or external storage URIs.

## 3. The Orchestration State Machine (Leader Loop)
Workers are unaware of the DAG. The Leader pod holding the Kubernetes Lease drives the graph forward by monitoring Layer 1 generic tasks.

1. **Reconcile:** The Leader polls for generic Layer 1 tasks tied to a `dag_run` that have reached a terminal state (COMPLETED or DEAD_LETTER). It updates the corresponding `dag_task_instance` status and saves the `output_data`.
2. **Identify Dependents:** The Leader queries the DAG topology to find all BLOCKED nodes that list the newly resolved node in their dependencies.
3. **Evaluate Trigger Rules:** For each dependent node, the Leader inspects all upstream parents. Based on the `trigger_rule`, it decides if the node transitions to READY or SKIPPED.
    * *Cascade Protocol:* If a node evaluates to SKIPPED, the Leader immediately recursively evaluates that node's dependents to propagate the skip down the branch.
4. **Dispatch:** The Leader merges the `global_context` and upstream `output_data` into a single payload, transitions READY nodes to RUNNING, and enqueues them into the Layer 1 task table.

## 4. Dynamic Branch Routing
To support runtime execution paths, handlers can inject a routing directive into their output.
* If a completed task's output JSON contains a reserved framework key (e.g., `__dag_route__` with an array of target node keys), the Leader intercepts this during the Reconcile phase.
* The Leader forcefully transitions any immediate downstream nodes *not* in the routing array to SKIPPED, effectively pruning the graph based on runtime business logic.