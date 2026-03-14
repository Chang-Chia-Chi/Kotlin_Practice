### AI Coding Guidelines: Distributed Task Framework

**Context:** You are writing components for a high-performance Task Queue, DAG, and Map-Reduce orchestrator using Kotlin, Quarkus, and an Oracle Database.
**Directive:** You MUST strictly adhere to the following technical constraints when generating any structural designs, interfaces, or configurations for this project.

#### Framework Patterns

* **Kotlin Classes:** All classes are final by default in Kotlin. Rely on the compiler plugins (like `all-open`) to handle proxying for application-scoped beans rather than using manual keywords.
* **DTOs:** Use Kotlin data classes for JSON serialization with modern JSON-B/Jackson tooling.
* **Validation:** Use Jakarta Bean Validation annotations on data class properties to enforce boundary contracts early.

#### Reactive & Asynchronous

* **Mutiny Integration:** Use suspending extension functions to convert Mutiny types to Kotlin Coroutines seamlessly at the architectural boundaries.
* **No Virtual Threads:** Rely exclusively on Kotlin Coroutines for concurrent and asynchronous work. Explicitly control thread boundaries using appropriate dispatchers rather than relying on Java virtual threads, which can introduce hidden pinning issues with older JDBC drivers.
* **Kotlin Flow as Default Streaming Primitive:**
* Use `Flow` for all data pipelines crossing boundaries: SPI contracts, database cursor results, and inter-component streaming.
* Use `SharedFlow` / `StateFlow` for event broadcasting and observable state.
* Reserve standard collections (`List`, `Set`) only for bounded, finite collections needed in their entirety upfront.
* Reserve `Sequence` only for synchronous, in-memory transformations with absolutely no I/O.


* **Suspend-First Contracts:** Prefer suspending functions for handler and SPI interfaces. This enables native coroutine composition and eliminates blocking bridges.
* **Threading Discipline:** Enforce strict dispatcher usage: I/O dispatchers for database, file, or network blocking operations, and default dispatchers for CPU-bound scheduling loops.

#### Testing Strategy

* **Core Testing:** Use Quarkus testing annotations for integration tests.
* **Mocking:** Leverage CDI bean mocking extensions for isolated unit tests.
* **API Validation:** Validate REST endpoints using behavioral testing frameworks like RestAssured.

#### Directory Index & Deep Context

* For RDBMS Mapping patterns: refer to `.claude/skills/data_layer/SKILL.md`
* For Observability/Distributed Patterns: refer to `.claude/skills/distributed_system/SKILL.md`

#### Concurrency & Database Interaction (Oracle)

* **Rule 1: No ORM for Polling.** Do not use heavyweight ORM features or standard JPA locking annotations for the worker polling loop. You must use native database capabilities for row-level locking with skip-locked semantics via JDBC or jOOQ.
* **Rule 2: Optimistic Fencing.** All database writes executed by a worker MUST include an execution-generation token (UUID) in the update condition criteria to actively fence off split-brain zombie workers.
* **Rule 3: Transaction Boundaries.** Framework state transitions (e.g., incrementing a Map-Reduce counter while marking a task complete) must be strictly wrapped in a single, atomic database transaction.
* **Rule 4: Connection Pool Awareness.** Assume the application connection pool is strictly limited. Do not spawn unbounded coroutines or asynchronous tasks that require independent, simultaneous database connections.

#### Handler Idempotency & State

* **Rule 5: Crash-Only Thinking.** Design every task handler assuming the underlying pod will be abruptly terminated halfway through execution.
* **Rule 6: Upsert Semantics.** Handlers must utilize database-native upsert operations. A speculative duplicate task might run concurrently with the original task; both must attempt to write safely without throwing unhandled constraint violations.
* **Rule 7: Externalize Blobs.** Never write payload data larger than a few kilobytes to the database. Design contracts that stream large payloads to an external blob store, returning only the resource URI as the task result.

#### Kotlin & Quarkus Idioms

* **Rule 8: Sealed Interfaces.** Use Kotlin sealed interfaces for all state machines to force exhaustive evaluations when handling state transitions.
* **Rule 9: CDI Dynamism.** Use Quarkus programmatic CDI lookup (e.g., injecting an `Instance` iterable with qualifiers) for runtime dynamic dispatch of routing keys. Avoid hardcoded conditional statements to route tasks.
* **Rule 10: Stream, Don't Load.** In the Map-Reduce phases, never load entire datasets into memory. You must stream data from the external blob store, preventing memory exhaustion.

#### Kubernetes Lifecycle & Telemetry

* **Rule 11: Graceful Shutdown.** Observe application shutdown events. The absolute first action must be flipping an atomic flag to halt the polling loop, followed by awaiting the completion of the active bulkhead processes.
* **Rule 12: Cheap Metrics.** When generating telemetry, never execute global aggregation queries from a standard worker pod. Only the pod holding the Kubernetes Leader Lease is permitted to evaluate global queue depths.

#### Output Formatting

* **Rule 13:** Strictly separate domain contexts. Keep classes focused, adhering to Domain-Driven Design boundaries between the queue, map-reduce, and DAG contexts.
* **Rule 14:** Rely on Kotlin's language features to eliminate repetitive boilerplate.