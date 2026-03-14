# AI Coding Guidelines: Distributed Task Framework

**Context:** You are writing code for a high-performance Task Queue, DAG, and Map-Reduce orchestrator using Kotlin, Quarkus, and Oracle Database.
**Directive:** You MUST strictly adhere to the following technical constraints when generating any code, interfaces, or configurations for this project.

## Framework Patterns
* Kotlin Classes: All classes are final by default in Kotlin. Rely on the `all-open` Maven plugin to handle proxying for @ApplicationScoped beans rather than using manual 'open' keywords.
* DTOs: Use Kotlin data classes for JSON serialization with Jackson/JSON-B.
* Validation: Use Jakarta Bean Validation (e.g., @NotNull, @Size) on data class properties.

## Reactive & Asynchronous
* Mutiny Integration: Use `Uni<T>.awaitSuspending()` to convert Mutiny types to Kotlin Coroutines.
* **No Virtual Threads:** NEVER use `Thread.ofVirtual()` or Java virtual threads. Always use Kotlin Coroutines (`launch`, `withContext(Dispatchers.IO)`) for concurrent/async work.
* **Kotlin Flow as Default Streaming Primitive:**
  * Use `Flow<T>` for all data pipelines crossing boundaries: SPI contracts, DB cursor results, inter-component streaming.
  * Use `SharedFlow` / `StateFlow` for event broadcasting and observable state.
  * Reserve `List<T>` only for bounded, finite collections needed in their entirety upfront (e.g., `split()` requires a count for `total_tasks`).
  * Reserve `Sequence<T>` only for synchronous, in-memory transformations with no I/O.
* **Suspend-first contracts:** Prefer `suspend fun` for handler and SPI interfaces. This enables native coroutine composition and eliminates `runBlocking` bridges.
* **Threading discipline:** Use `Dispatchers.IO` for all blocking I/O (JDBI, file, network). Use `Dispatchers.Default` for CPU-bound scheduling loops.

## Testing Strategy
* Core Testing: Use `@QuarkusTest` for integration tests.
* Mocking: Use `@InjectMock` for CDI bean mocking (requires `quarkus-junit5-mockito`).
* API Validation: Use `RestAssured` for testing REST endpoints.

## Directory Index & Deep Context
* For JDBI/RDBMS Mapping: see `.claude/skills/data_layer/SKILL.md`
* For Observability/Distributed Patterns: see `.claude/skills/distributed_system/SKILL.md`

## Concurrency & Database Interaction (Oracle)
* **Rule 1: No ORM for Polling.** Do not use Hibernate or JPA `@Lock` annotations for the worker polling loop. You must use native JDBC or jOOQ to execute `SELECT ... FOR UPDATE SKIP LOCKED`.
* **Rule 2: Optimistic Fencing.** All database writes executed by a worker (e.g., updating `mr_output` or `dag_task_instance`) MUST include the `execution_generation` UUID in the `WHERE` clause to actively fence off split-brain zombie workers.
* **Rule 3: Transaction Boundaries.** Framework state transitions (e.g., incrementing a Map-Reduce counter and marking a task complete) must be strictly wrapped in a single database transaction.
* **Rule 4: Connection Pool Awareness.** Assume the Agroal connection pool is strictly limited. Do not spawn unbounded coroutines or threads that require independent database connections.



## Handler Idempotency & State
* **Rule 5: Crash-Only Thinking.** Write every `TaskHandler` implementation assuming the pod will receive a `SIGKILL` halfway through execution.
* **Rule 6: Upsert Semantics.** Handlers must use `INSERT ... ON CONFLICT` (or Oracle `MERGE`) equivalents. A speculative duplicate task might run concurrently with the original task; both must attempt to write safely without throwing unhandled constraint violations.
* **Rule 7: Externalize Blobs.** Never write payload data larger than a few kilobytes to the database. Generate code that streams large JSON/Parquet files to an external blob store (e.g., S3) and returns only the URI as the task result.

## Kotlin & Quarkus Idioms
* **Rule 8: Sealed Interfaces.** Use Kotlin `sealed interface` for all state machines (`TaskResult`, `DagNodeState`, `JobState`). Force exhaustive `when` statements when evaluating states.
* **Rule 9: CDI Dynamism.** Use Quarkus `Arc.container().instance(TaskHandler::class.java, NamedLiteral.of(...))` for runtime dynamic dispatch of routing keys. Do not hardcode `when` or `switch` statements to route tasks.
* **Rule 10: Stream, Don't Load.** In the Map-Reduce `reduce` phase, never load the entire dataset into a `List`. You must use Kotlin `Sequence` or `Flow` to stream data from the external blob store, preventing memory exhaustion.



## Kubernetes Lifecycle & Telemetry
* **Rule 11: Graceful Shutdown.** Implement Quarkus `@Observes ShutdownEvent`. The very first action must be flipping an `AtomicBoolean` to halt the `SKIP LOCKED` polling loop, followed by awaiting the completion of the active bulkhead threads.
* **Rule 12: Cheap Metrics.** When generating Micrometer telemetry, never execute a database `COUNT(*)` from a worker pod. Only the pod holding the Kubernetes Leader Lease is permitted to evaluate global queue depth.

## Output Formatting
* **Rule 13:** Do not generate SQL scripts or database migration files unless explicitly requested.
* **Rule 14:** Do not generate repetitive boilerplate (like getters/setters); rely on Kotlin `data class` features.
* **Rule 15:** Keep classes focused and decoupled, strictly adhering to Domain-Driven Design boundaries between the `queue`, `mapreduce`, and `dag` contexts.
