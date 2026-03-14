# Idiomatic Quarkus-Kotlin Standards

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
