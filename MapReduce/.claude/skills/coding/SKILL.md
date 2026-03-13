# Idiomatic Quarkus-Kotlin Standards
## Framework Patterns
* Kotlin Classes: All classes are final by default in Kotlin. Rely on the `all-open` Maven plugin to handle proxying for @ApplicationScoped beans rather than using manual 'open' keywords.
* DTOs: Use Kotlin data classes for JSON serialization with Jackson/JSON-B.
* Validation: Use Jakarta Bean Validation (e.g., @NotNull, @Size) on data class properties.

## Reactive & Asynchronous
* Mutiny Integration: Use `Uni<T>.awaitSuspending()` to convert Mutiny types to Kotlin Coroutines.
* Stream Handling: Use `Multi<T>.asFlow()` for reactive streams in Kotlin.
* Threading: Use non-blocking reactive drivers for all database and network I/O.
* Using flow/sharedFlow for abstraction of streaming data flow.
* **No Virtual Threads:** NEVER use `Thread.ofVirtual()` or Java virtual threads. Always use Kotlin Coroutines (`launch`, `withContext(Dispatchers.IO)`) for concurrent/async work.
* **Kotlin-Native Streaming:** Prefer `Flow<T>` over `Sequence<T>` for any data pipeline that crosses boundaries (SPI, DB cursors, inter-component). `Flow` is coroutine-friendly, supports backpressure, and composes with `map`/`filter`/`collect`. Reserve `Sequence` only for purely synchronous, in-memory transformations.

## Testing Strategy
* Core Testing: Use `@QuarkusTest` for integration tests.
* Mocking: Use `@InjectMock` for CDI bean mocking (requires `quarkus-junit5-mockito`).
* API Validation: Use `RestAssured` for testing REST endpoints.


## Directory Index & Deep Context

* For JDBI/RDBMS Mapping: see `.claude/rules/data_layer.md`
* For Observability/Distributed Patterns: see `.claude/rules/distributed_systems.md`
* For MCP/Tooling Config: see `.claude/rules/mcp_config.md`
* Personal overrides: `.claude/local.md` (Gitignored)