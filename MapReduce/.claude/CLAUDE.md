# Project: Distributed Backend (Kotlin/Quarkus/Maven/JDBI)

* When asked to update memory, you must update `./.claude/CLAUDE.md` if it is not specified that another file should be modified.

## Core Stack

* **Runtime:** Kotlin 2.3.x (all-open active), Quarkus 3.x (Maven 3.9+)
* **Data:** JDBI 3 (SQL Object API) + Oracle (RDBMS)
* **Observability:** OpenTelemetry (OTel), RED Method metrics

## Essential Commands

* **Dev mode:** `./mvnw quarkus:dev`
* **Build & Package:** `./mvnw package` (Add `-Dnative` for GraalVM)
* **Testing:** `./mvnw test` (Continuous: `mvn quarkus:test`)
* **Dependency Tree:** `./mvnw quarkus:dependency-tree`

## Critical Guardrails (Tier 1)
* **Injection:** ALWAYS use primary constructor injection. NEVER use `@Inject` on fields to ensure native-image safety.
* **Concurrency:** Prefer Kotlin Coroutines (`suspend`) for REST. Use `awaitSuspending()` for Mutiny integration.
* **Secrets:** NEVER hardcode credentials. Use `application.properties` with env expansion.
* **SQL Safety:** Use named parameters (`:param`) to prevent injection.

## Architecture & Structure
* Framework: Quarkus (Kubernetes Native Java)
* Language: Kotlin 2.3.x (using all-open plugin)
* Build System: Apache Maven 3.9.x
* Structure:
* `src/main/kotlin`: Application sources
* `src/main/resources/application.properties`: Centralized configuration
* `src/test/kotlin`: JUnit 5 tests

## Coding Standards
* Dependency Injection: Use ArC (CDI). Prefer package-private access for @Inject fields/constructors to avoid reflection fallback in native images.
* Persistence: Hibernate ORM with Panache. Use the Active Record pattern by extending PanacheEntity.
* Concurrency: Prefer Kotlin Coroutines (suspend) for REST endpoints.
* Reactivity: Never block I/O threads; use `withContext(Dispatchers.IO)` if blocking calls are necessary.

## Local Environment
* **Maven:** No system `mvn` on PATH. Use `./mvnw` (Maven Wrapper) or the cached distribution at `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn`.

## Recent Decisions (Memory)
* 001: Standardized on Maven over Gradle for consistent plugin integration.
* 002: Implemented Kotlin Coroutines bridge for Mutiny reactive streams.
