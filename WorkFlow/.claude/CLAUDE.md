# Project: Lock-Free Workflow Engine (Kotlin/Quarkus/Maven/JDBI)
Welcome to Claude Code. In this project, you will operate as an Autonomous Backend Agent focusing on Kotlin development.

* When asked to update memory, you must update `./.claude/CLAUDE.md` if it is not specified that another file should be modified.
* What should be in CLAUDE.md is always things most important as highest guideline which will not changed overtime.

## Core Behavioral Guidelines
* **Plan Before Execution**: Before making destructive changes or cross-file modifications, you must use Plan Mode (`/plan` or Shift+Tab).
* **Hierarchical Context Loading**: Dynamically reference the following documents based on the directory you are modifying:
    * When modifying controllers, services, or routing logic in `src/main/kotlin/**`: reference @docs/skills/backend-api-patterns/SKILL.md
* **Test-Driven Development (TDD)**: All refactoring must begin with creating Characterization Tests (using JUnit 5 or Kotest). Ensure tests pass 100% before modifying the main logic.
* **Database Safety**: All database schema modifications must be done via declarative tools or migration scripts (e.g., Flyway or Liquibase). Never execute destructive `ALTER TABLE` SQL commands directly in production environments.

## Terminal & Automation Integration
* When large refactoring is needed, suggest using the `/batch` command to break tasks into 5-30 independent units.
* When outputting JSON data to downstream scripts, strictly adhere to the `--json-schema` constraints.

## Core Stack
* **Runtime:** Kotlin 2.3.x (all-open active), Quarkus 3.x (Maven 3.9+)
* **Data:** JDBI 3 (SQL Object API) + Oracle (RDBMS)

## Essential Commands
* **Dev mode:** `mvn quarkus:dev`
* **Build & Package:** `mvn package` (Add `-Dnative` for GraalVM)
* **Testing:** `mvn test` (Continuous: `mvn quarkus:test`)
* **Dependency Tree:** `mvn quarkus:dependency-tree`
* **Git:** Related to `/commands/commit.md`

## Critical Guardrails (Tier 1)
* **Injection:** ALWAYS use primary constructor injection. NEVER use `@Inject` on fields to ensure native-image safety.
* **Concurrency:** Prefer Kotlin Coroutines (`suspend`) for REST. Use `awaitSuspending()` for Mutiny integration.
* **Secrets:** NEVER hardcode credentials. Use `application.properties` with env expansion.
* **SQL Safety:** Use named parameters (`:param`) to prevent injection.
* **Oracle SQL:** `FETCH FIRST N ROWS ONLY` is incompatible with `FOR UPDATE` (ORA-02014). Use a subquery: `SELECT * FROM t WHERE id IN (SELECT id FROM t ... FETCH FIRST N ROWS ONLY) FOR UPDATE SKIP LOCKED`.
* **Oracle JDBC Nulls:** JDBI `.bind("col", null)` fails on Oracle — use `.bindNull("col", Types.TIMESTAMP)` or `.bindNull("col", Types.VARCHAR)` with explicit SQL type.
* **Oracle Timestamps:** Oracle JDBC returns `oracle.sql.TIMESTAMP`, not `java.sql.Timestamp`. Handle via reflection in row mappers. Truncate `LocalDateTime.now()` to `ChronoUnit.MICROS` (Oracle TIMESTAMP precision).

## Architecture & Structure
* Framework: Quarkus (Kubernetes Native Java)
* Language: Kotlin 2.3.x (using all-open plugin)
* Build System: Apache Maven 3.9.x
* Structure:
* `src/main/kotlin`: Application sources
* `src/main/resources/application.properties`: Centralized configuration
* `src/test/kotlin`: JUnit 5 tests + mockito

## Coding Standards
* File Naming: Use domain-prefixed file names, not generic ones (e.g., `WorkflowDslBuilders.kt` not `Builders.kt`). File names should convey what domain they belong to at a glance.
* Dependency Injection: Use ArC (CDI). Prefer package-private access for @Inject fields/constructors to avoid reflection fallback in native images.
* Concurrency: Prefer Kotlin Coroutines (suspend) for REST endpoints.
* Reactivity: Never block I/O threads; use `withContext(Dispatchers.IO)` if blocking calls are necessary.
* Domain Driven Develop: Code architecture should follow best practice of DDD & clean architecture with simple, concise & elegant interface for flexibility and future scalability. 

## Testing Standards

* **Constraint 1:** Do not use `Thread.sleep()` for assertions or waits. All asynchronous assertions must use `Awaitility.await().untilAsserted(...)`. Exception: `Thread.sleep()` inside mock callbacks to simulate blocking APIs (e.g., K8s `leaderElector.run()`) is acceptable.
* **Constraint 2:** All pure unit tests must use `runTest` from `kotlinx-coroutines-test` for deterministic time control.
* **Constraint 3:** Mock Kubernetes interactions strictly using `@InjectMock` on the Fabric8 `KubernetesClient`. Do not attempt to spin up a real Kubernetes cluster via Testcontainers for unit test.
* **Constraint 4:** Use `ToxiproxyContainer` for all network fault injection scenarios at the database layer.
* **Constraint 5:** Use Oracle Free container (via Testcontainers) for repository/adapter tests. This ensures full SQL compatibility (SKIP LOCKED, CHECK constraints, CLOB behavior) without H2 dialect gaps.
* **Constraint 9:** Share one Oracle container across test classes via `OracleTestContainer` singleton object (`src/test/kotlin/engine/OracleTestContainer.kt`). Do not create per-class containers.
* **Constraint 6:** Ensure test coverage of each component and overall is higher than 85%. Run: `python .claude/scripts/coverage.py target/site/jacoco/index.html --min-instruction 85 --min-branch 70`. Per-package thresholds in `.claude/skills/jacoco-coverage.md`.
* **Constraint 7:** Use .properties instead of .yaml for configuration file.
* **Constraint 8:** Test config lives in `src/test/resources/application.properties`. Do not use `%test.*` profile lines in main `application.properties`.

## Local Environment
* **Docker:** Docker Desktop must be running for Testcontainers tests. Verify with `docker info | grep "Server Version"`.
* **Maven:** No system `mvn` on PATH. `./mvnw` does not work in bash-on-Windows — always use the cached distribution at `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn`.
* **Running specific tests:** Use class names with surefire: `-Dtest="LeaderManagerTest,NotLeaderTest"`. Package glob patterns (`com.workflow.leader.*`) do not match.