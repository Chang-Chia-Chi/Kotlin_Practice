# Test Suite Speed — Design Spec

**Date:** 2026-04-01
**Status:** Approved

## Problem

`mvn test` takes ~8 minutes. The primary causes are:

1. **Stress tests run in the default suite.** Six `@Tag("stress")` classes are not excluded by surefire. Each starts its own `ToxiproxyContainer` (~5-10s × 6 = 30-60s in container overhead alone), and each test burns real wall-clock time waiting for watchdog cycles (`sweepInterval + staleTaskThreshold + gracePeriod ≈ 6s minimum per test`), with 30s Awaitility timeouts.
2. **Oracle container cold start** (~60-90s, one-time, unavoidable).
3. **Multiple ToxiproxyContainer instances.** Each stress test class creates and destroys its own container, wasting Docker lifecycle time even within the stress profile.

## Goals

- `mvn test` completes in ~2 min (Oracle start + unit/integration tests only).
- `mvn test -Pstress` runs the full stress suite, faster than today due to shared Toxiproxy.
- No change to test logic or coverage.

## Design

### 1. Test Tier Structure

Three Maven tiers:

| Tier | Command | surefire `groups` | surefire `excludedGroups` |
|------|---------|-------------------|--------------------------|
| Default (fast) | `mvn test` | *(none)* | `stress` |
| Stress | `mvn test -Pstress` | `stress` | *(none)* |
| All | `mvn test -Pall-tests` | *(none)* | *(none)* |

**surefire default config** gains `<excludedGroups>stress</excludedGroups>`.

**`stress` Maven profile** overrides surefire with `<groups>stress</groups>` and clears `<excludedGroups>`.

**`all-tests` Maven profile** overrides surefire to clear both `<groups>` and `<excludedGroups>`.

Oracle integration tests (`RepositoryTest`, `WorkflowIntegrationTest`, `QueryExporterIntegrationTest`, etc.) remain in the default tier — they are already fast enough via the shared `OracleTestContainer` singleton and provide the necessary integration backstop for CI.

### 2. Shared ToxiproxyContainer Singleton

A new `ToxiproxyTestContainer` singleton object mirrors the existing `OracleTestContainer` pattern:

```
src/test/kotlin/infrastructure/persistence/ToxiproxyTestContainer.kt
```

It starts one `ToxiproxyContainer` for the JVM lifetime. It also calls `Testcontainers.exposeHostPorts(oraclePort)` once at initialization so the container can reach the host Oracle port — this call must move out of `StressTestBase.@BeforeAll` into the singleton.

```kotlin
object ToxiproxyTestContainer {
    private val oraclePort = OracleTestContainer.oracle.getMappedPort(1521)
    val container: ToxiproxyContainer = ToxiproxyContainer(...).apply {
        start()
        Testcontainers.exposeHostPorts(oraclePort)  // moved here from StressTestBase
    }
    fun newOracleProxy(): ToxiproxyContainer.ContainerProxy =
        container.getProxy("host.testcontainers.internal", oraclePort)
}
```

Each call to `newOracleProxy()` allocates a new proxy on a distinct port within the container's exposed range (Testcontainers default: 10 slots). With 6 stress classes this is well within range.

**`StressTestBase` changes:**

- `@BeforeAll initInfrastructure()`: replace the `ToxiproxyContainer(...).apply { start() }` block and the `Testcontainers.exposeHostPorts(...)` call with `oracleProxy = ToxiproxyTestContainer.newOracleProxy()`.
- `@AfterAll tearDownInfrastructure()`: remove `toxiproxyContainer.stop()`. The proxy itself is left allocated (ports are plentiful); the container lives for the JVM.
- `@AfterEach cleanUp()`: no change — toxic removal logic is already proxy-scoped.

This collapses 6 Docker container startups into 1 for the entire stress profile run.

## Files to Change

| File | Change |
|------|--------|
| `pom.xml` | Add `<excludedGroups>stress</excludedGroups>` to default surefire config; add `stress` and `all-tests` profiles |
| `src/test/kotlin/infrastructure/persistence/ToxiproxyTestContainer.kt` | New singleton object |
| `src/test/kotlin/stress/StressTestBase.kt` | Use shared container; remove per-class start/stop |

## Expected Outcome

| Command | Before | After |
|---------|--------|-------|
| `mvn test` | ~8 min | ~2 min |
| `mvn test -Pstress` | *(same as above)* | ~3-4 min |
| `mvn test -Pall-tests` | ~8 min | ~5-6 min |
