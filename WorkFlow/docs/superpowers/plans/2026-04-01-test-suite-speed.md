# Test Suite Speed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut `mvn test` from ~8 min to ~2 min by excluding stress tests from the default suite and sharing a single ToxiproxyContainer across all stress test classes.

**Architecture:** Two changes — a Maven surefire config change to gate tests by JUnit tag, and a new `ToxiproxyTestContainer` singleton that mirrors the existing `OracleTestContainer` pattern. No test logic changes.

**Tech Stack:** Maven Surefire 3.5.2 (JUnit 5 tag filtering), Testcontainers (`ToxiproxyContainer`), Kotlin object singletons.

---

### Task 1: Exclude stress tests from default `mvn test`

**Files:**
- Modify: `pom.xml` (surefire `<configuration>` block at line 336; `<profiles>` block at line 367)

- [ ] **Step 1: Add `<excludedGroups>` to the default surefire config**

In `pom.xml`, find the surefire `<configuration>` block (around line 336) and add one line:

```xml
<configuration>
    <systemPropertyVariables>
        <java.util.logging.manager>org.jboss.logmanager.LogManager</java.util.logging.manager>
    </systemPropertyVariables>
    <!-- Pass JaCoCo agent argLine to the forked JVM -->
    <argLine>@{argLine} --add-opens java.base/java.lang=ALL-UNNAMED</argLine>
    <excludedGroups>stress</excludedGroups>
</configuration>
```

- [ ] **Step 2: Add the `stress` and `all-tests` Maven profiles**

In `pom.xml`, inside `<profiles>` just before `</profiles>` (line 384), add two profiles after the existing `benchmark` profile:

```xml
        <profile>
            <id>stress</id>
            <build>
                <plugins>
                    <plugin>
                        <artifactId>maven-surefire-plugin</artifactId>
                        <configuration>
                            <groups>stress</groups>
                            <excludedGroups/>
                        </configuration>
                    </plugin>
                </plugins>
            </build>
        </profile>
        <profile>
            <id>all-tests</id>
            <build>
                <plugins>
                    <plugin>
                        <artifactId>maven-surefire-plugin</artifactId>
                        <configuration>
                            <excludedGroups/>
                        </configuration>
                    </plugin>
                </plugins>
            </build>
        </profile>
```

- [ ] **Step 3: Verify stress tests are skipped in default run**

Run:
```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow --no-transfer-progress 2>&1 | grep -E "stress|Tests run:|BUILD"
```

Expected: No stress test class names appear in the output. You should see `BUILD SUCCESS` and test counts that exclude the ~30+ stress test cases.

- [ ] **Step 4: Verify stress tests run with `-Pstress`**

Run:
```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Pstress --no-transfer-progress 2>&1 | grep -E "LivenessStressTest|CorrectnessStressTest|Tests run:|BUILD"
```

Expected: Stress test class names appear (e.g. `LivenessStressTest`), `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add WorkFlow/pom.xml
git commit -m "build: exclude stress tests from default mvn test, add stress and all-tests profiles"
```

---

### Task 2: Create `ToxiproxyTestContainer` singleton

**Files:**
- Create: `WorkFlow/src/test/kotlin/infrastructure/persistence/ToxiproxyTestContainer.kt`

- [ ] **Step 1: Create the singleton file**

Create `WorkFlow/src/test/kotlin/infrastructure/persistence/ToxiproxyTestContainer.kt` with:

```kotlin
package com.workflow.infrastructure.persistence

import org.testcontainers.Testcontainers
import org.testcontainers.containers.ToxiproxyContainer
import org.testcontainers.utility.DockerImageName

/**
 * Shared Toxiproxy container for all stress tests.
 * Singleton object — one container per JVM / test run.
 *
 * Mirrors OracleTestContainer. Call [newOracleProxy] once per stress test class
 * in @BeforeAll to get an isolated proxy pointing at the shared Oracle container.
 * Do NOT stop the container in @AfterAll — it lives for the JVM lifetime.
 */
object ToxiproxyTestContainer {

    private val oraclePort = OracleTestContainer.oracle.getMappedPort(1521)

    val container: ToxiproxyContainer = run {
        Testcontainers.exposeHostPorts(oraclePort)
        ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.9.0"))
            .apply { start() }
    }

    fun newOracleProxy(): ToxiproxyContainer.ContainerProxy =
        container.getProxy("host.testcontainers.internal", oraclePort)
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow --no-transfer-progress 2>&1 | tail -5
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Commit**

```bash
git add "WorkFlow/src/test/kotlin/infrastructure/persistence/ToxiproxyTestContainer.kt"
git commit -m "test: add ToxiproxyTestContainer singleton to share one container across stress tests"
```

---

### Task 3: Update `StressTestBase` to use the shared container

**Files:**
- Modify: `WorkFlow/src/test/kotlin/stress/StressTestBase.kt`

- [ ] **Step 1: Remove the per-class `toxiproxyContainer` field**

In `StressTestBase.kt`, remove this line (line 72):

```kotlin
private lateinit var toxiproxyContainer: ToxiproxyContainer
```

- [ ] **Step 2: Replace the per-class container startup block**

In `initInfrastructure()`, replace these four lines (lines 163–171):

```kotlin
        // Toxiproxy wrapping Oracle
        val oraclePort = OracleTestContainer.oracle.getMappedPort(1521)
        Testcontainers.exposeHostPorts(oraclePort)

        toxiproxyContainer = ToxiproxyContainer(
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.9.0"),
        ).apply { start() }

        oracleProxy = toxiproxyContainer.getProxy("host.testcontainers.internal", oraclePort)
```

With:

```kotlin
        // Toxiproxy wrapping Oracle — shared singleton, one container per JVM
        oracleProxy = ToxiproxyTestContainer.newOracleProxy()
```

- [ ] **Step 3: Remove `toxiproxyContainer.stop()` from teardown**

In `tearDownInfrastructure()`, remove this line (currently line 210):

```kotlin
        toxiproxyContainer.stop()
```

The method should now read:

```kotlin
    @AfterAll
    fun tearDownInfrastructure() {
        proxyDataSource.close()
        directDataSource.close()
    }
```

- [ ] **Step 4: Remove unused imports**

Remove these two import lines from the top of `StressTestBase.kt`:

```kotlin
import org.testcontainers.Testcontainers
import org.testcontainers.utility.DockerImageName
```

The `import org.testcontainers.containers.ToxiproxyContainer` import stays — `oracleProxy` is still typed as `ToxiproxyContainer.ContainerProxy`.

- [ ] **Step 5: Verify compilation**

Run:
```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow --no-transfer-progress 2>&1 | tail -5
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 6: Run the stress suite and verify it passes**

Run:
```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Pstress --no-transfer-progress 2>&1 | tail -20
```

Expected: All stress tests pass, `BUILD SUCCESS`. The run should be noticeably faster than before because only one ToxiproxyContainer starts instead of six.

- [ ] **Step 7: Run the default suite and verify it is fast**

Run:
```
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow --no-transfer-progress 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`, no stress test classes in output, completes in ~2 min.

- [ ] **Step 8: Commit**

```bash
git add "WorkFlow/src/test/kotlin/stress/StressTestBase.kt"
git commit -m "test: use shared ToxiproxyTestContainer singleton in StressTestBase"
```
