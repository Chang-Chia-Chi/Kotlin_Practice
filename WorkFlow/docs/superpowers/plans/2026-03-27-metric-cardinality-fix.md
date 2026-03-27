# Metric Cardinality Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the unbounded `task_backlog_depth` metric (grouped by `workflow_id` + `sequence_number`) with two bounded metrics: `task_backlog_depth` grouped by `handler_key`, and `workflow_deep_backlog_count` as a single scalar.

**Architecture:** YAML-only change to `query-exporter.yaml`. No Kotlin code changes. The existing `ExporterConfigValidator` and YAML loader cover validation.

**Tech Stack:** query-exporter YAML config, Oracle SQL

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `src/main/resources/query-exporter.yaml` | Modify (lines 97-111) | Replace `task_backlog_depth` query, add `workflow_deep_backlog_count` query |
| `src/test/kotlin/queryexporter/ExporterConfigTest.kt` | Modify | Add test that loads production `query-exporter.yaml` and validates it |

---

### Task 1: Add production YAML validation test

**Files:**
- Modify: `src/test/kotlin/queryexporter/ExporterConfigTest.kt`

- [ ] **Step 1: Write test that loads and validates the production `query-exporter.yaml`**

Add a new `@Nested` class at the bottom of `ExporterConfigTest`:

```kotlin
@Nested
inner class ProductionYaml {

    @Test
    fun `production query-exporter yaml loads and passes validation`() {
        val input = Thread.currentThread().contextClassLoader
            .getResourceAsStream("query-exporter.yaml")!!
        val config = ExporterConfig.load(input)
        assertDoesNotThrow { ExporterConfigValidator.validate(config) }
    }
}
```

- [ ] **Step 2: Run test to verify it passes with the current YAML**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f "C:\Users\maxch\OneDrive\文件\GitHub\Kotlin_Practice\WorkFlow\pom.xml" test -Dtest="ExporterConfigTest"`
Expected: PASS — the current YAML is valid.

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/queryexporter/ExporterConfigTest.kt
git commit -m "test: add production query-exporter.yaml validation test"
```

---

### Task 2: Replace `task_backlog_depth` and add `workflow_deep_backlog_count`

**Files:**
- Modify: `src/main/resources/query-exporter.yaml` (lines 97-111)

- [ ] **Step 1: Replace the `task_backlog_depth` query block**

Replace lines 97-111 of `query-exporter.yaml` (the entire `task_backlog_depth` entry):

Old:
```yaml
  # -- Repository layer: pending task backlog per workflow sequence --
  task_backlog_depth:
    sql: >-
      SELECT w.id AS workflow_id, t.sequence_number AS seq, COUNT(*) AS cnt
      FROM task t JOIN workflow w ON t.workflow_id = w.id
      WHERE t.status = 'PENDING' AND w.status = 'RUNNING'
      GROUP BY w.id, t.sequence_number
    datasource: "default"
    schedule:
      interval: "PT60S"
    metrics:
      - name: task_backlog_depth
        type: GAUGE
        valueColumn: cnt
        tagColumns: [workflow_id, seq]
```

New:
```yaml
  # -- Pending task backlog per handler type (bounded cardinality) --
  task_backlog_depth:
    sql: >-
      SELECT handler_key, COUNT(*) AS depth
      FROM task
      WHERE status = 'PENDING'
      GROUP BY handler_key
    datasource: "default"
    schedule:
      interval: "PT30S"
    metrics:
      - name: task_backlog_depth
        type: GAUGE
        valueColumn: depth
        tagColumns: [handler_key]

  # -- Anomaly detection: workflow instances with unusually deep pending backlog --
  workflow_deep_backlog_count:
    sql: >-
      SELECT COUNT(*) AS cnt FROM (
        SELECT workflow_id FROM task
        WHERE status = 'PENDING'
        GROUP BY workflow_id
        HAVING COUNT(*) > 10
      )
    datasource: "default"
    schedule:
      interval: "PT60S"
    metrics:
      - name: workflow_deep_backlog_count
        type: GAUGE
        valueColumn: cnt
```

- [ ] **Step 2: Run the production YAML validation test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f "C:\Users\maxch\OneDrive\文件\GitHub\Kotlin_Practice\WorkFlow\pom.xml" test -Dtest="ExporterConfigTest"`
Expected: PASS — all tests including the new production YAML validation test.

- [ ] **Step 3: Run full test suite**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -f "C:\Users\maxch\OneDrive\文件\GitHub\Kotlin_Practice\WorkFlow\pom.xml" test`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/main/resources/query-exporter.yaml
git commit -m "fix: replace unbounded task_backlog_depth with handler_key grouping and deep backlog anomaly metric"
```
