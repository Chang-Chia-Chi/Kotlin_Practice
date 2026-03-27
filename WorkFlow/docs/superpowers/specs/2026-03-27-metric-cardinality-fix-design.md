# R4.8 — Fix `task_backlog_depth` Metric Cardinality

**Date:** 2026-03-27
**Session:** 11
**Scope:** Single file change — `src/main/resources/query-exporter.yaml`

---

## Problem

The current `task_backlog_depth` metric in `query-exporter.yaml` groups by `(workflow_id, sequence_number)`:

```yaml
task_backlog_depth:
  sql: >-
    SELECT w.id AS workflow_id, t.sequence_number AS seq, COUNT(*) AS cnt
    FROM task t JOIN workflow w ON t.workflow_id = w.id
    WHERE t.status = 'PENDING' AND w.status = 'RUNNING'
    GROUP BY w.id, t.sequence_number
  tagColumns: [workflow_id, seq]
```

Both `workflow_id` and `sequence_number` are unbounded — cardinality grows with traffic. With thousands of concurrent workflows this creates thousands of unique time series, risking Micrometer registry OOM and Prometheus scrape timeouts.

## Solution

Replace the single unbounded metric with two bounded metrics:

### 1. `task_backlog_depth` — pending count per handler type

```yaml
task_backlog_depth:
  sql: >-
    SELECT handler_key, COUNT(*) AS depth
    FROM task
    WHERE status = 'PENDING'
    GROUP BY handler_key
  datasource: "default"
  schedule:
    interval: PT30S
  metrics:
    - name: task_backlog_depth
      type: GAUGE
      valueColumn: depth
      tagColumns: [handler_key]
```

- **Cardinality:** Bounded by the number of registered handler types (a small, static set controlled by the codebase).
- **Use case:** "Which handler type is falling behind?" Answers the operational question of whether a specific task type (e.g., `send-email`, `process-payment`) is accumulating backlog.
- **Multiple workflow instances:** Independent runs of the same workflow definition share `handler_key` values. Aggregating by `handler_key` naturally sums across all instances — this is the desired behavior since the concern is handler throughput, not individual instance progress.

### 2. `workflow_deep_backlog_count` — anomaly detection for stuck instances

```yaml
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
    interval: PT60S
  metrics:
    - name: workflow_deep_backlog_count
      type: GAUGE
      valueColumn: cnt
```

- **Cardinality:** Single scalar (always one time series).
- **Use case:** "Are any individual workflow instances stuck with an unusually deep backlog?" Alerts when the count of workflows exceeding the threshold (10 pending tasks) is non-zero.
- **Threshold rationale:** 10 is a starting point. Most workflows should not have more than a handful of pending tasks at any time. Tunable via SQL change if needed.

## Files Changed

| File | Change |
|------|--------|
| `src/main/resources/query-exporter.yaml` | Replace `task_backlog_depth` query; add `workflow_deep_backlog_count` query |

## Testing

1. Existing `ExporterConfigValidator` tests confirm the updated YAML loads without validation errors.
2. Verify `/q/metrics` endpoint shows:
   - `task_backlog_depth` with `handler_key` label only (no `workflow_id` or `seq`)
   - `workflow_deep_backlog_count` with no labels
3. No other metrics are affected.

## Decisions Made

| Decision | Rationale |
|----------|-----------|
| Drop R4.5 (partitioning) | Throughput doesn't warrant it |
| Drop R4.6 (archive + purge job) | Not needed right now |
| Group backlog by `handler_key` not `workflow_id` | Bounded cardinality; multiple instances of the same workflow def are independent but share handler types |
| Add `workflow_deep_backlog_count` | Preserves per-instance anomaly detection without per-instance cardinality cost |
| Threshold of 10 for deep backlog | Conservative starting point; tunable via SQL |
