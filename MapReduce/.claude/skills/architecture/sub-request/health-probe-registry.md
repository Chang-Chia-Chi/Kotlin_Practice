# Health Probe Registry — Design Document

**Version:** 1.0 Draft  
**Date:** 2026-03-15  
**Status:** Proposal  
**Parent:** Task Queue & Map-Reduce Framework v2.0  
**Depends on:** Event Bus

---

## 1. Problem Statement

The framework has multiple subsystems, each with its own definition of "healthy." Without coordination:

- Each subsystem registers its own SmallRye Health check in isolation.
- There's no shared vocabulary for what "liveness" and "readiness" mean across subsystems.
- Aggregation semantics are undefined — does one open circuit breaker make the pod not-live? (No.) Does a dead worker loop? (Yes.) These decisions are architectural, not per-subsystem.
- The K8s probes see a flat list of checks with no semantic grouping. Operators can't tell which subsystem failed or why.

The health probe registry provides a unified contributor interface, clear aggregation rules, and a detailed diagnostic endpoint for debugging.

---

## 2. Goals & Non-Goals

### Goals

- **Unified interface.** Every subsystem contributes health checks through the same `HealthContributor` contract.
- **Clear aggregation semantics.** Liveness and readiness have defined rules for how individual contributor results combine into a final UP/DOWN.
- **Detailed diagnostics.** A `/q/health/detail` endpoint returns per-subsystem status with human-readable details — not just the binary UP/DOWN that K8s needs.
- **Automatic discovery.** Subsystems register as CDI beans implementing `HealthContributor`. No manual wiring.

### Non-Goals

- Custom health check UIs. The diagnostic endpoint returns JSON; dashboarding is handled by Grafana or similar.
- Health-based auto-scaling. K8s HPA uses metrics, not health probes. The registry feeds probes, not scaling decisions.
- Deep dependency checks (e.g., is Trino healthy? is MinIO reachable?). These are external systems; the framework checks its own internals. Handler-level circuit breakers cover downstream health indirectly.

---

## 3. Contributor Interface

Every subsystem implements a `HealthContributor` with three elements:

| Element | Purpose |
|---------|---------|
| **name** | Unique identifier for this contributor (e.g., `"worker-loop"`, `"oracle"`, `"leader-election"`) |
| **liveness()** | Returns UP, DOWN, or DEGRADED — or null if this contributor has no liveness opinion |
| **readiness()** | Returns UP, DOWN, or DEGRADED — or null if this contributor has no readiness opinion |

Each result includes a `details` map with human-readable context (e.g., `"lastPollAge": 2`, `"reason": "election loop heartbeat stale"`).

**Returning null** means "I have no opinion on this dimension." A subsystem that only cares about readiness (e.g., shutdown state) returns null for liveness. The aggregator skips null contributors.

---

## 4. Aggregation Rules

### 4.1 Liveness

The pod is live if all contributors that provide a liveness check return UP or DEGRADED. Any single DOWN means the pod should be restarted.

```
Liveness = AND(all non-null liveness checks are UP or DEGRADED)
```

Rationale: liveness means "this pod is not broken beyond recovery." A degraded subsystem (e.g., one circuit breaker open) is not broken — it's functioning with reduced capacity. A dead worker loop or a hung election thread IS broken — the pod should be restarted.

### 4.2 Readiness

The pod is ready if all contributors that provide a readiness check return UP or DEGRADED. Any single DOWN means the pod should be removed from the K8s Service.

```
Readiness = AND(all non-null readiness checks are UP or DEGRADED)
```

Rationale: readiness means "this pod can do useful work." A pod that is shutting down (readiness DOWN from the shutdown contributor) should not receive new config API traffic. A pod whose handler registry isn't initialized yet should not receive traffic. But a pod with one open circuit breaker (DEGRADED) can still process tasks for other handlers — removing it from the Service would be wasteful.

### 4.3 DEGRADED Semantics

DEGRADED is a third state between UP and DOWN. It means "functioning but not at full capacity." Both liveness and readiness treat it as UP for aggregation purposes.

The distinction matters for the diagnostic endpoint and dashboards: an operator sees DEGRADED and knows something needs attention even though the pod is still operational.

---

## 5. Standard Contributors

### 5.1 Worker Loop

| Dimension | Check | UP | DOWN |
|-----------|-------|-----|------|
| Liveness | Is the claim coroutine alive? Compares `lastPollTimestamp` against 3× poll interval. | Last poll was recent. | Claim coroutine hasn't polled in 3× the expected interval. Worker loop is dead or hung. |
| Readiness | Is the handler registry populated? | All handlers discovered and registered. | CDI hasn't finished initializing handlers. Pod isn't ready to process tasks. |

### 5.2 Oracle Connectivity

| Dimension | Check | UP | DOWN |
|-----------|-------|-----|------|
| Liveness | Can the pod execute `SELECT 1 FROM DUAL`? | Query succeeds within timeout. | Query fails or times out. Oracle is unreachable. |
| Readiness | Same as liveness. | — | — |

Note: this is a basic connectivity check, not a capacity check. Oracle might be reachable but overloaded — that's a metrics/alerting concern, not a health probe concern.

### 5.3 Leader Election

| Dimension | Check | UP | DOWN |
|-----------|-------|-----|------|
| Liveness | Is the election thread alive? Compares `lastHeartbeat` against `renewDeadline + retryPeriod`. | Election loop is running and updating its heartbeat. | Election thread is dead (deadlock, OOM in thread, thread death). |
| Readiness | *null* (no opinion). | — | — |

Leadership is not required for readiness. A non-leader pod is fully functional: it serves the config API, runs the worker loop, and claims tasks. Making leadership a readiness requirement would remove 2 of 3 pods from the Service, defeating the purpose of a homogeneous deployment.

Override: deployments that expose leader-only REST endpoints can configure the leader election contributor to return DOWN for readiness when not leading.

### 5.4 Shutdown State

| Dimension | Check | UP | DOWN |
|-----------|-------|-----|------|
| Liveness | *null* (no opinion). | — | — |
| Readiness | Is the pod in RUNNING state (not shutting down)? | Shutdown coordinator state is RUNNING. | State is DRAINING, RELEASING, or TERMINATED. Pod should be removed from Service. |

Shutting down doesn't mean the pod is dead (liveness null) — it means it should stop receiving new work (readiness DOWN). The K8s Service removes it from endpoints, and the pod drains gracefully.

### 5.5 Circuit Breakers

| Dimension | Check | UP | DEGRADED | DOWN |
|-----------|-------|-----|----------|------|
| Liveness | *null* (no opinion). | — | — | — |
| Readiness | What's the overall circuit breaker state? | All breakers closed. | Some breakers open, others closed. Pod can still process unaffected handlers. | ALL breakers open. Pod can't do any useful work. |

This is the primary use case for DEGRADED. One open breaker out of five doesn't warrant removing the pod from the Service. Five out of five does.

### 5.6 Stale Task Reaper (Leader Only)

| Dimension | Check | UP | DOWN |
|-----------|-------|-----|------|
| Liveness | Is the reaper coroutine alive? Compares `lastScanTimestamp` against 3× scan interval. Only checked if this pod is the leader. | Reaper scanned recently. | Reaper hasn't scanned in 3× the expected interval. Coroutine may be dead. |
| Readiness | *null* (no opinion). | — | — |

Returns null for both dimensions if this pod is not the leader. The reaper only runs on the leader — non-leader pods shouldn't be penalized for not running it.

---

## 6. Contributor Summary Matrix

| Contributor | Liveness | Readiness |
|-------------|----------|-----------|
| Worker Loop | Claim coroutine alive | Handler registry ready |
| Oracle | Connectivity check | Connectivity check |
| Leader Election | Election thread alive | null (optional: is leader) |
| Shutdown | null | Not shutting down |
| Circuit Breakers | null | Not all breakers open |
| Stale Reaper | Reaper coroutine alive (leader only) | null |

---

## 7. SmallRye Health Integration

The registry registers two SmallRye `HealthCheck` beans:

- **Liveness aggregator** (annotated `@Liveness`): iterates all `HealthContributor` beans, calls `liveness()`, aggregates per §4.1, returns a `HealthCheckResponse` for `/q/health/live`.
- **Readiness aggregator** (annotated `@Readiness`): same pattern for `readiness()`, aggregated per §4.2, returns response for `/q/health/ready`.

K8s probes hit these standard SmallRye endpoints. The response is the standard MicroProfile Health JSON format that K8s expects.

---

## 8. Diagnostic Endpoint

Beyond the binary UP/DOWN that K8s needs, operators need a detailed view for debugging. The registry exposes:

```
GET /q/health/detail
```

Returns a JSON document with the full per-subsystem breakdown:

```
{
  "pod": "dispatch-worker-2",
  "isLeader": true,
  "epoch": 1042,
  "shutdownState": "RUNNING",
  "liveness": {
    "status": "UP",
    "checks": {
      "worker-loop":      { "status": "UP",   "lastPollAge": 2 },
      "oracle":           { "status": "UP"  },
      "leader-election":  { "status": "UP",   "heartbeatAge": 1, "epoch": 1042 },
      "stale-reaper":     { "status": "UP",   "lastScanAge": 12 }
    }
  },
  "readiness": {
    "status": "DEGRADED",
    "checks": {
      "worker-loop":      { "status": "UP",   "handlers": 8 },
      "oracle":           { "status": "UP"  },
      "shutdown":         { "status": "UP"  },
      "circuit-breakers": { "status": "DEGRADED", "open": ["sftp.upload"] }
    }
  },
  "bulkhead": {
    "limit": 4,
    "active": 2,
    "idle": 2
  }
}
```

This endpoint is not used by K8s probes (which need fast, sub-second binary responses). It's for dashboards (Grafana polling every 30 seconds), runbooks ("step 1: check /q/health/detail on each pod"), and incident response.

**Access control:** In production, this endpoint should be restricted to internal traffic (K8s Service with `clusterIP`, not exposed via Ingress). It contains operational details (handler names, circuit breaker states) that are sensitive in a multi-tenant context.

---

## 9. K8s Probe Configuration

Recommended K8s probe settings that work with the registry:

### Liveness

```yaml
livenessProbe:
  httpGet:
    path: /q/health/live
    port: 8080
  initialDelaySeconds: 15    # allow CDI init + first election cycle
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3        # restart after 30s of consecutive failures
```

Why `failureThreshold: 3`: a single failed liveness check might be a transient Oracle blip. Three consecutive failures (30 seconds) confirms the pod is genuinely unhealthy.

### Readiness

```yaml
readinessProbe:
  httpGet:
    path: /q/health/ready
    port: 8080
  periodSeconds: 5
  failureThreshold: 1        # remove from Service immediately on failure
```

Why `failureThreshold: 1`: readiness should be fast. When the pod starts shutting down, it should be removed from the Service on the very next probe check, not after 3 failures. One failure is enough to stop routing traffic.

---

## 10. Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `taskqueue.health.oracle-check-timeout` | `5s` | Max time for the Oracle connectivity check |
| `taskqueue.health.worker-loop-stale-threshold` | `3× poll interval` | When to consider the worker loop dead |
| `taskqueue.health.detail-endpoint-enabled` | `true` | Whether to expose `/q/health/detail` |
| `taskqueue.health.leader-readiness-enabled` | `false` | Whether leadership is required for readiness (default: no) |

---

## 11. Testing Strategy

| Test | Validates |
|------|-----------|
| All contributors return UP → aggregated liveness and readiness are UP | Happy path |
| Worker loop contributor returns DOWN → aggregated liveness is DOWN | Single contributor failure |
| Circuit breaker contributor returns DEGRADED → aggregated readiness is UP (not DOWN) | DEGRADED treated as UP |
| All circuit breakers open (DOWN) → aggregated readiness is DOWN | Full degradation |
| Shutdown contributor returns DOWN → aggregated readiness is DOWN, liveness is UP | Shutdown doesn't kill liveness |
| Leader election contributor returns DOWN → aggregated liveness is DOWN | Election thread death restarts pod |
| Non-leader pod → stale reaper contributor returns null → no effect on liveness | Leader-only contributor |
| Oracle unreachable → oracle contributor returns DOWN → liveness and readiness both DOWN | Database dependency |
| `/q/health/detail` returns per-subsystem breakdown with details maps | Diagnostic endpoint completeness |
