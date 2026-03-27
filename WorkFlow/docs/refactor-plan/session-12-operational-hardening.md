# Session 12 — Operational Hardening

**Tier:** 4 (architecture improvements)
**Prerequisites:** Session 6 (concurrency config), Session 7 (metrics)
**Estimated scope:** Startup assertions + K8s manifests + documentation

---

## Items

### R4.9 — Connection pool / concurrency startup assertion

**Problem:** No code-level guard ties `quarkus.datasource.jdbc.max-size` (Agroal pool, default 20) to `framework.worker.concurrency` (default 4). If an operator increases concurrency to 16 without increasing the pool, each in-flight task can hold up to 2 connections (claim + barrier transaction), requiring 32 connections from a pool of 20. The result is connection starvation — tasks block on pool acquisition, appear stale to the reaper, and get reclaimed.

**Files to modify:**
- New startup observer in `src/main/kotlin/config/ConfigValidator.kt`:

```kotlin
@Singleton
class ConfigValidator(
    private val config: FrameworkConfig,
    @ConfigProperty(name = "quarkus.datasource.jdbc.max-size", defaultValue = "20")
    private val poolMaxSize: Int,
) {
    fun onStart(@Observes StartupEvent event) {
        val concurrency = config.worker().concurrency()
        val requiredPool = concurrency * 2

        check(poolMaxSize >= requiredPool) {
            "Connection pool max-size ($poolMaxSize) must be >= 2 * worker concurrency ($concurrency). " +
            "Set quarkus.datasource.jdbc.max-size >= $requiredPool or reduce framework.worker.concurrency."
        }

        val batchSize = config.worker().batchSize()
        check(batchSize in 1..100) {
            "framework.worker.batch-size must be between 1 and 100 (got $batchSize)"
        }

        val leaseDuration = config.leaderElection().leaseDuration()
        val renewDeadline = config.leaderElection().renewDeadline()
        val retryPeriod = config.leaderElection().retryPeriod()

        check(renewDeadline < leaseDuration) {
            "leader-election.renew-deadline ($renewDeadline) must be < lease-duration ($leaseDuration)"
        }
        check(retryPeriod < renewDeadline) {
            "leader-election.retry-period ($retryPeriod) must be < renew-deadline ($renewDeadline)"
        }

        log.infof(
            "Config validated: concurrency=%d, batchSize=%d, poolSize=%d, lease=%s/%s/%s",
            concurrency, batchSize, poolMaxSize,
            leaseDuration, renewDeadline, retryPeriod,
        )
    }

    companion object {
        private val log = Logger.getLogger(ConfigValidator::class.java)
    }
}
```

**Test:** In `FrameworkConfigTest`:
1. Set `concurrency=10`, `max-size=5` — assert startup fails with clear error message
2. Set `concurrency=4`, `max-size=20` — assert startup succeeds
3. Set `renew-deadline=20s`, `lease-duration=15s` — assert startup fails

---

### R4.10 — K8s RBAC manifests and `terminationGracePeriodSeconds`

**Problem:** No Kubernetes manifests exist in the codebase. Operators deploying to K8s must know:
1. The ServiceAccount needs Lease API RBAC (`get`, `create`, `update` on `coordination.k8s.io/v1/leases`)
2. `terminationGracePeriodSeconds` must exceed `framework.shutdown.global-timeout` by 10+ seconds

**Files to create:**

1. `k8s/rbac.yaml`:
```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: workflow-engine-leader
  namespace: default  # Match framework.leader-election.namespace
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "create", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: workflow-engine-leader-binding
  namespace: default
subjects:
  - kind: ServiceAccount
    name: workflow-engine
    namespace: default
roleRef:
  kind: Role
  name: workflow-engine-leader
  apiGroup: rbac.authorization.k8s.io
```

2. `k8s/deployment.yaml` (template):
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: workflow-engine
spec:
  replicas: 3
  selector:
    matchLabels:
      app: workflow-engine
  template:
    metadata:
      labels:
        app: workflow-engine
    spec:
      serviceAccountName: workflow-engine
      # CRITICAL: Must exceed framework.shutdown.global-timeout (30s) by >= 10s
      terminationGracePeriodSeconds: 40
      containers:
        - name: workflow-engine
          image: workflow-engine:latest
          ports:
            - containerPort: 8080
              name: http
          env:
            - name: FRAMEWORK_WORKER_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          livenessProbe:
            httpGet:
              path: /q/health/live
              port: http
            initialDelaySeconds: 10
            periodSeconds: 10
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /q/health/ready
              port: http
            initialDelaySeconds: 5
            periodSeconds: 5
          resources:
            requests:
              cpu: "500m"
              memory: "512Mi"
            limits:
              cpu: "2"
              memory: "1Gi"
```

Key decisions documented in comments:
- `terminationGracePeriodSeconds: 40` = `globalTimeout(30s) + 10s` margin
- `FRAMEWORK_WORKER_ID` from pod name for unique worker identity
- Health probes wired to SmallRye Health endpoints

3. Add leader metric prefix consistency fix:
   - `leader_election_is_leader` → `taskqueue_leader_is_leader`
   - `leader_election_epoch` → `taskqueue_leader_epoch`

   Or document the inconsistency and leave as-is (lower priority).

**Test:** No unit tests for K8s manifests. Validate YAML syntax:
```bash
kubectl apply --dry-run=client -f k8s/rbac.yaml
kubectl apply --dry-run=client -f k8s/deployment.yaml
```

---

## Verification

1. `mvn test` passes
2. `ConfigValidator` startup assertion tests pass
3. K8s manifests validate with `kubectl --dry-run=client`
4. Manual: deploy to a K8s cluster, verify leader election works with the RBAC, verify graceful shutdown completes within `terminationGracePeriodSeconds`
