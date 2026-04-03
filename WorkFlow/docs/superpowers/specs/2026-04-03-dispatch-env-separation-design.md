# Dispatch Environment Separation Design

## Overview

Add prod/stg environment support to the dispatch system. Each environment runs as a separate deployment against the same database, with isolated event tables, env-aware storage paths, and environment-specific endpoints (dry-run for prod, sync + export for stg).

## Goals

- Stg simulations run continuously without triggering real downstream dispatch
- Users can dry-run dispatch in prod to preview results without side effects
- Users can sync prod dispatch history into stg for accurate baseline computation
- Users can manually export stg batch results as Parquet to verify the full pipeline

## Non-Goals

- Separate databases per environment (same DB, separate tables)
- Stg-specific algorithm logic (simulation engine is env-agnostic)
- Reverse sync (stg → prod)

## Architecture Decision: Separate Deployments

Each environment is a separate Quarkus deployment configured by `dispatch.env` and `quarkus.profile`:

- **Prod deployment:** `dispatch.env=prod`, `quarkus.profile=prod`
- **Stg deployment:** `dispatch.env=stg`, `quarkus.profile=stg`

Rationale: clean isolation at infrastructure level. Handlers stay free of env-branching logic. CDI beans swap at startup, not at runtime.

## Database Schema

### dispatch_batch / dispatch_batch_stg

Tracks every batch run. Identical schema, physically separate tables per env.

| Column | Type | Notes |
|---|---|---|
| `batch_token` | `VARCHAR2(64) PK` | Time-based (normal) or random UUID (dry-run) |
| `status` | `VARCHAR2(16) NOT NULL` | `NORMAL` or `DRYRUN` |
| `created_at` | `TIMESTAMP NOT NULL` | Truncated to micros |
| `config_count` | `NUMBER` | Number of configs in this batch |

### dispatch_event / dispatch_event_stg

Stores dispatch decisions. Identical schema, physically separate tables per env.

| Column | Type | Notes |
|---|---|---|
| `id` | `NUMBER GENERATED ALWAYS AS IDENTITY PK` | |
| `batch_token` | `VARCHAR2(64) NOT NULL` | FK to corresponding dispatch_batch table |
| `config_id` | `VARCHAR2(64) NOT NULL` | |
| `dispatch_order` | `NUMBER NOT NULL` | 1-based within config |
| `product_id` | `VARCHAR2(64) NOT NULL` | |
| `source_bom_id` | `VARCHAR2(64) NOT NULL` | |
| `qty` | `NUMBER NOT NULL` | |
| `target_site_id` | `VARCHAR2(64) NOT NULL` | |
| `target_bom_id` | `VARCHAR2(64)` | Nullable (LV1-only) |
| `site_gap` | `NUMBER NOT NULL` | |
| `bom_gap` | `NUMBER` | Nullable |

### Indexes

- `dispatch_event` / `dispatch_event_stg`: `(batch_token, config_id)`, `(config_id, batch_token)`
- `dispatch_batch` / `dispatch_batch_stg`: `(status, created_at)`

### Baseline Query Filter

Prod baseline computation joins `dispatch_batch` with `status = 'NORMAL'` to exclude dry-run events. Stg baseline queries `dispatch_batch_stg` / `dispatch_event_stg` directly (no dry-runs in stg).

## CDI Wiring

### SimulationResultStore

Single implementation class `JdbiSimulationResultStore` parameterized by table names:

```kotlin
@Produces
fun resultStore(
    @ConfigProperty(name = "dispatch.env") env: String,
    jdbi: Jdbi
): SimulationResultStore {
    val (batchTable, eventTable) = when (env) {
        "prod" -> "dispatch_batch" to "dispatch_event"
        "stg"  -> "dispatch_batch_stg" to "dispatch_event_stg"
        else   -> throw IllegalArgumentException("Unknown dispatch.env: $env")
    }
    return JdbiSimulationResultStore(jdbi, batchTable, eventTable)
}
```

SQL within `JdbiSimulationResultStore` uses the injected table names. This avoids duplicating DAO logic across environments.

### Sync Data Access

The sync endpoint (stg deployment) needs read access to prod tables (`dispatch_batch`, `dispatch_event`) to copy data. The stg deployment's DB credentials must have read-only grants on prod tables in addition to read-write on stg tables. A dedicated `SyncRepository` handles cross-table reads, parameterized with both prod and stg table names.

### StorageGateway

Same implementation for both envs. The `env` prefix is injected from `dispatch.env` config and used in path construction. No behavioral branching.

## Handler Modifications

### DispatchScatterHandler

Accepts optional `configIds` in input:
- If `configIds` is provided: validate each config exists and is active, use that list
- If absent: fall back to `configRepo.findActiveConfigs(now)`

This supports both cron-triggered runs (no config list) and dry-run (explicit config list) without env-specific branching.

### DispatchSimulationHandler

Reads batch status from `dispatch_batch` table to determine the `mode` path segment. Builds CSV path as:

```
env={dispatch.env}/mode={batchStatus.lowercase()}/dispatch/{batchToken}/simulation/{configId}.csv.gz
```

Handler is decoupled from scatter output — it independently looks up batch status.

### DispatchJoinHandler

After aggregating decisions, checks env + batch status to decide Parquet export:
- `dispatch.env=prod` AND `status=NORMAL` → export to `env=prod/dispatch/result.parquet`
- Otherwise → skip Parquet export

## Storage Paths

All CSV paths follow a consistent format: `env={env}/mode={mode}/dispatch/{batchToken}/simulation/{configId}.csv.gz`

| Scenario | Path |
|---|---|
| Prod normal CSV | `env=prod/mode=normal/dispatch/{batchToken}/simulation/{configId}.csv.gz` |
| Prod normal Parquet | `env=prod/dispatch/result.parquet` |
| Prod dry-run CSV | `env=prod/mode=dryrun/dispatch/{batchToken}/simulation/{configId}.csv.gz` |
| Stg normal CSV | `env=stg/mode=normal/dispatch/{batchToken}/simulation/{configId}.csv.gz` |
| Stg manual Parquet | `env=stg/dispatch/{batchToken}/result.parquet` |

Prod Parquet path is fixed (overwritten each batch) — downstream polls this path. Stg Parquet path includes batchToken since there's no downstream consumer; it's for user verification.

## Endpoints

### POST /dispatch/dryrun (prod only, @IfBuildProfile("prod"))

Triggers a dry-run dispatch simulation.

- **Request:** `{ "configIds": ["cfg1", "cfg2"] }` (optional — null means all active configs)
- **Behavior:**
  1. Generates random batch token (`UUID.randomUUID()`)
  2. Creates `dispatch_batch` record with `status = DRYRUN`
  3. Starts `dispatchWorkflow` via workflow engine, passing batch token + optional config list
  4. Join handler sees DRYRUN status → skips Parquet export
  5. CSV uploaded to `env=prod/mode=dryrun/dispatch/{batchToken}/simulation/{configId}.csv.gz`
- **Response:** `{ "batchToken": "...", "status": "DRYRUN" }`

### POST /dispatch/sync (stg only, @IfBuildProfile("stg"))

Syncs prod dispatch history into stg for accurate baseline computation. Single transaction.

- **Request:** `{ "configIds": ["cfg1", "cfg2"] }` (required)
- **Behavior:**
  1. For each config: delete from `dispatch_event_stg` where `config_id = ?`
  2. Delete orphaned `dispatch_batch_stg` records (batches with no remaining events)
  3. Find all `dispatch_batch` records with `status = 'NORMAL'` that contain events for the requested configs
  4. Upsert matching batch records into `dispatch_batch_stg`
  5. Copy matching events from `dispatch_event` into `dispatch_event_stg`
  6. Commit as single transaction
- **Response:** `{ "syncedConfigs": [...], "batchesCopied": 12, "eventsCopied": 3400 }`

### POST /dispatch/export (stg only, @IfBuildProfile("stg"))

Exports a stg batch result as Parquet for pipeline verification.

- **Request:** `{ "batchToken": "20260403060000", "configIds": ["cfg1"] }` (configIds optional — null means whole batch)
- **Behavior:**
  1. Reads matching events from `dispatch_event_stg`
  2. Formats as Parquet via `ParquetFormatter`
  3. Uploads to `env=stg/dispatch/{batchToken}/result.parquet`
- **Response:** `{ "batchToken": "...", "exportedConfigs": [...], "path": "env=stg/dispatch/.../result.parquet" }`

## What Stays Unchanged

- `SimulationEngine` and all algorithm logic (gap computation, candidate matching, termination)
- `DispatchConfig`, `CandidateRepository`, `BaselineProvider` interfaces
- `CsvFormatter`, `ParquetFormatter` implementations
- Workflow DSL definition (`dispatchWorkflow`)
- Core domain models (`DispatchDecision`, `SimulationResult`, etc.)

## Configuration

```properties
# Prod application.properties
dispatch.env=prod

# Stg application.properties
dispatch.env=stg
```

Both deployments share all other config (DB connection, MinIO endpoint, cron schedule, etc.). The `dispatch.env` property and `quarkus.profile` are the only differentiators.
