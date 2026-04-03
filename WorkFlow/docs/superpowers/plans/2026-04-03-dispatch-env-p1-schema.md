# Dispatch Env P1: Database Schema Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Flyway migration for `dispatch_batch`, `dispatch_event`, `dispatch_batch_stg`, and `dispatch_event_stg` tables with indexes.

**Architecture:** Single migration file creates all four tables. Prod tables (`dispatch_batch`, `dispatch_event`) and stg tables (`dispatch_batch_stg`, `dispatch_event_stg`) have identical schemas. The `dispatch_batch` CHECK constraint limits status to `NORMAL` and `DRYRUN`.

**Tech Stack:** Oracle SQL, Flyway

---

### Task 1: Write the migration SQL

**Files:**
- Create: `src/main/resources/db/migration/V2__create_dispatch_tables.sql`

- [ ] **Step 1: Create the migration file**

Create `src/main/resources/db/migration/V2__create_dispatch_tables.sql`:

```sql
-- dispatch_batch: tracks batch runs in prod
CREATE TABLE dispatch_batch (
    batch_token  VARCHAR2(64)   NOT NULL,
    status       VARCHAR2(16)   NOT NULL,
    created_at   TIMESTAMP      NOT NULL,
    config_count NUMBER,
    CONSTRAINT pk_dispatch_batch PRIMARY KEY (batch_token),
    CONSTRAINT chk_dispatch_batch_status CHECK (status IN ('NORMAL', 'DRYRUN'))
);

CREATE INDEX idx_dispatch_batch_status_created ON dispatch_batch (status, created_at);

-- dispatch_event: stores dispatch decisions in prod
CREATE TABLE dispatch_event (
    id             NUMBER GENERATED ALWAYS AS IDENTITY,
    batch_token    VARCHAR2(64)  NOT NULL,
    config_id      VARCHAR2(64)  NOT NULL,
    dispatch_order NUMBER        NOT NULL,
    product_id     VARCHAR2(64)  NOT NULL,
    source_bom_id  VARCHAR2(64)  NOT NULL,
    qty            NUMBER        NOT NULL,
    target_site_id VARCHAR2(64)  NOT NULL,
    target_bom_id  VARCHAR2(64),
    site_gap       NUMBER        NOT NULL,
    bom_gap        NUMBER,
    CONSTRAINT pk_dispatch_event PRIMARY KEY (id),
    CONSTRAINT fk_dispatch_event_batch FOREIGN KEY (batch_token) REFERENCES dispatch_batch (batch_token)
);

CREATE INDEX idx_dispatch_event_batch_config ON dispatch_event (batch_token, config_id);
CREATE INDEX idx_dispatch_event_config_batch ON dispatch_event (config_id, batch_token);

-- dispatch_batch_stg: tracks batch runs in stg (identical schema)
CREATE TABLE dispatch_batch_stg (
    batch_token  VARCHAR2(64)   NOT NULL,
    status       VARCHAR2(16)   NOT NULL,
    created_at   TIMESTAMP      NOT NULL,
    config_count NUMBER,
    CONSTRAINT pk_dispatch_batch_stg PRIMARY KEY (batch_token),
    CONSTRAINT chk_dispatch_batch_stg_status CHECK (status IN ('NORMAL', 'DRYRUN'))
);

CREATE INDEX idx_dispatch_batch_stg_status_created ON dispatch_batch_stg (status, created_at);

-- dispatch_event_stg: stores dispatch decisions in stg (identical schema)
CREATE TABLE dispatch_event_stg (
    id             NUMBER GENERATED ALWAYS AS IDENTITY,
    batch_token    VARCHAR2(64)  NOT NULL,
    config_id      VARCHAR2(64)  NOT NULL,
    dispatch_order NUMBER        NOT NULL,
    product_id     VARCHAR2(64)  NOT NULL,
    source_bom_id  VARCHAR2(64)  NOT NULL,
    qty            NUMBER        NOT NULL,
    target_site_id VARCHAR2(64)  NOT NULL,
    target_bom_id  VARCHAR2(64),
    site_gap       NUMBER        NOT NULL,
    bom_gap        NUMBER,
    CONSTRAINT pk_dispatch_event_stg PRIMARY KEY (id),
    CONSTRAINT fk_dispatch_event_stg_batch FOREIGN KEY (batch_token) REFERENCES dispatch_batch_stg (batch_token)
);

CREATE INDEX idx_dispatch_event_stg_batch_config ON dispatch_event_stg (batch_token, config_id);
CREATE INDEX idx_dispatch_event_stg_config_batch ON dispatch_event_stg (config_id, batch_token);
```

- [ ] **Step 2: Verify migration file is syntactically valid**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn flyway:validate -pl WorkFlow`

If no Flyway plugin is configured, verify by checking that the existing `V1__create_workflow_tables.sql` follows the same naming convention and the new file is adjacent.

- [ ] **Step 3: Commit**

```bash
git add src/main/resources/db/migration/V2__create_dispatch_tables.sql
git commit -m "feat(dispatch): add dispatch_batch and dispatch_event tables for prod and stg"
```
