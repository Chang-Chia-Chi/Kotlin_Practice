-- ============================================================
-- Layer 2: DAG Orchestration V2 — Enhanced schema
-- Adds versioning, concurrency controls, retry management,
-- timeout tracking, and error classification.
-- ============================================================

-- ── dag_run: add versioning, trigger, SLA, and lifecycle columns ──

ALTER TABLE dag_run ADD (
    dag_version       NUMBER(10)    DEFAULT 1 NOT NULL,
    trigger_type      VARCHAR2(20)  DEFAULT 'MANUAL' NOT NULL,
    trigger_metadata  CLOB,
    parent_run_id     VARCHAR2(36),
    started_at        TIMESTAMP,
    completed_at      TIMESTAMP,
    deadline_at       TIMESTAMP
);

-- Index for SUB_DAG correlation
CREATE INDEX idx_dag_run_parent ON dag_run (parent_run_id);

-- Index for Leader polling by dag_id + status (concurrency checks)
CREATE INDEX idx_dag_run_dag_status ON dag_run (dag_id, status);

-- FK for SUB_DAG parent reference (self-referential)
ALTER TABLE dag_run ADD CONSTRAINT fk_dag_run_parent
    FOREIGN KEY (parent_run_id) REFERENCES dag_run (run_id);


-- ── dag_task_instance: add retry, timeout, error, and type columns ──

ALTER TABLE dag_task_instance ADD (
    task_type        VARCHAR2(50),
    attempt          NUMBER(5)     DEFAULT 1 NOT NULL,
    max_attempts     NUMBER(5)     DEFAULT 1 NOT NULL,
    resolved_config  CLOB,
    error            CLOB,
    timeout_at       TIMESTAMP,
    dispatched_at    TIMESTAMP,
    completed_at     TIMESTAMP
);

-- Index for timeout reaping: find timed-out instances efficiently
CREATE INDEX idx_dag_instance_timeout ON dag_task_instance (status, timeout_at);
