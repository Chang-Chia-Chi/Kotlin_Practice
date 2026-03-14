-- ============================================================
-- Layer 1: Generic Task Queue
-- ============================================================
CREATE TABLE task (
    task_id       VARCHAR2(36)  NOT NULL,
    handler       VARCHAR2(255) NOT NULL,
    queue         VARCHAR2(100) DEFAULT 'default' NOT NULL,
    payload       CLOB,
    status        VARCHAR2(20)  DEFAULT 'PENDING' NOT NULL,
    priority      NUMBER(5)     DEFAULT 0 NOT NULL,
    group_id      VARCHAR2(36),
    metadata      CLOB,
    claimed_by    VARCHAR2(255),
    claimed_at    TIMESTAMP,
    scheduled_at  TIMESTAMP,
    retry_count   NUMBER(5)     DEFAULT 0 NOT NULL,
    max_retries   NUMBER(5)     DEFAULT 3 NOT NULL,
    error_message VARCHAR2(4000),
    created_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
    completed_at  TIMESTAMP,
    CONSTRAINT pk_task PRIMARY KEY (task_id)
);

-- Claiming: status + queue + priority ordering + scheduling
CREATE INDEX idx_task_claim ON task (status, queue, priority DESC, created_at ASC);
-- Barrier detection: group correlation
CREATE INDEX idx_task_group ON task (group_id, status);
-- Stale detection: claimed tasks older than threshold
CREATE INDEX idx_task_stale ON task (status, claimed_at);

-- ============================================================
-- Layer 2: Map-Reduce — Job lifecycle
-- ============================================================
CREATE TABLE mr_job (
    job_id              VARCHAR2(36)  NOT NULL,
    job_type            VARCHAR2(255) NOT NULL,
    status              VARCHAR2(20)  NOT NULL,
    job_params          CLOB,
    total_tasks         NUMBER(10)    DEFAULT 0 NOT NULL,
    completed_tasks     NUMBER(10)    DEFAULT 0 NOT NULL,
    failed_tasks        NUMBER(10)    DEFAULT 0 NOT NULL,
    failure_policy      VARCHAR2(20)  DEFAULT 'FAIL_JOB' NOT NULL,
    failure_threshold   NUMBER(5,4)   DEFAULT 0 NOT NULL,
    reducing_fence_token VARCHAR2(255),
    result_metadata     CLOB,
    version             NUMBER(19)    DEFAULT 0 NOT NULL,
    created_at          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_mr_job PRIMARY KEY (job_id)
);

CREATE INDEX idx_mr_job_status ON mr_job (status);
CREATE INDEX idx_mr_job_type_status ON mr_job (job_type, status);

-- ============================================================
-- Layer 2: Map-Reduce — Intermediate output records
-- ============================================================
CREATE TABLE mr_output (
    output_id     VARCHAR2(36)  NOT NULL,
    job_id        VARCHAR2(36)  NOT NULL,
    task_id       VARCHAR2(36)  NOT NULL,
    output_data   CLOB,
    created_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_mr_output PRIMARY KEY (output_id),
    CONSTRAINT fk_mr_output_job FOREIGN KEY (job_id) REFERENCES mr_job (job_id)
);

CREATE INDEX idx_mr_output_job ON mr_output (job_id);
