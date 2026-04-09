-- Consolidated schema (V1–V9, V2-DAG)

CREATE TABLE workflow (
    id               VARCHAR2(36)   NOT NULL,
    definition       CLOB           NOT NULL,
    version          NUMBER(10)     DEFAULT 0 NOT NULL,
    status           VARCHAR2(20)   NOT NULL,
    created_at       TIMESTAMP      NOT NULL,
    updated_at       TIMESTAMP      NOT NULL,
    deadline_at      TIMESTAMP      NOT NULL,
    idempotency_key  VARCHAR2(255),
    CONSTRAINT pk_workflow PRIMARY KEY (id),
    CONSTRAINT chk_workflow_status CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'TIMED_OUT', 'CANCELLED'))
);

CREATE INDEX idx_workflow_status_updated ON workflow (status, updated_at);
CREATE INDEX idx_workflow_deadline       ON workflow (status, deadline_at);
CREATE UNIQUE INDEX uk_workflow_idempotency ON workflow (idempotency_key);

CREATE TABLE task (
    id               VARCHAR2(36)   NOT NULL,
    workflow_id      VARCHAR2(36)   NOT NULL,
    sequence_number  NUMBER(10)     NOT NULL,
    status           VARCHAR2(20)   NOT NULL,
    handler_key      VARCHAR2(255)  NOT NULL,
    item             CLOB,
    result           CLOB,
    claimed_by       VARCHAR2(100),
    claimed_at       TIMESTAMP,
    completed_at     TIMESTAMP,
    retry_count      NUMBER(10)     DEFAULT 0    NOT NULL,
    max_retries      NUMBER(10)     DEFAULT 0    NOT NULL,
    activity_name    VARCHAR2(255),
    deadline_at      TIMESTAMP,
    not_before       TIMESTAMP,
    backoff_base     NUMBER         DEFAULT 1    NOT NULL,
    backoff_cap      NUMBER         DEFAULT 300  NOT NULL,
    enqueued_at      TIMESTAMP      DEFAULT SYSTIMESTAMP NOT NULL,
    queue_name       VARCHAR2(100)  DEFAULT 'default'    NOT NULL,
    trigger_type     VARCHAR2(50),
    trigger_meta     CLOB,
    items            CLOB,
    CONSTRAINT pk_task PRIMARY KEY (id),
    CONSTRAINT fk_task_workflow FOREIGN KEY (workflow_id) REFERENCES workflow (id),
    CONSTRAINT chk_task_status CHECK (status IN (
        'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED',
        'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED', 'SKIPPED', 'DEFERRED'
    ))
);

CREATE INDEX idx_task_wf_seq_status      ON task (workflow_id, sequence_number, status);
CREATE INDEX idx_task_status_deadline    ON task (status, deadline_at);
CREATE INDEX idx_task_not_before         ON task (status, not_before);
CREATE INDEX idx_task_pending_enqueued   ON task (status, enqueued_at, id);
CREATE INDEX idx_task_processing_claimed ON task (status, claimed_at);
CREATE INDEX idx_task_queue_status       ON task (queue_name, status, not_before, claimed_at);
CREATE INDEX idx_task_deferred           ON task (status, trigger_type);
