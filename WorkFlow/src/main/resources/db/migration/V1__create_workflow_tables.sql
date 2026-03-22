-- Two-table workflow engine schema: workflow (CAS target) + task (worker-claimed units).
-- No activity table — activity metadata lives in the serialized WorkflowDefinition CLOB.

CREATE TABLE workflow (
    id               VARCHAR2(36)   NOT NULL,
    definition       CLOB           NOT NULL,
    current_sequence NUMBER(10)     NOT NULL,
    version          NUMBER(10)     DEFAULT 0 NOT NULL,
    status           VARCHAR2(20)   NOT NULL,
    created_at       TIMESTAMP      NOT NULL,
    updated_at       TIMESTAMP      NOT NULL,
    CONSTRAINT pk_workflow PRIMARY KEY (id),
    CONSTRAINT chk_workflow_status CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED'))
);

-- Sweeper query: find RUNNING workflows ordered by staleness
CREATE INDEX idx_workflow_status_updated ON workflow (status, updated_at);

CREATE TABLE task (
    id               VARCHAR2(36)   NOT NULL,
    workflow_id      VARCHAR2(36)   NOT NULL,
    sequence_number  NUMBER(10)     NOT NULL,
    status           VARCHAR2(20)   NOT NULL,
    handler_key      VARCHAR2(255)  NOT NULL,
    payload          CLOB,
    result           CLOB,
    claimed_by       VARCHAR2(100),
    claimed_at       TIMESTAMP,
    completed_at     TIMESTAMP,
    retry_count      NUMBER(10)     DEFAULT 0 NOT NULL,
    max_retries      NUMBER(10)     DEFAULT 0 NOT NULL,
    deadline_at      TIMESTAMP,
    CONSTRAINT pk_task PRIMARY KEY (id),
    CONSTRAINT fk_task_workflow FOREIGN KEY (workflow_id) REFERENCES workflow (id),
    CONSTRAINT chk_task_status CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED'))
);

-- Lock-free probe: count non-terminal tasks per workflow sequence (index-only scan)
CREATE INDEX idx_task_wf_seq_status ON task (workflow_id, sequence_number, status);

-- Stale task reaper: find expired PROCESSING tasks past their deadline
CREATE INDEX idx_task_status_deadline ON task (status, deadline_at);

-- SKIP LOCKED claiming: workers claim oldest unclaimed PENDING tasks
CREATE INDEX idx_task_status_claimed ON task (status, claimed_at);
