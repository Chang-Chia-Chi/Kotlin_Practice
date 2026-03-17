-- ============================================================
-- Task Groups — Generic Group Orchestration with Reactive Barriers
-- ============================================================
-- Replaces mr_job and mr_output with a generic task_group table.
-- Phase transitions are reactive (callback tasks) instead of polled.

-- New table: task_group (replaces mr_job)
CREATE TABLE task_group (
    group_id            VARCHAR2(36)    NOT NULL,
    group_type          VARCHAR2(255)   NOT NULL,
    status              VARCHAR2(20)    NOT NULL,
    params              CLOB,
    queue               VARCHAR2(100)   DEFAULT 'default' NOT NULL,

    phase               VARCHAR2(50)    NOT NULL,
    phase_total         NUMBER(10)      DEFAULT 0 NOT NULL,
    phase_completed     NUMBER(10)      DEFAULT 0 NOT NULL,
    phase_failed        NUMBER(10)      DEFAULT 0 NOT NULL,

    on_complete_handler VARCHAR2(255),

    failure_policy      VARCHAR2(20)    DEFAULT 'FAIL_GROUP' NOT NULL,
    failure_threshold   NUMBER(5,4)     DEFAULT 0 NOT NULL,

    result_metadata     CLOB,

    version             NUMBER(19)      DEFAULT 0 NOT NULL,
    last_epoch          NUMBER(19)      DEFAULT 0 NOT NULL,

    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_task_group PRIMARY KEY (group_id)
);

CREATE INDEX idx_task_group_status ON task_group (status);
CREATE INDEX idx_task_group_type_status ON task_group (group_type, status);

-- Add output columns to task table
ALTER TABLE task ADD output_uri      VARCHAR2(1000);
ALTER TABLE task ADD output_metadata CLOB;

-- Drop MR-specific tables (order matters for FK constraints)
DROP TABLE mr_output;
DROP TABLE mr_job;
