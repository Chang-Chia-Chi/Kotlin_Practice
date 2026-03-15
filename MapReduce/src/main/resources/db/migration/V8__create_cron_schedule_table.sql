-- ============================================================
-- Layer 2: Scheduled / Cron Pattern
-- Stores schedule definitions and execution tracking state.
-- The trigger loop (leader-only) reads due schedules and
-- enqueues tasks into the generic task queue (Layer 1).
-- ============================================================

CREATE TABLE cron_schedule (
    schedule_id       VARCHAR2(36)   NOT NULL,
    name              VARCHAR2(255)  NOT NULL,
    handler           VARCHAR2(255)  NOT NULL,
    cron_expression   VARCHAR2(100)  NOT NULL,
    payload           CLOB           DEFAULT '{}',
    queue             VARCHAR2(100)  DEFAULT 'default' NOT NULL,
    max_retries       NUMBER(5)      DEFAULT 3 NOT NULL,
    overlap_policy    VARCHAR2(20)   DEFAULT 'SKIP' NOT NULL,
    enabled           NUMBER(1)      DEFAULT 1 NOT NULL,
    last_fired_at     TIMESTAMP,
    last_completed_at TIMESTAMP,
    last_task_id      VARCHAR2(36),
    last_status       VARCHAR2(20),
    next_fire_at      TIMESTAMP,
    version           NUMBER(19)     DEFAULT 0 NOT NULL,
    created_at        TIMESTAMP      DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at        TIMESTAMP      DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_cron_schedule PRIMARY KEY (schedule_id),
    CONSTRAINT uq_cron_schedule_name UNIQUE (name)
);

-- Leader trigger loop: find all due schedules
CREATE INDEX idx_cron_schedule_due ON cron_schedule (enabled, next_fire_at);

-- Overlap policy check: find in-flight tasks for a schedule
-- (uses task table metadata JSON — application-level query)
