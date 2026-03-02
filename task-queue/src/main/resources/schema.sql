-- ================================================================
-- Distributed Task Queue — Oracle DDL
-- ================================================================

CREATE SEQUENCE TASK_QUEUE_SEQ START WITH 1 INCREMENT BY 1 CACHE 100;

CREATE TABLE TASK_QUEUE (
    TASK_ID        NUMBER(19)     DEFAULT TASK_QUEUE_SEQ.NEXTVAL PRIMARY KEY,
    PARENT_TASK_ID NUMBER(19)     REFERENCES TASK_QUEUE(TASK_ID),
    TASK_TYPE      VARCHAR2(64)   NOT NULL,
    PAYLOAD        CLOB,
    STATUS         VARCHAR2(16)   DEFAULT 'PENDING' NOT NULL,
    PRIORITY       NUMBER(3)      DEFAULT 5,
    DEADLINE_AT    TIMESTAMP,
    SCHEDULED_AT   TIMESTAMP,
    RETRY_COUNT    NUMBER(3)      DEFAULT 0,
    MAX_RETRIES    NUMBER(3)      DEFAULT 3,
    CREATED_AT     TIMESTAMP      DEFAULT SYSTIMESTAMP NOT NULL,
    UPDATED_AT     TIMESTAMP      DEFAULT SYSTIMESTAMP NOT NULL,
    STARTED_AT     TIMESTAMP,
    COMPLETED_AT   TIMESTAMP,
    ERROR_MESSAGE  VARCHAR2(4000),
    ERROR_HISTORY  CLOB,
    UNIQUE_KEY     VARCHAR2(128),

    CONSTRAINT CHK_STATUS CHECK (STATUS IN ('PENDING','SCHEDULED','PROCESSING','RETRYABLE','DONE','CANCELLED','DISCARDED','EXPIRED')),
    CONSTRAINT CHK_PRIORITY CHECK (PRIORITY BETWEEN 1 AND 9)
);

-- Consumer claim: PENDING + not-expired, ordered by priority then age
CREATE INDEX IDX_TASK_CONSUME ON TASK_QUEUE (STATUS, PRIORITY, CREATED_AT);

-- Leader housekeeping: stale reclaim (PROCESSING + old UPDATED_AT)
CREATE INDEX IDX_TASK_STALE ON TASK_QUEUE (STATUS, UPDATED_AT);

-- Leader housekeeping: deadline expiry scan
CREATE INDEX IDX_TASK_DEADLINE ON TASK_QUEUE (STATUS, DEADLINE_AT);

-- Leader housekeeping: promote RETRYABLE/SCHEDULED tasks when SCHEDULED_AT <= now
CREATE INDEX IDX_TASK_SCHEDULED ON TASK_QUEUE (STATUS, SCHEDULED_AT);

-- Monitoring: child count per parent
CREATE INDEX IDX_TASK_PARENT ON TASK_QUEUE (PARENT_TASK_ID, STATUS);

-- Unique jobs: prevent duplicate active tasks. Oracle ignores NULLs in unique indexes,
-- so terminal tasks (where the CASE returns NULL) don't block new insertions.
CREATE UNIQUE INDEX IDX_TASK_UNIQUE_KEY ON TASK_QUEUE (
    CASE WHEN STATUS NOT IN ('DONE','CANCELLED','DISCARDED','EXPIRED') THEN UNIQUE_KEY END
);

COMMENT ON TABLE TASK_QUEUE IS 'Distributed task queue — single table for root and child tasks at any depth';
COMMENT ON COLUMN TASK_QUEUE.PARENT_TASK_ID IS 'NULL for root tasks; FK to parent for child tasks';
COMMENT ON COLUMN TASK_QUEUE.PRIORITY IS '1=highest, 9=lowest. Default 5';
COMMENT ON COLUMN TASK_QUEUE.DEADLINE_AT IS 'Tasks not started by this time are marked EXPIRED. NULL=no expiry';
COMMENT ON COLUMN TASK_QUEUE.SCHEDULED_AT IS 'For RETRYABLE/SCHEDULED tasks: earliest time to promote to PENDING';
COMMENT ON COLUMN TASK_QUEUE.ERROR_HISTORY IS 'JSON array of {attempt, at, error} entries for diagnostics';
COMMENT ON COLUMN TASK_QUEUE.UNIQUE_KEY IS 'SHA-256 of taskType+payload for deduplication; NULL=no dedup';
