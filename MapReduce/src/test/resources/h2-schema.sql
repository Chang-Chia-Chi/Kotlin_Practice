-- H2-compatible schema combining all Oracle migrations (V1, V2, V4, V5, V9)
-- Used for integration testing with H2 in-memory database.

-- NUMTODSINTERVAL alias: returns fractional days so TIMESTAMP + result works in H2
CREATE ALIAS IF NOT EXISTS NUMTODSINTERVAL FOR "com.mapreduce.testinfra.H2Functions.numToDsInterval";

-- Layer 1: Generic Task Queue
CREATE TABLE task (
    task_id              VARCHAR(36)   NOT NULL,
    handler              VARCHAR(255)  NOT NULL,
    queue                VARCHAR(100)  DEFAULT 'default' NOT NULL,
    payload              CLOB,
    status               VARCHAR(20)   DEFAULT 'PENDING' NOT NULL,
    priority             INT           DEFAULT 0 NOT NULL,
    group_id             VARCHAR(36),
    metadata             CLOB,
    claimed_by           VARCHAR(255),
    claimed_at           TIMESTAMP,
    scheduled_at         TIMESTAMP,
    retry_count          INT           DEFAULT 0 NOT NULL,
    max_retries          INT           DEFAULT 3 NOT NULL,
    error_message        VARCHAR(4000),
    created_at           TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
    completed_at         TIMESTAMP,
    execution_generation VARCHAR(36),
    speculative          INT           DEFAULT 0 NOT NULL,
    last_heartbeat       TIMESTAMP,
    last_epoch           BIGINT        DEFAULT 0 NOT NULL,
    CONSTRAINT pk_task PRIMARY KEY (task_id)
);

CREATE INDEX idx_task_claim ON task (status, queue, priority DESC, created_at ASC);
CREATE INDEX idx_task_group ON task (group_id, status);
CREATE INDEX idx_task_stale ON task (status, last_heartbeat);
CREATE INDEX idx_task_group_handler ON task (group_id, handler);
CREATE INDEX idx_task_exec_gen ON task (task_id, execution_generation);

-- Layer 2: Map-Reduce Job
CREATE TABLE mr_job (
    job_id               VARCHAR(36)   NOT NULL,
    job_type             VARCHAR(255)  NOT NULL,
    status               VARCHAR(20)   NOT NULL,
    job_params           CLOB,
    total_tasks          INT           DEFAULT 0 NOT NULL,
    completed_tasks      INT           DEFAULT 0 NOT NULL,
    failed_tasks         INT           DEFAULT 0 NOT NULL,
    failure_policy       VARCHAR(20)   DEFAULT 'FAIL_JOB' NOT NULL,
    failure_threshold    DECIMAL(5,4)  DEFAULT 0 NOT NULL,
    reducing_fence_token VARCHAR(255),
    result_metadata      CLOB,
    total_partitions     INT           DEFAULT 1 NOT NULL,
    last_epoch           BIGINT        DEFAULT 0 NOT NULL,
    version              BIGINT        DEFAULT 0 NOT NULL,
    created_at           TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at           TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_mr_job PRIMARY KEY (job_id)
);

CREATE INDEX idx_mr_job_status ON mr_job (status);
CREATE INDEX idx_mr_job_type_status ON mr_job (job_type, status);

-- Layer 2: Map-Reduce Intermediate Outputs
CREATE TABLE mr_output (
    output_id      VARCHAR(36)   NOT NULL,
    job_id         VARCHAR(36)   NOT NULL,
    task_id        VARCHAR(36)   NOT NULL,
    output_data    CLOB,
    blob_uri       VARCHAR(2000),
    partition_hash INT           DEFAULT 0 NOT NULL,
    created_at     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_mr_output PRIMARY KEY (output_id),
    CONSTRAINT fk_mr_output_job FOREIGN KEY (job_id) REFERENCES mr_job (job_id)
);

CREATE INDEX idx_mr_output_job ON mr_output (job_id);
CREATE INDEX idx_mr_output_job_partition ON mr_output (job_id, partition_hash);
