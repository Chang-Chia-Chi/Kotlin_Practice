-- ============================================================
-- V6: Chained Tasks pattern — sequential pipeline orchestration
-- ============================================================

CREATE TABLE chain_job (
    chain_id        VARCHAR2(36)   PRIMARY KEY,
    chain_type      VARCHAR2(255)  NOT NULL,
    status          VARCHAR2(20)   NOT NULL,  -- RUNNING, COMPLETED, FAILED
    current_step    NUMBER(10)     NOT NULL,
    total_steps     NUMBER(10)     NOT NULL,
    chain_params    CLOB,                     -- Original input parameters (JSON)
    failure_policy  VARCHAR2(20)   DEFAULT 'FAIL_CHAIN' NOT NULL,  -- FAIL_CHAIN, SKIP_STEP
    last_step_output CLOB,                    -- Output of the most recently completed step
    error_message   VARCHAR2(4000),
    version         NUMBER(19)     DEFAULT 0  NOT NULL,
    created_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
);

-- Query chains by status (e.g. find RUNNING or FAILED chains)
CREATE INDEX idx_chain_job_status ON chain_job (status);

-- Query chains by type + status (e.g. find all running ETL pipelines)
CREATE INDEX idx_chain_job_type_status ON chain_job (chain_type, status);
