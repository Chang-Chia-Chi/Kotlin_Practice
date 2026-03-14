-- ============================================================
-- Layer 2: DAG Orchestration — Run lifecycle
-- ============================================================
CREATE TABLE dag_run (
    run_id         VARCHAR2(36)  NOT NULL,
    dag_id         VARCHAR2(255) NOT NULL,
    status         VARCHAR2(20)  DEFAULT 'RUNNING' NOT NULL,
    global_context CLOB,
    created_at     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_dag_run PRIMARY KEY (run_id)
);

CREATE INDEX idx_dag_run_status ON dag_run (status);

-- ============================================================
-- Layer 2: DAG Orchestration — Task instances (nodes)
-- ============================================================
CREATE TABLE dag_task_instance (
    instance_id   VARCHAR2(36)  NOT NULL,
    run_id        VARCHAR2(36)  NOT NULL,
    task_key      VARCHAR2(255) NOT NULL,
    node_type     VARCHAR2(255) NOT NULL,
    dependencies  CLOB,
    status        VARCHAR2(20)  DEFAULT 'BLOCKED' NOT NULL,
    trigger_rule  VARCHAR2(20)  DEFAULT 'ALL_SUCCESS' NOT NULL,
    output_data   CLOB,
    task_id       VARCHAR2(36),
    created_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_dag_task_instance PRIMARY KEY (instance_id),
    CONSTRAINT fk_dag_instance_run FOREIGN KEY (run_id) REFERENCES dag_run (run_id),
    CONSTRAINT uq_dag_instance_run_key UNIQUE (run_id, task_key)
);

-- Orchestrator polls by run + status
CREATE INDEX idx_dag_instance_run_status ON dag_task_instance (run_id, status);
-- Reconcile: look up instance by Layer 1 task_id
CREATE INDEX idx_dag_instance_task ON dag_task_instance (task_id);
