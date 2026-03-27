-- Session 9: cancel/timeout statuses + workflow deadline

-- Task: add TIMED_OUT, CANCELLED statuses
ALTER TABLE task DROP CONSTRAINT chk_task_status;
ALTER TABLE task ADD CONSTRAINT chk_task_status
    CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED'));

-- Workflow: add TIMED_OUT, CANCELLED statuses
ALTER TABLE workflow DROP CONSTRAINT chk_workflow_status;
ALTER TABLE workflow ADD CONSTRAINT chk_workflow_status
    CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'TIMED_OUT', 'CANCELLED'));

-- Workflow: add deadline_at for DAG-level timeout (default 1 hour)
ALTER TABLE workflow ADD deadline_at TIMESTAMP;
UPDATE workflow SET deadline_at = created_at + INTERVAL '1' HOUR WHERE deadline_at IS NULL;
ALTER TABLE workflow MODIFY deadline_at NOT NULL;

-- Index for sweeper to find timed-out workflows
CREATE INDEX idx_workflow_deadline ON workflow (status, deadline_at);
