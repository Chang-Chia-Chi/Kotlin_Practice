-- V14: Rename task_group → workflow_step, update columns/indexes for workflow engine

-- Table rename
ALTER TABLE task_group RENAME TO workflow_step;

-- Column renames on workflow_step
ALTER TABLE workflow_step RENAME COLUMN group_id TO step_id;
ALTER TABLE workflow_step RENAME COLUMN group_type TO workflow_name;
ALTER TABLE workflow_step RENAME COLUMN phase TO step_label;
ALTER TABLE workflow_step RENAME COLUMN phase_total TO step_total;

-- Add new columns
ALTER TABLE workflow_step ADD run_id VARCHAR2(36);
ALTER TABLE workflow_step ADD queue VARCHAR2(100) DEFAULT 'default';
UPDATE workflow_step SET run_id = step_id WHERE run_id IS NULL;
ALTER TABLE workflow_step MODIFY run_id NOT NULL;
ALTER TABLE workflow_step ADD CONSTRAINT uq_wf_step UNIQUE (workflow_name, run_id, step_label);

-- FK rename on task table
ALTER TABLE task RENAME COLUMN group_id TO step_id;

-- Rebuild indexes referencing old column names
DROP INDEX idx_task_group;
CREATE INDEX idx_task_step ON task (step_id, status);
DROP INDEX idx_task_group_handler;
CREATE INDEX idx_task_step_handler ON task (step_id, handler);

-- Index for job-level queries
CREATE INDEX idx_wf_step_run ON workflow_step (run_id);

-- Rename failure_policy enum value
UPDATE workflow_step SET failure_policy = 'FAIL_STEP' WHERE failure_policy = 'FAIL_GROUP';
