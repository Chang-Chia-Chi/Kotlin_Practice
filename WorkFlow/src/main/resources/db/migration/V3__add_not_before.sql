-- R4.7: exponential backoff via not_before column + per-activity backoff config
ALTER TABLE task ADD not_before TIMESTAMP;
ALTER TABLE task ADD backoff_base NUMBER DEFAULT 1 NOT NULL;
ALTER TABLE task ADD backoff_cap NUMBER DEFAULT 300 NOT NULL;
CREATE INDEX idx_task_not_before ON task (status, not_before);
