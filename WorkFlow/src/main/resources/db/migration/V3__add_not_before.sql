-- R4.7: exponential backoff via not_before column
ALTER TABLE task ADD not_before TIMESTAMP;
CREATE INDEX idx_task_not_before ON task (status, not_before);
