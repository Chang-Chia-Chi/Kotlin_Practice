ALTER TABLE task ADD queue_name VARCHAR2(100) DEFAULT 'default' NOT NULL;
CREATE INDEX idx_task_queue_status ON task (queue_name, status, not_before, claimed_at);
