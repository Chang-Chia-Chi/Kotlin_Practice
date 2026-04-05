ALTER TABLE task ADD (trigger_type VARCHAR2(50), trigger_meta CLOB);

ALTER TABLE task DROP CONSTRAINT chk_task_status;
ALTER TABLE task ADD CONSTRAINT chk_task_status CHECK (status IN (
    'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED',
    'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED', 'WAITING_FOR_SIGNAL', 'SKIPPED', 'DEFERRED'
));

CREATE INDEX idx_task_deferred ON task (status, trigger_type);
