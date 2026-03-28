-- Drop and recreate CHECK constraint to include WAITING_FOR_SIGNAL.
ALTER TABLE task DROP CONSTRAINT chk_task_status;
ALTER TABLE task ADD CONSTRAINT chk_task_status CHECK (
    status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'TIMED_OUT', 'DEAD_LETTER', 'CANCELLED', 'WAITING_FOR_SIGNAL')
);
