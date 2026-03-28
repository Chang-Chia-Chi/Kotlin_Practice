-- Session 5: enqueued_at column for FIFO ordering + index fixes

-- R2.1: Add enqueued_at with server-time default
ALTER TABLE task ADD enqueued_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL;

-- Backfill existing rows with best-effort enqueue time
UPDATE task SET enqueued_at = COALESCE(claimed_at, SYSTIMESTAMP) WHERE 1=1;

-- R2.2: Drop ineffective index (claimed_at is NULL for PENDING rows)
DROP INDEX idx_task_status_claimed;

-- R2.2: Claim query — WHERE status='PENDING' ORDER BY enqueued_at ASC, id
CREATE INDEX idx_task_pending_enqueued ON task (status, enqueued_at, id);

-- R2.2: Reaper query — WHERE status='PROCESSING' AND claimed_at < threshold
CREATE INDEX idx_task_processing_claimed ON task (status, claimed_at);
