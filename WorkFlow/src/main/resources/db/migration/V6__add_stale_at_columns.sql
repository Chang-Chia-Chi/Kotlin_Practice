ALTER TABLE task ADD (stale_threshold_secs NUMBER DEFAULT 600 NOT NULL, stale_at TIMESTAMP);

-- Backfill stale_at for currently PROCESSING tasks
UPDATE task SET stale_at = claimed_at + NUMTODSINTERVAL(stale_threshold_secs, 'SECOND')
WHERE status = 'PROCESSING' AND claimed_at IS NOT NULL;

-- Replace old index with sargable stale_at index
DROP INDEX idx_task_processing_claimed;
CREATE INDEX idx_task_stale_at ON task (status, stale_at);
