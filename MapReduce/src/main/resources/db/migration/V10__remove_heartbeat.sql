-- ============================================================
-- Remove Heartbeat — Switch to claimed_at-Based Stale Detection
-- ============================================================
-- Heartbeat-based liveness detection is replaced by simple
-- claimed_at age detection. A task is stale if it has been
-- CLAIMED longer than the configured stale threshold.

ALTER TABLE task DROP COLUMN last_heartbeat;

DROP INDEX idx_task_stale;
CREATE INDEX idx_task_stale ON task (status, claimed_at);
