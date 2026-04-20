-- ============================================================
-- Heartbeat-Based Stale Task Reaper
-- ============================================================
-- Replaces claimed_at age detection with heartbeat liveness.
-- Workers update last_heartbeat periodically while executing.
-- The reaper detects stale heartbeats to reclaim orphaned tasks.

-- Heartbeat column: updated every heartbeat_interval while task is CLAIMED.
-- NULL when task is not claimed (PENDING, COMPLETED, FAILED, DEAD_LETTER).
ALTER TABLE task ADD last_heartbeat TIMESTAMP;

-- Fencing column: the leader's epoch at time of last leader-write.
-- Prevents zombie leaders from reclaiming tasks that the current leader
-- has already handled. Uses the standard fence pattern:
--   SET last_epoch = :epoch WHERE ... AND last_epoch <= :epoch
ALTER TABLE task ADD last_epoch NUMBER(19) DEFAULT 0 NOT NULL;

-- Replace claimed_at-based stale index with heartbeat-based index.
-- The reaper queries: WHERE status = 'CLAIMED' AND last_heartbeat < :threshold
DROP INDEX idx_task_stale;
CREATE INDEX idx_task_stale ON task (status, last_heartbeat);
