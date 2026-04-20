-- ============================================================
-- Countdown barrier: replace count-up counters with tasks_pending
-- ============================================================
-- tasks_pending counts down to 0 (barrier met).
-- tasks_failed tracks terminal failures for policy evaluation.
-- deadline_at provides a safety net for stuck groups.

ALTER TABLE task_group ADD tasks_pending  NUMBER(10) DEFAULT 0 NOT NULL;
ALTER TABLE task_group ADD tasks_failed   NUMBER(10) DEFAULT 0 NOT NULL;
ALTER TABLE task_group ADD deadline_at    TIMESTAMP;

-- Migrate existing data: tasks_pending = phase_total - phase_completed - phase_failed
UPDATE task_group SET tasks_pending = phase_total - phase_completed - phase_failed,
                      tasks_failed = phase_failed;

ALTER TABLE task_group DROP COLUMN phase_completed;
ALTER TABLE task_group DROP COLUMN phase_failed;

CREATE INDEX idx_task_group_deadline ON task_group (status, deadline_at);
