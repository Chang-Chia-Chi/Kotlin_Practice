-- V15: Remove countdown barrier columns.
-- Barrier detection now uses lock-free COUNT + CAS on task table.
-- Failure counting computed on demand via COUNT(status='DEAD_LETTER').

ALTER TABLE workflow_step DROP COLUMN tasks_pending;
ALTER TABLE workflow_step DROP COLUMN tasks_failed;
