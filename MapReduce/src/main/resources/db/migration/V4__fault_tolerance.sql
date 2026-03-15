-- ============================================================
-- Fault Tolerance: Zombie Worker Fencing
-- ============================================================
-- Execution generation UUID prevents split-brain commits.
-- When a task is reclaimed and reassigned, the generation changes.
-- All worker writes fence on this column.
ALTER TABLE task ADD execution_generation VARCHAR2(36);
CREATE INDEX idx_task_exec_gen ON task (task_id, execution_generation);

-- ============================================================
-- Fault Tolerance: External Shuffle Architecture
-- ============================================================
-- Map outputs stored as external blob URIs, not inline CLOBs.
-- partition_hash enables sharded parallel reduce.
ALTER TABLE mr_output ADD blob_uri VARCHAR2(2000);
ALTER TABLE mr_output ADD partition_hash NUMBER(10) DEFAULT 0 NOT NULL;
CREATE INDEX idx_mr_output_job_partition ON mr_output (job_id, partition_hash);

-- ============================================================
-- Fault Tolerance: Sharded Reduce
-- ============================================================
ALTER TABLE mr_job ADD total_partitions NUMBER(10) DEFAULT 1 NOT NULL;

-- ============================================================
-- Fault Tolerance: Speculative Execution
-- ============================================================
-- Track whether a task is a speculative duplicate
ALTER TABLE task ADD speculative NUMBER(1) DEFAULT 0 NOT NULL;
