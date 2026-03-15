-- ============================================================
-- Fenced Leader Election: Add last_epoch columns
-- ============================================================
-- Every table that receives leader-only writes gets a last_epoch
-- column. The SQL fence pattern is:
--   SET last_epoch = :epoch WHERE ... AND last_epoch <= :epoch
-- A zombie leader carrying a stale epoch gets 0 rows affected.

-- Map-Reduce Job table (leader writes: status transitions, counter updates)
ALTER TABLE mr_job ADD last_epoch NUMBER(19) DEFAULT 0 NOT NULL;

-- DAG Run table (leader writes: status transitions)
ALTER TABLE dag_run ADD last_epoch NUMBER(19) DEFAULT 0 NOT NULL;

-- DAG Task Instance table (leader writes: status transitions, dispatch)
ALTER TABLE dag_task_instance ADD last_epoch NUMBER(19) DEFAULT 0 NOT NULL;
