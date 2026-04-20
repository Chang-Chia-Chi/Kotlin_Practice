-- Index for findByGroupAndHandler (reduce task lookup by orchestrator)
CREATE INDEX idx_task_group_handler ON task (group_id, handler);
