-- V8: Replace payload column with item column (explicit inputs design).
-- item stores the scatter chunk for parallel tasks only.
-- Input resolution now happens at execution time from previous activities' resultJson.

ALTER TABLE task ADD (item CLOB);

ALTER TABLE task DROP COLUMN payload;
