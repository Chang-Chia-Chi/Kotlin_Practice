-- V9__idempotency_key.sql
ALTER TABLE workflow ADD idempotency_key VARCHAR2(255);
CREATE UNIQUE INDEX uk_workflow_idempotency ON workflow(idempotency_key);
