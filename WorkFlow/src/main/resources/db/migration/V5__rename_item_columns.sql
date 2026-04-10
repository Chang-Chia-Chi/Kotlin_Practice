ALTER TABLE task RENAME COLUMN item TO task_payload;
ALTER TABLE task RENAME COLUMN items TO fan_out_payloads;
