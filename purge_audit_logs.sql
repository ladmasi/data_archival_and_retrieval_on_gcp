-- purge_audit_logs.sql
-- Deletes ops.audit_log rows older than the configured retention window,
-- keeping the table from growing unbounded. Retention is an Airflow
-- Variable (audit_log_retention_days) so it can be tuned without a code change.
-- Params: @project, @ops_dataset, @retention_days

DELETE FROM `@project.@ops_dataset.audit_log`
WHERE started_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @retention_days DAY);
