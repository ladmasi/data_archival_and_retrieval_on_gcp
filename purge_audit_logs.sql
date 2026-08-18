DELETE FROM `@project.@ops_dataset.audit_log`
WHERE started_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @retention_days DAY);
