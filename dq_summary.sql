INSERT INTO `{{ params.project }}.{{ params.ops_dataset }}.dq_summary_weekly`
  (week_ending, table_name, rule_name, failure_count)
SELECT
  DATE('{{ ds }}')            AS week_ending,
  table_name,
  rule_name,
  COUNT(*)                    AS failure_count
FROM `{{ params.project }}.{{ params.ops_dataset }}.dq_failures`
WHERE checked_at >= TIMESTAMP_SUB(TIMESTAMP('{{ ds }}'), INTERVAL 7 DAY)
GROUP BY table_name, rule_name;





