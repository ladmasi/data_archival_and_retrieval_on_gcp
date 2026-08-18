-- schema_curated_ops.sql
-- Run once, before the first DAG trigger. Creates every table that
-- MERGE/INSERT tasks write into. Raw tables are NOT created here — they're
-- auto-created by GCSToBigQueryOperator's CREATE_IF_NEEDED on first load.

-- ===================== CURATED =====================

CREATE TABLE IF NOT EXISTS `@project.curated.customers` (
  customer_id STRING NOT NULL,
  customer_name STRING,
  email STRING,
  phone STRING,
  created_date DATE,
  country STRING,
  _curated_at TIMESTAMP
)
CLUSTER BY customer_id;

CREATE TABLE IF NOT EXISTS `@project.curated.orders` (
  order_id STRING NOT NULL,
  customer_id STRING,
  order_date DATE,
  order_status STRING,
  order_total NUMERIC,
  source_updated_at TIMESTAMP,
  _curated_at TIMESTAMP
)
PARTITION BY order_date
CLUSTER BY customer_id, order_status;

CREATE TABLE IF NOT EXISTS `@project.curated.order_items` (
  order_item_id STRING NOT NULL,
  order_id STRING,
  product_id STRING,
  product_name STRING,
  quantity INT64,
  unit_price NUMERIC,
  _curated_at TIMESTAMP
)
CLUSTER BY order_id, product_id;

CREATE TABLE IF NOT EXISTS `@project.curated.payments` (
  payment_id STRING NOT NULL,
  order_id STRING,
  payment_date DATE,
  payment_method STRING,
  amount NUMERIC,
  payment_status STRING,
  _curated_at TIMESTAMP
)
PARTITION BY payment_date
CLUSTER BY order_id;

CREATE TABLE IF NOT EXISTS `@project.curated.returns` (
  return_id STRING NOT NULL,
  order_id STRING,
  return_date DATE,
  reason STRING,
  refund_amount NUMERIC,
  _curated_at TIMESTAMP
)
PARTITION BY return_date
CLUSTER BY order_id;

CREATE TABLE IF NOT EXISTS `@project.curated.pdf_manifest` (
  file_name STRING NOT NULL,
  order_id STRING,
  document_type STRING,
  created_date DATE,
  _curated_at TIMESTAMP
)
CLUSTER BY order_id;

-- ===================== OPS =====================

CREATE TABLE IF NOT EXISTS `@project.ops.audit_log` (
  run_id STRING,
  dag_id STRING,
  task_id STRING,
  status STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  execution_date DATE
)
PARTITION BY execution_date;

CREATE TABLE IF NOT EXISTS `@project.ops.dq_failures` (
  table_name STRING,
  rule_name STRING,
  record_id STRING,
  reason STRING,
  run_id STRING,
  checked_at TIMESTAMP
)
PARTITION BY DATE(checked_at);

CREATE TABLE IF NOT EXISTS `@project.ops.pdf_index` (
  file_name STRING,
  order_id STRING,
  gcs_archive_path STRING,
  document_type STRING,
  filename_order_date STRING,
  content_order_date STRING,
  status STRING,
  run_id STRING,
  processed_at TIMESTAMP
)
PARTITION BY DATE(processed_at)
CLUSTER BY order_id, status;

CREATE TABLE IF NOT EXISTS `@project.ops.dq_summary_weekly` (
  week_ending DATE,
  table_name STRING,
  rule_name STRING,
  failure_count INT64
)
PARTITION BY week_ending;
