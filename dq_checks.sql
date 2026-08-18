INSERT INTO `@project.@ops_dataset.dq_failures`
  (table_name, rule_name, record_id, reason, run_id, checked_at)

-- Rule 1: orders must reference a valid customer
SELECT
  'orders', 'invalid_customer_fk', o.order_id,
  CONCAT('customer_id ', IFNULL(o.customer_id, 'NULL'), ' not found in customers'),
  '@run_id', CURRENT_TIMESTAMP()
FROM `@project.@curated_dataset.orders` o
LEFT JOIN `@project.@curated_dataset.customers` c USING (customer_id)
WHERE c.customer_id IS NULL

UNION ALL

-- Rule 2: order total must match sum of order items
SELECT
  'orders', 'order_total_mismatch', o.order_id,
  CONCAT('order_total=', CAST(o.order_total AS STRING),
         ' vs items_sum=', CAST(IFNULL(i.items_sum, 0) AS STRING)),
  '@run_id', CURRENT_TIMESTAMP()
FROM `@project.@curated_dataset.orders` o
LEFT JOIN (
  SELECT order_id, SUM(quantity * unit_price) AS items_sum
  FROM `@project.@curated_dataset.order_items`
  GROUP BY order_id
) i USING (order_id)
WHERE ABS(o.order_total - IFNULL(i.items_sum, 0)) > 1

UNION ALL

-- Rule 3: payment amount must match order total
SELECT
  'payments', 'amount_mismatch', p.payment_id,
  CONCAT('amount=', CAST(p.amount AS STRING), ' vs order_total=', CAST(o.order_total AS STRING)),
  '@run_id', CURRENT_TIMESTAMP()
FROM `@project.@curated_dataset.payments` p
JOIN `@project.@curated_dataset.orders` o USING (order_id)
WHERE ABS(p.amount - o.order_total) > 1

UNION ALL

-- Rule 4: PDF order_id must exist in curated orders
SELECT
  'pdf_index', 'orphan_pdf', pi.order_id,
  'PDF references an order_id not present in curated.orders',
  '@run_id', CURRENT_TIMESTAMP()
FROM `@project.@ops_dataset.pdf_index` pi
LEFT JOIN `@project.@curated_dataset.orders` o USING (order_id)
WHERE o.order_id IS NULL

UNION ALL

-- Rule 5: mandatory fields cannot be null 
SELECT
  'orders', 'null_mandatory_field', IFNULL(order_id, 'UNKNOWN'),
  'one or more mandatory fields (order_id, customer_id, order_date, order_status) is NULL',
  '@run_id', CURRENT_TIMESTAMP()
FROM `@project.@curated_dataset.orders`
WHERE order_id IS NULL OR customer_id IS NULL OR order_date IS NULL OR order_status IS NULL;
