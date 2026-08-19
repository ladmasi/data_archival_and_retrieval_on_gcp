-- dq_checks.sql
-- Runs all 6 brief-mandated checks and logs every failing record into ops.dq_failures.
-- Params: params.project, params.curated_dataset, params.ops_dataset, params.run_id

INSERT INTO `{{ params.project }}.{{ params.ops_dataset }}.dq_failures`
  (table_name, rule_name, record_id, reason, run_id, checked_at)

-- Rule 1: orders must reference a valid customer
SELECT
  'orders', 'invalid_customer_fk', o.order_id,
  CONCAT('customer_id ', IFNULL(o.customer_id, 'NULL'), ' not found in customers'),
  '{{ params.run_id }}', CURRENT_TIMESTAMP()
 -- a placeholder for a validation/run identifier.
FROM `{{ params.project }}.{{ params.curated_dataset }}.orders` o
LEFT JOIN `{{ params.project }}.{{ params.curated_dataset }}.customers` c USING (customer_id)
-- A LEFT JOIN keeps every order, even if a matching customer cannot be found.
WHERE c.customer_id IS NULL
-- Only return orders for which we couldn't find a matching customer.

UNION ALL

-- Rule 2: order total must match sum of order items
SELECT
  'orders', 'order_total_mismatch', o.order_id,
  CONCAT('order_total=', CAST(o.order_total AS STRING),
         ' vs items_sum=', CAST(IFNULL(i.items_sum, 0) AS STRING)),
-- CONCAT() works with strings, so the numeric values need to be converted to text.
  '{{ params.run_id }}', CURRENT_TIMESTAMP()
FROM `{{ params.project }}.{{ params.curated_dataset }}.orders` o
LEFT JOIN (
  SELECT order_id, SUM(quantity * unit_price) AS items_sum
  FROM `{{ params.project }}.{{ params.curated_dataset }}.order_items`
  GROUP BY order_id
) i USING (order_id)
WHERE ABS(o.order_total - IFNULL(i.items_sum, 0)) > 1
-- If there are no order items, treat the item total as zero
-- orders
--    │
--    │ order_id
-- LEFT JOIN
--    │
-- Calculate SUM(quantity × unit_price)
--    │
-- Compare: orders.order_total vs items_sum
--    │
-- Difference > 1?
--    │
--    ├── NO  -> Valid
--    │
--    └── YES -> Create data-quality error

UNION ALL

-- -- Rule : no future-dated records (orders / payments / returns)
-- SELECT 'orders', 'future_dated', order_id, 'order_date is in the future', '{{ params.run_id }}', CURRENT_TIMESTAMP()
-- FROM `{{ params.project }}.{{ params.curated_dataset }}.orders`
-- WHERE order_date > CURRENT_DATE()

-- UNION ALL

-- SELECT 'payments', 'future_dated', payment_id, 'payment_date is in the future', '{{ params.run_id }}', CURRENT_TIMESTAMP()
-- FROM `{{ params.project }}.{{ params.curated_dataset }}.payments`
-- WHERE payment_date > CURRENT_DATE()

-- UNION ALL

-- Rule 3: payment amount must match order total
SELECT
  'payments', 'amount_mismatch', p.payment_id,
  CONCAT('amount=', CAST(p.amount AS STRING), ' vs order_total=', CAST(o.order_total AS STRING)),
  '{{ params.run_id }}', CURRENT_TIMESTAMP()
FROM `{{ params.project }}.{{ params.curated_dataset }}.payments` p
JOIN `{{ params.project }}.{{ params.curated_dataset }}.orders` o USING (order_id)
WHERE ABS(p.amount - o.order_total) > 1

UNION ALL

-- Rule 4: PDF order_id must exist in curated orders (orphan PDFs)
SELECT
  'pdf_manifest', 'orphan_pdf', pi.order_id,
  'PDF references an order_id not present in curated.orders',
  '{{ params.run_id }}', CURRENT_TIMESTAMP()
FROM `{{ params.project }}.{{ params.curated_dataset }}.pdf_manifest` pi
LEFT JOIN `{{ params.project }}.{{ params.curated_dataset }}.orders` o USING (order_id)
WHERE o.order_id IS NULL

UNION ALL

-- Rule 5a: mandatory fields in customers cannot be null
SELECT
  'customers', 'null_mandatory_field', IFNULL(CAST(customer_id AS STRING), 'UNKNOWN'),
  'one or more mandatory fields (customer_id, customer_name) is NULL',
  '{{ params.run_id }}', CURRENT_TIMESTAMP()
FROM `{{ params.project }}.{{ params.curated_dataset }}.customers`
WHERE customer_id IS NULL OR customer_name IS NULL

UNION ALL

-- Rule 5b: mandatory fields in orders cannot be null
SELECT
  'orders', 'null_mandatory_field', IFNULL(CAST(order_id AS STRING), 'UNKNOWN'),
  'one or more mandatory fields (order_id, customer_id, order_date, order_status) is NULL',
  '{{ params.run_id }}', CURRENT_TIMESTAMP()
FROM `{{ params.project }}.{{ params.curated_dataset }}.orders`
WHERE order_id IS NULL OR customer_id IS NULL OR order_date IS NULL OR order_status IS NULL

        --          ORDERS TABLE
        --               │
        --  Check mandatory fields     
        --                │
        --      Are any fields NULL?
        --          /             \
        --        NO               YES
        --         │                │
        --   ✅ PASS          ❌ DQ FAILURE
        --                           │
        --                    
        --                Create DQ error record
        --                           │
        --                    
        --                "null_mandatory_field"


UNION ALL

-- Rule 5c: mandatory fields in order_items cannot be null
SELECT
  'order_items', 'null_mandatory_field', IFNULL(CAST(order_item_id AS STRING), 'UNKNOWN'),
  'one or more mandatory fields (order_item_id, order_id, product_id, quantity, unit_price) is NULL',
  '{{ params.run_id }}', CURRENT_TIMESTAMP()
FROM `{{ params.project }}.{{ params.curated_dataset }}.order_items`
WHERE order_item_id IS NULL OR order_id IS NULL OR product_id IS NULL OR quantity IS NULL OR unit_price IS NULL

UNION ALL

-- Rule 5d: mandatory fields in payments cannot be null
SELECT
  'payments', 'null_mandatory_field', IFNULL(CAST(payment_id AS STRING), 'UNKNOWN'),
  'one or more mandatory fields (payment_id, order_id, amount) is NULL',
  '{{ params.run_id }}', CURRENT_TIMESTAMP()
FROM `{{ params.project }}.{{ params.curated_dataset }}.payments`
WHERE payment_id IS NULL OR order_id IS NULL OR amount IS NULL

UNION ALL

-- Rule 5e: mandatory fields in returns cannot be null
SELECT
  'returns', 'null_mandatory_field', IFNULL(CAST(return_id AS STRING), 'UNKNOWN'),
  'one or more mandatory fields (return_id, order_id) is NULL',
  '{{ params.run_id }}', CURRENT_TIMESTAMP()
FROM `{{ params.project }}.{{ params.curated_dataset }}.returns`
WHERE return_id IS NULL OR order_id IS NULL

UNION ALL

-- Rule 5f: mandatory fields in pdf_manifest cannot be null
SELECT
  'pdf_manifest', 'null_mandatory_field', IFNULL(CAST(file_name AS STRING), 'UNKNOWN'),
  'one or more mandatory fields (file_name, order_id) is NULL',
  '{{ params.run_id }}', CURRENT_TIMESTAMP()
FROM `{{ params.project }}.{{ params.curated_dataset }}.pdf_manifest`
WHERE file_name IS NULL OR order_id IS NULL;
