

MERGE `{{ params.project }}.{{ params.curated_dataset }}.orders` T
USING (
  SELECT
    order_id,
    customer_id,
    SAFE.PARSE_DATE('%m/%d/%Y', order_date)      AS order_date,
    UPPER(TRIM(order_status))                     AS order_status,
    SAFE_CAST(order_total AS NUMERIC)              AS order_total,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', source_updated_at) AS source_updated_at,
    CURRENT_TIMESTAMP()                            AS _curated_at
  FROM `{{ params.project }}.{{ params.raw_dataset }}.orders`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id ORDER BY _PARTITIONTIME DESC
  ) = 1
) S
ON T.order_id = S.order_id
WHEN MATCHED THEN
  UPDATE SET
    customer_id       = S.customer_id,
    order_date        = S.order_date,
    order_status      = S.order_status,
    order_total       = S.order_total,
    source_updated_at = S.source_updated_at,
    _curated_at       = S._curated_at
WHEN NOT MATCHED THEN
  INSERT (order_id, customer_id, order_date, order_status, order_total, source_updated_at, _curated_at)
  VALUES (S.order_id, S.customer_id, S.order_date, S.order_status, S.order_total, S.source_updated_at, S._curated_at);