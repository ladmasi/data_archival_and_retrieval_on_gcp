
MERGE `{{ params.project }}.{{ params.curated_dataset }}.order_items` T
USING (
  SELECT
    order_item_id,
    order_id,
    product_id,
    product_name,
    SAFE_CAST(quantity AS INT64)     AS quantity,
    SAFE_CAST(unit_price AS NUMERIC) AS unit_price,
    CURRENT_TIMESTAMP()              AS _curated_at
  FROM `{{ params.project }}.{{ params.raw_dataset }}.order_items`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_item_id ORDER BY _PARTITIONTIME DESC
  ) = 1
) S
ON T.order_item_id = S.order_item_id
WHEN MATCHED THEN
  UPDATE SET
    order_id     = S.order_id,
    product_id   = S.product_id,
    product_name = S.product_name,
    quantity     = S.quantity,
    unit_price   = S.unit_price,
    _curated_at  = S._curated_at
WHEN NOT MATCHED THEN
  INSERT (order_item_id, order_id, product_id, product_name, quantity, unit_price, _curated_at)
  VALUES (S.order_item_id, S.order_id, S.product_id, S.product_name, S.quantity, S.unit_price, S._curated_at);