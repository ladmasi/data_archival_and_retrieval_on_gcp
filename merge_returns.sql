

MERGE `{{ params.project }}.{{ params.curated_dataset }}.returns` T
USING (
  SELECT
    return_id,
    order_id,
    SAFE.PARSE_DATE('%m/%d/%Y', return_date) AS return_date,
    reason,
    SAFE_CAST(refund_amount AS NUMERIC)       AS refund_amount,
    CURRENT_TIMESTAMP()                       AS _curated_at
  FROM `{{ params.project }}.{{ params.raw_dataset }}.returns`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY return_id ORDER BY _PARTITIONTIME DESC
  ) = 1
) S
ON T.return_id = S.return_id
WHEN MATCHED THEN
  UPDATE SET
    order_id      = S.order_id,
    return_date   = S.return_date,
    reason        = S.reason,
    refund_amount = S.refund_amount,
    _curated_at   = S._curated_at
WHEN NOT MATCHED THEN
  INSERT (return_id, order_id, return_date, reason, refund_amount, _curated_at)
  VALUES (S.return_id, S.order_id, S.return_date, S.reason, S.refund_amount, S._curated_at);