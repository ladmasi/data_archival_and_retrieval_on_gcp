

MERGE `{{ params.project }}.{{ params.curated_dataset }}.payments` T
USING (
  SELECT
    payment_id,
    order_id,
    SAFE.PARSE_DATE('%m/%d/%Y', payment_date) AS payment_date,
    UPPER(TRIM(payment_method))                AS payment_method,
    SAFE_CAST(amount AS NUMERIC)               AS amount,
    UPPER(TRIM(payment_status))                AS payment_status,
    CURRENT_TIMESTAMP()                        AS _curated_at
  FROM `{{ params.project }}.{{ params.raw_dataset }}.payments`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY payment_id ORDER BY _PARTITIONTIME DESC
  ) = 1
) S
ON T.payment_id = S.payment_id
WHEN MATCHED THEN
  UPDATE SET
    order_id       = S.order_id,
    payment_date   = S.payment_date,
    payment_method = S.payment_method,
    amount         = S.amount,
    payment_status = S.payment_status,
    _curated_at    = S._curated_at
WHEN NOT MATCHED THEN
  INSERT (payment_id, order_id, payment_date, payment_method, amount, payment_status, _curated_at)
  VALUES (S.payment_id, S.order_id, S.payment_date, S.payment_method, S.amount, S.payment_status, S._curated_at);