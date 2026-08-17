
MERGE `{{ params.project }}.{{ params.curated_dataset }}.customers` T
USING (
  SELECT
    customer_id,
    customer_name,
    TRIM(email)                                    AS email,
    phone,
    SAFE.PARSE_DATE('%m/%d/%Y', created_date)      AS created_date,
    UPPER(TRIM(country))                            AS country,
    CURRENT_TIMESTAMP()                             AS _curated_at
  FROM `{{ params.project }}.{{ params.raw_dataset }}.customers`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id ORDER BY _PARTITIONTIME DESC
  ) = 1
) S
ON T.customer_id = S.customer_id
WHEN MATCHED THEN
  UPDATE SET
    customer_name = S.customer_name,
    email         = S.email,
    phone         = S.phone,
    created_date  = S.created_date,
    country       = S.country,
    _curated_at   = S._curated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, customer_name, email, phone, created_date, country, _curated_at)
  VALUES (S.customer_id, S.customer_name, S.email, S.phone, S.created_date, S.country, S._curated_at);