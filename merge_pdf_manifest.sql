
MERGE `{{ params.project }}.{{ params.curated_dataset }}.pdf_manifest` T
USING (
  SELECT
    file_name,
    order_id,
    UPPER(TRIM(document_type))                  AS document_type,
    SAFE.PARSE_DATE('%m/%d/%Y', created_date)   AS created_date,
    CURRENT_TIMESTAMP()                         AS _curated_at
  FROM `{{ params.project }}.{{ params.raw_dataset }}.pdf_manifest`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY file_name ORDER BY _PARTITIONTIME DESC
  ) = 1
) S
ON T.file_name = S.file_name
WHEN MATCHED THEN
  UPDATE SET
    order_id      = S.order_id,
    document_type = S.document_type,
    created_date  = S.created_date,
    _curated_at   = S._curated_at
WHEN NOT MATCHED THEN
  INSERT (file_name, order_id, document_type, created_date, _curated_at)
  VALUES (S.file_name, S.order_id, S.document_type, S.created_date, S._curated_at);