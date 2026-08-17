-- reconcile_orphans.sql
-- Re-checks any pdf_index row currently marked ORPHAN against the latest
-- curated.orders. If the order_id now exists (it arrived in a later load),
-- flip the status to VALID so it stops showing up in the orphan report.
-- Params: @project, @curated_dataset, @ops_dataset

UPDATE `@project.@ops_dataset.pdf_index` pi
SET status = 'VALID'
WHERE pi.status = 'ORPHAN'
  AND pi.order_id IN (
    SELECT order_id FROM `@project.@curated_dataset.orders`
  );
