-- BigQuery scheduled query: call the refresh procedure every morning.
--
-- BigQuery has no CREATE SCHEDULED QUERY DDL. Use one of the deployment options below.
-- Replace TARGET_TABLE with your table name (must match refresh_table_ctas.sql).
--
-- =============================================================================
-- Scheduled query body (paste into console or bq --params)
-- =============================================================================

call `i-dss-streaming-data.payment_ops_sandbox.refresh_d2c_fraud_email_name`();


-- =============================================================================
-- Option A: BigQuery console
-- =============================================================================
-- 1. BigQuery → Scheduled queries → Create scheduled query
-- 2. Paste the CALL statement above
-- 3. Schedule: "every day 08:00" (UTC by default — adjust for your timezone)
-- 4. Destination: None (procedure writes the table directly)
-- 5. Use a dedicated service account with:
--      bigquery.routines.create / bigquery.routines.update  (one-time deploy)
--      bigquery.tables.create, bigquery.tables.update, bigquery.tables.delete
--      bigquery.jobs.create
--      Read access on all source tables/views in the SELECT


-- =============================================================================
-- Option B: bq CLI
-- =============================================================================
-- bq mk --transfer_config \
--   --project_id=i-dss-streaming-data \
--   --target_dataset=payment_ops_sandbox \
--   --display_name='refresh_TARGET_TABLE daily' \
--   --data_source=scheduled_query \
--   --schedule='every day 08:00' \
--   --params='{"query":"CALL `i-dss-streaming-data.payment_ops_sandbox.refresh_TARGET_TABLE`();"}'


-- =============================================================================
-- Timezone note
-- =============================================================================
-- Scheduled queries run in UTC unless configured otherwise.
-- Example: 08:00 UTC = 03:00 ET (EST) / 04:00 ET (EDT).
-- To run at 08:00 US/Eastern, use schedule equivalent in UTC or set timezone
-- in the console scheduled-query settings.


-- =============================================================================
-- CTAS caveats
-- =============================================================================
-- - Schema changes: adding/removing/renaming columns in the SELECT breaks downstream
--   consumers; CTAS does not merge schema incrementally.
-- - Partition/cluster: must be declared in the CREATE OR REPLACE TABLE statement
--   inside the procedure; otherwise metadata is dropped on each rebuild.
-- - Table description/labels: not preserved; add ALTER TABLE in the procedure if needed.
-- - Cost: full scan + rewrite every run; for very large tables consider incremental
--   MERGE instead (see scripts/download_cybersource_daily_report.py).


-- =============================================================================
-- Test plan
-- =============================================================================
-- 1. Run queries/routines/refresh_table_ctas.sql to create/update the procedure
-- 2. Run the CALL statement above manually; verify row count and schema
-- 3. Create the scheduled query via console or bq CLI
-- 4. After first scheduled run, confirm job history and table last-modified time
