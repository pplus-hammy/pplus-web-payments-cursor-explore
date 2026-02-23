# Data refresh and freshness: payment_ops_vw

Expectations for the `i-dss-streaming-data.payment_ops_vw` dataset used by the Payments & Fraud Monitoring Dashboard.

## What payment_ops_vw is

- **Dataset:** `i-dss-streaming-data.payment_ops_vw`.
- **Objects used:** Views over Recurly-sourced data, filtered to `src_system_id` in (115, 134, 139):
  - `recurly_transaction_fct` — transaction fact (from `i-dss-cdm-data.cdm_vw.recurly_transaction_fct`).
  - `recurly_subscription_dim` — subscription dimension.
  - `recurly_adjustments_fct` — adjustments/refunds/credits.

These are **views**, not materialized tables; freshness follows the underlying CDM tables in `i-dss-cdm-data.cdm_vw`.

## Refresh cadence

- **Not defined in this repo.** Refresh and latency are determined by the pipeline that feeds `i-dss-cdm-data.cdm_vw` (and thus `payment_ops_vw`).
- **Action:** Confirm with the data/platform owner:
  - How often the CDM Recurly tables are updated (e.g. hourly, daily, real-time stream).
  - Any delay (e.g. T+1 for daily batch, or minutes for streaming).
- Document the agreed cadence here or in the dashboard runbook so analysts know how “live” the dashboard is.

## NRT vs batch expectations

- **NRT (near real-time):** If the upstream pipeline is streaming or frequent (e.g. every few minutes), the dashboard can be used for near real-time monitoring; still expect a short lag (e.g. 5–15 minutes) depending on the pipeline.
- **Batch (e.g. daily):** If the CDM is updated once per day, the dashboard is suitable for daily/weekly reporting and anomaly checks over the last 3 months; do not expect same-day transactions to appear until after the batch run.
- **Dashboard behavior:** The dashboard does not currently show “data as of” or “last refreshed”; adding a small note or footer with the last refresh time (if available from the pipeline or a metadata table) is recommended in Phase 3.

## Summary

| Topic | Status |
|-------|--------|
| Refresh cadence | To be confirmed with data/platform owner; document once agreed. |
| NRT vs batch | Depends on upstream CDM pipeline; set expectation in runbook. |
| Views vs tables | payment_ops_vw objects are views; no separate refresh for this dataset. |
