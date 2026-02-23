# Payments & Fraud Monitoring Dashboard — Implementation Checklist

Dashboard runs off **BigQuery** using tables/views in `i-dss-streaming-data.payment_ops_vw` for a streaming subscription service. Use this checklist so multiple analysts/engineers can work in parallel (backend vs frontend vs shared).

**Data source:** `payment_ops_vw` — Recurly-sourced (`src_system_id` in 115, 134, 139).

| Object | Purpose |
|--------|---------|
| **recurly_transaction_fct** | Transactions: `trans_dt`, `trans_amt`, `trans_status_desc`, `trans_type_desc`, `failure_type`, `gateway_error_cd`, `country_cd`, `payment_method_desc`, `card_brand_nm`, `fraud_decision_desc`, `fraud_score_cd`, `avs_result_cd`, `cvv_result_desc`, `cc_first_6_nbr`, `gateway_cd`, `origin_desc` |
| **recurly_subscription_dim** | Subscriptions: `plan_cd`, `plan_nm`, `status_desc`, `unit_amt`, `total_recur_amt`, `start_dt`, `cancel_dt`, `expiration_dt`, trial dates, `current_ind` |
| **recurly_adjustments_fct** | Adjustments/refunds/credits: `adj_amt`, `type_desc`, `status_desc`, `creation_dt_ut`, invoice/subscription links |

**Filter dimensions (slicing and dicing):** Date range, `src_system_id`, `country_cd`, `gateway_cd`, `payment_method_desc`, `card_brand_nm`, `plan_cd`/plan tier, `trans_type_desc`, `trans_status_desc`, `failure_type`, and optionally DOW/hour.

---

## Phase 0 — Shared (do first)

| Done | Task | Owner |
|------|------|--------|
| [x] | Document metric definitions: payments (success rate, decline rate, volume, retry/recovery), fraud (fraud_decision_desc, fraud_score_cd, AVS/CVV), subscription (MRR, churn, recovery), anomaly (BIN deviation). → [docs/METRIC_DEFINITIONS.md](docs/METRIC_DEFINITIONS.md) | Shared |
| [x] | Define filter contract: dimensions and allowed values (date range, src_system_id, country_cd, gateway_cd, payment_method_desc, card_brand_nm, plan_cd, trans_type_desc, trans_status_desc, failure_type). → [docs/FILTER_CONTRACT.md](docs/FILTER_CONTRACT.md) | Shared |

---

## Phase 1 — Backend

| Done | Task | Owner |
|------|------|--------|
<!-- | [ ] | BigQuery: create view or saved query for payments summary (success/decline rate, count, volume by period) with filter params (date range, src_system_id, etc.). | Backend |
| [ ] | BigQuery: create view or saved query for fraud summary (fraud_decision_desc, fraud_score_cd, AVS/CVV rates) with same filter params. | Backend |
| [ ] | BigQuery: create view or saved query for subscription MRR/churn/recovery using recurly_subscription_dim + recurly_transaction_fct, with filter params. | Backend | -->
| [x] | BigQuery: create view or saved query for anomaly/BIN deviation (reuse pattern in `queries/acct_bin_rate_stddev.sql`) with filter params. → [queries/dashboard_anomaly_bin_stddev.sql](queries/dashboard_anomaly_bin_stddev.sql) | Backend |
| [x] | Optional: implement thin API (e.g. Cloud Run/Cloud Functions) that runs BQ and returns JSON; or document “direct BQ from frontend” as approach. → [docs/BACKEND_APPROACH.md](docs/BACKEND_APPROACH.md) | Backend |
| [x] | Document data refresh cadence and NRT vs batch expectations for payment_ops_vw. → [docs/DATA_REFRESH.md](docs/DATA_REFRESH.md) | Backend |

---

## Phase 2 — Frontend

| Done | Task | Owner |
|------|------|--------|
| [x] | Set up dashboard app (framework, auth if required, BigQuery or API client). → [dashboard/](dashboard/) (Vite + React + TypeScript; mock data; ready to wire to API/BQ) | Frontend |
| [x] | Build global filter bar: date range + dimension filters (src_system_id, country, gateway, payment method, card brand, plan, etc.) that apply to all panels. | Frontend |
| [x] | Payments panel: success/decline rate, volume, failure_type and gateway_error_cd breakdown; charts/tables with export or drill-down. | Frontend |
| [x] | Fraud panel: share by fraud_decision_desc and fraud_score_cd; AVS/CVV result rates; charts/tables. | Frontend |
| [x] | Subscription panel: MRR, churn (voluntary vs involuntary), recovery rate; charts/tables. | Frontend |
| [x] | Anomaly panel: BIN/card-share deviation from baseline; charts/tables with export/drill-down where useful. | Frontend |

---

## Phase 3 — Integration & polish

| Done | Task | Owner |
|------|------|--------|
| [x] | Wire global filters to all backend queries/views (or API) so every panel respects the same filters. (Anomaly panel wired to API with date range + src_system_id; other panels still mock.) | Backend / Frontend |
| [ ] | QA: filter combinations, empty states, and query/UI performance. | Shared |
| [x] | Document how to run the dashboard and where payment_ops_vw (and views) are documented. → [docs/DASHBOARD_RUNBOOK.md](docs/DASHBOARD_RUNBOOK.md) | Shared |

---

## Metrics reference

| Category | Metrics |
|----------|--------|
| **Payments** | Success rate, decline rate, transaction count/volume (by day/week/month), retry/recovery rate; breakdown by failure_type and gateway_error_cd. |
| **Fraud** | Share of transactions by fraud_decision_desc and fraud_score_cd; AVS/CVV result rates; optional dispute rate if data exists elsewhere. |
| **Subscription** | MRR (from subscription + successful charges), gross churn (cancel/expire), voluntary vs involuntary; recovery rate (failed then succeeded). |
| **Anomaly** | BIN/card-share deviation from baseline (see `queries/acct_bin_rate_stddev.sql`). |
