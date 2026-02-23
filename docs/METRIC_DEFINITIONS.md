# Dashboard Metric Definitions

Definitions for the Payments & Fraud Monitoring Dashboard. Data source: `i-dss-streaming-data.payment_ops_vw` (Recurly).

---

## Payments

| Metric | Definition | Source / Notes |
|--------|------------|----------------|
| **Success rate** | Percentage of payment attempts that result in success or void (authorized). | `recurly_transaction_fct`: count where `trans_status_desc` in ('success','void') / count where `trans_status_desc` in ('success','void','declined'). Filter: `trans_type_desc` in ('purchase','verify'), `origin_desc` in ('api','token_api'). |
| **Decline rate** | Percentage of payment attempts that are declined. | Same denominator; numerator = count where `trans_status_desc` = 'declined'. |
| **Transaction volume** | Sum of transaction amount and/or count over a period. | `trans_amt` from `recurly_transaction_fct`; aggregate by day/week/month. Exclude test: `test_transaction_ind = false` where applicable. |
| **Retry / recovery rate** | Among initially failed (declined) subscription charges, the percentage later succeeded (e.g. on retry or dunning). | Requires matching by account/subscription/invoice across attempts over time; compare first-attempt outcome to eventual success within a time window. |

---

## Fraud

| Metric | Definition | Source / Notes |
|--------|------------|----------------|
| **Fraud decision share** | Percentage of transactions in each `fraud_decision_desc` value (e.g. allow, block, review). | `recurly_transaction_fct.fraud_decision_desc`. |
| **Fraud score share** | Distribution of transactions by `fraud_score_cd` (risk tier). | `recurly_transaction_fct.fraud_score_cd`. |
| **AVS result rate** | For successful/void transactions, share with AVS pass (Y/X/V) vs other. | `avs_result_cd`: typical pass = 'Y', 'X', 'V'; others = fail or no match. |
| **CVV result rate** | For successful/void transactions, share with CVV match vs no match. | `recurly_transaction_fct.cvv_result_desc`. |
| **Dispute rate** | (Optional) Disputes or chargebacks as % of volume/count. | Add if data exists in payment_ops_vw or linked dataset. |

---

## Subscription

| Metric | Definition | Source / Notes |
|--------|------------|----------------|
| **MRR** | Monthly recurring revenue: sum of active subscription recurring amount, normalized to monthly (e.g. annual/12). | `recurly_subscription_dim`: `total_recur_amt`, `plan_cd`/billing interval; restrict to active/current (e.g. `current_ind = true` or status active). |
| **Gross churn** | Subscriptions canceled or expired in period as count or % of prior-period active base. | `recurly_subscription_dim`: cancel_dt, expiration_dt; classify voluntary vs involuntary per business rules (e.g. retention_ltv.sql logic). |
| **Voluntary vs involuntary churn** | Split of churn by cancel reason (e.g. voluntary cancel vs payment failure / dunning). | Use subscription and transaction logic (e.g. cancel during dunning window = involuntary_fail_dunning). |
| **Recovery rate** | Among failed subscription payments in a period, % that later succeeded (retry/dunning). | Join `recurly_transaction_fct` by account/subscription/invoice; compare failed then succeeded within window. |

---

## Anomaly

| Metric | Definition | Source / Notes |
|--------|------------|----------------|
| **BIN/card-share deviation** | For each day and BIN (`cc_first_6_nbr`), the share of transactions (rate % of daily total) vs a prior baseline (e.g. 3-month avg and stddev); flag when rate exceeds Z threshold (e.g. 3). | Reuse pattern in `queries/acct_bin_rate_stddev.sql`: daily rate per BIN, baseline = avg and stddev of that rate over prior 3 months; flag bins where (rate - baseline_avg) / baseline_stddev > threshold. |

---

## Scope and filters

- All metrics respect the dashboard **filter contract** (date range, src_system_id, country_cd, gateway_cd, etc.) where applicable.
- Transaction-based metrics typically scope to `trans_type_desc` in ('purchase','verify') and `trans_status_desc` in ('success','void','declined') unless otherwise specified.
- Exclude test transactions where the source has `test_transaction_ind` (or equivalent).
