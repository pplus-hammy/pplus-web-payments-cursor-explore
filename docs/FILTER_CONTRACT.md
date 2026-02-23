# Dashboard Filter Contract

Dimensions and allowed values for slicing and dicing the Payments & Fraud Monitoring Dashboard. All panels should apply these filters consistently. Source: `i-dss-streaming-data.payment_ops_vw`.

---

## Dimensions and allowed values

| Dimension | Type | Allowed values / Notes |
|-----------|------|------------------------|
| **date_range** | Range (start, end) | `trans_dt` or subscription date fields. Start/end date (inclusive). Default: e.g. last 30 days. |
| **src_system_id** | Multi-select | `115`, `134`, `139` (Recurly source systems in payment_ops_vw). |
| **country_cd** | Multi-select | ISO country from `recurly_transaction_fct.country_cd`. Populate from distinct values in data (e.g. US, CA, GB, AU, …). |
| **gateway_cd** | Multi-select | Gateway identifier from `recurly_transaction_fct.gateway_cd`. Raw codes and/or region mapping below. |
| **gateway_region** | Multi-select (optional) | Derived region from gateway_cd: US, AU, LATAM, UK, BR, CA, GSA_DE_AT_CH, MX, FR, IE, IT (see mapping below). |
| **payment_method_desc** | Multi-select | From `recurly_transaction_fct.payment_method_desc` (e.g. `Credit Card`). Populate from distinct values. |
| **card_brand_nm** | Multi-select | From `recurly_transaction_fct.card_brand_nm` (e.g. Visa, Mastercard, Amex). Populate from distinct values. |
| **plan_cd** | Multi-select | From `recurly_subscription_dim.plan_cd` (e.g. plans containing 'monthly', 'annual'). Populate from distinct values. |
| **plan_tier** | Multi-select (optional) | Derived: `monthly`, `annual`, `other` (e.g. from plan_cd or plan_nm). |
| **trans_type_desc** | Multi-select | From `recurly_transaction_fct.trans_type_desc`. Typical dashboard scope: `purchase`, `verify`. |
| **trans_status_desc** | Multi-select | From `recurly_transaction_fct.trans_status_desc`. Typical: `success`, `void`, `declined`. |
| **failure_type** | Multi-select | From `recurly_transaction_fct.failure_type`. Populate from distinct values (e.g. soft decline, hard decline, etc.). |
| **origin_desc** | Multi-select (optional) | From `recurly_transaction_fct.origin_desc`. Typical scope: `api`, `token_api`. |
| **DOW** | Multi-select (optional) | Day of week (0–6 or 1–7) for time-based analysis. |
| **hour** | Multi-select (optional) | Hour of day (0–23) for time-based analysis. |

---

## Gateway code → region mapping

Used in existing queries (e.g. Tableau). Use for **gateway_region** filter or display.

| Region | gateway_cd values |
|--------|-------------------|
| US | cljvv4pluxdo, i3z3apzipbp7, wf3nj05ig027, o8kozk9x9qb0 |
| AU | p3oy7jtrnbzu, jt7jdrftfjyv |
| LATAM | ob5kihh2l5ht, ob5lkextdfrs |
| UK | qpdcpwym9258, qwazckp2zkjj, t0gcwmn4afqk |
| BR | obkq1bdafejc |
| CA | inkqcylsbc8b |
| GSA_DE_AT_CH | sj7av2xpiqik, rnuns7r579dp, rnuoyv9lls4l, rnum8iuze17e, rnunet43fjj0, rnuplcrtgiuv, rnupcldy1szk |
| MX | pnpypk0ag0nv |
| FR | rs7oav8d5e0f |
| IE | qpdel7kior3j, qwazjh3p96l7 |
| IT | reyg6kvzzovm, ra6ffzgs9s8q, t0h4mkezczbd |

---

## Implementation notes

- **Backend:** All dashboard queries/views MUST accept parameters for the dimensions above (or a subset). Omitted filter = no filter (all values).
- **Frontend:** Global filter bar sends the same dimension set to every panel; panels do not maintain separate filter state for these dimensions.
- **Allowed values:** For enum-like dimensions (country_cd, gateway_cd, card_brand_nm, plan_cd, failure_type, etc.), either (a) query distinct values from BigQuery at load time or (b) maintain a static list in config; document choice in dashboard runbook.
