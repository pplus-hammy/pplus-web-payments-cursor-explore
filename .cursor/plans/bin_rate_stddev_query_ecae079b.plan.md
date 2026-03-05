---
name: Bin rate stddev query
overview: Add a new SQL query that computes avg, stddev, and z-score for the **rate** (share of transactions) at which each bin appears per day, using a **leave-one-out** baseline (each day compared to avg/stddev over all other days in the period) and removing all day-of-week logic.
todos: []
isProject: false
---

# Bin appearance rate stddev query (full-period baseline)

## Goal

New query similar to [queries/txn_bin_stddev_union.sql](queries/txn_bin_stddev_union.sql) that:

- Uses **rate** (bin’s share of transactions for the day) instead of raw counts.
- Computes **avg**, **stddev**, and **z-score** for that rate per bin per day.
- Uses **leave-one-out** baseline: for each day, baseline avg/stddev is computed over all other days in the period (current day excluded). No day-of-week matching.
- Keeps the same segmenting (src_system_id, gateway_country, cc_first_6_nbr) and optional stacked output (total_volume, success, decline, success_avs_fail).

## Reference patterns

- **Rate definition**: Reuse the approach from [queries/tableau_queries/tableau_bin_stddev_pct.sql](queries/tableau_queries/tableau_bin_stddev_pct.sql): daily rate = (bin’s daily count) / (segment’s daily total count). Segment = (src_system_id, gateway_country); denominator = daily total transactions in that segment (total, success, decline, success_avs_fail as needed).
- **Structure**: Reuse from `txn_bin_stddev_union.sql`: same `txn` CTE (gateway_country mapping, filters), same high-level flow, and optional `stacked` output with `z_threshold` and `excluded_dts`.

## Design choices


| Aspect         | txn_bin_stddev_union (current)                   | New query                                                                         |
| -------------- | ------------------------------------------------ | --------------------------------------------------------------------------------- |
| Metric         | Counts (daily_ct, daily_success_ct, …)           | **Rates** (bin_ct / segment_total_ct per day)                                     |
| Baseline       | Same day-of-week over last 6 months              | **Leave-one-out**: all other days in the period (no DOW)                          |
| Baseline scope | Per run_dt (each day has its own baseline dates) | **Per (bin, run_dt)**: baseline = avg/stddev of rate over all dates except run_dt |


## Implementation outline

1. **Declare** `excluded_dts` and `z_threshold` as in the reference (optional use of excluded_dts in baseline).
2. **txn** – Same as reference: same source, gateway_country mapping, filters (trans_dt last 12 months, purchase/verify, success/void/declined, api/token_api, Credit Card).
3. **run_dates** – All days in the analysis window (e.g. last 6 or 12 months to match reference).
4. **daily_totals** – One row per (src_system_id, gateway_country, trans_dt) with:
  - daily_total_ct, daily_total_success_ct, daily_total_decline_ct, daily_total_success_avs_fail_ct (or equivalent) so rates can be computed for each metric.
5. **daily_volume_all** – One row per (src_system_id, gateway_country, cc_first_6_nbr, trans_dt):
  - Bin-level counts: daily_ct, daily_success_ct, daily_decline_ct, daily_success_avs_fail_ct (same definitions as reference).
  - Join to `daily_totals` and compute **rates**: e.g. `daily_rate = daily_ct / daily_total_ct`, and similarly for success, decline, success_avs_fail (guard with `nullif(..., 0)`).
6. **Remove DOW** – Do **not** use `baseline_dates` with `run_dt`/`baseline_dt` or `extract(dayofweek ...)`. No `bins_and_dates` grid; only dates that appear in the data (or run_dates) are needed for the baseline and for joining.
7. **rate_baseline** – One row per (src_system_id, gateway_country, cc_first_6_nbr, **run_dt**):
  - For each run_dt, compute baseline from **all other days** in the period (exclude run_dt). E.g. from `daily_volume_all` b, join to `run_dates` rd where b.trans_dt <> rd.run_dt (and optionally b.trans_dt not in unnest(excluded_dts)).
  - Aggregate by (src_system_id, gateway_country, cc_first_6_nbr, run_dt): `baseline_avg_rate`, `baseline_stddev_rate` (and same for success, decline, success_avs_fail).
  - Optional: require a minimum number of days or minimum total count (e.g. baseline_avg_ct >= 300) to avoid noisy bins.
8. **chg_chk** – For each (bin, trans_dt) in `daily_volume_all`:
  - Join `rate_baseline` on (src_system_id, gateway_country, cc_first_6_nbr, **trans_dt = run_dt**).
  - For each metric: `z_score = (daily_rate - baseline_avg_rate) / nullif(baseline_stddev_rate, 0)`.
  - Keep baseline columns and daily counts for context; apply minimum baseline filter here if used (e.g. baseline_avg_ct >= 300).
9. **stacked** – Same pattern as reference: UNION ALL of four flag_types (total_volume, decline, success, success_avs_fail), each selecting the corresponding rate, baseline avg/stddev, daily rate, diff, z_score, and chg_flag; filter with `abs(vol_z_score) >= z_threshold`.
10. **Final SELECT** – From `stacked`, add tableau_end_dt/tableau_start_dt if desired, filter `trans_dt < current_date()` (or local date), order by src_system_id, gateway_country, trans_dt desc, vol_z_score desc.

## File and style

- **New file**: e.g. `queries/txn_bin_rate_stddev_union.sql` (or a name you prefer).
- **SQL style**: Follow existing rules (leading commas, one line per select/where/join, `1=1` in WHERE, CASE formatting, subquery parens on new line).

## Optional considerations

- **Excluded dates**: Use `excluded_dts` in the baseline aggregation (exclude those dates when computing avg/stddev for each run_dt) and optionally in run_dates so event days don't “entire period”’t skew the baseline.

