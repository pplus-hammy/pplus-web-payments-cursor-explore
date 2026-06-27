---
name: ""
overview: ""
todos: []
isProject: false
---

# Active subscriptions by `account_cd` last digit and `plan_dur` (monthly / annual)

## Goal

Using the full `sub` CTE from [queries/sub.sql](queries/sub.sql), produce **10 rows** (one per last digit `0`–`9`) and **5 columns**:

1. **Last digit** of `account_cd` (grouping key), e.g. `account_cd_last_digit`
2. Monthly: count of distinct `subscription_guid` with `status_desc = 'active'` and `plan_dur = 'monthly'`
3. Monthly: percent of **total active monthly** (monthly percents sum to 100% across the 10 rows)
4. Annual: count of distinct `subscription_guid` with `status_desc = 'active'` and `plan_dur = 'annual'`
5. Annual: percent of **total active annual** (annual percents sum to 100% across the 10 rows)

`plan_dur` in `sub` is derived from `plan_cd` (`monthly`, `annual`, or `other` per lines 68–72). This report focuses on **monthly and annual only**; `other` is omitted from the two metric pairs (use a separate query or extra columns if you need `other`).

## Implementation sketch

1. **Reuse** the entire `with sub as ( ... )` from [queries/sub.sql](queries/sub.sql) unchanged (including `src_system_id` filters as you use them).
2. **Last digit** of `account_cd` (string-safe):
  `right(trim(cast(account_cd as string)), 1)`  
   Alias e.g. `account_cd_last_digit`. If `account_cd` can be null, exclude or bucket separately (`where account_cd is not null`).
3. **Base filter**: `where status_desc = 'active'` (computed column from the outer `sub` select).
4. **Single grouped query** (no `group by plan_dur` — that would yield 20+ rows). Group only by last digit and use conditional counts:
  - `count(distinct case when plan_dur = 'monthly' then subscription_guid end) as monthly_cnt`
  - `count(distinct case when plan_dur = 'annual' then subscription_guid end) as annual_cnt`
5. **Percents** on the grouped result (window over the 10 rows):
  - `100.0 * monthly_cnt / nullif(sum(monthly_cnt) over (), 0) as monthly_pct`
  - `100.0 * annual_cnt / nullif(sum(annual_cnt) over (), 0) as annual_pct`
   If every monthly/annual subscriber must fall into one of ten digits, the two `sum(...) over ()` values match the totals you would get from filtering `plan_dur` alone. Use `nullif` to avoid divide-by-zero when a slice is empty.
6. **Exactly 10 rows**: sparse digits (no accounts ending in `3`) drop rows unless you **left join** a spine. To always return 0–9:
  - Build `digits` from `unnest(['0','1',...,'9'])` or `generate_array(0,9)` with `cast(d as string)`.
  - `left join` the aggregation on `digits.last_digit = agg.account_cd_last_digit` and `coalesce(agg.monthly_cnt, 0)`, etc. Percents then use the same window on the coalesced counts (zeros contribute correctly to the denominator).

## Column naming (suggested)


| Column                         | Meaning                                           |
| ------------------------------ | ------------------------------------------------- |
| `account_cd_last_digit`        | `'0'` … `'9'`                                     |
| `monthly_active_sub_cnt`       | Distinct active monthly subs in this digit bucket |
| `monthly_pct_of_monthly_total` | Share of all active monthly across digits         |
| `annual_active_sub_cnt`        | Distinct active annual subs in this digit bucket  |
| `annual_pct_of_annual_total`   | Share of all active annual across digits          |


**Total: 5 columns** (last digit plus the four monthly/annual count and percent fields). Adjust names to taste.

## Edge cases

- `**other` `plan_dur`**: Not included in monthly/annual counts; document or extend if needed.
- **Double-counting**: One row per subscription in `sub` with one `plan_dur`; conditional `count(distinct ...)` is safe.
- **Rounding**: `round(..., 2)` on percents for display.

## Deliverable

- Optional new file under `queries/`, e.g. `sub_active_by_last_digit_plan_dur.sql`, containing full `sub` CTE + final select as above, with project SQL style (leading commas, `where 1=1`, etc.).

