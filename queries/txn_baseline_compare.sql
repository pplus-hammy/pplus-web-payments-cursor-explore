-- Baseline: Jan 1–Jan 18 transaction volume per cc_first_6_nbr (count distinct transaction_guid).
-- Compare to daily volume per cc_first_6_nbr for Jan 19–Jan 31; report large increases or decreases.
-- Uses same dataset/filters as txn.sql.

with txn as (
    select
        transaction_guid,
        trans_dt,
        cc_first_6_nbr
    from `i-dss-streaming-data.payment_ops_vw.recurly_transaction_fct` txn
    where 1 = 1
        and txn.src_system_id = 115
        and txn.trans_dt >= date('2026-01-01')
        and txn.trans_dt <= date('2026-01-31')
        and txn.trans_type_desc in ('purchase', 'verify')
        and txn.trans_status_desc in ('success', 'void', 'declined')
        and txn.origin_desc in ('api', 'token_api')
        and txn.cc_first_6_nbr in ('601100', '601101', '414720')
),

-- Baseline: total distinct transactions per BIN over Jan 1–18, then daily average
baseline as (
    select
        cc_first_6_nbr,
        count(distinct transaction_guid) as baseline_total_txns,
        count(distinct transaction_guid) / 18.0 as baseline_daily_avg
    from txn
    where trans_dt between date('2026-01-01') and date('2026-01-18')
    group by cc_first_6_nbr
),

-- Daily volume per BIN for Jan 19–31
daily_volume as (
    select
        trans_dt,
        cc_first_6_nbr,
        count(distinct transaction_guid) as daily_txn_cnt
    from txn
    where trans_dt between date('2026-01-19') and date('2026-01-31')
    group by trans_dt, cc_first_6_nbr
)

select
    d.trans_dt,
    d.cc_first_6_nbr,
    b.baseline_total_txns,
    round(b.baseline_daily_avg, 1) as baseline_daily_avg,
    d.daily_txn_cnt,
    round(d.daily_txn_cnt - b.baseline_daily_avg, 1) as volume_change,
    round((d.daily_txn_cnt - b.baseline_daily_avg) / nullif(b.baseline_daily_avg, 0) * 100, 1) as pct_change,
    case
        when (d.daily_txn_cnt - b.baseline_daily_avg) / nullif(b.baseline_daily_avg, 0) >= 0.50 then 'large_increase'
        when (d.daily_txn_cnt - b.baseline_daily_avg) / nullif(b.baseline_daily_avg, 0) <= -0.50 then 'large_decrease'
        else 'within_range'
    end as flag
from daily_volume d
join baseline b
    on d.cc_first_6_nbr = b.cc_first_6_nbr
order by d.trans_dt,
    d.cc_first_6_nbr;
