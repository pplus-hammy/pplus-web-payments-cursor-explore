-- Dashboard anomaly panel: BIN (cc_first_6_nbr) share deviation from prior-3-month baseline.
-- Reuses pattern from acct_bin_rate_stddev.sql. Filter params via DECLARE; output: run_dt, cc_first_6_nbr, rate_pct, baseline_avg, z_score, flagged.
-- Frontend can pass date range, src_system_ids, and optional filters (expand WHERE in txn CTE).

declare run_range_end date default date_add(current_date(), interval -1 day);
declare run_range_start date default date_add(run_range_end, interval -13 day);
declare z_threshold float64 default 3;
declare src_system_ids array<int64> default [115, 134, 139];
declare excluded_dts array<date> default [date('2999-12-31')];

with txn as
    (
        select
            src_system_id
            , account_cd
            , trans_dt
            , cc_first_6_nbr
        from i-dss-streaming-data.payment_ops_vw.recurly_transaction_fct txn
        where 1=1
            and txn.trans_dt >= date_add(run_range_start, interval -3 month)
            and txn.trans_dt <= run_range_end
            and (array_length(src_system_ids) = 0 or txn.src_system_id in unnest(src_system_ids))
            and txn.trans_type_desc in ('purchase', 'verify')
            and txn.trans_status_desc in ('success', 'void', 'declined')
            and txn.origin_desc in ('api', 'token_api')
            and txn.payment_method_desc = 'Credit Card'
    )

, run_dates as
    (
        select run_dt
        from unnest(generate_date_array(run_range_start, run_range_end)) as run_dt
    )

, baseline_dates as
    (
        select
            rd.run_dt
            , bd as baseline_dt
        from run_dates rd
        , unnest(generate_date_array(date_sub(rd.run_dt, interval 3 month), date_sub(rd.run_dt, interval 1 day))) as bd
        where 1=1
    )

, daily_totals as
    (
        select
            src_system_id
            , trans_dt
            , count(distinct account_cd) as daily_total_ct
        from txn
        where 1=1
            and trans_dt >= date_sub(run_range_start, interval 3 month)
            and trans_dt <= run_range_end
            and trans_dt not in unnest(excluded_dts)
        group by src_system_id, trans_dt
    )

, daily_volume_all as
    (
        select
            t.src_system_id
            , t.trans_dt
            , t.cc_first_6_nbr
            , count(distinct t.account_cd) as daily_bin_ct
            , dt.daily_total_ct
            , round(100.0 * count(distinct t.account_cd) / nullif(dt.daily_total_ct, 0), 5) as daily_rate_pct
        from txn t
        join daily_totals dt
            on t.src_system_id = dt.src_system_id
            and t.trans_dt = dt.trans_dt
        where 1=1
            and t.trans_dt >= date_sub(run_range_start, interval 3 month)
            and t.trans_dt <= run_range_end
            and t.trans_dt not in unnest(excluded_dts)
        group by t.src_system_id, t.trans_dt, t.cc_first_6_nbr, dt.daily_total_ct
    )

, baseline as
    (
        select
            dva.src_system_id
            , bd.run_dt
            , dva.cc_first_6_nbr
            , round(avg(dva.daily_rate_pct), 5) as baseline_avg_rate_pct
            , round(stddev_samp(dva.daily_rate_pct), 5) as baseline_stddev_rate_pct
        from baseline_dates bd
        join daily_volume_all dva
            on bd.baseline_dt = dva.trans_dt
        where 1=1
            and bd.baseline_dt not in unnest(excluded_dts)
        group by dva.src_system_id, bd.run_dt, dva.cc_first_6_nbr
    )

, daily_volume as
    (
        select
            src_system_id
            , trans_dt
            , cc_first_6_nbr
            , daily_bin_ct
            , daily_total_ct
            , daily_rate_pct
        from daily_volume_all
        where 1=1
            and trans_dt between run_range_start and run_range_end
    )

, with_z as
    (
        select
            dv.src_system_id
            , dv.trans_dt as run_dt
            , dv.cc_first_6_nbr
            , dv.daily_bin_ct
            , dv.daily_rate_pct as rate_pct
            , bl.baseline_avg_rate_pct as baseline_avg
            , round((dv.daily_rate_pct - bl.baseline_avg_rate_pct) / nullif(bl.baseline_stddev_rate_pct, 0), 5) as z_score
            , (dv.daily_rate_pct - bl.baseline_avg_rate_pct) / nullif(bl.baseline_stddev_rate_pct, 0) >= z_threshold
                or (dv.daily_rate_pct - bl.baseline_avg_rate_pct) / nullif(bl.baseline_stddev_rate_pct, 0) <= -z_threshold as flagged
        from daily_volume dv
        join baseline bl
            on dv.src_system_id = bl.src_system_id
            and dv.trans_dt = bl.run_dt
            and dv.cc_first_6_nbr = bl.cc_first_6_nbr
        where 1=1
            and bl.baseline_avg_rate_pct is not null
    )

select
    run_dt
    , cc_first_6_nbr
    , rate_pct
    , baseline_avg
    , z_score
    , flagged
from with_z
where 1=1
    and daily_bin_ct >= 100
order by run_dt desc, z_score desc
limit 500