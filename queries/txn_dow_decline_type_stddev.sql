-- DOW baseline: variable run range for transaction volume per failure_type + gateway_error_cd (count distinct transaction_guid).
-- For each run date, baseline = same day-of-week in rolling 3-month lookback (excl. run date); flag anomalies by z-score.
-- Run range is variable (default last 14 days). Uses same dataset/filters as txn.sql. Optional excluded_dts drops dates from baseline.

declare run_range_end date default date_add(current_date(), interval -1 day);
declare run_range_start date default date_add(run_range_end, interval -13 day);
declare z_threshold float64 default 3;
declare excluded_dts array<date> default [date('2026-01-24'), date('2999-12-31')]; -- days to exclude from baseline (like big event days)

with txn as
    (
        select
            src_system_id
            , account_cd
            , subscription_guid
            , invoice_guid
            , transaction_guid
            , trans_dt
            , trans_dt_ut
            , origin_desc
            , trans_type_desc
            , trans_status_desc
            , trans_amt
            , tax_amt
            , currency_cd
            , country_cd
            , trans_gateway_type_desc
            , gateway_cd
            , gateway_error_cd
            , failure_type
            , payment_method_desc
            , cc_type_desc
            , cc_first_6_nbr
            , card_brand_nm
            , card_type_cd
            , card_level_cd
            , card_issuer_nm
            , card_issuing_country_cd
            , approval_desc
            , trans_msg_desc
            , reference_cd
        from i-dss-streaming-data.payment_ops_vw.recurly_transaction_fct txn
        where 1=1
            and txn.src_system_id = 115
            and txn.trans_dt >= date_add(run_range_start, interval -3 month)
            and txn.trans_dt <= run_range_end
            and txn.trans_type_desc in ('purchase', 'verify')
            and txn.trans_status_desc in ('success', 'void', 'declined')
            and txn.origin_desc in ('api', 'token_api')
            -- and txn.failure_type in ('...')
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
            and extract(dayofweek from bd) = extract(dayofweek from rd.run_dt)
    )

, daily_volume_all as
    (
        select
            trans_dt
            , failure_type
            , gateway_error_cd
            , count(distinct transaction_guid) as daily_ct
            , min(case when trans_status_desc in ('declined') then account_cd else null end) as decline_acct_ex1
            , max(case when trans_status_desc in ('declined') then account_cd else null end) as decline_acct_ex2
        from txn
        where 1=1
            and trans_dt >= date_sub(run_range_start, interval 3 month)
            and trans_dt <= run_range_end
            and trans_dt not in unnest(excluded_dts)
        group by trans_dt, failure_type, gateway_error_cd
    )

, dow_baseline as
    (
        select
            bd.run_dt
            , dva.failure_type
            , dva.gateway_error_cd
            , cast(avg(dva.daily_ct) as int64) as baseline_avg_ct
            , cast(stddev_samp(dva.daily_ct) as int64) as baseline_stddev_ct
        from baseline_dates bd
        join daily_volume_all dva
            on bd.baseline_dt = dva.trans_dt
        where 1=1
            and bd.baseline_dt not in unnest(excluded_dts)
        group by bd.run_dt, dva.failure_type, dva.gateway_error_cd
    )

, daily_volume as
    (
        select
            trans_dt
            , failure_type
            , gateway_error_cd
            , daily_ct
            , decline_acct_ex1
            , decline_acct_ex2
        from daily_volume_all
        where 1=1
            and trans_dt between run_range_start and run_range_end
    )

, chg_chk as
    (
        select
            dv.trans_dt
            , dv.gateway_error_cd
            , dv.failure_type
            
            , bl.baseline_avg_ct
            , bl.baseline_stddev_ct
            , dv.daily_ct
            , cast((dv.daily_ct - bl.baseline_avg_ct) as integer) as vol_diff
            , round((dv.daily_ct - bl.baseline_avg_ct) / nullif(bl.baseline_stddev_ct, 0), 2) as vol_z_score
            , case
                when (dv.daily_ct - bl.baseline_avg_ct) / nullif(bl.baseline_stddev_ct, 0) >= z_threshold then 'large_increase'
                when (dv.daily_ct - bl.baseline_avg_ct) / nullif(bl.baseline_stddev_ct, 0) <= -z_threshold then 'large_decrease'
                else null
            end as chg_flag
            , decline_acct_ex1
            , decline_acct_ex2

        from daily_volume dv
        join dow_baseline bl
            on dv.trans_dt = bl.run_dt
            and dv.failure_type = bl.failure_type
            and (dv.gateway_error_cd is not distinct from bl.gateway_error_cd)
        where 1=1
    )
select
    *
from chg_chk
where 1=1
    and baseline_avg_ct >= 500
    and chg_flag is not null
order by 1 desc, 2, 3
