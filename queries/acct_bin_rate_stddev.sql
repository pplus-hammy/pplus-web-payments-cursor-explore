-- Rolling baseline: flag bins (cc_first_6_nbr) whose share of daily transactions (rate % of total) deviates from prior-3-month baseline.
-- Uses percent of total transactions per bin per day; baseline = avg/stddev of that rate over prior 3 months (excl. run date, minus excluded_dts). No DOW filter.

declare run_range_end date default date_add(current_date(), interval -1 day);
declare run_range_start date default date_add(run_range_end, interval -13 day);
declare z_threshold float64 default 3;
declare excluded_dts array<date> default [date('2026-01-24'),date('2026-01-25'),date('2026-01-31'), date('2026-02-01'),date('2999-12-31')];

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
            , avs_result_cd
            , trans_msg_desc
            , reference_cd
        from i-dss-streaming-data.payment_ops_vw.recurly_transaction_fct txn
        where 1=1
            and txn.trans_dt >= date_add(run_range_start, interval -3 month)
            and txn.trans_dt <= run_range_end
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
            , cast(avg(dva.daily_bin_ct) as int64) as baseline_bin_ct
            , cast(avg(dva.daily_total_ct) as int64) as baseline_total_ct
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

, chg_chk as
    (
        select
            dv.src_system_id
            , dv.trans_dt
            , dv.cc_first_6_nbr
            , dv.daily_bin_ct
            , dv.daily_total_ct
            , dv.daily_rate_pct
            , bl.baseline_bin_ct
            , bl.baseline_total_ct
            , bl.baseline_avg_rate_pct
            , bl.baseline_stddev_rate_pct
            , round(dv.daily_rate_pct - bl.baseline_avg_rate_pct, 5) as rate_diff_pct
            , round((dv.daily_rate_pct - bl.baseline_avg_rate_pct) / nullif(bl.baseline_stddev_rate_pct, 0), 5) as rate_z_score
            , case
                when (dv.daily_rate_pct - bl.baseline_avg_rate_pct) / nullif(bl.baseline_stddev_rate_pct, 0) >= z_threshold then 'large_increase'
                when (dv.daily_rate_pct - bl.baseline_avg_rate_pct) / nullif(bl.baseline_stddev_rate_pct, 0) <= -z_threshold then 'large_decrease'
                else null
            end as rate_chg_flag
        from daily_volume dv
        join baseline bl
            on dv.src_system_id = bl.src_system_id
            and dv.trans_dt = bl.run_dt
            and dv.cc_first_6_nbr = bl.cc_first_6_nbr
        where 1=1
    )

, bin_attack_bins as
    (
        select
            *
        from chg_chk
        where 1=1
            and baseline_avg_rate_pct is not null
            and rate_chg_flag is not null
        order by 1, 2 desc, 3
    )

select
    *
from bin_attack_bins
where 1=1
    and baseline_bin_ct >= 100
order by 1, 2 desc, rate_z_score desc
