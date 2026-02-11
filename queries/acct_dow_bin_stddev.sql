-- DOW baseline: variable run range for transaction volume per cc_first_6_nbr (count distinct transaction_guid).
-- For each run date, baseline = same day-of-week in rolling 3-month lookback (excl. run date); flag anomalies by z-score.
-- Run range is variable (default last 14 days). Uses same dataset/filters as txn.sql. Optional excluded_dts drops dates from baseline.

declare run_range_end date default date_add(current_date(), interval -1 day);
declare run_range_start date default date_add(run_range_end, interval -13 day);
declare z_threshold float64 default 3;
declare excluded_dts array<date> default [date('2026-01-24'),date('2026-01-25'),date('2026-01-31'), date('2026-02-01'),date('2999-12-31')]; -- days to exclude from baseline (like big event days)

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
            -- and txn.src_system_id = 115
            and txn.trans_dt >= date_add(run_range_start, interval -3 month)
            and txn.trans_dt <= run_range_end
            and txn.trans_type_desc in ('purchase', 'verify')
            and txn.trans_status_desc in ('success', 'void', 'declined')
            and txn.origin_desc in ('api', 'token_api')
            -- and txn.cc_first_6_nbr in ('601100', '601101', '414720')
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
            src_system_id
            , trans_dt
            , cc_first_6_nbr
            , count(distinct account_cd) as daily_ct
            , count(distinct case when trans_status_desc in ('success', 'void') then account_cd else null end) as daily_success_ct
            , count(distinct case when trans_status_desc in ('success', 'void') and avs_result_cd not in ('Y','X','V') then account_cd else null end) as daily_success_avs_fail_ct
            , count(distinct case when trans_status_desc = 'declined' then account_cd else null end) as daily_decline_ct
            , min(case when trans_status_desc in ('success', 'void') then account_cd else null end) as success_acct_ex1
            , max(case when trans_status_desc in ('success', 'void') then account_cd else null end) as success_acct_ex2
            , min(case when trans_status_desc in ('declined') then account_cd else null end) as decline_acct_ex1
            , max(case when trans_status_desc in ('declined') then account_cd else null end) as decline_acct_ex2
            , min(case when trans_status_desc in ('success', 'void') and avs_result_cd not in ('Y','X','V') then account_cd else null end) as success_avs_fail_ex1
            , max(case when trans_status_desc in ('success', 'void') and avs_result_cd not in ('Y','X','V') then account_cd else null end) as success_avs_fail_ex2  
        from txn
        where 1=1
            and trans_dt >= date_sub(run_range_start, interval 3 month)
            and trans_dt <= run_range_end
            and trans_dt not in unnest(excluded_dts)
        group by src_system_id, trans_dt, cc_first_6_nbr
    )

, dow_baseline as
    (
        select
            dva.src_system_id
            , bd.run_dt
            , dva.cc_first_6_nbr
            , cast(avg(dva.daily_ct) as int64) as baseline_avg_ct
            , cast(stddev_samp(dva.daily_ct) as int64) as baseline_stddev_ct
            , cast(avg(dva.daily_success_ct) as int64) as baseline_success_avg_ct
            , cast(stddev_samp(dva.daily_success_ct) as int64) as baseline_success_stddev_ct
            , cast(avg(dva.daily_success_avs_fail_ct) as int64) as baseline_success_avs_fail_avg_ct
            , cast(stddev_samp(dva.daily_success_avs_fail_ct) as int64) as baseline_success_avs_fail_stddev_ct
            , cast(avg(dva.daily_decline_ct) as int64) as baseline_decline_avg_ct
            , cast(stddev_samp(dva.daily_decline_ct) as int64) as baseline_decline_stddev_ct
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
            , daily_ct
            , daily_success_ct
            , daily_success_avs_fail_ct
            , daily_decline_ct
            , success_acct_ex1
            , success_acct_ex2
            , decline_acct_ex1
            , decline_acct_ex2
            , success_avs_fail_ex1
            , success_avs_fail_ex2
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

            , bl.baseline_success_avg_ct
            , bl.baseline_success_stddev_ct
            , dv.daily_success_ct
            , cast((dv.daily_success_ct - bl.baseline_success_avg_ct) as integer) as success_diff
            , round((dv.daily_success_ct - bl.baseline_success_avg_ct) / nullif(bl.baseline_success_stddev_ct, 0), 2) as success_z_score
            , case
                when (dv.daily_success_ct - bl.baseline_success_avg_ct) / nullif(bl.baseline_success_stddev_ct, 0) >= z_threshold then 'large_increase'
                when (dv.daily_success_ct - bl.baseline_success_avg_ct) / nullif(bl.baseline_success_stddev_ct, 0) <= -z_threshold then 'large_decrease'
                else null
            end as success_chg_flag

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

            , bl.baseline_success_avs_fail_avg_ct
            , bl.baseline_success_avs_fail_stddev_ct
            , dv.daily_success_avs_fail_ct
            , cast((dv.daily_success_avs_fail_ct - bl.baseline_success_avs_fail_avg_ct) as integer) as success_avs_fail_diff
            , round((dv.daily_success_avs_fail_ct - bl.baseline_success_avs_fail_avg_ct) / nullif(bl.baseline_success_avs_fail_stddev_ct, 0), 2) as success_avs_fail_z_score
            , case
                when (dv.daily_success_avs_fail_ct - bl.baseline_success_avs_fail_avg_ct) / nullif(bl.baseline_success_avs_fail_stddev_ct, 0) >= z_threshold then 'large_increase'
                when (dv.daily_success_avs_fail_ct - bl.baseline_success_avs_fail_avg_ct) / nullif(bl.baseline_success_avs_fail_stddev_ct, 0) <= -z_threshold then 'large_decrease'
                else null
            end as success_avs_fail_chg_flag

            , bl.baseline_decline_avg_ct
            , bl.baseline_decline_stddev_ct
            , dv.daily_decline_ct
            , cast((dv.daily_decline_ct - bl.baseline_decline_avg_ct) as integer) as decline_diff
            , round((dv.daily_decline_ct - bl.baseline_decline_avg_ct) / nullif(bl.baseline_decline_stddev_ct, 0), 2) as decline_z_score
            , case
                when (dv.daily_decline_ct - bl.baseline_decline_avg_ct) / nullif(bl.baseline_decline_stddev_ct, 0) >= z_threshold then 'large_increase'
                when (dv.daily_decline_ct - bl.baseline_decline_avg_ct) / nullif(bl.baseline_decline_stddev_ct, 0) <= -z_threshold then 'large_decrease'
                else null
            end as decline_chg_flag

            , success_acct_ex1
            , success_acct_ex2
            , decline_acct_ex1
            , decline_acct_ex2
            , success_avs_fail_ex1
            , success_avs_fail_ex2

        from daily_volume dv
        join dow_baseline bl
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
            and baseline_avg_ct >= 300
            and
                (
                    chg_flag is not null
                    or
                    success_chg_flag is not null
                    or
                    success_avs_fail_chg_flag is not null
                    or
                    decline_chg_flag is not null
                )
        order by 1, 2 desc, 3
    )
, bin_attackers as
    (
        select
            bab.src_system_id
            , bab.trans_dt
            , bab.cc_first_6_nbr
            , txn.account_cd
            -- , txn.
        from bin_attack_bins bab
        join txn
            on txn.src_system_id = bab.src_system_id
            and txn.trans_dt = bab.trans_dt
            and txn.cc_first_6_nbr = bab.cc_first_6_nbr
        where 1=1
        
    )
select
*
from bin_attack_bins
where 1=1
order by 1,2 desc, baseline_avg_ct desc