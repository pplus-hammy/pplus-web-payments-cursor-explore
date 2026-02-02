-- Baseline: variable date range for transaction volume per cc_first_6_nbr (count distinct transaction_guid).
-- Compare to daily volume per cc_first_6_nbr for days after baseline; flag anomalies by z-score (std devs from mean).
-- Uses same dataset/filters as txn.sql. Optional excluded_dts drops specific dates from the baseline.

declare baseline_start date default date('2025-10-15');
declare baseline_end date default date('2026-01-15');
declare z_threshold float64 default 2;
declare excluded_dts array<date> default [date('2999-12-31')]; -- days to exclude from baseline (like big event days)

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
            and txn.trans_dt >= baseline_start
            and txn.trans_dt < date_add(current_date, interval 1 day)
            and txn.trans_type_desc in ('purchase', 'verify')
            and txn.trans_status_desc in ('success', 'void', 'declined')
            and txn.origin_desc in ('api', 'token_api')
            and txn.cc_first_6_nbr in ('601100', '601101', '414720')
    )

, baseline_daily as
    (
        select
            src_system_id
            , trans_dt
            , cc_first_6_nbr
            , count(distinct transaction_guid) as daily_ct
            , count(distinct case when trans_status_desc in ('success', 'void') then transaction_guid else null end) as daily_success_ct
        from txn
        where 1=1
            and trans_dt between baseline_start and baseline_end
            and trans_dt not in unnest(excluded_dts)
        group by src_system_id, trans_dt, cc_first_6_nbr
    )

, baseline as
    (
        select
            src_system_id
            , cc_first_6_nbr
            , cast(avg(daily_ct) as int64) as baseline_avg_ct
            , stddev_samp(daily_ct) as baseline_stddev_ct
            , cast(avg(daily_success_ct) as int64) as baseline_success_avg_ct
            , stddev_samp(daily_success_ct) as baseline_success_stddev_ct
        from baseline_daily
        where 1=1
        group by src_system_id, cc_first_6_nbr
    )

, daily_volume as
    (
        select
            src_system_id
            , trans_dt
            , cc_first_6_nbr
            , count(distinct transaction_guid) as daily_ct
            , count(distinct case when trans_status_desc in ('success', 'void') then transaction_guid else null end) as daily_success_ct
        from txn
        where 1=1
            and trans_dt > baseline_end
            and trans_dt < date_add(current_date, interval 1 day)
        group by src_system_id, trans_dt, cc_first_6_nbr
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

        from daily_volume dv
        join baseline bl
            on dv.src_system_id = bl.src_system_id
            and dv.cc_first_6_nbr = bl.cc_first_6_nbr
        where 1=1
    )
select
    *
from chg_chk
where 1=1
    and
        (
            chg_flag is not null
            or
            success_chg_flag is not null
        )
order by 1, 2 desc, 3
