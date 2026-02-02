-- Baseline: variable date range for transaction volume per cc_first_6_nbr (count distinct transaction_guid).
-- Compare to daily volume per cc_first_6_nbr for days after baseline; report volume_change and pct_change.
-- Uses same dataset/filters as txn.sql.

declare baseline_start date default date('2025-10-15');
declare baseline_end date default date('2026-01-15');

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
            -- , processor_response_cd	
            -- , issuer_response_cd	
            , payment_method_desc	
            , cc_type_desc
            , cc_first_6_nbr	
            , card_brand_nm	
            , card_type_cd	
            , card_level_cd	
            , card_issuer_nm	
            , card_issuing_country_cd	
            -- , cc_last_4	
            -- , cc_exp_mth	
            -- , cc_exp_yr
            , approval_desc
            , avs_result_cd
            , trans_msg_desc
            -- , cc_payment_id
            , reference_cd
        from i-dss-streaming-data.payment_ops_vw.recurly_transaction_fct txn
        where 1=1
            -- and txn.src_system_id = 115
            and txn.trans_dt >= baseline_start
            and txn.trans_dt <= date('2026-01-31')
            and txn.trans_type_desc in ('purchase', 'verify')
            and txn.trans_status_desc in ('success', 'void', 'declined')
            and txn.origin_desc in ('api', 'token_api')
            and txn.cc_first_6_nbr in ('601100', '601101', '414720')
    )

, baseline as 
    (
        select
            src_system_id
            , cc_first_6_nbr
            , count(distinct transaction_guid) as baseline_tot_ct
            , cast(count(distinct transaction_guid) / (date_diff(baseline_end, baseline_start, day) + 1) as integer) as baseline_avg_ct
            , count(distinct case when trans_status_desc in ('success', 'void') then transaction_guid else null end) as baseline_success_tot_ct
            , cast(count(distinct case when trans_status_desc in ('success', 'void') then transaction_guid else null end) / (date_diff(baseline_end, baseline_start, day) + 1) as integer) as baseline_success_avg_ct
        from txn
        where 1=1 
            and trans_dt between baseline_start and baseline_end
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
            , dv.daily_success_ct
            , cast((dv.daily_success_ct - bl.baseline_success_avg_ct) as integer) as success_diff
            , round((dv.daily_success_ct - bl.baseline_success_avg_ct) / nullif(bl.baseline_success_avg_ct, 0) * 100, 2) as success_pct_chg
            , case
                when (dv.daily_success_ct - bl.baseline_success_avg_ct) / nullif(bl.baseline_success_avg_ct, 0) >= .5 then 'large_increase'
                when (dv.daily_success_ct - bl.baseline_success_avg_ct) / nullif(bl.baseline_success_avg_ct, 0) <= -.5 then 'large_decrease'
                else null
            end as success_chg_flag

            , bl.baseline_tot_ct
            , bl.baseline_avg_ct
            , dv.daily_ct
            , cast((dv.daily_ct - bl.baseline_avg_ct) as integer) as vol_diff
            , round((dv.daily_ct - bl.baseline_avg_ct) / nullif(bl.baseline_avg_ct, 0) * 100, 2) as pct_change
            , case
                when (dv.daily_ct - bl.baseline_avg_ct) / nullif(bl.baseline_avg_ct, 0) >= 0.50 then 'large_increase'
                when (dv.daily_ct - bl.baseline_avg_ct) / nullif(bl.baseline_avg_ct, 0) <= -0.50 then 'large_decrease'
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
