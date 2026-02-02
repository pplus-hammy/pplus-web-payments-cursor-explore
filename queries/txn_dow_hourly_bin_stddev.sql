-- DOW + hourly baseline: variable run range for transaction volume per cc_first_6_nbr (count distinct transaction_guid).
-- For each run date and hour, baseline = same day-of-week and same hour in rolling 3-month lookback (excl. run date); flag anomalies by z-score.
-- Run range is variable (default last 14 days). Uses same dataset/filters as txn.sql. Optional excluded_dts drops dates from baseline.

declare run_range_end date default date_add(current_date(), interval 0 day);
declare run_range_start date default date_add(run_range_end, interval -14 day);
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
            , extract(hour from datetime(trans_dt_ut, 'America/Los_Angeles')) as trans_hr
            -- , max(case when trans_dt = date(current_datetime('America/Los_Angeles')) then extract(hour from datetime(trans_dt_ut, 'America/Los_Angeles')) else null end) as max_hr
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
        -- from i-dss-streaming-data.payment_ops_vw.recurly_transaction_fct txn
        from
            (
                select distinct
                    *
                from
                    (
                        select
                            src_system_id
                            , cast(account_code as string) as account_cd	
                            , subscription_id as subscription_guid	
                            , invoice_id as invoice_guid	
                            , transaction_id as transaction_guid
                            , date(datetime(date, 'America/Los_Angeles')) as trans_dt	
                            , date as trans_dt_ut	
                            , origin as origin_desc
                            , type as trans_type_desc	
                            , status as trans_status_desc	
                            , amount as trans_amt	
                            , tax_amount as tax_amt	
                            , currency as currency_cd	
                            , country as country_cd
                            , transaction_gateway_type as trans_gateway_type_desc	
                            , gateway_code as gateway_cd	
                            , gateway_error_codes as gateway_error_cd	
                            , failure_type as failure_type	
                            -- , processor_response_code as processor_response_cd	
                            -- , issuer_response_code as issuer_response_cd	
                            , payment_method as payment_method_desc	
                            , cc_type as cc_type_desc
                            , cast(round(cast(cc_first_6 as float64),0) as string) as cc_first_6_nbr	
                            , card_brand as card_brand_nm	
                            , card_type as card_type_cd	
                            , card_level as card_level_cd	
                            , card_issuer as card_issuer_nm	
                            , card_issuing_country as card_issuing_country_cd	
                            -- , cc_last_4 as cc_last_4	
                            -- , expire_month as cc_exp_mth	
                            -- , expire_year as cc_exp_yr
                            , approval_code as approval_desc
                            , avs_result as avs_result_cd
                            , message as trans_msg_desc
                            -- , payment_method_identifier as cc_payment_id
                            , reference as reference_cd
                        from i-dss-streaming-data.payment_ops_sandbox.transactions_to_bq
                        where 1=1
                            -- and src_system_id = 115
                            and type in ('purchase','verify')
                            and status in ('success', 'void', 'declined')
                            
                        union all

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
                            -- and txn.src_system_id in (115)
                            -- and txn.trans_dt >= date('2025-04-01')
                            and txn.trans_dt >= date_add(run_range_start, interval -3 month)
                            and txn.trans_dt <= run_range_end
                            and txn.trans_type_desc in ('purchase','verify')
                            and txn.trans_status_desc in ('success', 'void', 'declined')
                    ) txn
            ) txn
        where 1=1
            and txn.trans_dt >= date_add(run_range_start, interval -3 month)
            and txn.trans_dt <= run_range_end
            and txn.trans_type_desc in ('purchase', 'verify')
            and txn.trans_status_desc in ('success', 'void', 'declined')
            and txn.origin_desc in ('api', 'token_api')
            -- and txn.cc_first_6_nbr in ('601100', '601101', '414720')
            and txn.payment_method_desc = 'Credit Card'
    )

, max_hr as
    (
        select
            src_system_id
            , trans_dt
            , max(extract(hour from datetime(trans_dt_ut, 'America/Los_Angeles'))) as max_hr
        from txn
        where 1=1
            and trans_dt = date(current_datetime('America/Los_Angeles'))
        group by all
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

, hourly_volume_all as
    (
        select
            src_system_id
            , trans_dt
            , trans_hr
            , cc_first_6_nbr
            , count(distinct transaction_guid) as hourly_ct
            , count(distinct case when trans_status_desc in ('success', 'void') then transaction_guid else null end) as hourly_success_ct
            , min(case when trans_status_desc in ('success', 'void') then account_cd else null end) as success_acct_ex1
            , max(case when trans_status_desc in ('success', 'void') then account_cd else null end) as success_acct_ex2
            , min(case when trans_status_desc in ('declined') then account_cd else null end) as decline_acct_ex1
            , max(case when trans_status_desc in ('declined') then account_cd else null end) as decline_acct_ex2
        from txn
        where 1=1
            and trans_dt >= date_sub(run_range_start, interval 3 month)
            and trans_dt <= run_range_end
            and trans_dt not in unnest(excluded_dts)
        group by src_system_id, trans_dt, trans_hr, cc_first_6_nbr
    )

, dow_hour_baseline as
    (
        select
            hva.src_system_id
            , bd.run_dt
            , hva.trans_hr
            , hva.cc_first_6_nbr
            , cast(avg(hva.hourly_ct) as int64) as baseline_avg_ct
            , cast(stddev_samp(hva.hourly_ct) as int64) as baseline_stddev_ct
            , cast(avg(hva.hourly_success_ct) as int64) as baseline_success_avg_ct
            , cast(stddev_samp(hva.hourly_success_ct) as int64) as baseline_success_stddev_ct
        from baseline_dates bd
        join hourly_volume_all hva
            on bd.baseline_dt = hva.trans_dt
        where 1=1
            and bd.baseline_dt not in unnest(excluded_dts)
        group by hva.src_system_id, bd.run_dt, hva.trans_hr, hva.cc_first_6_nbr
    )

, hourly_volume as
    (
        select
            src_system_id
            , trans_dt
            , trans_hr
            , cc_first_6_nbr
            , hourly_ct
            , hourly_success_ct
            , success_acct_ex1
            , success_acct_ex2
            , decline_acct_ex1
            , decline_acct_ex2
        from hourly_volume_all
        where 1=1
            and trans_dt between run_range_start and run_range_end
    )

, chg_chk as
    (
        select
            hv.src_system_id
            , hv.trans_dt
            , hv.trans_hr
            , hv.cc_first_6_nbr

            , bl.baseline_success_avg_ct
            , bl.baseline_success_stddev_ct
            , hv.hourly_success_ct
            , cast((hv.hourly_success_ct - bl.baseline_success_avg_ct) as integer) as success_diff
            , round((hv.hourly_success_ct - bl.baseline_success_avg_ct) / nullif(bl.baseline_success_stddev_ct, 0), 2) as success_z_score
            , case
                when (hv.hourly_success_ct - bl.baseline_success_avg_ct) / nullif(bl.baseline_success_stddev_ct, 0) >= z_threshold then 'large_increase'
                when (hv.hourly_success_ct - bl.baseline_success_avg_ct) / nullif(bl.baseline_success_stddev_ct, 0) <= -z_threshold then 'large_decrease'
                else null
            end as success_chg_flag

            , bl.baseline_avg_ct
            , bl.baseline_stddev_ct
            , hv.hourly_ct
            , cast((hv.hourly_ct - bl.baseline_avg_ct) as integer) as vol_diff
            , round((hv.hourly_ct - bl.baseline_avg_ct) / nullif(bl.baseline_stddev_ct, 0), 2) as vol_z_score
            , case
                when (hv.hourly_ct - bl.baseline_avg_ct) / nullif(bl.baseline_stddev_ct, 0) >= z_threshold then 'large_increase'
                when (hv.hourly_ct - bl.baseline_avg_ct) / nullif(bl.baseline_stddev_ct, 0) <= -z_threshold then 'large_decrease'
                else null
            end as chg_flag
            , success_acct_ex1
            , success_acct_ex2
            , decline_acct_ex1
            , decline_acct_ex2

        from hourly_volume hv
        join dow_hour_baseline bl
            on hv.src_system_id = bl.src_system_id
            and hv.trans_dt = bl.run_dt
            and hv.trans_hr = bl.trans_hr
            and hv.cc_first_6_nbr = bl.cc_first_6_nbr
        where 1=1
    )
select
    *
from chg_chk
join max_hr mh
    on hv.src_system_id = mh.src_system_id
    and hv.trans_dt = mh.trans_dt
    and hv.trans_hr = mh.max_hr
where 1=1
    and baseline_avg_ct >= 20
    and
        (
            chg_flag is not null
            or
            success_chg_flag is not null
        )
order by 1, 2 desc, 3, 4, 5
