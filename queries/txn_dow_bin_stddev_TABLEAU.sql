with txn as
    (
        select
            src_system_id
            , case
                when txn.gateway_cd in ('cljvv4pluxdo', 'i3z3apzipbp7', 'wf3nj05ig027', 'o8kozk9x9qb0') then 'US'
                when txn.gateway_cd in ('p3oy7jtrnbzu', 'jt7jdrftfjyv') then 'AU'
                when txn.gateway_cd in ('ob5kihh2l5ht', 'ob5lkextdfrs') then 'LATAM'
                when txn.gateway_cd in ('qpdcpwym9258', 'qwazckp2zkjj', 't0gcwmn4afqk') then 'UK'
                when txn.gateway_cd = 'obkq1bdafejc' then 'BR'
                when txn.gateway_cd = 'inkqcylsbc8b' then 'CA'
                when txn.gateway_cd in ('sj7av2xpiqik', 'rnuns7r579dp', 'rnuoyv9lls4l', 'rnum8iuze17e', 'rnunet43fjj0', 'rnuplcrtgiuv', 'rnupcldy1szk') then 'GSA_DE_AT_CH'
                when txn.gateway_cd = 'pnpypk0ag0nv' then 'MX'
                when txn.gateway_cd = 'rs7oav8d5e0f' then 'FR'
                when txn.gateway_cd in ('qpdel7kior3j', 'qwazjh3p96l7') then 'IE'
                when txn.gateway_cd in ('reyg6kvzzovm', 'ra6ffzgs9s8q', 't0h4mkezczbd') then 'IT'
                else txn.gateway_cd
            end as gateway_country
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
            , avs_result_cd
            , approval_desc
            , trans_msg_desc
            , reference_cd
        from i-dss-streaming-data.payment_ops_vw.recurly_transaction_fct txn
        where 1=1
            -- and txn.src_system_id = 115
            and txn.trans_dt >= date_add(<Parameters.Start_Dt>, interval -3 month)
            and txn.trans_dt <= <Parameters.End_Dt>
            and txn.trans_type_desc in ('purchase', 'verify')
            and txn.trans_status_desc in ('success', 'void', 'declined')
            and txn.origin_desc in ('api', 'token_api')
            -- and txn.cc_first_6_nbr in ('601100', '601101', '414720')
            and txn.payment_method_desc = 'Credit Card'
    )

, run_dates as
    (
        select run_dt
        from unnest(generate_date_array(<Parameters.Start_Dt>, <Parameters.End_Dt>)) as run_dt
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
            , gateway_country
            , trans_dt
            , cc_first_6_nbr
            , count(distinct transaction_guid) as daily_ct
            , count(distinct case when trans_status_desc in ('success', 'void') then transaction_guid else null end) as daily_success_ct
            , count(distinct case when trans_status_desc in ('success', 'void') and avs_result_cd not in ('Y','X','V') then transaction_guid else null end) as daily_success_avs_fail_ct 
            , min(case when trans_status_desc in ('success', 'void') then account_cd else null end) as success_acct_ex1
            , max(case when trans_status_desc in ('success', 'void') then account_cd else null end) as success_acct_ex2
            , min(case when trans_status_desc in ('declined') then account_cd else null end) as decline_acct_ex1
            , max(case when trans_status_desc in ('declined') then account_cd else null end) as decline_acct_ex2
            , min(case when trans_status_desc in ('success', 'void') and avs_result_cd not in ('Y','X','V') then account_cd else null end) as success_avs_fail_ex1
            , max(case when trans_status_desc in ('success', 'void') and avs_result_cd not in ('Y','X','V') then account_cd else null end) as success_avs_fail_ex2    
        from txn
        where 1=1
            and trans_dt >= date_sub(<Parameters.Start_Dt>, interval 3 month)
            and trans_dt <= <Parameters.End_Dt>
            and trans_dt not in (date('2026-01-24'), date('2026-01-25')) --exclude big event days, which skew the deviations
        group by 1,2,3,4
    )

, dow_baseline as
    (
        select
            dva.src_system_id
            , dva.gateway_country
            , bd.run_dt
            , dva.cc_first_6_nbr
            , cast(avg(dva.daily_ct) as int64) as baseline_avg_ct
            , cast(stddev_samp(dva.daily_ct) as int64) as baseline_stddev_ct
            , cast(avg(dva.daily_success_ct) as int64) as baseline_success_avg_ct
            , cast(stddev_samp(dva.daily_success_ct) as int64) as baseline_success_stddev_ct
            , cast(avg(dva.daily_success_avs_fail_ct) as int64) as baseline_success_avs_fail_avg_ct
            , cast(stddev_samp(dva.daily_success_avs_fail_ct) as int64) as baseline_success_avs_fail_stddev_ct
        from baseline_dates bd
        join daily_volume_all dva
            on bd.baseline_dt = dva.trans_dt
        where 1=1
            and bd.baseline_dt not in (date('2026-01-24'), date('2026-01-25'))
        group by 1,2,3,4
    )

, daily_volume as
    (
        select
            src_system_id
            , gateway_country
            , trans_dt
            , cc_first_6_nbr
            , daily_ct
            , daily_success_ct
            , daily_success_avs_fail_ct
            , success_acct_ex1
            , success_acct_ex2
            , decline_acct_ex1
            , decline_acct_ex2
            , success_avs_fail_ex1
            , success_avs_fail_ex2

        from daily_volume_all
        where 1=1
            and trans_dt between <Parameters.Start_Dt> and <Parameters.End_Dt>
    )

, chg_chk as
    (
        select
            dv.src_system_id
            , dv.gateway_country
            , dv.trans_dt
            , dv.cc_first_6_nbr

            , bl.baseline_success_avg_ct
            , bl.baseline_success_stddev_ct
            , dv.daily_success_ct
            , cast((dv.daily_success_ct - bl.baseline_success_avg_ct) as integer) as success_diff
            , round((dv.daily_success_ct - bl.baseline_success_avg_ct) / nullif(bl.baseline_success_stddev_ct, 0), 2) as success_z_score
            , case
                when (dv.daily_success_ct - bl.baseline_success_avg_ct) / nullif(bl.baseline_success_stddev_ct, 0) >= <Parameters.Z-Score> then 'large_increase'
                when (dv.daily_success_ct - bl.baseline_success_avg_ct) / nullif(bl.baseline_success_stddev_ct, 0) <= -<Parameters.Z-Score> then 'large_decrease'
                else null
            end as success_chg_flag

            , bl.baseline_avg_ct
            , bl.baseline_stddev_ct
            , dv.daily_ct
            , cast((dv.daily_ct - bl.baseline_avg_ct) as integer) as vol_diff
            , round((dv.daily_ct - bl.baseline_avg_ct) / nullif(bl.baseline_stddev_ct, 0), 2) as vol_z_score
            , case
                when (dv.daily_ct - bl.baseline_avg_ct) / nullif(bl.baseline_stddev_ct, 0) >= <Parameters.Z-Score> then 'large_increase'
                when (dv.daily_ct - bl.baseline_avg_ct) / nullif(bl.baseline_stddev_ct, 0) <= -<Parameters.Z-Score> then 'large_decrease'
                else null
            end as chg_flag

            , bl.baseline_success_avs_fail_avg_ct
            , bl.baseline_success_avs_fail_stddev_ct
            , dv.daily_success_avs_fail_ct
            , cast((dv.daily_success_avs_fail_ct - bl.baseline_success_avs_fail_avg_ct) as integer) as success_avs_fail_diff
            , round((dv.daily_success_avs_fail_ct - bl.baseline_success_avs_fail_avg_ct) / nullif(bl.baseline_success_avs_fail_stddev_ct, 0), 2) as success_avs_fail_z_score
            , case
                when (dv.daily_success_avs_fail_ct - bl.baseline_success_avs_fail_avg_ct) / nullif(bl.baseline_success_avs_fail_stddev_ct, 0) >= <Parameters.Z-Score> then 'large_increase'
                when (dv.daily_success_avs_fail_ct - bl.baseline_success_avs_fail_avg_ct) / nullif(bl.baseline_success_avs_fail_stddev_ct, 0) <= -<Parameters.Z-Score> then 'large_decrease'
                else null
            end as success_avs_fail_chg_flag
            
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
            and dv.gateway_country = bl.gateway_country
            and dv.cc_first_6_nbr = bl.cc_first_6_nbr
            and dv.src_system_id = bl.src_system_id
        where 1=1
    )
select
    *
    , current_date as tableau_end_dt
    , date_add(current_date, interval -4 month) as tableau_start_dt
from chg_chk
where 1=1
    and baseline_avg_ct >= 500
    and
        (
            chg_flag is not null
            or
            success_chg_flag is not null
        )
order by 1, 2, 3 desc, vol_z_score desc