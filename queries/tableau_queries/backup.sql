with sub as
    (
        select
            case
                when datetime(sub.expiration_dt_ut, 'America/New_York') between date_add(date_trunc(current_datetime('America/New_York'), DAY), INTERVAL -1 SECOND) and datetime('2999-12-01 23:59:59') then 'canceled'
                when datetime(sub.expiration_dt_ut, 'America/New_York') < date_trunc(current_datetime('America/New_York'), DAY) then 'expired'
                when sub.expiration_dt_ut = timestamp('2999-12-31 23:59:59 UTC') then 'active'
                else sub.status_desc
            end as status_desc
            , case
                when sub.cancel_dt_ut between sub.curr_period_start_dt_ut and sub.curr_period_end_dt_ut
                        and sub.cancel_dt_ut = sub.expiration_dt_ut
                        and (
                                (date(sub.curr_period_start_dt_ut) > date('2025-11-06') and date_diff(sub.expiration_dt_ut, sub.curr_period_start_dt_ut, DAY) = 27)
                                or (date(sub.curr_period_start_dt_ut) <= date('2025-11-06') and date_diff(sub.expiration_dt_ut, sub.curr_period_start_dt_ut, DAY) = 28)
                            )
                    then 'involuntary_fail_dunning'
                when sub.cancel_dt_ut between sub.curr_period_start_dt_ut and sub.curr_period_end_dt_ut
                        and sub.cancel_dt_ut != sub.expiration_dt_ut
                        and (
                                (date(sub.curr_period_start_dt_ut) > date('2025-11-06') and date_diff(sub.expiration_dt_ut, sub.curr_period_start_dt_ut, DAY) = 27)
                                or (date(sub.curr_period_start_dt_ut) <= date('2025-11-06') and date_diff(sub.expiration_dt_ut, sub.curr_period_start_dt_ut, DAY) = 28)
                            )
                    then 'voluntary_cancel_in_dunning_fail'
                when sub.cancel_dt_ut between sub.curr_period_start_dt_ut and sub.curr_period_end_dt_ut
                        and sub.cancel_dt_ut != sub.expiration_dt_ut
                        and sub.curr_period_end_dt_ut = sub.expiration_dt_ut
                    then 'voluntary_cancel_expire_end_of_cycle'
                when sub.cancel_dt_ut = sub.expiration_dt_ut and sub.curr_period_start_dt_ut = sub.cancel_dt_ut
                    then 'involuntary_gift_card_expired_no_more_credit'
                when date(sub.cancel_dt_ut) = date('2999-12-31')
                    then null
                else 'voluntary_likely_agent_cancel'
            end as cancel_chk
            , datetime(sub.activate_dt_ut, 'America/New_York') as activate_dt_et
            , sub.* except (status_desc)
            , coalesce(lag(sub.expiration_dt_ut) over (partition by sub.src_system_id, sub.account_cd order by creation_dt_ut, activate_dt_ut, sub.expiration_dt_ut), timestamp('1900-01-01 00:00:01 UTC')) as prior_expiration
            , max(case when sub.frn = 1 then sub.activate_dt_ut else null end) over (partition by sub.src_system_id, sub.account_cd order by creation_dt_ut, activate_dt_ut, sub.expiration_dt_ut) as original_activation
        from
            (
                select
                    sub.src_system_id
                    , sub.account_cd
                    , sub.subscription_guid
                    , sub.plan_cd
                    , sub.plan_nm
                    , trim(split(regexp_replace(sub.plan_nm, r'\([^()]*\)',''), ' -')[offset(0)]) as base_plan_nm
                    , regexp_extract(sub.plan_nm, r'\((.*?)\)') as plan_qualifier
                    , case
                        when lower(sub.plan_cd) like '%monthly%' then 'monthly'
                        when lower(sub.plan_cd) like '%annual%' then 'annual'
                        else 'other'
                    end as plan_dur
                    , sub.unit_amt
                    , sub.currency_cd
                    , sub.status_desc
                    , sub.creation_dt_ut
                    , sub.activate_dt_ut
                    , sub.trial_start_dt_ut
                    , sub.trial_end_dt_ut
                    , sub.curr_period_start_dt_ut
                    , sub.curr_period_end_dt_ut
                    , coalesce(sub.cancel_dt_ut, timestamp('2999-12-31 23:59:59 UTC')) as cancel_dt_ut
                    , case
                        when sub.expiration_dt_ut is null and lower(sub.plan_cd) like '%monthly%' and date(sub.curr_period_end_dt_ut) <= date_add(current_date, interval -2 MONTH)
                            then sub.curr_period_end_dt_ut
                        when sub.expiration_dt_ut is null and lower(sub.plan_cd) like '%annual%' and date(sub.curr_period_end_dt_ut) <= date_add(current_date, interval -2 MONTH)
                            then sub.curr_period_end_dt_ut
                        when sub.expiration_dt_ut is null
                            then timestamp('2999-12-31 23:59:59 UTC')
                        else sub.expiration_dt_ut
                    end as expiration_dt_ut
                    , case when sub.src_system_id = 134 then date_add(sub.activate_dt_ut, interval -150 SECOND) else date_add(sub.activate_dt_ut, interval -120 SECOND) end as pre_activate
                    , case when sub.src_system_id = 134 then date_add(sub.activate_dt_ut, interval 150 SECOND) else date_add(sub.activate_dt_ut, interval 120 SECOND) end as post_activate
                    , date_add(sub.trial_end_dt_ut, interval -120 SECOND) as pre_trial_end
                    , date_add(sub.trial_end_dt_ut, interval 120 SECOND) as post_trial_end
                    , row_number() over (partition by sub.src_system_id, sub.account_cd order by sub.creation_dt_ut, sub.activate_dt_ut, ifnull(sub.expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC'))) as frn
                    , row_number() over (partition by sub.src_system_id, sub.account_cd order by sub.creation_dt_ut desc, sub.activate_dt_ut desc, ifnull(sub.expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC')) desc) as lrn
                    , lag(sub.plan_nm) over (partition by sub.src_system_id, sub.account_cd order by creation_dt_ut, activate_dt_ut, ifnull(sub.expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC'))) as prior_plan
                    , coalesce(lag(sub.activate_dt_ut) over (partition by sub.src_system_id, sub.account_cd order by creation_dt_ut, activate_dt_ut, ifnull(sub.expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC'))), timestamp('1900-01-01 00:00:01 UTC')) as prior_activation
                    , lead(sub.plan_nm) over (partition by sub.src_system_id, sub.account_cd order by creation_dt_ut, activate_dt_ut, ifnull(sub.expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC'))) as next_plan
                    , coalesce(lead(sub.activate_dt_ut) over (partition by sub.src_system_id, sub.account_cd order by creation_dt_ut, activate_dt_ut, ifnull(sub.expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC'))), timestamp('2999-12-31 23:59:59 UTC')) as next_activation
                from i-dss-streaming-data.payment_ops_vw.recurly_subscription_dim sub
                where 1=1
                    -- and src_system_id = 115
            ) sub
        where 1=1
    )
, txn as
    (
        select
            txn.src_system_id
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
            , txn.account_cd
            , txn.subscription_guid
            , invoice_guid
            , transaction_guid
            , trans_dt
            , trans_dt_ut
            , origin_desc
            , trans_type_desc
            , trans_status_desc
            , trans_amt
            , tax_amt
            , txn.currency_cd
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
        left join sub
            on sub.src_system_id = txn.src_system_id
            and sub.account_cd = txn.account_cd
            and sub.subscription_guid = txn.subscription_guid
        where 1=1
            -- and txn.src_system_id = 115
            and txn.trans_dt >= date_add(current_date(), interval -4 month)
            and txn.trans_dt <= current_date()
            and txn.trans_type_desc in ('purchase', 'verify')
            and txn.trans_status_desc in ('success', 'void', 'declined')
            -- and txn.origin_desc in ('api', 'token_api')
            -- and txn.cc_first_6_nbr in ('601100', '601101', '414720')
            and txn.payment_method_desc = 'Credit Card'
            and 
                (
                    sub.subscription_guid is not null
                    or
                    txn.subscription_guid is null
                )
    )

, run_dates as
    (
        select run_dt
        from unnest(generate_date_array(date_add(current_date(), interval -4 month), current_date())) as run_dt
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
            , count(distinct account_cd) as daily_ct
            , count(distinct case when trans_status_desc in ('success', 'void') then account_cd else null end) as daily_success_ct
            , count(distinct case when trans_status_desc in ('success', 'void') and avs_result_cd not in ('Y','X','V') then account_cd else null end) as daily_success_avs_fail_ct
            , count(distinct case when trans_status_desc = 'declined' then account_cd else null end) as daily_decline_ct
        from txn
        where 1=1
            and trans_dt >= date_sub(date_add(current_date(), interval -4 month), interval 3 month)
            and trans_dt <= current_date()
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
            , cast(avg(dva.daily_decline_ct) as int64) as baseline_decline_avg_ct
            , cast(stddev_samp(dva.daily_decline_ct) as int64) as baseline_decline_stddev_ct
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
            , daily_decline_ct
        from daily_volume_all
        where 1=1
            and trans_dt between date_add(current_date(), interval -4 month) and current_date()
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
            , dv.daily_success_ct as daily_success_ct
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
            , bl.baseline_decline_avg_ct
            , bl.baseline_decline_stddev_ct
            , dv.daily_decline_ct
            , cast((dv.daily_decline_ct - bl.baseline_decline_avg_ct) as integer) as decline_diff
            , round((dv.daily_decline_ct - bl.baseline_decline_avg_ct) / nullif(bl.baseline_decline_stddev_ct, 0), 2) as decline_z_score
            , case
                when (dv.daily_decline_ct - bl.baseline_decline_avg_ct) / nullif(bl.baseline_decline_stddev_ct, 0) >= <Parameters.Z-Score> then 'large_increase'
                when (dv.daily_decline_ct - bl.baseline_decline_avg_ct) / nullif(bl.baseline_decline_stddev_ct, 0) <= -<Parameters.Z-Score> then 'large_decrease'
                else null
            end as decline_chg_flag
        from daily_volume dv
        join dow_baseline bl
            on dv.src_system_id = bl.src_system_id
            and dv.trans_dt = bl.run_dt
            and dv.gateway_country = bl.gateway_country
            and dv.cc_first_6_nbr = bl.cc_first_6_nbr
            and bl.baseline_avg_ct >= 300
        where 1=1
            
    )

, stacked as
    (
        select
            src_system_id
            , gateway_country
            , trans_dt
            , cc_first_6_nbr
            , 'total_volume' as flag_type
            , baseline_avg_ct
            , baseline_stddev_ct
            , daily_ct
            , vol_diff
            , vol_z_score
            , chg_flag as chg_flag
        from chg_chk
        where 1=1
            -- and baseline_avg_ct >= 500
            and chg_flag is not null

        union all

        select
            src_system_id
            , gateway_country
            , trans_dt
            , cc_first_6_nbr
            , 'decline' as flag_type
            , baseline_decline_avg_ct as baseline_avg_ct
            , baseline_decline_stddev_ct as baseline_stddev_ct
            , daily_decline_ct as daily_ct
            , decline_diff as vol_diff
            , decline_z_score as vol_z_score
            , decline_chg_flag as chg_flag
        from chg_chk
        where 1=1
            -- and baseline_decline_avg_ct >= 500
            and decline_chg_flag is not null

        union all

        select
            src_system_id
            , gateway_country
            , trans_dt
            , cc_first_6_nbr
            , 'success' as flag_type
            , baseline_success_avg_ct as baseline_avg_ct
            , baseline_success_stddev_ct as baseline_stddev_ct
            , daily_success_ct as daily_ct
            , success_diff as vol_diff
            , success_z_score as vol_z_score
            , success_chg_flag as chg_flag
        from chg_chk
        where 1=1
            -- and baseline_success_avg_ct >= 500
            and success_chg_flag is not null

        union all

        select
            src_system_id
            , gateway_country
            , trans_dt
            , cc_first_6_nbr
            , 'success_avs_fail' as flag_type
            , baseline_success_avs_fail_avg_ct as baseline_avg_ct
            , baseline_success_avs_fail_stddev_ct as baseline_stddev_ct
            , daily_success_avs_fail_ct as daily_ct
            , success_avs_fail_diff as vol_diff
            , success_avs_fail_z_score as vol_z_score
            , success_avs_fail_chg_flag as chg_flag
        from chg_chk
        where 1=1
            -- and baseline_success_avs_fail_avg_ct >= 500
            and success_avs_fail_chg_flag is not null
    )

select
    *
    , current_date as tableau_end_dt
    , date_add(current_date, interval -4 month) as tableau_start_dt
from stacked
where 1=1
order by 1, 2, 3 desc, vol_z_score desc