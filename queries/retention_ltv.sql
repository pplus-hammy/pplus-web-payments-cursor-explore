declare dt_filter date;
declare src_id array<int64> default [115];
set dt_filter = date('2024-01-01');

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
                        and 
                            (
                                (
                                    date(sub.curr_period_start_dt_ut) > date('2025-11-06')
                                    and date_diff(sub.expiration_dt_ut, sub.curr_period_start_dt_ut, DAY) = 27
                                )
                                or
                                (
                                    date(sub.curr_period_start_dt_ut) <= date('2025-11-06')
                                    and date_diff(sub.expiration_dt_ut, sub.curr_period_start_dt_ut, DAY) = 28
                                )
                            )
                    then 'involuntary_fail_dunning'
                when sub.cancel_dt_ut between sub.curr_period_start_dt_ut and sub.curr_period_end_dt_ut 
                        and sub.cancel_dt_ut != sub.expiration_dt_ut 
                        and 
                            (
                                (
                                    date(sub.curr_period_start_dt_ut) > date('2025-11-06')
                                    and date_diff(sub.expiration_dt_ut, sub.curr_period_start_dt_ut, DAY) = 27
                                )
                                or
                                (
                                    date(sub.curr_period_start_dt_ut) <= date('2025-11-06')
                                    and date_diff(sub.expiration_dt_ut, sub.curr_period_start_dt_ut, DAY) = 28
                                )
                            )
                    then 'voluntary_cancel_in_dunning_fail'
                when sub.cancel_dt_ut between sub.curr_period_start_dt_ut and sub.curr_period_end_dt_ut 
                        and sub.cancel_dt_ut != sub.expiration_dt_ut 
                        and sub.curr_period_end_dt_ut = sub.expiration_dt_ut
                    then 'voluntary_cancel_expire_end_of_cycle'
                when sub.cancel_dt_ut = sub.expiration_dt_ut 
                        and sub.curr_period_start_dt_ut = sub.cancel_dt_ut
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
                    , case
                        when sub.src_system_id = 134 then date_add(sub.activate_dt_ut, interval -150 SECOND) 
                        else date_add(sub.activate_dt_ut, interval -120 SECOND) 
                    end as pre_activate
                    , case
                        when sub.src_system_id = 134 then date_add(sub.activate_dt_ut, interval 150 SECOND) 
                        else date_add(sub.activate_dt_ut, interval 120 SECOND) 
                    end as post_activate
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
                    and sub.src_system_id in unnest(src_id)
            ) sub
        where 1=1
    )

, txn as
    (
        select distinct
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
            , nullif(trim(txn.subscription_guid),'') as subscription_guid
            , txn.invoice_guid
            , txn.trans_dt
            , txn.trans_dt_ut
            , datetime(txn.trans_dt_ut, 'America/New_York') as trans_dt_et
            , txn.transaction_guid
            , txn.origin_desc
            , txn.trans_type_desc
            , txn.trans_status_desc
            , txn.trans_amt 
            , txn.tax_amt 
            , txn.currency_cd 
            , txn.country_cd
            , txn.trans_gateway_type_desc
            , case  
                when txn.trans_gateway_type_desc = 'cybersource' and txn.reference_cd like '%;%;%' then split(txn.reference_cd,';')[offset(1)]
                else txn.reference_cd
            end as reference_cd
            , txn.cc_type_desc
            , txn.cc_first_6_nbr
            , txn.approval_desc
            , txn.trans_msg_desc
            , txn.failure_type
            , txn.gateway_error_cd
            , case
                when ifnull(trim(txn.approval_desc), '') = 'Collected by Vindicia' then 'vindicia'
                else 'recurly'
            end as txn_origin
            , dense_rank() over (partition by txn.src_system_id, txn.account_cd, txn.subscription_guid, coalesce(nullif(trim(txn.invoice_guid),''), string(txn.trans_dt)) order by txn.trans_dt_ut) as earliest_txn
            , dense_rank() over (partition by txn.src_system_id, txn.account_cd, txn.subscription_guid, coalesce(nullif(trim(txn.invoice_guid),''), string(txn.trans_dt)) order by txn.trans_dt_ut desc) as latest_txn
        from i-dss-streaming-data.payment_ops_vw.recurly_transaction_fct txn
        where 1=1
            and txn.src_system_id in unnest(src_id)
            and txn.trans_dt >= dt_filter
            and txn.trans_type_desc in ('purchase','verify')
            and txn.trans_status_desc in ('success', 'void', 'declined')
    )

, adj as
    (
        select
            adj.src_system_id
            , adj.account_cd
            , adj.subscription_guid
            , adj.invoice_guid
            , nullif(trim(adj.invoice_type_cd),'') as invoice_type_cd
            , adj.invoice_state_desc
            , sum(adj.adj_amt) as adj_amt
            , sum(adj.adj_total_amt) as adj_total_amt
        from i-dss-streaming-data.payment_ops_vw.recurly_adjustments_fct adj
        where 1=1
            and adj.creation_dt >= date_add(dt_filter, interval -1 MONTH)
            and adj.src_system_id in unnest(src_id)
        group by
            adj.src_system_id
            , adj.account_cd
            , adj.subscription_guid
            , adj.invoice_guid
            , nullif(trim(adj.invoice_type_cd),'')
            , adj.invoice_state_desc
    )

, paid_sub as
    (
        select
            sub.src_system_id
            , sub.account_cd
            , sub.subscription_guid
            , sum(case when adj.invoice_state_desc = 'paid' then adj.adj_amt else 0 end) as sub_paid_amt
            , sum(case when adj.invoice_state_desc = 'paid' then adj.adj_total_amt else 0 end) as sub_paid_total_amt
        from sub
        join adj
            on adj.src_system_id = sub.src_system_id
            and adj.account_cd = sub.account_cd
            and adj.subscription_guid = sub.subscription_guid
        group by
            sub.src_system_id
            , sub.account_cd
            , sub.subscription_guid
    )

, cohort_sub as
    (
        select
            sub.src_system_id
            , date_trunc(date(sub.activate_dt_et), MONTH) as cohort_dt
            , date_trunc(date(datetime(sub.original_activation, 'America/New_York')), MONTH) as cohort_dt_account
            , sub.subscription_guid
            , sub.account_cd
            , date_diff(date(case when date(sub.expiration_dt_ut) >= date('2999-01-01') then current_timestamp() else sub.expiration_dt_ut end), date(sub.activate_dt_ut), MONTH) as mths_active
            , paid_sub.sub_paid_total_amt
            , paid_sub.sub_paid_amt
            , sub.activate_dt_et
            , sub.plan_dur
            , sub.status_desc
            , sub.frn
        from sub
        left join paid_sub
            on paid_sub.src_system_id = sub.src_system_id
            and paid_sub.account_cd = sub.account_cd
            and paid_sub.subscription_guid = sub.subscription_guid
        where 1=1
    )

select
    src_system_id
    , cohort_dt
    , cohort_dt_account
    , subscription_guid
    , account_cd
    , mths_active
    , sub_paid_total_amt
    , sub_paid_amt
    , activate_dt_et
    , plan_dur
    , status_desc
    , frn
from cohort_sub
where 1=1
    -- and date(activate_dt_et) between date('2024-01-01') and date('2024-12-31')
    -- and sub_paid_amt > 0
    -- and plan_dur = 'monthly'

-- Optional: pre-aggregated retention curve (cohort_dt, month_number, retained_count, retained_pct)
-- select
--     cohort_dt
--     , month_number
--     , retained_count
--     , safe_divide(retained_count, cohort_size) as retained_pct
-- from (
--     select
--         cohort_dt
--         , month_number
--         , count(*) as retained_count
--         , max(count(*)) over (partition by cohort_dt) as cohort_size
--     from cohort_sub
--     cross join unnest(generate_array(0, (select max(mths_active) from cohort_sub))) as month_number
--     where 1=1
--         and mths_active >= month_number
--     group by cohort_dt, month_number
-- )

-- Optional: pre-aggregated cohort LTV (cohort_dt, cohort_size, total_rev, ltv, avg_tenure)
-- select
--     cohort_dt
--     , count(distinct subscription_guid) as cohort_size
--     , sum(sub_paid_total_amt) as total_rev
--     , safe_divide(sum(sub_paid_total_amt), count(distinct subscription_guid)) as ltv
--     , safe_divide(sum(mths_active), count(distinct subscription_guid)) as avg_tenure
-- from cohort_sub
-- where 1=1
-- group by cohort_dt
