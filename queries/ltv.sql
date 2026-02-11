declare dt_filter date;
declare src_id array<int64> default [115];
set dt_filter = date('2024-01-01');

with sub as
    (
        select
            -- sst.overall_sub_start_type_desc
            -- , 
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
                        when sub.expiration_dt_ut is null and lower(sub.plan_cd) like '%annual%' and date(sub.curr_period_end_dt_ut) <= date_add(current_date, interval -2 MONTH) -- should it be longer?
                            then sub.curr_period_end_dt_ut
                        when sub.expiration_dt_ut is null
                            then timestamp('2999-12-31 23:59:59 UTC')
                        else sub.expiration_dt_ut
                    end as expiration_dt_ut
                    -- , ifnull(sub.expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC')) as expiration_dt_ut
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
                    -- and src_system_id = 134
                    -- and src_system_id = 115
            ) sub
        -- left join i-dss-streaming-data.strata_vw.subscription_fct sst
        --     on sst.subscription_guid = sub.subscription_guid
        --     and sst.src_system_id = sub.src_system_id 
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
            -- , txn.gateway_cd
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
            -- , txn.reference_cd
            , case  
                when txn.trans_gateway_type_desc = 'cybersource' and txn.reference_cd like '%;%;%' then split(txn.reference_cd,';')[1]
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
            -- and txn.trans_dt >= date('2025-04-01')
            and txn.trans_dt >= dt_filter
            and txn.trans_type_desc in ('purchase','verify')
            and txn.trans_status_desc in ('success', 'void', 'declined')
    )

, dunt as
    (
        select
            src_system_id
            , account_cd
            , subscription_guid
            , invoice_guid
            , max(case when earliest_txn = 1 and trans_type_desc = 'purchase' and trans_status_desc = 'declined' then 1 else 0 end) as dun_chk
            , max(case when txn_origin = 'vindicia' then 1 else 0 end) as vindicia_chk
            -- , max(case when latest_txn = 1 and trans_status_desc = 'success' and origin_desc in ('api','token_api') then 1 else 0 end) as dun_cust_paid_chk
            -- , max(case when latest_txn = 1 and trans_status_desc = 'success' and txn_origin = 'vindicia' then 1 else 0 end) as dun_vindicia_paid_chk
            -- , max(case when latest_txn = 1 and trans_status_desc = 'success' and origin_desc = 'recurring' and txn_origin = 'recurly' then 1 else 0 end) as dun_recurly_paid_chk
            -- , max(case when latest_txn = 1 and trans_status_desc = 'success' then 1 else 0 end) as dun_paid_chk
        from txn
        where 1=1
            and subscription_guid is not null
            and invoice_guid is not null
        group by all
        -- having dun_chk = 1
    )

, dunp as 
    (
        select
            dunt.*
            , case
                when trans_status_desc = 'success' and txn_origin = 'vindicia' then 'vindicia_paid'
                when trans_status_desc = 'success' and origin_desc in ('api','token_api') then 'cust_paid'
                when trans_status_desc = 'success' and origin_desc = 'recurring' and txn_origin = 'recurly' then 'recurly_paid'
                when trans_status_desc = 'success' then 'paid'
                else null
            end as dun_paid_type
        from dunt
        join txn
            on txn.src_system_id = dunt.src_system_id
            and txn.account_cd = dunt.account_cd
            and txn.subscription_guid = dunt.subscription_guid
            and txn.invoice_guid = dunt.invoice_guid
        where 1=1
            and dunt.dun_chk = 1
            and txn.latest_txn = 1

    )

, adj as
    (
        select  
            adj.*
            , case
                -- prior amount is negative (gift card redemption)
                when lag(adj.adj_total_amt) over (partition by adj.src_system_id, adj.account_cd order by adj.invoice_billed_dt_ut, adj.invoice_nbr) < 0
                        -- current invoice amount + gift card redemption amount is still < 0 (gift card covered full invoice amount)
                        and adj.adj_total_amt + lag(adj.adj_total_amt) over (partition by adj.src_system_id, adj.account_cd order by adj.invoice_billed_dt_ut, adj.invoice_nbr) < 0
                    then 'paid_by_gift_card'
                else 'null'
            end as paid_by_gc_chk
            , case
                when invoice_type_cd = 'renewal' and dunt.dun_chk = 1 then 'entered_dunning'
                else null
            end as dun_chk
            , dunp.dun_paid_type
        from 
            (
                select distinct
                    adj.src_system_id
                    , adj.account_cd
                    , adj.subscription_guid
                    , adj.invoice_guid
                    , adj.invoice_nbr
                    , adj.invoice_billed_dt_ut
                    , adj.start_dt_ut
                    , adj.end_dt_ut
                    , nullif(trim(adj.invoice_type_cd),'') as invoice_type_cd
                    , adj.invoice_state_desc
                    , adj.invoice_closed_dt_ut
                    , adj.coupon_cd
                    -- , cp.amount as credit_applied_amt
                    , sum(adj.adj_amt) over (partition by adj.src_system_id, adj.account_cd, adj.invoice_guid) as adj_amt
                    , sum(adj.adj_total_amt) over (partition by adj.src_system_id, adj.account_cd, adj.invoice_guid) as adj_total_amt
                    , max(case when invoice_type_cd = 'gift_card' and type_desc = 'credit' and lower(adj_desc) like '%gift%card%' then 1 else 0 end) over (partition by adj.src_system_id, adj.account_cd, adj.invoice_guid) as gift_card_chk
                    , dense_rank() over (partition by adj.src_system_id, adj.account_cd, adj.subscription_guid order by adj.invoice_billed_dt_ut, adj.invoice_nbr) as earliest_inv
                from i-dss-streaming-data.payment_ops_vw.recurly_adjustments_fct adj
                -- left join i-dss-streaming-data.payment_ops_vw.credit_payments cp
                --     on cp.src_system_id = adj.src_system_id
                --     and cp.account_cd = adj.account_cd
                --     and cp.applied_to_invoice_nbr = adj.invoice_nbr
                where 1=1
                    -- and adj.creation_dt >= date('2025-03-01')
                    and adj.creation_dt >= date_add(dt_filter, interval -1 MONTH)
                    and adj.src_system_id in unnest(src_id)
                    -- and adj.invoice_type_cd = 'renewal'
            ) adj
        left join dunt
            on dunt.src_system_id = adj.src_system_id
            and dunt.account_cd = adj.account_cd
            and dunt.subscription_guid = adj.subscription_guid
            and dunt.invoice_guid = adj.invoice_guid
        left join dunp
            on dunp.src_system_id = adj.src_system_id
            and dunp.account_cd = adj.account_cd
            and dunp.subscription_guid = adj.subscription_guid
            and dunp.invoice_guid = adj.invoice_guid
        where 1=1
    )

, paid_sub as
    (
        select
            sub.src_system_id
            , sub.account_cd
            , sub.subscription_guid
            , sum(case when invoice_state_desc = 'paid' then adj_amt else 0 end) as sub_paid_amt
            , sum(case when invoice_state_desc = 'paid' then adj_total_amt else 0 end) as sub_paid_total_amt
        from sub
        join adj
            on adj.src_system_id = sub.src_system_id
            and adj.account_cd = sub.account_cd
            and adj.subscription_guid = sub.subscription_guid
        group by all
    )

-- , txn_inv_sub as
--     (
--         select
--             sub.*
--             , paid_sub.sub_paid_amt
--             , paid_sub.sub_paid_total_amt
--             , date_diff(date(case when date(sub.expiration_dt_ut) >= date('2999-01-01') then current_timestamp else sub.expiration_dt_ut end), date(sub.activate_dt_ut), MONTH) as mths_active
--             , adj.* except (src_system_id, account_cd, subscription_guid, invoice_guid)
--             , txn.* except (src_system_id, account_cd, subscription_guid, invoice_guid)
--         from sub
--         left join adj
--             on adj.src_system_id = sub.src_system_id
--             and adj.account_cd = sub.account_cd
--             and adj.subscription_guid = sub.subscription_guid
--         left join txn
--             on txn.src_system_id = sub.src_system_id
--             and txn.account_cd = sub.account_cd
--             and txn.subscription_guid = sub.subscription_guid
--         left join paid_sub
--             on paid_sub.src_system_id = sub.src_system_id
--             and paid_sub.account_cd = sub.account_cd
--             and paid_sub.subscription_guid = sub.subscription_guid
--         where 1=1
--     )
, sub_info as
    (
        select distinct
            sub.*
            , paid_sub.sub_paid_amt
            , paid_sub.sub_paid_total_amt
            , date_diff(date(case when date(sub.expiration_dt_ut) >= date('2999-01-01') then current_timestamp else sub.expiration_dt_ut end), date(sub.activate_dt_ut), MONTH) as mths_active
            -- , adj.* except (src_system_id, account_cd, subscription_guid, invoice_guid)
        from sub
        -- left join adj
        --     on adj.src_system_id = sub.src_system_id
        --     and adj.account_cd = sub.account_cd
        --     and adj.subscription_guid = sub.subscription_guid
        left join paid_sub
            on paid_sub.src_system_id = sub.src_system_id
            and paid_sub.account_cd = sub.account_cd
            and paid_sub.subscription_guid = sub.subscription_guid
    )

-- select
--     src_system_id
--     -- , date(activate_dt_et) as activate_dt_et
--     , plan_dur
--     , case 
--         when sub_paid_amt > 0 then 'paid'
--         when sub_paid_amt = 0 then 'not_paid'
--         else null
--     end as sub_paid_chk
--     , mths_active
--     , count(distinct case when dun_chk = 'entered_dunning' then invoice_nbr else null end) as enter_dunning_ct
--     , count(distinct case when dun_paid_type = 'cust_paid' then invoice_nbr else null end) as cust_paid_ct
--     , count(distinct case when dun_paid_type = 'vindicia_paid' then invoice_nbr else null end) as vindicia_paid_ct
--     , count(distinct case when dun_paid_type = 'recurly_paid' then invoice_nbr else null end) as recurly_paid_ct
--     , count(distinct case when dun_paid_type = 'paid' then invoice_nbr else null end) as tot_paid_ct

--     , count(distinct subscription_guid) as sub_ct
--     , min(account_cd) as min_account_cd
--     , max(account_cd) as max_account_cd
-- from txn_inv_sub
-- where 1=1
--     and date(activate_dt_et) between date('2025-01-01') and date('2025-03-31')
-- group by all
-- order by 1,2,3,4


select
    src_system_id
    -- , plan_dur
    -- , case 
    --     when sub_paid_amt > 0 then 'paid'
    --     when sub_paid_amt = 0 then 'not_paid'
    --     else null
    -- end as sub_paid_chk
    , count(distinct subscription_guid) as sub_ct
    , sum(sub_paid_total_amt) as tot_rev
    , sum(mths_active) as tot_mths_active
    , safe_divide(sum(sub_paid_total_amt), count(distinct subscription_guid)) as arpu
    -- , safe_divide(safe_divide(sum(sub_paid_total_amt), count(distinct subscription_guid)), sum(mths_active)) as arpu -- monthly
    , safe_divide(sum(mths_active), count(distinct subscription_guid)) as avg_tenure
    , safe_divide(sum(sub_paid_total_amt), count(distinct subscription_guid)) * safe_divide(sum(mths_active), count(distinct subscription_guid)) as ltv

from sub_info
where 1=1
    -- and date(activate_dt_et) between date('2025-01-01') and date('2025-03-31')
    and date(activate_dt_et) between date('2024-01-01') and date('2024-12-31')
    and sub_paid_amt > 0
    and plan_dur = 'monthly'
group by all