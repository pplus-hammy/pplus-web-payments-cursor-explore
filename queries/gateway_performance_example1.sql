declare dt_filter date;
declare src_id array<int64> default [115, 134];
set dt_filter = date('2025-12-01');
-- set src_id = [115,134];

/*
Assigns all transactions to an invoice if there is an eventual successful activation otherwise generates an id using account_cd
*/

with sub as 
    (
        select
            sst.overall_sub_start_type_desc
            , 
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
        left join i-dss-streaming-data.strata_vw.subscription_fct sst
            on sst.subscription_guid = sub.subscription_guid
            and sst.src_system_id = sub.src_system_id 
        where 1=1
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
            , nullif(trim(txn.subscription_guid),'') as txn_subscription_guid
            , txn.invoice_guid
            , txn.trans_dt
            , txn.trans_dt_ut
            , datetime(txn.trans_dt_ut, 'America/New_York') as trans_dt_et
            , txn.transaction_guid
            , txn.origin_desc
            , txn.trans_type_desc
            , txn.trans_status_desc
            , case
                when txn.trans_status_desc in ('success','void') then 'success'
                when txn.trans_status_desc = 'declined' then txn.trans_status_desc
                else txn.trans_status_desc
            end as trans_status_desc_2
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
            -- , case
            --     when (txn.gateway_error_cd = '481' or (txn.gateway_error_cd = 'Refused' and txn.trans_msg_desc = 'FRAUD') then 'gateway_rule_decline'
            --     else null
            -- end as rule_decline_chk
            , case
                when txn.trans_type_desc != 'verify' and nullif(trim(txn.subscription_guid),'') is not null
                    then 'purch_on_sub'
                when txn.trans_type_desc = 'verify' 
                        or (txn.trans_type_desc != 'verify' and nullif(trim(txn.subscription_guid),'') is null)
                    then 'verify_or_purch_pre_sub'
            end as txn_type
        from i-dss-streaming-data.payment_ops_vw.recurly_transaction_fct txn
        where 1=1
            and txn.src_system_id in unnest(src_id)
            -- and txn.trans_dt >= date('2025-04-01')
            and txn.trans_dt >= dt_filter
            and txn.trans_type_desc in ('purchase','verify')
            and txn.trans_status_desc in ('success', 'void', 'declined')

        -- -- Adyen/Cybersource test
        --     and trans_gateway_type_desc in ('adyen','cybersource')
        --     and ifnull(trim(txn.failure_type),'') not in ('fraud_velocity', 'gateway_timeout') -- recurly velocity limit that prevents the txn from going to gateway
        --     and 
        --         (
        --             nullif(trim(reference_cd),'') is not null -- reference cd has the gateway transaction id (psp for adyen), if it's empty the transaction probably did not go to the gateway
        --             or
        --             ifnull(trim(txn.approval_desc), '') = 'Collected by Vindicia' -- or, if ref_cd is blank but it's collected by vindicia, include
        --         )
    )

, txn_to_inv as
    (
        select distinct
            txn.* except (invoice_guid)
            -- add earliest & most recent decline info
            -- , txn.trans_msg_desc
            -- , txn.failure_type
            -- , txn.gateway_error_cd
            , nullif(trim(txn.invoice_guid),'') as invoice_guid
            -- , adj.invoice_nbr
            , case
                -- assign any decline purchase attempts after prior expiration and before current activation to the first invoice on the subscription
                -- all attempts get lumped together at order level, even if spread over multiple days/weeks
                -- ex: 2025-11-26 https://cbsi-entertainment.recurly.com/accounts/11790022 - 4 purchase attempts & then a trial
                when nullif(trim(txn.invoice_guid),'') is null 
                        and sub.subscription_guid is null 
                        and presub.subscription_guid is not null -- attempts after prior expiration and before current activation
                        and txn.trans_type_desc = 'purchase'
                    then coalesce(
                                    cast(max(case when adj.earliest_inv = 1 then adj.invoice_nbr else null end) over (partition by txn.src_system_id, txn.account_cd, presub.subscription_guid) as int64) 
                                    , cast(min(trialinv.invoice_nbr) over (partition by txn.src_system_id, txn.account_cd, presub.subscription_guid) as int64)
                                )   
                    
                -- assign any decline verify attempts to the first $0 invoice on a trial subscription 
                -- all attempts get lumped together at order level, even if spread over multiple days/weeks
                -- otherwise use acct cd to create id for verify attempts with no subsequent trial activation (fail verify then go dtp)
                -- ex: '2025-11-18' https://cbsi-entertainment.recurly.com/transactions/7c061ea618e8714fd5de5448dab4a99f
                -- ex: 2025-11-27, 8 failed trial verify attempts then DTP success via paypal https://cbsi-entertainment.recurly.com/transactions?end_date=2026-01-09&order=descending&page=1&q=322892924040&sort=collected_at&start_date=2025-10-09 
                when nullif(trim(txn.invoice_guid),'') is null 
                        and sub.subscription_guid is null 
                        and presub.subscription_guid is not null 
                        and txn.trans_type_desc = 'verify'
                    -- then cast(((cast(txn.account_cd as int64) * -100000) || presub.frn) as int64) 
                    then coalesce(
                                    cast(min(trialinv.invoice_nbr) over (partition by txn.src_system_id, txn.account_cd, presub.subscription_guid) as int64)
                                    , cast(-999 || (cast(txn.account_cd as int64) * 10000) || dense_rank() over (partition by txn.src_system_id, txn.account_cd order by 1=1) as bignumeric)
                                ) 
               
                -- new card attempted, verify txn
                -- assign to next invoice (if available, otherwise create id)
                when nullif(trim(txn.invoice_guid),'') is null 
                        and cursub.subscription_guid is not null
                        and txn.trans_type_desc = 'verify'
                    then coalesce(cast(cardchg.invoice_nbr as int64), cast(-888 || (cast(txn.account_cd as int64) * 10000) || dense_rank() over (partition by txn.src_system_id, txn.account_cd order by 1=1) as bignumeric))

                -- no successful activation, generate invoice number
                -- ex: 2025-12-24 https://cbsi-entertainment.recurly.com/transactions/7cc2fd3b603e2d27f7eeb248c6ac70fc restart attempt declined and no activation (as of early jan 2026)
                when nullif(trim(txn.invoice_guid),'') is null 
                        and coalesce(sub.subscription_guid, presub.subscription_guid) is null 
                    then cast(-777 || (cast(txn.account_cd as int64) * 10000) || dense_rank() over (partition by txn.src_system_id, txn.account_cd order by 1=1) as bignumeric)

                else adj.invoice_nbr
            end as invoice_nbr
            , coalesce(sub.subscription_guid, presub.subscription_guid) as sub_guid
            , presub.prior_expiration
            , presub.post_activate
        from txn
        left join adj
            on adj.src_system_id = txn.src_system_id
            and adj.account_cd = txn.account_cd
            and adj.invoice_guid = txn.invoice_guid
        left join sub
            on sub.src_system_id = txn.src_system_id
            and sub.account_cd = txn.account_cd
            and sub.subscription_guid = txn.txn_subscription_guid
        left join sub presub -- helps get declined dtp activation attempts, must have had a success at some point to have an account_cd in the sub table
            on presub.src_system_id = txn.src_system_id
            and presub.account_cd = txn.account_cd
            and txn.trans_dt_ut between coalesce(presub.prior_expiration, timestamp('1900-01-01 00:00:01 UTC')) and coalesce(presub.post_activate, current_timestamp)
        left join adj trialinv -- connect txn to the first invoice in trial ($0 due to trial or promo code/coupon)
            on trialinv.src_system_id = txn.src_system_id
            and trialinv.account_cd = txn.account_cd
            and trialinv.subscription_guid = presub.subscription_guid
            and trialinv.earliest_inv = 1
            -- and trialinv.adj_total_amt = 0
            and case
                    when trialinv.paid_by_gc_chk = 'paid_by_gift_card' then 0 -- workaround till credit payments export ingested ex: 2025-11-17 - https://cbsi-entertainment.recurly.com/accounts/11662568
                    else trialinv.adj_total_amt
                end = 0
            and nullif(trim(txn.invoice_guid),'') is null
        left join sub cursub -- assign verify txn to the current subscription
            on cursub.src_system_id = txn.src_system_id
            and cursub.account_cd = txn.account_cd
            and txn.trans_type_desc = 'verify'
            and txn.trans_dt_ut between cursub.activate_dt_ut and cursub.expiration_dt_ut 
        left join adj cardchg -- connect card change verify attempts to subsequent renewal invoice
            on cardchg.src_system_id = txn.src_system_id
            and cardchg.account_cd = txn.account_cd
            and txn.trans_type_desc = 'verify'
            -- and date_add(txn.trans_dt_ut, interval 1 MONTH) between cardchg.start_dt_ut and cardchg.end_dt_ut
            and case
                    when cursub.plan_dur = 'annual' then date_add(date(txn.trans_dt_ut), interval 1 YEAR)
                    else date_add(date(txn.trans_dt_ut), interval 1 MONTH)
                end between date(cardchg.start_dt_ut) and date(cardchg.end_dt_ut)
        where 1=1
    )

, inv_key_bill_dt as
    (
        select
            src_system_id
            , account_cd
            , invoice_nbr
            , max(trans_dt_ut) as invoice_billed_dt_ut
        from txn_to_inv txn
        where 1=1
            and invoice_nbr < 0
        group by all
    )

-- , txn_to_inv_add_inv_dt as
--     (
--         select
--             txn.* except()
--         from txn_to_inv txn
--         left join adj
--             on adj.src_system_id = txn.src_system_id
--             and adj.account_cd = txn.account_cd
--             and adj.invoice_guid = txn.invoice_guid
--     )

, txn_inv_sub as
    (
        select
            txn.*
            , sc.success_ind
            , sub.activate_dt_ut
            , date(coalesce(datetime(sub.activate_dt_ut, 'America/New_York'), datetime('2999-12-31 23:59:59', 'UTC'))) as activate_dt_et
            , adj.adj_amt
            -- , coalesce(adj.invoice_billed_dt_ut, txn.trans_dt_ut) as invoice_billed_dt_ut
            , coalesce(adj.invoice_billed_dt_ut, ikb.invoice_billed_dt_ut) as invoice_billed_dt_ut
            , case
                when txn.invoice_nbr < 0 and sc.success_ind = 1 then 'paid'
                when txn.invoice_nbr < 0 and sc.success_ind = 0 then 'not_paid'
                else adj.invoice_state_desc
            end as invoice_state_desc
            , adj.invoice_closed_dt_ut
            , adj.invoice_type_cd
            , adj.earliest_inv
        from txn_to_inv txn
        left join 
            (
                select
                    src_system_id
                    , account_cd
                    , sub_guid
                    , invoice_nbr
                    , max(case when trans_status_desc in ('success', 'void') then 1 else 0 end) as success_ind
                from txn_to_inv txn
                group by all
            ) sc
            on sc.src_system_id = txn.src_system_id
            and sc.account_cd = txn.account_cd
            -- and sc.sub_guid = txn.sub_guid
            and sc.invoice_nbr = txn.invoice_nbr
        left join adj
            on adj.src_system_id = txn.src_system_id
            and adj.account_cd = txn.account_cd
            and adj.invoice_nbr = txn.invoice_nbr
        left join sub
            on sub.src_system_id = txn.src_system_id
            and sub.account_cd = txn.account_cd
            and sub.subscription_guid = txn.sub_guid
        left join inv_key_bill_dt ikb
            on ikb.src_system_id = txn.src_system_id
            and ikb.account_cd = txn.account_cd
            and ikb.invoice_nbr = txn.invoice_nbr
        where 1=1
    )

, txn_classify as
    (
        select distinct
            txn.* except (activate_dt_ut)
            , case
                when txn.origin_desc in ('token_api', 'api') 
                        and txn.trans_type_desc = 'verify' 
                        and 
                            (
                                (
                                    txn.trans_status_desc = 'declined' 
                                    and
                                        (
                                            (
                                                txn.trans_dt_ut between sub.prior_activation and sub.post_activate
                                                and sub.prior_activation = timestamp('1900-01-01 00:00:01 UTC')
                                            )
                                            or
                                            eversub.account_cd is null
                                        )
                                )
                                or
                                (
                                    txn.trans_status_desc in ('success', 'void')
                                    and txn.trans_dt_ut between sub.prior_activation and sub.post_activate 
                                    and sub.prior_activation = timestamp('1900-01-01 00:00:01 UTC')
                                )
                            )
                    then 'auth_initial'

                when txn.origin_desc in ('token_api', 'api') 
                        and txn.trans_type_desc = 'verify' 
                        and txn.trans_status_desc in ('success', 'void', 'declined')
                        and
                            (
                                (
                                    txn.trans_dt_ut between sub.prior_expiration and sub.post_activate 
                                    and sub.prior_expiration > timestamp('1900-01-01 00:00:01 UTC')
                                )
                                or
                                (
                                    txn.trans_dt_ut between sub.expiration_dt_ut and date_add(sub.next_activation, interval 90 SECOND)
                                    -- txn.trans_dt_ut between sub.expiration_dt_ut and date_add(sub.next_activation, interval 10 MINUTE) 
                                    and sub.next_activation = timestamp('2999-12-31 23:59:59 UTC')
                                )
                            )
                    then 'auth_restart'

                when txn.origin_desc in ('token_api', 'api') 
                        and txn.trans_type_desc = 'verify' 
                    then 'auth_card-update'
                
                when txn.trans_type_desc = 'verify'
                    then 'auth_other'
/*
^^^ subscription based classification, auth/verify
*/
            -- single transactions for a direct to pay activation
                when txn.origin_desc in ('token_api', 'api') 
                        and txn.trans_type_desc = 'purchase' 
                        and
                            (
                                (
                                    txn.trans_status_desc in ('success', 'void', 'declined')
                                    and txn.trans_dt_ut between sub.prior_activation and sub.post_activate 
                                    and sub.prior_activation = timestamp('1900-01-01 00:00:01 UTC')
                                )
                                or
                                (
                                    txn.trans_status_desc = 'declined' 
                                    and eversub.account_cd is null
                                )
                            )
                        -- and ifnull(trim(txn.origin_desc),'') != 'force_collect'
                    then 'purch_initial'

            -- all transactions associated with invoice for direct to pay (weird process that activates a trial subscription, changes trial to a few minutes, then a renewal invoice generated and it's more like trial to paid)
            -- 5+ minute delay from activation to invoice create/transaction -- https://cbsi-entertainment.recurly.com/transactions/785e95d4afdfcedc647f7340acab2559
                -- when txn.origin_desc in ('token_api', 'api', 'recurring') 
                        -- and txn.trans_type_desc = 'purchase'
                    when txn.trans_type_desc = 'purchase'
                        and txn.trans_status_desc in ('success', 'void', 'declined')
                        and txn.invoice_type_cd = 'renewal'
                        and txn.invoice_billed_dt_ut between sk.activate_dt_ut and date_add(sk.post_activate, interval 8 MINUTE)
                        and sk.prior_activation = timestamp('1900-01-01 00:00:01 UTC') 
                        -- and txn.trans_dt_ut between sub.activate_dt_ut and date_add(sub.post_activate, interval 8 MINUTE)
                        -- and sub.prior_activation = timestamp('1900-01-01 00:00:01 UTC') 
                        -- and ifnull(trim(txn.origin_desc),'') != 'force_collect'
                    then 'purch_initial_trial_removed' 

                when txn.origin_desc in ('token_api', 'api') 
                        and txn.trans_type_desc = 'purchase' 
                        and 
                            (
                                (
                                    txn.trans_status_desc = 'declined' 
                                    and
                                        (
                                            txn.trans_dt_ut between sub.prior_expiration and sub.post_activate 
                                            and sub.prior_expiration > timestamp('1900-01-01 00:00:01 UTC') 
                                        )
                                        or
                                        (
                                            txn.trans_dt_ut between sub.expiration_dt_ut and date_add(sub.next_activation, interval 90 SECOND) 
                                            -- txn.trans_dt_ut between sub.expiration_dt_ut and date_add(sub.next_activation, interval 10 MINUTE) 
                                            and sub.next_activation = timestamp('2999-12-31 23:59:59 UTC')
                                        )
                                )
                                or
                                (
                                    txn.trans_status_desc in ('success', 'void')
                                    and
                                        (
                                            txn.trans_dt_ut between sub.prior_expiration and sub.post_activate 
                                            -- txn.trans_dt_ut between date_add(sub.prior_expiration, interval -30 SECOND) and sub.post_activate 
                                            and sub.prior_expiration > timestamp('1900-01-01 00:00:01 UTC')
                                        )
                                )
                            )
                        -- and ifnull(trim(txn.origin_desc),'') != 'force_collect'
                    then 'purch_restart'

            -- all transactions associated with invoice for direct to pay (weird process that activates a trial subscription, changes trial to a few minutes, then a renewal invoice generated and it's more like trial to paid)
             -- 5+ minute delay from activation to invoice create/transaction -- https://cbsi-entertainment.recurly.com/transactions/785e861e51eb6ff1f1f1c74f278c8a31
                -- when txn.origin_desc in ('token_api', 'api', 'recurring') 
                        -- and txn.trans_type_desc = 'purchase'
                    when txn.trans_type_desc = 'purchase'
                        and txn.trans_status_desc in ('success', 'void', 'declined')
                        and txn.invoice_type_cd = 'renewal'
                        and txn.invoice_billed_dt_ut between sk.activate_dt_ut and date_add(sk.post_activate, interval 8 MINUTE)
                        and sk.prior_expiration > timestamp('1900-01-01 00:00:01 UTC') 
                        -- and txn.trans_dt_ut between sub.activate_dt_ut and date_add(sub.post_activate, interval 8 MINUTE)
                        -- and sub.prior_expiration > timestamp('1900-01-01 00:00:01 UTC') 
                        and ifnull(trim(txn.origin_desc),'') != 'force_collect'
                    then 'purch_restart_trial_removed' 
/*
^^^ subscription based classification, direct to paid
*/
                when txn.trans_type_desc = 'purchase' 
                        and txn.invoice_type_cd = 'renewal'
                        and txn.invoice_billed_dt_ut between sk.pre_trial_end and sk.post_trial_end 
                        and txn.trans_status_desc in ('success', 'void', 'declined')
                        -- and ifnull(trim(txn.origin_desc),'') != 'force_collect'
                    then 'purch_trial-to-paid'  -- invoice within a few minutes of trial ending

                when txn.trans_type_desc = 'purchase' 
                        and txn.invoice_type_cd in ('renewal', 'immediate_change')
                        and txn.invoice_billed_dt_ut between sk.post_trial_end and date_add(sk.post_trial_end, interval 75 MINUTE)
                        and txn.trans_status_desc in ('success', 'void', 'declined')
                        -- and ifnull(trim(txn.origin_desc),'') != 'force_collect'
                    then 'purch_trial-upgrade-to-paid' -- upgrading from essential to premium while in trial causes trial to end & invoiced for premium amt (takes like an hour for the invoice to get generated)
/*
^^^ subscription based classification, trial to paid
*/
                when txn.invoice_type_cd = 'renewal'
                    then 'recurring' -- renewal invoice (any txn type associated with it will be recurring)

                -- when txn.origin_desc in ('token_api') and txn.trans_type_desc = 'purchase' and adj.invoice_type_cd = 'renewal'
                --         and txn.trans_status_desc in ('success', 'void', 'declined')
                --     then 'purch_dunning-cust'

                -- when txn.origin_desc in ('external_recovery') and txn.trans_type_desc = 'purchase' and adj.invoice_type_cd = 'renewal' 
                --         and txn.trans_status_desc in ('success', 'void', 'declined')
                --     then 'purch_dunning-vindicia'

                -- when txn.origin_desc = 'recurring' and txn.inv_to_txn_mins > 5
                --     then 'purch_dunning-automated'

                when txn.origin_desc = 'force_collect' and txn.trans_type_desc = 'purchase' 
                        and txn.trans_status_desc in ('success', 'void', 'declined')
                    then 'purch_auto-card-update'

                -- when txn.origin_desc in ('recurly_admin_sub_ch', 'api_sub_change')
                when 
                    (
                        txn.trans_type_desc = 'purchase'
                        and txn.invoice_type_cd = 'immediate_change'
                    )
                    or
                    (
                        txn.trans_type_desc = 'purchase'
                        and txn.origin_desc like '%sub%change%'
                    )
                    then 'purch_sub_change'

                else 'unclassified_' || txn.origin_desc || '_' || ifnull(txn.invoice_type_cd,'')
                -- else null
            end as txn_classify
            -- , ref.transaction_guid as refund_guid
            -- , ref.trans_dt_ut as refund_ts
            -- , ref.trans_amt as refund_amt
            -- , ref.tax_amt as refund_tax
            -- , ref.currency_cd as refund_currency
            -- , sub.* except (subscription_guid, activate_dt_ut, src_system, src_system_id, account_cd)

            , max(coalesce(txn.txn_subscription_guid, sub.subscription_guid, sk.subscription_guid)) over (partition by txn.src_system_id, txn.account_cd, txn.transaction_guid) as subscription_guid
            , max(coalesce(sub.plan_cd, sk.plan_cd)) over (partition by txn.src_system_id, txn.account_cd, txn.transaction_guid) as plan_cd
            , max(coalesce(sub.plan_nm, sk.plan_nm)) over (partition by txn.src_system_id, txn.account_cd, txn.transaction_guid) as plan_nm
            , max(coalesce(sub.plan_dur, sk.plan_dur)) over (partition by txn.src_system_id, txn.account_cd, txn.transaction_guid) as plan_dur
            , max(coalesce(sub.original_activation, sk.original_activation, timestamp('1900-01-01 00:00:01 UTC'))) over (partition by txn.src_system_id, txn.account_cd, txn.transaction_guid) as original_activation
            , max(coalesce(sub.prior_activation, sk.prior_activation, timestamp('1900-01-01 00:00:01 UTC'))) over (partition by txn.src_system_id, txn.account_cd, txn.transaction_guid) as prior_activation
            , max(coalesce(sub.creation_dt_ut, sk.creation_dt_ut)) over (partition by txn.src_system_id, txn.account_cd, txn.transaction_guid) as creation_dt_ut
            , max(coalesce(sub.activate_dt_ut, sk.activate_dt_ut)) over (partition by txn.src_system_id, txn.account_cd, txn.transaction_guid) as activate_dt_ut
            , max(coalesce(sub.trial_start_dt_ut, sk.trial_start_dt_ut)) over (partition by txn.src_system_id, txn.account_cd, txn.transaction_guid) as trial_start_dt_ut
            , max(coalesce(sub.trial_end_dt_ut, sk.trial_end_dt_ut)) over (partition by txn.src_system_id, txn.account_cd, txn.transaction_guid) as trial_end_dt_ut
            , max(coalesce(sub.expiration_dt_ut, sk.expiration_dt_ut)) over (partition by txn.src_system_id, txn.account_cd, txn.transaction_guid) as expiration_dt_ut
            , max(coalesce(sub.prior_expiration, sk.prior_expiration)) over (partition by txn.src_system_id, txn.account_cd, txn.transaction_guid) as prior_expiration
            , max(coalesce(sub.next_activation, sk.next_activation)) over (partition by txn.src_system_id, txn.account_cd, txn.transaction_guid) as next_activation
            , max(coalesce(sub.overall_sub_start_type_desc, sk.overall_sub_start_type_desc)) over (partition by txn.src_system_id, txn.account_cd, txn.transaction_guid) as overall_sub_start_type_desc

            -- , coalesce(txn.txn_subscription_guid, sub.subscription_guid, sk.subscription_guid) as subscription_guid
            -- , coalesce(sub.plan_cd, sk.plan_cd) as plan_cd
            -- , coalesce(sub.plan_nm, sk.plan_nm) as plan_nm
            -- , coalesce(sub.plan_dur, sk.plan_dur) as plan_dur
            -- , coalesce(sub.original_activation, sk.original_activation) as original_activation
            -- , coalesce(sub.creation_dt_ut, sk.creation_dt_ut) as creation_dt_ut
            -- , coalesce(sub.activate_dt_ut, sk.activate_dt_ut) as activate_dt_ut
            -- , coalesce(sub.trial_start_dt_ut, sk.trial_start_dt_ut) as trial_start_dt_ut
            -- , coalesce(sub.trial_end_dt_ut, sk.trial_end_dt_ut) as trial_end_dt_ut
            -- , coalesce(sub.expiration_dt_ut, sk.expiration_dt_ut) as expiration_dt_ut
            -- , coalesce(sub.prior_expiration, sk.prior_expiration) as prior_expiration
            -- , coalesce(sub.next_activation, sk.next_activation) as next_activation
            
        from txn_inv_sub txn
        left join sub eversub
            on eversub.src_system_id = txn.src_system_id
            and eversub.account_cd = txn.account_cd
        left join sub -- using subscription dates associated with the account rather than joining on subscription_guid to ensure declines before activation are categorized
            on sub.src_system_id = txn.src_system_id
            and sub.account_cd = txn.account_cd
            and
                (
                    ( --success near an activation where subsn guids match (purch) or no subsn guid (verify)
                        txn.trans_dt_ut between sub.pre_activate and sub.post_activate
                        and txn.trans_status_desc in ('success','void')
                        and txn.origin_desc in ('token_api', 'api')
                        and
                            (
                                (
                                    txn.trans_type_desc = 'purchase' 
                                    and txn.txn_subscription_guid = sub.subscription_guid
                                )
                                or
                                (
                                    txn.trans_type_desc = 'verify'
                                    and ifnull(trim(txn.txn_subscription_guid),'') = ''
                                )
                            )
                    )
                    or
                    ( -- declines 
                        ifnull(trim(txn.txn_subscription_guid),'') = ''
                        and txn.origin_desc in ('token_api', 'api')
                        and
                            (
                                (
                                    -- purchase declined before initial activation or verify, shouldn't be a subsn guid since no success 
                                    txn.trans_dt_ut between sub.prior_activation and sub.post_activate
                                    and sub.prior_activation = timestamp('1900-01-01 00:00:01 UTC')
                                )
                                or
                                (
                                    -- decline on restart attempt that eventually is successful
                                    txn.trans_dt_ut between date_add(sub.prior_expiration, interval -30 SECOND) and sub.post_activate 
                                    and sub.prior_expiration > timestamp('1900-01-01 00:00:01 UTC')
                                )
                                or
                                (
                                    -- purchase declined on restart attempt that is not ever successful (no next_activation)
                                    txn.trans_dt_ut between sub.expiration_dt_ut and date_add(sub.next_activation, interval 90 SECOND)
                                    -- txn.trans_dt_ut between sub.expiration_dt_ut and date_add(sub.next_activation, interval 10 MINUTE)
                                    and sub.next_activation = timestamp('2999-12-31 23:59:59 UTC')
                                )
                            )
                        and
                            (
                                (
                                    txn.trans_type_desc = 'purchase'
                                    and txn.trans_status_desc = 'declined'
                                )
                                or
                                (
                                    txn.trans_type_desc = 'verify'
                                    and txn.trans_status_desc in ('void','declined')
                                )
                            )
                    )
                    -- or
                    -- ( 
                    --     txn.origin_desc in ('recurring') 
                    --     and txn.trans_type_desc = 'purchase' 
                    --     and txn.trans_status_desc in ('success', 'void', 'declined')
                    --     and txn.trans_dt_ut between sub.activate_dt_ut and date_add(sub.post_activate, interval 8 MINUTE)
                    --     and 
                    --         (
                    --             -- restart 5+ minute delay from activation to invoice create/transaction -- https://cbsi-entertainment.recurly.com/transactions/785e861e51eb6ff1f1f1c74f278c8a31
                    --             sub.prior_expiration > timestamp('1900-01-01 00:00:01 UTC') 
                    --             or
                    --             -- initial 5+ minute delay from activation to invoice create/transaction -- https://cbsi-entertainment.recurly.com/transactions/785e95d4afdfcedc647f7340acab2559
                    --             sub.prior_activation = timestamp('1900-01-01 00:00:01 UTC') 
                    --         )
                    -- )
                )
        left join sub sk -- join using subscription guid for all others
            on sk.src_system_id = txn.src_system_id
            and sk.account_cd = txn.account_cd
            and sk.subscription_guid = txn.txn_subscription_guid
        where 1=1
            -- and txn.trans_type_desc in ('purchase','verify')
            -- and txn.trans_status_desc in ('success', 'void', 'declined')
            -- and txn.account_cd = '10002609'
    ) 

-- select
--     *
-- from txn_classify tc
-- where 1=1
-- qualify count(transaction_guid) over (partition by src_system_id, account_cd, transaction_guid) > 1
-- order by account_cd, transaction_guid, trans_dt_ut

, dtls as 
    (
        select distinct
            tc.*
            , max(case when ifnull(trim(failure_type),'') = 'fraud_gateway' then 1 else 0 end) over (partition by src_system_id, account_cd, invoice_nbr) as fraud_chk
            , lead(tc.invoice_nbr) over (partition by src_system_id, account_cd, sub_guid order by trans_dt_ut) as next_inv_nbr
            , lead(tc.invoice_billed_dt_ut) over (partition by src_system_id, account_cd, sub_guid order by trans_dt_ut) as next_inv_dt
            , lead(tc.txn_classify) over (partition by src_system_id, account_cd, sub_guid order by trans_dt_ut) as next_txn_classify
        from txn_classify tc
        where 1=1
        -- -- Adyen/Cybersource test
        --     -- and trans_gateway_type_desc in ('adyen','cybersource')
        --     and date(coalesce(invoice_billed_dt_ut, trans_dt_ut)) >= date('2025-05-15')
        --     and 
        --         (
        --             date(activate_dt_ut) >= date('2025-04-01')
        --             or
        --             date(activate_dt_ut) = date('1900-01-01')
        --             or 
        --             subscription_guid is null
        --         )
    )

-- select
-- *
-- from dtls
-- where 1=1
-- and txn_classify in ('purch_initial','purch_restart')
-- and date_trunc(date(coalesce(invoice_billed_dt_ut, trans_dt_ut)), MONTH) = date('2025-11-01')

, agg_recs as
    (
        select
            src_system_id
            -- , date(coalesce(invoice_billed_dt_ut, trans_dt_ut)) as dt
            , date_trunc(date(coalesce(invoice_billed_dt_ut, trans_dt_ut)), MONTH) as dt
            -- , date(datetime(coalesce(invoice_billed_dt_ut, trans_dt_ut), 'America/Los_Angeles')) as dt
            -- , extract(hour from datetime(coalesce(invoice_billed_dt_ut, trans_dt_ut), 'America/Los_Angeles')) as hr
            -- , account_cd
            , coalesce(overall_sub_start_type_desc, 'new_start') as overall_sub_start_type_desc
            , trans_gateway_type_desc
            , case
                -- when txn_classify like 'auth%' then 'trial_verify' -- all verify ties out with tableau dashboard
                when txn_classify in ('auth_initial','auth_restart') then 'trial_verify'
                -- when txn_classify like 'auth%' then 'verify_other' 
                when txn_classify like 'purch_trial%' then 'trial_to_paid'
                -- when txn_classify in ('purch_initial', 'purch_initial_trial_removed', 'purch_restart', 'purch_restart_trial_removed', 'purch_auto-card-update', 'purch_sub-change') then 'direct_to_paid' -- include sub change?
                when txn_classify in ('purch_initial', 'purch_initial_trial_removed', 'purch_restart', 'purch_restart_trial_removed', 'purch_auto-card-update') then 'direct_to_paid' 
                when txn_classify = 'purch_sub-change' then 'sub_change'
                when txn_classify = 'recurring' then 'renewals'
                else txn_classify
            end as txn_classify
            -- , txn_classify
            -- , case
            --     when txn_classify in ('auth_initial','auth_restart','purch_initial','purch_initial_trial_removed','purch_restart','purch_restart_trial_removed') then txn_classify
            --     when txn_classify like 'auth%' then 'auth_other'
            --     when txn_classify like 'purch%' then 'purch_other'
            --     when txn_classify = 'recurring' then txn_classify
            --     when txn_classify = 'purch_sub_change' then txn_classify
            --     else 'other'
            -- end as txn_classify
            -- , case
            --     when trans_status_desc in ('success','void') then 'success'
            --     else trans_status_desc
            -- end as trans_status_desc
            , count(distinct case when trans_status_desc in ('success','void') then transaction_guid else null end) as txn_success_ct
            , count(distinct case when trans_status_desc = 'declined' then transaction_guid else null end) as txn_decline_ct
            , count(distinct transaction_guid) as txn_tot_ct
            , round(safe_divide(count(distinct case when trans_status_desc in ('success','void') then transaction_guid else null end), count(distinct transaction_guid)),5) as txn_success_rate

            , count(distinct case when trans_status_desc in ('success','void') and ifnull(trim(failure_type),'') != 'fraud_gateway' then transaction_guid else null end) as no_fraud_txn_success_ct
            , count(distinct case when trans_status_desc = 'declined' and ifnull(trim(failure_type),'') != 'fraud_gateway' then transaction_guid else null end) as no_fraud_txn_decline_ct
            , count(distinct case when ifnull(trim(failure_type),'') != 'fraud_gateway' then transaction_guid else null end) as no_fraud_txn_tot_ct
            , round(safe_divide(count(distinct case when trans_status_desc in ('success','void') and ifnull(trim(failure_type),'') != 'fraud_gateway' then transaction_guid else null end), count(distinct case when ifnull(trim(failure_type),'') != 'fraud_gateway' then transaction_guid else null end)),5) as no_fraud_txn_success_rate

            -- , count(distinct case when invoice_state_desc = 'paid' then invoice_nbr else null end) as inv_success_ct
            , count(distinct case when success_ind = 1 then invoice_nbr else null end) as inv_success_ct
            -- , count(distinct case when (invoice_state_desc is not null and invoice_state_desc != 'paid') or (invoice_state_desc is null and trans_status_desc = 'declined') then invoice_nbr else null end) as inv_decline_ct
            , count(distinct case when success_ind = 0 then invoice_nbr else null end) as inv_decline_ct
            , count(distinct invoice_nbr) as inv_tot_ct
            -- , round(safe_divide(count(distinct case when invoice_state_desc = 'paid' then invoice_nbr else null end), count(distinct invoice_nbr)),5) as inv_success_rate
            , round(safe_divide(count(distinct case when success_ind = 1 then invoice_nbr else null end), count(distinct invoice_nbr)),5) as inv_success_rate

            -- , count(distinct case when invoice_state_desc = 'paid' and fraud_chk = 0 then invoice_nbr else null end) as no_fraud_inv_success_ct
            , count(distinct case when success_ind = 1 and fraud_chk = 0 then invoice_nbr else null end) as no_fraud_inv_success_ct
            -- , count(distinct case when fraud_chk = 0 and ((invoice_state_desc is not null and invoice_state_desc != 'paid') or (invoice_state_desc is null and trans_status_desc = 'declined')) then invoice_nbr else null end) as no_fraud_inv_decline_ct
            , count(distinct case when success_ind = 0 and fraud_chk = 0 then invoice_nbr else null end) as no_fraud_inv_decline_ct
            , count(distinct case when fraud_chk = 0 then invoice_nbr else null end) as no_fraud_inv_tot_ct
            -- , round(safe_divide(count(distinct case when invoice_state_desc = 'paid' and fraud_chk = 0 then invoice_nbr else null end), count(distinct case when fraud_chk = 0 then invoice_nbr else null end)),5) as no_fraud_inv_success_rate
            , round(safe_divide(count(distinct case when success_ind = 1 and fraud_chk = 0 then invoice_nbr else null end), count(distinct case when fraud_chk = 0 then invoice_nbr else null end)),5) as no_fraud_inv_success_rate


            , min(case when trans_status_desc in ('success','void') then transaction_guid else null end) as txn_success_ex1
            , max(case when trans_status_desc in ('success','void') then transaction_guid else null end) as txn_success_ex2
            , min(case when trans_status_desc = 'declined' then transaction_guid else null end) as txn_decline_ex1
            , max(case when trans_status_desc = 'declined' then transaction_guid else null end) as txn_decline_ex2

            , min(case when trans_status_desc = 'declined' and ifnull(trim(failure_type),'') != 'fraud_gateway' then transaction_guid else null end) as no_fraud_txn_decline_ex1
            , max(case when trans_status_desc = 'declined' and ifnull(trim(failure_type),'') != 'fraud_gateway' then transaction_guid else null end) as no_fraud_txn_decline_ex2

            , min(case when invoice_state_desc = 'paid' then invoice_nbr else null end) as inv_success_ex1
            , max(case when invoice_state_desc = 'paid' then invoice_nbr else null end) as inv_success_ex2
            -- , min(case when (invoice_state_desc is not null and invoice_state_desc != 'paid') or (invoice_state_desc is null and trans_status_desc = 'declined') then invoice_nbr else null end) as inv_decline_ex1
            -- , max(case when (invoice_state_desc is not null and invoice_state_desc != 'paid') or (invoice_state_desc is null and trans_status_desc = 'declined') then invoice_nbr else null end) as inv_decline_ex2
            , min(case when success_ind = 0 then invoice_nbr else null end) as inv_decline_ex1
            , max(case when success_ind = 0 then invoice_nbr else null end) as inv_decline_ex2
        from dtls 
        -- join
        --     (
        --         select
        --             src_system_id
        --             , dt
        --             , account_cd
        --             , txn_classify
        --             , row_number() over (partition by src_system_id, dt, account_cd order by txn_classify desc) as rn
        --         from dtls
        --         where 1=1
        --     )
        where 1=1 
            -- and txn_classify in ('auth_initial','auth_restart','purch_initial','purch_initial_trial_removed','purch_restart','purch_restart_trial_removed')
            -- and txn_classify not like 'unclassified%'
            -- and txn_classify not in ('auth_other','purch_auto-card-update')
        group by all
        order by 1,2
    )

-- , all_recs as 
--     (
--         select
--             src_system_id
--             , dt
--             , txn_classify

--             , sum(txn_success_ct) as txn_success_ct
--             , sum(txn_decline_ct) as txn_decline_ct
--             , sum(txn_tot_ct) as txn_tot_ct
--             , round(safe_divide(sum(txn_success_ct), sum(txn_tot_ct)), 5) as txn_success_rate

--             , sum(inv_success_ct) as inv_success_ct
--             , sum(inv_decline_ct) as inv_decline_ct
--             , sum(inv_tot_ct) as inv_tot_ct
--             , round(safe_divide(sum(inv_success_ct), sum(inv_tot_ct)), 5) as inv_success_rate
--         from agg_recs
--         where 1=1
--         group by all
--     )

-- /*
-- Filter to just a single record per account per day?
-- Or, try to re-classify auth_ to _trial_removed?
-- Ex:
-- https://cbsi-entertainment.recurly.com/accounts/5713859 shouldn't have both auth_restart and purch_restart_trial_removed
-- it should just be purch_restart_trial_removed
-- https://cbsi-entertainment.recurly.com/accounts/46925516 should just be purch_initial_trial_removed
-- */
-- , one_per_acct as
--     (
--         select
--             src_system_id
--             , dt
--             , txn_classify

--             , sum(txn_success_ct) as txn_success_ct
--             , sum(txn_decline_ct) as txn_decline_ct
--             , sum(txn_tot_ct) as txn_tot_ct
--             , round(safe_divide(sum(txn_success_ct), sum(txn_tot_ct)), 5) as txn_success_rate

--             , sum(inv_success_ct) as inv_success_ct
--             , sum(inv_decline_ct) as inv_decline_ct
--             , sum(inv_tot_ct) as inv_tot_ct
--             , round(safe_divide(sum(inv_success_ct), sum(inv_tot_ct)), 5) as inv_success_rate
--         from 
--             (
--                 select
--                     *
--                 from agg_recs
--                 where 1=1
--                 qualify row_number() over (partition by src_system_id, dt, account_cd order by txn_classify desc) = 1
--             )
--         where 1=1
--         group by all
--     )

select
    *
from agg_recs
-- from all_recs
-- from one_per_acct
where 1=1
    -- -- and txn_classify in ('auth_initial','auth_restart','purch_initial','purch_initial_trial_removed','purch_restart','purch_restart_trial_removed')
    -- and txn_classify not like 'unclassified%'
    -- and txn_classify not in ('auth_other','purch_auto-card-update')
    -- and trans_gateway_type_desc in ('adyen','cybersource')
    -- and txn_classify in ('trial_verify', 'direct_to_paid', 'renewals', 'trial_to_paid')
order by 1,2,3,4
