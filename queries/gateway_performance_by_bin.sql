-- Gateway performance comparison by gateway and card bin.
-- Step 1: Categorize subscriptions, transactions, and invoices.
-- Step 2: Aggregate success rates at transaction and invoice level by timeframe.
-- Based on queries/txn.sql, queries/adj.sql, queries/sub.sql.

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
                    and src_system_id = 115
            ) sub
        where 1=1
    )

, sub_categorized as
    (
        select
            sub.*
            , sub.original_activation as account_earliest_activation
            , max(case when sub.frn = 1 then sub.expiration_dt_ut end) over (partition by sub.src_system_id, sub.account_cd) as account_earliest_expiration
            , case when sub.frn = 1 then 'initial' else 'restart' end as subscription_category
        from sub
        where 1=1
    )

, adj as
    (
        select
            adj.*
            , case
                when lag(adj.adj_total_amt) over (partition by adj.src_system_id, adj.account_cd order by adj.invoice_billed_dt_ut, adj.invoice_nbr) < 0
                        and adj.adj_total_amt + lag(adj.adj_total_amt) over (partition by adj.src_system_id, adj.account_cd order by adj.invoice_billed_dt_ut, adj.invoice_nbr) < 0
                    then 'paid_by_gift_card'
                else 'null'
            end as paid_by_gc_chk
            , case
                when adj.invoice_type_cd = 'renewal'
                    then row_number() over (partition by adj.src_system_id, adj.account_cd, adj.subscription_guid order by case when adj.invoice_type_cd = 'renewal' then 0 else 1 end, adj.invoice_billed_dt_ut, adj.invoice_nbr)
                else null
            end as renewal_earliest_inv
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
                    , sum(adj.adj_amt) over (partition by adj.src_system_id, adj.account_cd, adj.invoice_guid) as adj_amt
                    , sum(adj.adj_total_amt) over (partition by adj.src_system_id, adj.account_cd, adj.invoice_guid) as adj_total_amt
                    , max(case when invoice_type_cd = 'gift_card' and type_desc = 'credit' and lower(adj_desc) like '%gift%card%' then 1 else 0 end) over (partition by adj.src_system_id, adj.account_cd, adj.invoice_guid) as gift_card_chk
                    , dense_rank() over (partition by adj.src_system_id, adj.account_cd, adj.subscription_guid order by adj.invoice_billed_dt_ut, adj.invoice_nbr) as earliest_inv
                from i-dss-streaming-data.payment_ops_vw.recurly_adjustments_fct adj
                where 1=1
                    and adj.creation_dt >= date('2025-01-01')
                    and adj.src_system_id = 115
            ) adj
        where 1=1
    )

, adj_categorized as
    (
        select
            adj.*
            , sub.trial_end_dt_ut
            , sub.activate_dt_ut as sub_activate_dt_ut
            , sub.pre_activate
            , sub.post_activate
            , sub.pre_trial_end
            , sub.post_trial_end
            , case
                when adj.invoice_type_cd = 'renewal'
                    and adj.adj_total_amt > 0
                    and sub.trial_end_dt_ut is not null
                    and adj.invoice_billed_dt_ut between sub.pre_trial_end and sub.post_trial_end
                    and adj.renewal_earliest_inv = 1
                    then 'trial_to_paid'
                when adj.invoice_type_cd = 'renewal'
                    and sub.trial_end_dt_ut is null
                    and date(adj.invoice_billed_dt_ut) < date('2025-11-06')  -- prior to the change on 2025-11-06, subscriptions were activated with a trial ($1 auth) then the trial was removed a few minutes later and a renewal was charged
                    and adj.invoice_billed_dt_ut between date_add(sub.activate_dt_ut, interval -10 MINUTE) and date_add(sub.activate_dt_ut, interval 10 MINUTE)
                    then 'direct_to_paid'
                when adj.invoice_type_cd = 'purchase'
                    and sub.trial_end_dt_ut is null
                    and adj.invoice_billed_dt_ut between sub.pre_activate and sub.post_activate
                    -- and date(adj.invoice_billed_dt_ut) >= date('2025-11-06') -- this use case applies before and after the change on 2025-11-06 
                    then 'direct_to_paid'
                when adj.invoice_type_cd = 'renewal' and adj.renewal_earliest_inv > 1
                    then 'recurring'
                else 'other'
            end as invoice_category_raw
        from adj
        left join sub_categorized sub
            on sub.src_system_id = adj.src_system_id
            and sub.account_cd = adj.account_cd
            and sub.subscription_guid = adj.subscription_guid
        where 1=1
    )

, adj_categorized_final as
    (
        select
            adj.*
            , case
                when adj.invoice_category_raw = 'trial_to_paid' then 'trial_to_paid'
                when adj.invoice_category_raw = 'direct_to_paid' then 'direct_to_paid'
                when adj.invoice_category_raw = 'recurring' then 'recurring'
                when inv_change_sub.change_sub_inv = 1 then 'change_sub'
                else 'other'
            end as invoice_category
        from adj_categorized adj
        left join
            (
                select distinct
                    txn.src_system_id
                    , txn.account_cd
                    , txn.invoice_guid
                    , 1 as change_sub_inv
                from i-dss-streaming-data.payment_ops_vw.recurly_transaction_fct txn
                where 1=1
                    and txn.src_system_id = 115
                    and txn.trans_type_desc = 'purchase'
                    and txn.trans_status_desc in ('success', 'void', 'declined')
                    and txn.origin_desc = 'api_sub_change'
                    and nullif(trim(txn.invoice_guid), '') is not null
            ) inv_change_sub
            on inv_change_sub.src_system_id = adj.src_system_id
            and inv_change_sub.account_cd = adj.account_cd
            and inv_change_sub.invoice_guid = adj.invoice_guid
        where 1=1
    )

, txn as
    (
        select
            txn.src_system_id
            , txn.account_cd
            , nullif(trim(txn.subscription_guid),'') as subscription_guid
            , nullif(trim(txn.invoice_guid),'') as invoice_guid
            , txn.transaction_guid
            , txn.trans_dt
            , txn.trans_dt_ut
            , txn.origin_desc
            , txn.trans_type_desc
            , txn.trans_status_desc
            , txn.trans_amt
            , txn.tax_amt
            , txn.currency_cd
            , txn.country_cd
            , txn.trans_gateway_type_desc
            , txn.gateway_cd
            , txn.gateway_error_cd
            , txn.failure_type
            , txn.payment_method_desc
            , txn.cc_type_desc
            , txn.cc_first_6_nbr
            , txn.card_brand_nm
            , txn.card_type_cd
            , txn.card_level_cd
            , txn.card_issuer_nm
            , txn.card_issuing_country_cd
            , txn.approval_desc
            , txn.trans_msg_desc
            , txn.reference_cd
        from i-dss-streaming-data.payment_ops_vw.recurly_transaction_fct txn
        where 1=1
            and txn.src_system_id = 115
            and txn.trans_dt >= date('2025-01-01')
            and txn.trans_type_desc in ('purchase', 'verify')
            and txn.trans_status_desc in ('success', 'void', 'declined')
    )

, txn_adj as
    (
        select
            txn.*
            , adj.subscription_guid as adj_subscription_guid
            , adj.invoice_type_cd
            , adj.invoice_billed_dt_ut
            , adj.adj_total_amt
            , adj.renewal_earliest_inv
            , adj.invoice_category
        from txn
        left join adj_categorized_final adj
            on adj.src_system_id = txn.src_system_id
            and adj.account_cd = txn.account_cd
            and adj.invoice_guid = txn.invoice_guid
        where 1=1
    )

-- txn_with_sub_win: join each transaction to the subscription whose activation window contains the txn.
-- Match is by (src_system_id, account_cd) and trans_dt_ut in [prior_expiration, activate_dt_ut).
-- Used to categorize verifications (no subscription_guid/invoice_guid) and non-renewal declined purchases
-- that fall in that window. QUALIFY keeps one sub per txn when multiple windows could match.
, txn_with_sub_win as
    (
        select
            txn_adj.*
            , sub_win.subscription_guid as sub_win_subscription_guid
            , sub_win.activate_dt_ut as sub_win_activate_dt_ut
            , sub_win.prior_expiration as sub_win_prior_expiration
            , sub_win.trial_end_dt_ut as sub_win_trial_end_dt_ut
            , sub_win.pre_activate as sub_win_pre_activate
            , sub_win.post_activate as sub_win_post_activate
        from txn_adj
        left join sub_categorized sub_win
            on sub_win.src_system_id = txn_adj.src_system_id
            and sub_win.account_cd = txn_adj.account_cd
            and txn_adj.trans_dt_ut between coalesce(sub_win.prior_expiration, timestamp('1900-01-01 00:00:01 UTC')) and coalesce(sub_win.post_activate, timestamp('2999-12-31 23:59:59 UTC'))
            -- and txn_adj.trans_dt_ut >= coalesce(sub_win.prior_expiration, timestamp('1900-01-01 00:00:01 UTC'))
            -- and txn_adj.trans_dt_ut < coalesce(sub_win.activate_dt_ut, timestamp('2999-12-31 23:59:59 UTC'))
        where 1=1
        qualify row_number() over (partition by txn_adj.transaction_guid order by sub_win.activate_dt_ut) = 1
    )

-- txn_with_subs: join each transaction to the subscription by subscription_guid (when present).
-- Supplies trial/activation timestamps (e.g. sub_sub_trial_end_dt_ut, sub_sub_pre_activate) used
-- to categorize trial_to_paid, direct_to_paid, and recurring transactions linked to an invoice.
, txn_with_subs as
    (
        select
            txn_win.*
            , sub_sub.subscription_guid as sub_sub_subscription_guid
            , sub_sub.trial_end_dt_ut as sub_sub_trial_end_dt_ut
            , sub_sub.activate_dt_ut as sub_sub_activate_dt_ut
            , sub_sub.pre_activate as sub_sub_pre_activate
            , sub_sub.post_activate as sub_sub_post_activate
            , sub_sub.pre_trial_end as sub_sub_pre_trial_end
            , sub_sub.post_trial_end as sub_sub_post_trial_end
        from txn_with_sub_win txn_win
        left join sub_categorized sub_sub
            on sub_sub.src_system_id = txn_win.src_system_id
            and sub_sub.account_cd = txn_win.account_cd
            and sub_sub.subscription_guid = txn_win.subscription_guid
        where 1=1
    )

, txn_categorized as
    (
        select
            txn_with_subs.*
            , case
                when txn_with_subs.trans_type_desc = 'purchase'
                    and txn_with_subs.trans_status_desc in ('success', 'void', 'declined')
                    and txn_with_subs.origin_desc = 'api_sub_change'
                    then 'change_sub'
                when txn_with_subs.trans_type_desc = 'verify'
                    and txn_with_subs.trans_status_desc in ('success', 'void')
                    and txn_with_subs.sub_win_trial_end_dt_ut is not null
                    and txn_with_subs.trans_dt_ut between txn_with_subs.sub_win_pre_activate and txn_with_subs.sub_win_post_activate
                    then 'trial_verify'
                when txn_with_subs.trans_type_desc = 'verify'
                    and txn_with_subs.trans_status_desc = 'declined'
                    and txn_with_subs.sub_win_subscription_guid is not null
                    then 'trial_verify'
                when txn_with_subs.trans_type_desc = 'purchase'
                    and txn_with_subs.invoice_type_cd = 'renewal'
                    -- and coalesce(txn_with_subs.adj_total_amt, 0) > 0
                    and txn_with_subs.sub_sub_trial_end_dt_ut is not null
                    and txn_with_subs.invoice_billed_dt_ut between txn_with_subs.sub_sub_pre_trial_end and txn_with_subs.sub_sub_post_trial_end
                    and txn_with_subs.renewal_earliest_inv = 1
                    then 'trial_to_paid'
                when txn_with_subs.trans_type_desc = 'purchase'
                    and txn_with_subs.trans_status_desc in ('success', 'void', 'declined')
                    and txn_with_subs.sub_sub_trial_end_dt_ut is null
                    and txn_with_subs.invoice_type_cd = 'renewal'
                    and date(coalesce(txn_with_subs.invoice_billed_dt_ut, txn_with_subs.trans_dt_ut)) < date('2025-11-06')
                    and txn_with_subs.invoice_billed_dt_ut between date_add(txn_with_subs.sub_sub_activate_dt_ut, interval -10 MINUTE) and date_add(txn_with_subs.sub_sub_activate_dt_ut, interval 10 MINUTE)
                    then 'direct_to_paid'
                when txn_with_subs.trans_type_desc = 'purchase'
                    and txn_with_subs.trans_status_desc in ('success', 'void')
                    and txn_with_subs.sub_sub_trial_end_dt_ut is null
                    and txn_with_subs.invoice_type_cd = 'purchase'
                    and date(txn_with_subs.trans_dt_ut) >= date('2025-11-06')
                    and txn_with_subs.trans_dt_ut between txn_with_subs.sub_sub_pre_activate and txn_with_subs.sub_sub_post_activate
                    then 'direct_to_paid'
                when txn_with_subs.trans_type_desc = 'purchase'
                    and txn_with_subs.trans_status_desc = 'declined'
                    and txn_with_subs.origin_desc in ('token_api', 'api')
                    and (txn_with_subs.invoice_guid is null or txn_with_subs.invoice_type_cd != 'renewal')
                    and txn_with_subs.sub_win_subscription_guid is not null
                    then 'direct_to_paid'
                when txn_with_subs.trans_type_desc = 'purchase'
                    and txn_with_subs.trans_status_desc in ('success', 'void', 'declined')
                    and txn_with_subs.invoice_type_cd = 'renewal'
                    and coalesce(txn_with_subs.renewal_earliest_inv, 0) > 1
                    then 'recurring'
                when txn_with_subs.trans_type_desc = 'verify'
                    then 'other_verify'
                else 'other'
            end as transaction_category
        from txn_with_subs
        where 1=1
    )

-- Top 100 card bins by transaction volume for the filtered period; used to restrict all downstream aggregations.
, top_100_bins as
    (
        select
            cc_first_6_nbr
        from
            (
                select
                    txn.cc_first_6_nbr
                    , count(*) as txn_cnt
                from txn
                where 1=1
                    and txn.cc_first_6_nbr is not null
                group by txn.cc_first_6_nbr
                order by txn_cnt desc
                limit 100
            ) r
        where 1=1
    )

-- Base for aggregation: one row per transaction with a consistent invoice identifier.
-- Real invoices: invoice_grouper = invoice_guid. Verifications and non-renewal declines (no invoice):
-- invoice_grouper = derived key from (src_system_id, account_cd, sub_win_prior_expiration, sub_win_activate_dt_ut).
-- invoice_dt_ut is used for invoice-level timeframe (day/week/month/quarter); for real invoices = invoice_billed_dt_ut, else trans_dt_ut.
, txn_invoice_base as
    (
        select
            txn_categorized.*
            , coalesce(
                nullif(trim(txn_categorized.invoice_guid), ''),
                format('window_%s_%s_%s_%s'
                    , cast(txn_categorized.src_system_id as string)
                    , txn_categorized.account_cd
                    , cast(coalesce(txn_categorized.sub_win_prior_expiration, timestamp('1900-01-01 00:00:01 UTC')) as string)
                    , cast(coalesce(txn_categorized.sub_win_activate_dt_ut, timestamp('2999-12-31 23:59:59 UTC')) as string)
                )
            ) as invoice_grouper
            , coalesce(txn_categorized.invoice_billed_dt_ut, txn_categorized.trans_dt_ut) as invoice_dt_ut
        from txn_categorized
        where 1=1
            and txn_categorized.cc_first_6_nbr in (select cc_first_6_nbr from top_100_bins)
    )

, invoice_success as
    (
        select
            txn_invoice_base.src_system_id
            , txn_invoice_base.account_cd
            , txn_invoice_base.invoice_grouper
            , max(case
                when txn_invoice_base.trans_type_desc = 'purchase' and txn_invoice_base.trans_status_desc in ('success', 'void') then 1
                when txn_invoice_base.trans_type_desc = 'verify' and txn_invoice_base.trans_status_desc in ('success', 'void') then 1
                else 0
            end) as has_success_purchase
        from txn_invoice_base
        where 1=1
        group by
            txn_invoice_base.src_system_id
            , txn_invoice_base.account_cd
            , txn_invoice_base.invoice_grouper
    )

, verify_window_agg as
    (
        select
            txn_invoice_base.src_system_id
            , txn_invoice_base.account_cd
            , txn_invoice_base.invoice_grouper
            , min(txn_invoice_base.trans_dt) as window_trans_dt
            , txn_invoice_base.trans_gateway_type_desc
            , txn_invoice_base.cc_first_6_nbr
            , txn_invoice_base.transaction_category
            , count(*) as attempt_cnt
            , countif(txn_invoice_base.trans_status_desc in ('success', 'void')) as success_cnt
        from txn_invoice_base
        where 1=1
            and txn_invoice_base.transaction_category in ('trial_verify', 'other_verify')
            and txn_invoice_base.invoice_grouper like 'window_%'
        group by
            txn_invoice_base.src_system_id
            , txn_invoice_base.account_cd
            , txn_invoice_base.invoice_grouper
            , txn_invoice_base.trans_gateway_type_desc
            , txn_invoice_base.cc_first_6_nbr
            , txn_invoice_base.transaction_category
    )

, inv_agg as
    (
        select
            adj.src_system_id
            , adj.account_cd
            , adj.invoice_guid
            , adj.invoice_billed_dt_ut
            , adj.invoice_category
            , txn_invoice_base.trans_gateway_type_desc
            , txn_invoice_base.cc_first_6_nbr
            , max(invoice_success.has_success_purchase) as is_success_invoice
        from adj_categorized_final adj
        inner join txn_invoice_base
            on txn_invoice_base.src_system_id = adj.src_system_id
            and txn_invoice_base.account_cd = adj.account_cd
            and txn_invoice_base.invoice_guid = adj.invoice_guid
        left join invoice_success
            on invoice_success.src_system_id = txn_invoice_base.src_system_id
            and invoice_success.account_cd = txn_invoice_base.account_cd
            and invoice_success.invoice_grouper = txn_invoice_base.invoice_grouper
        where 1=1
        group by
            adj.src_system_id
            , adj.account_cd
            , adj.invoice_guid
            , adj.invoice_billed_dt_ut
            , adj.invoice_category
            , txn_invoice_base.trans_gateway_type_desc
            , txn_invoice_base.cc_first_6_nbr
    )

-- Transaction-level aggregation: by gateway, timeframe (day/week/month/quarter), optional card bin
, txn_metrics as
    (
        select
            txn_invoice_base.trans_gateway_type_desc
            , date(txn_invoice_base.trans_dt) as trans_dt_day
            , date_trunc(txn_invoice_base.trans_dt, week) as trans_dt_week
            , date_trunc(txn_invoice_base.trans_dt, month) as trans_dt_month
            , date_trunc(txn_invoice_base.trans_dt, quarter) as trans_dt_quarter
            , txn_invoice_base.cc_first_6_nbr
            , txn_invoice_base.transaction_category
            , count(*) as total_transactions
            , countif(txn_invoice_base.trans_status_desc in ('success', 'void')) as successful_transactions
        from txn_invoice_base
        where 1=1
        group by
            txn_invoice_base.trans_gateway_type_desc
            , date(txn_invoice_base.trans_dt)
            , date_trunc(txn_invoice_base.trans_dt, week)
            , date_trunc(txn_invoice_base.trans_dt, month)
            , date_trunc(txn_invoice_base.trans_dt, quarter)
            , txn_invoice_base.cc_first_6_nbr
            , txn_invoice_base.transaction_category
    )

-- Invoice-level aggregation: by gateway, timeframe (day/week/month/quarter), optional card bin (real invoices + verification windows)
, inv_metrics_base as
    (
        select
            inv_agg.trans_gateway_type_desc
            , date(inv_agg.invoice_billed_dt_ut) as invoice_dt_day
            , date_trunc(date(inv_agg.invoice_billed_dt_ut), week) as invoice_dt_week
            , date_trunc(date(inv_agg.invoice_billed_dt_ut), month) as invoice_dt_month
            , date_trunc(date(inv_agg.invoice_billed_dt_ut), quarter) as invoice_dt_quarter
            , inv_agg.cc_first_6_nbr
            , inv_agg.invoice_category
            , 1 as invoice_cnt
            , inv_agg.is_success_invoice as success_cnt
        from inv_agg
        where 1=1
            and inv_agg.invoice_guid is not null
    )

, inv_metrics_verify as
    (
        select
            verify_window_agg.trans_gateway_type_desc
            , date(verify_window_agg.window_trans_dt) as invoice_dt_day
            , date_trunc(date(verify_window_agg.window_trans_dt), week) as invoice_dt_week
            , date_trunc(date(verify_window_agg.window_trans_dt), month) as invoice_dt_month
            , date_trunc(date(verify_window_agg.window_trans_dt), quarter) as invoice_dt_quarter
            , verify_window_agg.cc_first_6_nbr
            , verify_window_agg.transaction_category as invoice_category
            , 1 as invoice_cnt
            , if(verify_window_agg.success_cnt > 0, 1, 0) as success_cnt
        from verify_window_agg
        where 1=1
    )

, inv_metrics as
    (
        select
            trans_gateway_type_desc
            , invoice_dt_day
            , invoice_dt_week
            , invoice_dt_month
            , invoice_dt_quarter
            , cc_first_6_nbr
            , invoice_category
            , sum(invoice_cnt) as total_invoices
            , sum(success_cnt) as successful_invoices
        from
            (
                select * from inv_metrics_base
                union all
                select * from inv_metrics_verify
            ) u
        where 1=1
        group by
            trans_gateway_type_desc
            , invoice_dt_day
            , invoice_dt_week
            , invoice_dt_month
            , invoice_dt_quarter
            , cc_first_6_nbr
            , invoice_category
    )

-- Final output: transaction-level and invoice-level metrics (same schema; transaction_category holds category for both)
select
    'transaction' as metric_level
    , 'day' as timeframe_type
    , txn_metrics.trans_dt_day as timeframe_value
    , txn_metrics.trans_gateway_type_desc
    , txn_metrics.cc_first_6_nbr
    , txn_metrics.transaction_category as category
    , txn_metrics.total_transactions
    , txn_metrics.successful_transactions
    , safe_divide(txn_metrics.successful_transactions, txn_metrics.total_transactions) as txn_success_rate
    , cast(null as int64) as total_invoices
    , cast(null as int64) as successful_invoices
    , cast(null as float64) as inv_success_rate
from txn_metrics
where 1=1
union all
select
    'transaction'
    , 'week'
    , txn_metrics.trans_dt_week
    , txn_metrics.trans_gateway_type_desc
    , txn_metrics.cc_first_6_nbr
    , txn_metrics.transaction_category
    , sum(txn_metrics.total_transactions)
    , sum(txn_metrics.successful_transactions)
    , safe_divide(sum(txn_metrics.successful_transactions), sum(txn_metrics.total_transactions))
    , cast(null as int64)
    , cast(null as int64)
    , cast(null as float64)
from txn_metrics
where 1=1
group by
    txn_metrics.trans_dt_week
    , txn_metrics.trans_gateway_type_desc
    , txn_metrics.cc_first_6_nbr
    , txn_metrics.transaction_category
union all
select
    'transaction'
    , 'month'
    , txn_metrics.trans_dt_month
    , txn_metrics.trans_gateway_type_desc
    , txn_metrics.cc_first_6_nbr
    , txn_metrics.transaction_category
    , sum(txn_metrics.total_transactions)
    , sum(txn_metrics.successful_transactions)
    , safe_divide(sum(txn_metrics.successful_transactions), sum(txn_metrics.total_transactions))
    , cast(null as int64)
    , cast(null as int64)
    , cast(null as float64)
from txn_metrics
where 1=1
group by
    txn_metrics.trans_dt_month
    , txn_metrics.trans_gateway_type_desc
    , txn_metrics.cc_first_6_nbr
    , txn_metrics.transaction_category
union all
select
    'transaction'
    , 'quarter'
    , txn_metrics.trans_dt_quarter
    , txn_metrics.trans_gateway_type_desc
    , txn_metrics.cc_first_6_nbr
    , txn_metrics.transaction_category
    , sum(txn_metrics.total_transactions)
    , sum(txn_metrics.successful_transactions)
    , safe_divide(sum(txn_metrics.successful_transactions), sum(txn_metrics.total_transactions))
    , cast(null as int64)
    , cast(null as int64)
    , cast(null as float64)
from txn_metrics
where 1=1
group by
    txn_metrics.trans_dt_quarter
    , txn_metrics.trans_gateway_type_desc
    , txn_metrics.cc_first_6_nbr
    , txn_metrics.transaction_category
union all
select
    'invoice'
    , 'day'
    , inv_metrics.invoice_dt_day
    , inv_metrics.trans_gateway_type_desc
    , inv_metrics.cc_first_6_nbr
    , inv_metrics.invoice_category
    , cast(null as int64)
    , cast(null as int64)
    , cast(null as float64)
    , inv_metrics.total_invoices
    , inv_metrics.successful_invoices
    , safe_divide(inv_metrics.successful_invoices, inv_metrics.total_invoices)
from inv_metrics
where 1=1
union all
select
    'invoice'
    , 'week'
    , inv_metrics.invoice_dt_week
    , inv_metrics.trans_gateway_type_desc
    , inv_metrics.cc_first_6_nbr
    , inv_metrics.invoice_category
    , cast(null as int64)
    , cast(null as int64)
    , cast(null as float64)
    , sum(inv_metrics.total_invoices)
    , sum(inv_metrics.successful_invoices)
    , safe_divide(sum(inv_metrics.successful_invoices), sum(inv_metrics.total_invoices))
from inv_metrics
where 1=1
group by
    inv_metrics.invoice_dt_week
    , inv_metrics.trans_gateway_type_desc
    , inv_metrics.cc_first_6_nbr
    , inv_metrics.invoice_category
union all
select
    'invoice'
    , 'month'
    , inv_metrics.invoice_dt_month
    , inv_metrics.trans_gateway_type_desc
    , inv_metrics.cc_first_6_nbr
    , inv_metrics.invoice_category
    , cast(null as int64)
    , cast(null as int64)
    , cast(null as float64)
    , sum(inv_metrics.total_invoices)
    , sum(inv_metrics.successful_invoices)
    , safe_divide(sum(inv_metrics.successful_invoices), sum(inv_metrics.total_invoices))
from inv_metrics
where 1=1
group by
    inv_metrics.invoice_dt_month
    , inv_metrics.trans_gateway_type_desc
    , inv_metrics.cc_first_6_nbr
    , inv_metrics.invoice_category
union all
select
    'invoice'
    , 'quarter'
    , inv_metrics.invoice_dt_quarter
    , inv_metrics.trans_gateway_type_desc
    , inv_metrics.cc_first_6_nbr
    , inv_metrics.invoice_category
    , cast(null as int64)
    , cast(null as int64)
    , cast(null as float64)
    , sum(inv_metrics.total_invoices)
    , sum(inv_metrics.successful_invoices)
    , safe_divide(sum(inv_metrics.successful_invoices), sum(inv_metrics.total_invoices))
from inv_metrics
where 1=1
group by
    inv_metrics.invoice_dt_quarter
    , inv_metrics.trans_gateway_type_desc
    , inv_metrics.cc_first_6_nbr
    , inv_metrics.invoice_category
order by
    metric_level
    , timeframe_type
    , timeframe_value
    , trans_gateway_type_desc
    , cc_first_6_nbr
    , category
;
