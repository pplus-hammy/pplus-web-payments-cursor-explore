-- Gateway performance comparison by gateway and card bin.
-- Categorizes subscriptions/invoices/transactions; assigns each txn to invoice or window grouper; aggregates
-- txn and invoice success rates at day level (date field in full_results; final SELECT can truncate to week/month/quarter). Bin breakdown limited to top 100 by volume.
-- Recurly joins: always src_system_id + account_cd. See gateway_performance_example1/2 for output style.

declare dt_filter date;
declare src_id int64;
set dt_filter = date('2025-10-01');
set src_id = 115;

-- Subscriptions with prior/next windows and subscription_category (initial | restart).
with sub_categorized as
    (
        select
            case
                when datetime(s.expiration_dt_ut, 'America/New_York') between date_add(date_trunc(current_datetime('America/New_York'), DAY), INTERVAL -1 SECOND) and datetime('2999-12-01 23:59:59')
                    then 'canceled'
                when datetime(s.expiration_dt_ut, 'America/New_York') < date_trunc(current_datetime('America/New_York'), DAY)
                    then 'expired'
                when s.expiration_dt_ut = timestamp('2999-12-31 23:59:59 UTC')
                    then 'active'
                else s.status_desc
            end as status_desc
            , s.* except (status_desc)
            , coalesce(lag(s.expiration_dt_ut) over (partition by s.src_system_id, s.account_cd order by creation_dt_ut, activate_dt_ut, s.expiration_dt_ut), timestamp('1900-01-01 00:00:01 UTC')) as prior_expiration
            , max(case when s.frn = 1 then s.activate_dt_ut end) over (partition by s.src_system_id, s.account_cd order by creation_dt_ut, activate_dt_ut, s.expiration_dt_ut) as original_activation
            , max(case when s.frn = 1 then s.expiration_dt_ut end) over (partition by s.src_system_id, s.account_cd) as account_earliest_expiration
            , case
                when s.frn = 1
                    then 'initial'
                else 'restart'
            end as subscription_category
        from (
            select
                src_system_id
                , account_cd
                , subscription_guid
                , plan_cd
                , plan_nm
                , trim(split(regexp_replace(plan_nm, r'\([^()]*\)',''), ' -')[offset(0)]) as base_plan_nm
                , regexp_extract(plan_nm, r'\((.*?)\)') as plan_qualifier
                , case
                    when lower(plan_cd) like '%monthly%'
                        then 'monthly'
                    when lower(plan_cd) like '%annual%'
                        then 'annual'
                    else 'other'
                end as plan_dur
                , unit_amt
                , currency_cd
                , status_desc
                , creation_dt_ut
                , activate_dt_ut
                , trial_start_dt_ut
                , trial_end_dt_ut
                , curr_period_start_dt_ut
                , curr_period_end_dt_ut
                , coalesce(cancel_dt_ut, timestamp('2999-12-31 23:59:59 UTC')) as cancel_dt_ut
                , case
                    when expiration_dt_ut is null
                            and lower(plan_cd) like '%monthly%'
                            and date(curr_period_end_dt_ut) <= date_add(current_date, interval -2 month)
                        then curr_period_end_dt_ut
                    when expiration_dt_ut is null
                            and lower(plan_cd) like '%annual%'
                            and date(curr_period_end_dt_ut) <= date_add(current_date, interval -2 month)
                        then curr_period_end_dt_ut
                    when expiration_dt_ut is null
                        then timestamp('2999-12-31 23:59:59 UTC')
                    else expiration_dt_ut
                end as expiration_dt_ut
                , case
                    when src_system_id = 134
                        then date_add(activate_dt_ut, interval -150 second)
                    else date_add(activate_dt_ut, interval -120 second)
                end as pre_activate
                , case
                    when src_system_id = 134
                        then date_add(activate_dt_ut, interval 150 second)
                    else date_add(activate_dt_ut, interval 120 second)
                end as post_activate
                , date_add(trial_end_dt_ut, interval -120 second) as pre_trial_end
                , date_add(trial_end_dt_ut, interval 120 second) as post_trial_end
                , row_number() over (partition by src_system_id, account_cd order by creation_dt_ut, activate_dt_ut, ifnull(expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC'))) as frn
                , lag(plan_nm) over (partition by src_system_id, account_cd order by creation_dt_ut, activate_dt_ut, ifnull(expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC'))) as prior_plan
                , coalesce(lag(activate_dt_ut) over (partition by src_system_id, account_cd order by creation_dt_ut, activate_dt_ut, ifnull(expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC'))), timestamp('1900-01-01 00:00:01 UTC')) as prior_activation
                , lead(activate_dt_ut) over (partition by src_system_id, account_cd order by creation_dt_ut, activate_dt_ut, ifnull(expiration_dt_ut, timestamp('2999-12-31 23:59:59 UTC'))) as next_activation
            from i-dss-streaming-data.payment_ops_vw.recurly_subscription_dim
            where 1=1
                and src_system_id = src_id
        ) s
        where 1=1
    )

-- ---------------------------------------------------------------------------
-- Invoices (adj): renewal rank + invoice_category (trial_to_paid, direct_to_paid, recurring, change_sub, other).
-- change_sub: invoice has at least one purchase with origin_desc = 'api_sub_change'.
-- ---------------------------------------------------------------------------
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
                    then row_number() over (partition by adj.src_system_id, adj.account_cd, adj.subscription_guid order by case
                        when adj.invoice_type_cd = 'renewal'
                            then 0
                        else 1
                    end, adj.invoice_billed_dt_ut, adj.invoice_nbr)
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
                    , max(case
                        when invoice_type_cd = 'gift_card'
                            and type_desc = 'credit'
                            and lower(adj_desc) like '%gift%card%'
                            then 1
                        else 0
                    end) over (partition by adj.src_system_id, adj.account_cd, adj.invoice_guid) as gift_card_chk
                    , dense_rank() over (partition by adj.src_system_id, adj.account_cd, adj.subscription_guid order by adj.invoice_billed_dt_ut, adj.invoice_nbr) as earliest_inv
                from i-dss-streaming-data.payment_ops_vw.recurly_adjustments_fct adj
                where 1=1
                    and adj.creation_dt >= date_add(dt_filter, interval -1 month)
                    and adj.src_system_id = src_id
            ) adj
        where 1=1
    )

, adj_categorized_final as
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
                when inv_change_sub.change_sub_inv = 1
                    then 'change_sub'
                when adj.invoice_type_cd = 'renewal'
                        and adj.adj_total_amt > 0
                        and sub.trial_end_dt_ut is not null
                        and adj.invoice_billed_dt_ut between sub.pre_trial_end and sub.post_trial_end
                        and adj.renewal_earliest_inv = 1
                    then 'trial_to_paid'
                when adj.invoice_type_cd = 'renewal'
                        and sub.trial_end_dt_ut is null
                        and date(adj.invoice_billed_dt_ut) < date('2025-11-06')
                        and adj.invoice_billed_dt_ut between date_add(sub.activate_dt_ut, interval -10 minute) and date_add(sub.activate_dt_ut, interval 10 minute)
                    then 'direct_to_paid'
                when adj.invoice_type_cd = 'purchase'
                        and sub.trial_end_dt_ut is null
                        and adj.invoice_billed_dt_ut between sub.pre_activate and sub.post_activate
                    then 'direct_to_paid'
                when adj.invoice_type_cd = 'renewal'
                        and adj.renewal_earliest_inv > 1
                    then 'recurring'
                else 'other'
            end as invoice_category
        from adj
        left join sub_categorized sub
            on sub.src_system_id = adj.src_system_id
            and sub.account_cd = adj.account_cd
            and sub.subscription_guid = adj.subscription_guid
        left join 
            (
                select distinct
                    src_system_id
                    , account_cd
                    , invoice_guid
                    , 1 as change_sub_inv
                from i-dss-streaming-data.payment_ops_vw.recurly_transaction_fct
                where 1=1
                    and src_system_id = src_id
                    and trans_type_desc = 'purchase'
                    and trans_status_desc in ('success', 'void', 'declined')
                    and origin_desc = 'api_sub_change'
                    and nullif(trim(invoice_guid), '') is not null
            ) inv_change_sub
            on inv_change_sub.src_system_id = adj.src_system_id
            and inv_change_sub.account_cd = adj.account_cd
            and inv_change_sub.invoice_guid = adj.invoice_guid
        where 1=1
    )

-- ---------------------------------------------------------------------------
-- Transactions: base txn + join to adj (txn_adj) + join to sub by activation window (txn_with_sub_win)
-- and by subscription_guid (txn_with_subs). Verifications have no subscription_guid/invoice_guid;
-- window join assigns them to (prior_expiration, activate_dt_ut) for invoice-level grouping.
-- ---------------------------------------------------------------------------
, txn as
    (
        select
            txn.src_system_id
            , txn.account_cd
            , nullif(trim(txn.subscription_guid), '') as subscription_guid
            , nullif(trim(txn.invoice_guid), '') as invoice_guid
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
            and txn.src_system_id = src_id
            and txn.trans_dt >= dt_filter
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
            , adj.invoice_state_desc
        from txn
        left join adj_categorized_final adj
            on adj.src_system_id = txn.src_system_id
            and adj.account_cd = txn.account_cd
            and adj.invoice_guid = txn.invoice_guid
        where 1=1
    )

-- One sub per txn: by activation window (sub_win) and by subscription_guid (sub) for categorization.
, txn_with_subs as
    (
        select
            txn_adj.*
            , sub_win.subscription_guid as sub_win_subscription_guid
            , sub_win.activate_dt_ut as sub_win_activate_dt_ut
            , sub_win.prior_expiration as sub_win_prior_expiration
            , sub_win.trial_end_dt_ut as sub_win_trial_end_dt_ut
            , sub_win.pre_activate as sub_win_pre_activate
            , sub_win.post_activate as sub_win_post_activate
            , sub.trial_end_dt_ut as sub_trial_end_dt_ut
            , sub.activate_dt_ut as sub_activate_dt_ut
            , sub.pre_activate as sub_pre_activate
            , sub.post_activate as sub_post_activate
            , sub.pre_trial_end as sub_pre_trial_end
            , sub.post_trial_end as sub_post_trial_end
            , sub.prior_expiration as sub_prior_expiration
        from txn_adj
        left join sub_categorized sub_win
            on sub_win.src_system_id = txn_adj.src_system_id
            and sub_win.account_cd = txn_adj.account_cd
            and txn_adj.trans_dt_ut between coalesce(sub_win.prior_expiration, timestamp('1900-01-01 00:00:01 UTC')) and coalesce(sub_win.post_activate, timestamp('2999-12-31 23:59:59 UTC'))
        left join sub_categorized sub
            on sub.src_system_id = txn_adj.src_system_id
            and sub.account_cd = txn_adj.account_cd
            and sub.subscription_guid = txn_adj.subscription_guid
        where 1=1
        qualify row_number() over (partition by txn_adj.transaction_guid order by sub_win.activate_dt_ut) = 1
    )

-- ---------------------------------------------------------------------------
-- Transaction category: single CASE, first match wins.
-- Order: change_sub -> trial_verify -> trial_to_paid -> direct_to_paid -> recurring -> other_verify -> other.
-- ---------------------------------------------------------------------------
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
                        and txn_with_subs.sub_trial_end_dt_ut is not null
                        and txn_with_subs.invoice_billed_dt_ut between txn_with_subs.sub_pre_trial_end and txn_with_subs.sub_post_trial_end
                        and txn_with_subs.renewal_earliest_inv = 1
                    then 'trial_to_paid'
                when txn_with_subs.trans_type_desc = 'purchase'
                        and txn_with_subs.trans_status_desc in ('success', 'void', 'declined')
                        and txn_with_subs.sub_trial_end_dt_ut is null
                        and txn_with_subs.invoice_type_cd = 'renewal'
                        and date(coalesce(txn_with_subs.invoice_billed_dt_ut, txn_with_subs.trans_dt_ut)) < date('2025-11-06')
                        and txn_with_subs.invoice_billed_dt_ut between date_add(txn_with_subs.sub_activate_dt_ut, interval -10 minute) and date_add(txn_with_subs.sub_activate_dt_ut, interval 10 minute)
                    then 'direct_to_paid'
                when txn_with_subs.trans_type_desc = 'purchase'
                        and txn_with_subs.trans_status_desc in ('success', 'void', 'declined')
                        and txn_with_subs.origin_desc in ('token_api', 'api')
                        and txn_with_subs.sub_trial_end_dt_ut is null
                        and (txn_with_subs.invoice_guid is null or txn_with_subs.invoice_type_cd != 'renewal')
                        and date(txn_with_subs.trans_dt_ut) >= date('2025-11-06')
                        and txn_with_subs.trans_dt_ut between txn_with_subs.sub_prior_expiration and txn_with_subs.sub_post_activate
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

-- Base for aggregation: one row per transaction with invoice_grouper (invoice_guid or window key) and invoice_dt_ut.
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
    )

, invoice_success as
    (
        select
            src_system_id
            , account_cd
            , invoice_grouper
            , max(case
                when 
                    (
                        trans_status_desc in ('success', 'void')
                        or 
                        invoice_state_desc = 'paid'
                    )
                    then 1
                else 0
            end) as has_success_purchase
        from txn_invoice_base
        where 1=1
        group by
            src_system_id
            , account_cd
            , invoice_grouper
    )

-- Transaction-level aggregation: by gateway, day, cc_first_6_nbr, transaction_category.
, txn_metrics as
    (
        select
            trans_gateway_type_desc
            , date(trans_dt) as trans_dt_day
            , cc_first_6_nbr
            , transaction_category
            , count(*) as total_transactions
            , countif(trans_status_desc in ('success', 'void')) as successful_transactions
            , min(transaction_guid) as example_transaction_min
            , max(transaction_guid) as example_transaction_max
            , min(account_cd) as example_account_min
            , max(account_cd) as example_account_max
        from txn_invoice_base
        where 1=1
        group by
            trans_gateway_type_desc
            , date(trans_dt)
            , cc_first_6_nbr
            , transaction_category
    )

-- Top 100 card bins by total transaction volume (overall); limits bin breakdown to reduce complexity.
, top_100_bins as
    (
        select
            cc_first_6_nbr
        from 
            (
                select
                    cc_first_6_nbr
                    , sum(total_transactions) as vol
                from txn_metrics
                where 1=1
                    and cc_first_6_nbr is not null
                group by cc_first_6_nbr
                order by vol desc
                limit 100
            ) x
        where 1=1
    )

-- Verification windows as "invoices" for invoice-level metrics.
, verify_window_agg as
    (
        select
            src_system_id
            , account_cd
            , invoice_grouper
            , min(trans_dt) as window_trans_dt
            , trans_gateway_type_desc
            , cc_first_6_nbr
            , transaction_category
            , count(*) as attempt_cnt
            , countif(trans_status_desc in ('success', 'void')) as success_cnt
        from txn_invoice_base
        where 1=1
            and transaction_category in ('trial_verify', 'other_verify')
            and invoice_grouper like 'window_%'
        group by
            src_system_id
            , account_cd
            , invoice_grouper
            , trans_gateway_type_desc
            , cc_first_6_nbr
            , transaction_category
    )

-- Invoice-level: real invoices (with gateway/bin from txn) + verification windows.
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

, inv_metrics_base as
    (
        select
            inv_agg.trans_gateway_type_desc
            , date(inv_agg.invoice_billed_dt_ut) as invoice_dt_day
            , inv_agg.cc_first_6_nbr
            , inv_agg.invoice_category
            , 1 as invoice_cnt
            , inv_agg.is_success_invoice as success_cnt
            , inv_agg.invoice_guid as example_invoice
            , inv_agg.account_cd as example_account
        from inv_agg
        where 1=1
            and inv_agg.invoice_guid is not null
    )

, inv_metrics_verify as
    (
        select
            verify_window_agg.trans_gateway_type_desc
            , date(verify_window_agg.window_trans_dt) as invoice_dt_day
            , verify_window_agg.cc_first_6_nbr
            , verify_window_agg.transaction_category as invoice_category
            , 1 as invoice_cnt
            , if(verify_window_agg.success_cnt > 0, 1, 0) as success_cnt
            , verify_window_agg.invoice_grouper as example_invoice
            , verify_window_agg.account_cd as example_account
        from verify_window_agg
        where 1=1
    )

, inv_metrics as
    (
        select
            trans_gateway_type_desc
            , invoice_dt_day
            , cc_first_6_nbr
            , invoice_category
            , sum(invoice_cnt) as total_invoices
            , sum(success_cnt) as successful_invoices
            , min(example_invoice) as example_invoice_min
            , max(example_invoice) as example_invoice_max
            , min(example_account) as example_account_min
            , max(example_account) as example_account_max
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
            , cc_first_6_nbr
            , invoice_category
    )

-- Day-level combined metrics: full outer join txn + inv at day grain; all BINs; has both trans and invoice dates.
-- Use this CTE for aggregations; filter by top_100_bins in final SELECT to restrict to top 100 BINs.
, metrics_day_all_bins as
    (
        select
            t.trans_dt_day
            , i.invoice_dt_day
            , coalesce(t.trans_dt_day, i.invoice_dt_day) as dt
            , coalesce(t.trans_gateway_type_desc, i.trans_gateway_type_desc) as trans_gateway_type_desc
            , coalesce(t.cc_first_6_nbr, i.cc_first_6_nbr) as cc_first_6_nbr
            , coalesce(t.transaction_category, i.invoice_category) as category
            , coalesce(t.total_transactions, 0) as total_transactions
            , coalesce(t.successful_transactions, 0) as successful_transactions
            , coalesce(i.total_invoices, 0) as total_invoices
            , coalesce(i.successful_invoices, 0) as successful_invoices
            , t.example_transaction_min
            , t.example_transaction_max
            , i.example_invoice_min
            , i.example_invoice_max
            , coalesce(t.example_account_min, i.example_account_min) as example_account_min
            , coalesce(t.example_account_max, i.example_account_max) as example_account_max
        from (
            select
                trans_dt_day
                , trans_gateway_type_desc
                , cc_first_6_nbr
                , transaction_category
                , total_transactions
                , successful_transactions
                , example_transaction_min
                , example_transaction_max
                , example_account_min
                , example_account_max
            from txn_metrics
            where 1=1
        ) t
        full outer join (
            select
                invoice_dt_day
                , trans_gateway_type_desc
                , cc_first_6_nbr
                , invoice_category
                , total_invoices
                , successful_invoices
                , example_invoice_min
                , example_invoice_max
                , example_account_min
                , example_account_max
            from inv_metrics
            where 1=1
        ) i
            on t.trans_dt_day = i.invoice_dt_day
            and t.trans_gateway_type_desc = i.trans_gateway_type_desc
            and t.cc_first_6_nbr = i.cc_first_6_nbr
            and t.transaction_category = i.invoice_category
        where 1=1
    )

-- Day-level result set with date field; rates rounded to 5 decimal places.
-- Column order: date, category, bin, txn success/total/rate, inv success/total/rate, example transaction/invoice/account (min/max).
-- Final SELECT can truncate dt at week/month/quarter and aggregate as needed.
, full_results as
    (
        select
            dt
            , trans_gateway_type_desc
            , category
            , cc_first_6_nbr
            , successful_transactions
            , total_transactions
            , round(safe_divide(successful_transactions, nullif(total_transactions, 0)), 5) as txn_success_rate
            , successful_invoices
            , total_invoices
            , round(safe_divide(successful_invoices, nullif(total_invoices, 0)), 5) as inv_success_rate
            , example_transaction_min
            , example_transaction_max
            , example_invoice_min
            , example_invoice_max
            , example_account_min
            , example_account_max
        from metrics_day_all_bins
        where 1=1
    )
-- All BINs by default. To restrict to top 100 BINs by volume, add: and cc_first_6_nbr in (select cc_first_6_nbr from top_100_bins)
-- To roll up to week/month/quarter: group by date_trunc(dt, week) (or month/quarter) and sum the count columns.
select
    date_trunc(dt, MONTH) as mth
    , trans_gateway_type_desc
    , category
    -- , cc_first_6_nbr
    , sum(total_transactions) as total_transactions
    , sum(successful_transactions) as successful_transactions
    , round(safe_divide(sum(successful_transactions), nullif(sum(total_transactions), 0)), 5) as txn_success_rate
    , sum(total_invoices) as total_invoices
    , sum(successful_invoices) as successful_invoices
    , round(safe_divide(sum(successful_invoices), nullif(sum(total_invoices), 0)), 5) as inv_success_rate
    , min(example_transaction_min) as example_transaction_min
    , max(example_transaction_max) as example_transaction_max
    , min(example_invoice_min) as example_invoice_min
    , max(example_invoice_max) as example_invoice_max
    , min(example_account_min) as example_account_min
    , max(example_account_max) as example_account_max
from full_results
where 1=1
    and dt >= dt_filter
    and trans_gateway_type_desc is not null
group by all
order by 1,2,3

