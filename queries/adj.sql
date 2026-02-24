with adj as
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
                    and adj.creation_dt >= date('2025-03-01')
                    and adj.src_system_id = 115
            ) adj
    )