with inv as
    (
        select distinct
            inv.src_system_id
            , inv.account_cd
            , inv.invoice_guid
            , inv.invoice_nbr
            , inv.invoice_type_desc
            , inv.status_desc
            , inv.billed_dt_ut 
            , date(inv.billed_dt_ut) as billed_dt
            , inv.closed_dt_ut
            , date(inv.closed_dt_ut) as closed_dt
            , inv.total_amt
            , case
                when nullif(inv.dunning_campaign_id, 'nan') is null then 'dunning'
                else null
            end as dun_chk
        from i-dss-streaming-data.payment_ops_vw.recurly_invoice_sum_fct inv
        where 1=1
            and inv.src_system_id = 115
            and inv.partition_month_start_dt >= date('2026-01-01')
            and date(inv.billed_dt_ut) >= date('2026-01-01')
            -- and inv.invoice_type_desc = 'renewal'
            -- and inv.account_cd = '322325063970'
    )