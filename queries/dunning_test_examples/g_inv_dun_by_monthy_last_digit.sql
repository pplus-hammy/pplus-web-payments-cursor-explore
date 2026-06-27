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
, tot_ct as 
    (
        select
            date_trunc(billed_dt, MONTH) as inv_mth
            , count(distinct case when dun_chk is not null then invoice_guid else null end) as tot_dun_ct
        from inv
        group by all
    )
, cd_ct as
    (
        select
            date_trunc(billed_dt, MONTH) as inv_mth
            , right(trim(cast(account_cd as string)), 1) as account_cd_last_digit
            , count(distinct case when dun_chk is not null then invoice_guid else null end) as dun_ct
        from inv
        where 1=1
        group by all
    )

select
    cd_ct.*
    , safe_divide(cd_ct.dun_ct, tot_ct.tot_dun_ct) as dun_pct
from cd_ct
left join tot_ct
    on tot_ct.inv_mth = cd_ct.inv_mth
where 1=1
group by all
order by 1,2