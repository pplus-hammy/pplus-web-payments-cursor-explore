-- Invoices in dunning (dun_chk not null): count distinct invoice_guid by calendar month
-- (billed_dt_ut) and account_cd last digit; pct within each month across digit buckets.

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
, digits as
    (
        select
            format('%d', digit) as account_cd_last_digit
        from unnest(generate_array(0, 9)) as digit
    )
, months as
    (
        select distinct
            date_trunc(date(inv.billed_dt_ut), month) as bill_month
        from inv
        where 1=1
            and inv.dun_chk is not null
            and inv.account_cd is not null
            and regexp_contains(right(trim(cast(inv.account_cd as string)), 1), r'^[0-9]$')
    )
, month_digit_spine as
    (
        select
            months.bill_month
            , digits.account_cd_last_digit
        from months
        cross join digits
    )
, agg as
    (
        select
            date_trunc(date(inv.billed_dt_ut), month) as bill_month
            , right(trim(cast(inv.account_cd as string)), 1) as account_cd_last_digit
            , count(distinct inv.invoice_guid) as dun_invoice_guid_cnt
        from inv
        where 1=1
            and inv.dun_chk is not null
            and inv.account_cd is not null
            and regexp_contains(right(trim(cast(inv.account_cd as string)), 1), r'^[0-9]$')
        group by
            bill_month
            , account_cd_last_digit
    )
select
    month_digit_spine.bill_month
    , month_digit_spine.account_cd_last_digit
    , coalesce(agg.dun_invoice_guid_cnt, 0) as dun_invoice_guid_cnt
    , round(
        100.0 * coalesce(agg.dun_invoice_guid_cnt, 0)
            / nullif(sum(coalesce(agg.dun_invoice_guid_cnt, 0)) over (partition by month_digit_spine.bill_month), 0)
        , 4
    ) as pct_of_month_total
from month_digit_spine
left join agg
    on agg.bill_month = month_digit_spine.bill_month
    and agg.account_cd_last_digit = month_digit_spine.account_cd_last_digit
order by
    month_digit_spine.bill_month
    , cast(month_digit_spine.account_cd_last_digit as int64)
