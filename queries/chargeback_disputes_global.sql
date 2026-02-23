with inv as
    (
        select distinct
            inv.src_system_id
            , inv.account_cd
            , case
                when inv.src_system_id = 115 then ifnull(trim(inv.country_cd), 'US')
                else inv.country_cd
            end as country_cd
            , adj.subscription_guid
            , inv.invoice_guid
            , inv.invoice_nbr
            , inv.invoice_type_desc
            , inv.status_desc
            , inv.billed_dt_ut
            , inv.closed_dt_ut
            , inv.total_amt
            , nullif(inv.dunning_campaign_id,'nan') as dun_chk
            , case
                when inv.total_amt > 0 then dense_rank() over (partition by inv.src_system_id, inv.account_cd, adj.subscription_guid, case when inv.total_amt > 0 then 1 else 0 end order by inv.billed_dt_ut) 
                else 1000000
            end as fin
            , min(inv.invoice_nbr) over (partition by inv.src_system_id, inv.account_cd, adj.subscription_guid) as min_sub_inv
        from i-dss-streaming-data.payment_ops_vw.recurly_invoice_sum_fct inv
        join i-dss-streaming-data.payment_ops_vw.recurly_adjustments_fct adj
            on inv.src_system_id = adj.src_system_id
            and inv.account_cd = adj.account_cd
            and inv.invoice_guid = adj.invoice_guid
            and adj.creation_dt >= date('2024-06-01')
            and adj.subscription_guid is not null
        where 1=1
            -- and inv.src_system_id = 115
            and inv.partition_month_start_dt >= date('2024-06-01')
    )
, txn_cb as
    (
        select distinct
            txn.src_system_id
            , txn.account_cd
            , nullif(trim(txn.subscription_guid),'') as txn_subscription_guid
            , inv.invoice_nbr
            , txn.invoice_guid
            , txn.transaction_guid
            , txn.origin_desc
            , case
                when inv.fin = 1 then 'signup'
                else null
            end as signup_chk
        from i-dss-streaming-data.payment_ops_vw.recurly_transaction_fct txn
        left join inv
            on inv.src_system_id = txn.src_system_id
            and inv.account_cd = txn.account_cd
            and inv.invoice_guid = txn.invoice_guid
        where 1=1
            -- and txn.src_system_id = 115
            and txn.trans_dt >= date('2024-06-01')
    )
, fiserv as
    (
        select distinct
            -- fd.site_id_be
            -- , parse_date('%m/%d/%Y', fd.txn_date) as txn_dt
            -- , transaction_id
            -- , account_first_6
            -- , account_last_4
            *
            , row_number() over (partition by site_id_be, transaction_id order by parse_date('%m/%d/%Y', fd.received_date)) as rn
        from i-dss-streaming-data.payment_ops_sandbox.fiserv_disputes fd
        where 1=1
            -- and fd.site_id_be = 372658052884
            and fd.chargeback_work_type = 'First Chargeback'
            and parse_date('%m/%d/%Y', fd.txn_date) >= date('2024-06-01')
            and nullif(trim(fd.transaction_id),'') is not null
        qualify rn = 1
    )
, cybs as
    (
        select distinct
            ct.* except (transactionrefnumber)
            , cb.transactionrefnumber
            , case
                when length(regexp_replace(ct.networktransactionid, r'[0-9]','')) = 0 then ct.networktransactionid
                else trim(substr(ct.networktransactionid, 5)) 
            end as transaction_id
            , case
                when length(regexp_replace(ct.customerid, r'[0-9.]','')) > 0 and nullif(trim(ct.customerid),'') is not null then 'vindicia'
                when ifnull(ct.merchantreferencenumber,'') like 'PAR%' then 'vindicia'
                else 'recurly'
            end as txn_origin
            , case
                when length(ct.merchantreferencenumber) = 32 then ct.merchantreferencenumber
                else null
            end as txn_id
            , cast(case
                when length(ct.merchantreferencenumber) < 32 and length(regexp_replace(ct.merchantreferencenumber, r'[0-9.]','')) = 0 then ct.merchantreferencenumber
                when regexp_contains(er.invoice_number, r'[A-Za-z]') = true then substr(er.invoice_number, 3, 20)
                when er.invoice_number is not null then er.invoice_number
                else null
            end as integer) as invoice_nbr
            , date_add(date(ct.requestdate), interval -1 DAY) as pre_dt
            , date_add(date(ct.requestdate), interval 1 DAY) as post_dt
            , row_number() over (partition by ct.merchantid, ct.merchantreferencenumber, ct.customerid order by ct.requestdate) as rn
            , lead(ct.amount) over (partition by ct.merchantid, ct.merchantreferencenumber, ct.customerid  order by ct.requestdate) as lead_amt
            , lag(ct.amount) over (partition by ct.merchantid, ct.merchantreferencenumber, ct.customerid  order by ct.requestdate) as lag_amt
        from i-dss-streaming-data.payment_ops_sandbox.cybs_txn ct
        left join i-dss-streaming-data.payment_ops_sandbox.recurly_external_recoveries er
            on er.external_reference = ct.merchantreferencenumber
        left join i-dss-streaming-data.payment_ops_sandbox.cybs_batch cb
            on cb.requestid = ct.requestid
            and cb.merchantid = ct.merchantid
            and date(cb.requestdate) >= date('2024-06-01')
        where 1=1
            and date(ct.requestdate) >= date('2024-06-01')
            and ct.amount > 0
            and ct.amount != 1
            and ct.overallreasoncode = 100
            -- and ct.overallrcode = 1
            -- and ct.merchantid = 'cbsi_entertainment'
    )
, cbs as
    (
        select
            date(ct.requestdate) as dt
            , ct.requestdate
            , ct.requestid
            , ct.networktransactionid
            , ct.transaction_id
            , ct.merchantreferencenumber
            , ct.transactionrefnumber
            , ct.txn_origin
            , ct.invoice_nbr
            , ct.customerid
            , ct.rn
            , parse_date('%m/%d/%Y', fd.txn_date) as txn_date
            , parse_date('%m/%d/%Y', fd.received_date) as received_date
            , parse_date('%m/%d/%Y', fd.disposition_date) as disposition_date
            , fd.auth_network	
            , fd.site_id_be
            , fd.account_first_6
            , fd.account_last_4
            , fd.chargeback_status
            , fd.chargeback_category
            , fd.chargeback_work_type
            , fd.chargeback_win_loss
            , fd.chargeback_disposition
            , fd.dispute_reason
            , fd.processed_currency
            , fd.chargeback_amount
            , fd.case_number
            , case
                when inv.fin = 1 then 'signup'
                else null
            end as signup_chk
        from fiserv fd
        join cybs ct
            on fd.transaction_id = ct.transaction_id
            and parse_date('%m/%d/%Y', fd.txn_date) between pre_dt and post_dt
        left join inv
            on inv.invoice_nbr = ct.invoice_nbr
            and inv.account_cd = ct.customerid
        where 1=1
            -- and date(requestdate) = date('2025-01-15')
        order by 1,2,3
    )
, all_cb as
    (
        select
            'cybersource' as gateway
            , case
                when site_id_be = 372658052884 then 'US'
                when site_id_be = 311205465884 then 'AU'
                when site_id_be = 372826763883 then 'AU'
                when site_id_be = 311206343882 then 'CA'
                when site_id_be = 372826761887 then 'CA'
                when site_id_be = 311183522888 then 'AT'
                when site_id_be = 311183521880 then 'DE'
                when site_id_be = 311183071886 then 'IE'
                when site_id_be = 311183520882 then 'IT'
                when site_id_be = 311183523886 then 'CH'
                when site_id_be = 311183070888 then 'UK'
                when site_id_be = 345141800887 then 'AR'
                when site_id_be = 345141801885 then 'BO'
                when site_id_be = 345141811884 then 'NI'
                when site_id_be = 345141812882 then 'PA'
                when site_id_be = 345141813880 then 'PY'
                when site_id_be = 345141815885 then 'UY'
                when site_id_be = 345141802883 then 'CL'
                when site_id_be = 345141803881 then 'CO'
                when site_id_be = 345141804889 then 'CR'
                when site_id_be = 345141805886 then 'DR'
                when site_id_be = 345141806884 then 'EC'
                when site_id_be = 345141807882 then 'SV'
                when site_id_be = 345141808880 then 'GT'
                when site_id_be = 345141809888 then 'HN'
                when site_id_be = 345141810886 then 'MX'
                when site_id_be = 345141814888 then 'PE'
                when site_id_be = 345141816883 then 'VE'
                when site_id_be = 311180922883 then 'DK'
                when site_id_be = 311180923881 then 'FI'
                when site_id_be = 311180921885 then 'NO'
                when site_id_be = 311180920887 then 'SE'
                else null
            end as mid_country
            , requestid as gateway_txn_id
            -- , merchantreferencenumber as merchant_reference
            , coalesce(
                case
                    when length(merchantreferencenumber) < 32 and length(regexp_replace(merchantreferencenumber, r'[0-9.]','')) = 0 then merchantreferencenumber
                    when er.invoice_number is not null then cast(er.invoice_number as string)
                    -- when er.invoice_number is not null and regexp_contains(er.invoice_number, r'[A-Za-z]') = true then cast(substr(er.invoice_number, 3, 20) as string)
                    -- when er.invoice_number is not null and regexp_contains(er.invoice_number, r'[A-Za-z]') = false then cast(er.invoice_number as string)
                    else null
                end
                , merchantreferencenumber) as merchant_reference
            , networktransactionid
            , transaction_id
            , transactionrefnumber
            , cast(received_date as timestamp) as dispute_dt
            , date(cast(received_date as timestamp)) as dispute_date
            , cast(txn_date as timestamp) as txn_dt
            , date(cast(txn_date as timestamp)) as txn_date
            , dispute_reason
            , chargeback_amount as dispute_amount
            , txn_origin
            , case
                when regexp_contains(customerid, r'[A-Za-z]') = true then inv.account_cd
                else customerid
            end as account_cd
            , case
                when coalesce(inv.fin, invv.fin) = 1 then 'signup'
                when cbs.signup_chk is not null then cbs.signup_chk
                else null
            end as signup_chk
            , coalesce(inv.subscription_guid, invv.subscription_guid) as subscription_guid
            , coalesce(cbs.invoice_nbr, inv.invoice_nbr, invv.invoice_nbr) as invoice_nbr
        from cbs
        left join i-dss-streaming-data.payment_ops_sandbox.recurly_external_recoveries er
            on er.external_reference = cbs.merchantreferencenumber
        left join inv
            on cast(inv.invoice_nbr as string) = cbs.merchantreferencenumber
        left join inv invv
            -- on inv.invoice_nbr = er.invoice_number
            on invv.invoice_nbr = cast(case
                                    when regexp_contains(er.invoice_number, r'[A-Za-z]') = true then substr(er.invoice_number, 3, 20) -- country specific invoicing in recurly, ex: IT2162152 from Italy
                                    else er.invoice_number
                                end as integer)
            and invv.country_cd = case 
                                    when regexp_contains(er.invoice_number, r'[A-Za-z]') = true then substr(er.invoice_number, 0, 2) 
                                    else 'US' 
                                end

        union all

        select distinct
            'adyen' as gateway
            , case
                when merchant_account = 'PPLUS_US_Card_Charge' then 'US'
                when merchant_account = 'PPlus_INTL_UK_Card_Charge' then 'UK'
                when merchant_account = 'PPlus_INTL_FR_Card_Charge' then 'FR'
                when merchant_account = 'PPlus_INTL_BR_Card_Charge' then 'BR'
                when merchant_account = 'PPlus_INTL_DE_Card_Charge' then 'DE'
                when merchant_account = 'PPlus_INTL_IT_Card_Charge' then 'IT'
                when merchant_account = 'PPlus_INTL_AT_Card_Charge' then 'AT'
                when merchant_account = 'PPlus_INTL_IE_Card_Charge' then 'IE'
                when merchant_account = 'PPlus_INTL_MX_Card_Charge' then 'MX'
                else null
            end as mid_country
            , psp_reference as gateway_txn_id
            , case
                when merchant_reference like 'PAR%' then cast(er.invoice_number as string)
                -- when merchant_reference like 'PAR%' and regexp_contains(er.invoice_number, r'[A-Za-z]') = false then cast(er.invoice_number as string)
                -- when merchant_reference like 'PAR%' and regexp_contains(er.invoice_number, r'[A-Za-z]') = true then cast(substr(er.invoice_number, 3, 20) as string)
                else merchant_reference
            end as merchant_reference
            , '' as networktransactionid
            , '' as transaction_id
            , '' as transactionrefnumber
            , cast(dispute_date as timestamp) as dispute_dt
            , date(dispute_date) as dispute_date
            , cast(payment_date as timestamp) as txn_dt
            , date(payment_date) as txn_date
            , dispute_reason
            , dispute_amount
            , case
                when merchant_reference like 'PAR%' then 'vindicia'
                else 'recurly'
            end as txn_origin
            , case
                when regexp_contains(ad.shopper_reference, r'[A-Za-z]') = true then inv.account_cd
                else ad.shopper_reference
            end as account_cd
            , case
                when inv.fin = 1 then 'signup'
                when txn_cb.signup_chk is not null then txn_cb.signup_chk
                else null
            end as signup_chk
            , coalesce(txn_subscription_guid, inv.subscription_guid) as subscription_guid
            , coalesce(txn_cb.invoice_nbr, inv.invoice_nbr) as invoice_nbr
        from i-dss-streaming-data.payment_ops_sandbox.adyen_disputes ad
        left join txn_cb
            on txn_cb.transaction_guid = ad.merchant_reference
        left join i-dss-streaming-data.payment_ops_sandbox.recurly_external_recoveries er
            on er.external_reference = ad.merchant_reference
        left join inv
            -- on inv.invoice_nbr = er.invoice_number
            on inv.invoice_nbr = cast(case
                                    when regexp_contains(er.invoice_number, r'[A-Za-z]') = true then substr(er.invoice_number, 3, 20) -- country specific invoicing in recurly, ex: IT2162152 from Italy
                                    else er.invoice_number
                                end as integer)
            and inv.country_cd = case 
                                    when regexp_contains(er.invoice_number, r'[A-Za-z]') = true then substr(er.invoice_number, 0, 2) 
                                    else 'US' 
                                end
        where 1=1
            and ad.record_type = 'Chargeback'
            and date(payment_date) >= date('2025-01-01')
            -- and ad.merchant_account = 'PPLUS_US_Card_Charge'
    )

-- select distinct
--     case
--         when mid_country = 'US' then 'domestic'
--         else 'intl'
--     end as chk
-- from all_cb
-- where 1=1
--     and txn_date >= date('2025-01-01')
-- order by mid_country, 1, dispute_date

select
    case
        when mid_country = 'US' then 'domestic'
        else 'intl'
    end as chk
    , mid_country
    , date(txn_date) as purch_dt
    , txn_origin
    , count(distinct gateway_txn_id) as cb_ct
    , count(distinct case when signup_chk = 'signup' then gateway_txn_id else null end) as signup_cb_ct
    , min(case when signup_chk = 'signup' then gateway_txn_id else null end) as signup_cb_ex1
    , max(case when signup_chk = 'signup' then gateway_txn_id else null end) as signup_cb_ex2
    , sum(dispute_amount) as dispute_amount
    , count(distinct case when lower(dispute_reason) like '%fraud%' or lower(dispute_reason) in ('no authorization','chip liability shift') then gateway_txn_id else null end) as fraud_ct
    , count(distinct case when gateway = 'cybersource' and (lower(dispute_reason) like '%fraud%' or lower(dispute_reason) in ('no authorization','chip liability shift')) then gateway_txn_id else null end) as cybs_fraud_ct

from all_cb
group by all
order by 1,2,3




